"""Matrix client ownership and transport boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from nio import AsyncClient, AsyncClientConfig
from pydantic import SecretStr

from agentteams_manager.config import ManagerConfig
from agentteams_manager.domain.models import InboundEvent

from .crypto import CryptoStore, maintain_e2ee

InboundHandler = Callable[[InboundEvent], Awaitable[None]]


class MatrixState(Protocol):
    """Small durable state surface needed by Matrix transport."""

    async def get_value(self, key: str) -> str | None: ...

    async def set_value(self, key: str, value: str) -> None: ...

    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class MatrixClientConfig:
    """Validated values required by the Matrix adapter."""

    homeserver: str
    user_id: str
    access_token: SecretStr
    device_name: str
    crypto_store: Path
    media_dir: Path
    password: SecretStr | None = None
    sync_timeout_ms: int = 30_000
    history_limit: int = 50
    encryption: bool = True
    vision_enabled: bool = True
    mention_pill_in_body: bool = False
    outbound_structured_mentions: bool = True

    @classmethod
    def from_manager_config(
        cls,
        config: ManagerConfig,
    ) -> MatrixClientConfig:
        return cls(
            homeserver=config.matrix_url,
            user_id=config.manager_user_id,
            access_token=config.matrix_access_token,
            device_name="agentteams-manager",
            crypto_store=config.workspace / "matrix-e2ee",
            media_dir=config.workspace / "media",
            password=config.matrix_password,
        )


class MatrixUnknownTokenError(RuntimeError):
    """The homeserver rejected the configured Matrix access token."""


class MatrixClient:
    """Own the sole ``nio.AsyncClient`` instance used by the Manager.

    Construction is side-effect free. Network activity starts only from
    :meth:`start`, which makes the class straightforward to test with an
    injected nio-compatible client.
    """

    def __init__(
        self,
        config: MatrixClientConfig,
        state: MatrixState,
        *,
        nio_client: AsyncClient | Any | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self._state = state
        self._client = nio_client
        self._handler: InboundHandler | None = None
        self._sync_task: asyncio.Task[None] | None = None
        self._sleeper = sleeper
        self.ready = asyncio.Event()

    async def start(self, handler: InboundHandler) -> None:
        """Prepare encryption state and start the owned sync loop."""
        self.bind_handler(handler)
        CryptoStore(self.config.crypto_store).prepare()
        self.config.media_dir.mkdir(parents=True, exist_ok=True)
        client = self._ensure_client()
        await self._prepare_crypto(client)
        self._sync_task = asyncio.create_task(
            self.run_sync_loop(),
            name="matrix-sync",
        )

    def bind_handler(self, handler: InboundHandler) -> None:
        """Bind the normalized inbound consumer without starting I/O."""
        self._handler = handler

    def _ensure_client(self) -> Any:
        created = self._client is None
        if self._client is None:
            request_timeout = max(
                self.config.sync_timeout_ms / 1000 + 30,
                60,
            )
            nio_config = AsyncClientConfig(
                encryption_enabled=self.config.encryption,
                store_sync_tokens=False,
                request_timeout=request_timeout,
            )
            self._client = AsyncClient(
                self.config.homeserver,
                self.config.user_id,
                device_id=None,
                store_path=str(self.config.crypto_store),
                config=nio_config,
            )
        if created or not getattr(self._client, "access_token", None):
            self._client.access_token = (
                self.config.access_token.get_secret_value()
            )
        self._client.user_id = self.config.user_id
        self._client.user = self.config.user_id
        return self._client

    async def _prepare_crypto(self, client: Any) -> None:
        if not self.config.encryption:
            return
        whoami = getattr(client, "whoami", None)
        if whoami is not None:
            identity = await whoami()
            user_id = getattr(identity, "user_id", None)
            if user_id != self.config.user_id:
                raise RuntimeError(
                    "Matrix token identity does not match Manager user",
                )
            client.user_id = user_id
            client.user = user_id
            device_id = getattr(identity, "device_id", None)
            if not device_id:
                raise RuntimeError(
                    "Matrix E2EE requires a token-bound device_id",
                )
            client.device_id = device_id
        load_store = getattr(client, "load_store", None)
        if load_store is not None and getattr(client, "device_id", None):
            load_store()
        await maintain_e2ee(client, enabled=True)

    async def stop(self) -> None:
        """Stop owned background work and close the injected client."""
        if self._sync_task is not None:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None
        close = getattr(self._client, "close", None)
        if close is not None:
            await close()
        self.ready.clear()

    async def sync_once(self) -> None:
        """Run one resumable sync and durably advance its cursor."""
        client = self._ensure_client()
        since = await self._state.get_value("matrix.sync_token")
        try:
            response = await client.sync(
                timeout=self.config.sync_timeout_ms,
                since=since,
                full_state=since is None,
            )
        except Exception as exc:
            if _is_unknown_token(exc):
                raise MatrixUnknownTokenError("M_UNKNOWN_TOKEN") from exc
            raise

        if _is_unknown_token(response):
            raise MatrixUnknownTokenError("M_UNKNOWN_TOKEN")
        if not hasattr(response, "next_batch"):
            raise RuntimeError(f"Matrix sync failed: {response}")

        rooms = getattr(response, "rooms", None)
        invites = getattr(rooms, "invite", {}) if rooms is not None else {}
        for room_id in invites:
            await client.join(room_id)

        joined = getattr(rooms, "join", {}) if rooms is not None else {}
        await self._dispatch_joined_timelines(joined)

        next_batch = getattr(response, "next_batch", None)
        if next_batch:
            await self._state.set_value("matrix.sync_token", next_batch)
        await maintain_e2ee(client, enabled=self.config.encryption)
        self.ready.set()

    async def run_sync_loop(self) -> None:
        """Sync forever with bounded password-based token recovery."""
        refresh_attempts = 0
        delays = (5, 10, 20)
        while True:
            try:
                await self.sync_once()
                refresh_attempts = 0
            except MatrixUnknownTokenError as exc:
                self.ready.clear()
                if self.config.password is None:
                    raise RuntimeError(
                        "Matrix token expired and no password is available",
                    ) from exc
                if refresh_attempts >= len(delays):
                    raise RuntimeError(
                        "Matrix sync failed after three token refresh attempts",
                    ) from exc
                await self._sleeper(delays[refresh_attempts])
                refresh_attempts += 1
                if not await self._refresh_token():
                    if refresh_attempts >= len(delays):
                        raise RuntimeError(
                            "Matrix sync failed after three token "
                            "refresh attempts",
                        ) from exc
            except asyncio.CancelledError:
                raise
            except Exception:
                self.ready.clear()
                await self._sleeper(1)

    async def _refresh_token(self) -> bool:
        password = self.config.password
        if password is None:
            return False
        response = await self._ensure_client().login(
            password.get_secret_value(),
            device_name=self.config.device_name,
        )
        access_token = getattr(response, "access_token", None)
        if not access_token:
            return False
        self._client.access_token = access_token
        if getattr(response, "user_id", None):
            self._client.user_id = response.user_id
            self._client.user = response.user_id
        if getattr(response, "device_id", None):
            self._client.device_id = response.device_id
        return True

    async def _dispatch_joined_timelines(
        self,
        joined: dict[str, Any],
    ) -> None:
        if self._handler is None:
            return
        for room_id, room_info in joined.items():
            timeline = getattr(room_info, "timeline", None)
            events = getattr(timeline, "events", ())
            for event in events:
                inbound = self._normalize_event(room_id, event)
                if inbound is not None:
                    await self._handler(inbound)

    def _normalize_event(
        self,
        room_id: str,
        event: Any,
    ) -> InboundEvent | None:
        sender = getattr(event, "sender", "")
        event_id = getattr(event, "event_id", "")
        if not sender or not event_id or sender == self.config.user_id:
            return None
        source = getattr(event, "source", {}) or {}
        content = source.get("content", {}) if isinstance(source, dict) else {}
        body = getattr(event, "body", None) or content.get("body")
        if not isinstance(body, str):
            return None
        milliseconds = getattr(event, "server_timestamp", 0) or 0
        timestamp = datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
        relates_to = content.get("m.relates_to", {})
        thread_id = None
        if isinstance(relates_to, dict):
            if relates_to.get("rel_type") == "m.thread":
                thread_id = relates_to.get("event_id")
            elif isinstance(relates_to.get("m.in_reply_to"), dict):
                thread_id = relates_to["m.in_reply_to"].get("event_id")
        mention_data = content.get("m.mentions", {})
        mentions = (
            tuple(mention_data.get("user_ids", ()))
            if isinstance(mention_data, dict)
            else ()
        )
        return InboundEvent(
            room_id=room_id,
            event_id=event_id,
            sender=sender,
            body=body,
            timestamp=timestamp,
            thread_id=thread_id,
            mentions=mentions,
        )

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        """Send text through the Matrix port.

        The concrete content mapping is added with the transport contract.
        """
        del room_id, text, txn_id, thread_id, mentions
        raise NotImplementedError("Matrix outbound transport is not ready")


def _is_unknown_token(value: object) -> bool:
    text = str(value)
    return "M_UNKNOWN_TOKEN" in text or (
        "401" in text and "token" in text.lower()
    )
