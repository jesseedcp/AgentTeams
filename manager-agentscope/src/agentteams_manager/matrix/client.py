"""Matrix client ownership and transport boundary.

封装 Matrix 登录、同步、房间管理、消息收发和传输重试。

Matrix homeserver 是消息与房间成员关系的权威来源。本模块把 matrix-nio 事件转换成
Manager 的 ``InboundEvent``，并用稳定 transaction ID 发送回复，以便网络重试不会
产生重复消息。sync token 只在事件安全交给上层后推进；认证失效和普通网络中断也会
被区别处理，防止同步循环无声停止。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
from nio import (
    AsyncClient,
    AsyncClientConfig,
    ErrorResponse,
    RoomPreset,
    RoomVisibility,
)
from pydantic import SecretStr

from agentteams_manager.config import ManagerConfig
from agentteams_manager.domain.models import InboundEvent, MediaReference

from .crypto import CryptoStore, maintain_e2ee
from .formatting import markdown_to_matrix_html
from .media import MediaAdapter
from .threads import RoomHistory, ThreadProjector

InboundHandler = Callable[[InboundEvent], Awaitable[None]]
logger = logging.getLogger(__name__)


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
    registration_token: SecretStr | None = None
    sync_timeout_ms: int = 30_000
    sync_watchdog_timeout_seconds: float = 55.0
    sync_stale_after_seconds: float = 90.0
    sync_retry_delay_seconds: float = 5.0
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
            registration_token=config.matrix_registration_token,
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
        registration_http: Any | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self._state = state
        self._client = nio_client
        self._client_injected = nio_client is not None
        self._client_prepared = nio_client is not None
        self._access_token = config.access_token.get_secret_value()
        self._registration_http = registration_http
        self._handler: InboundHandler | None = None
        self._sync_task: asyncio.Task[None] | None = None
        self._needs_full_state = True
        self._sleeper = sleeper
        self._clock = time.monotonic
        self._last_sync_success_monotonic: float | None = None
        self.last_sync_success_at: datetime | None = None
        self.ready = asyncio.Event()
        self.history = RoomHistory(limit=config.history_limit)

    @property
    def sync_healthy(self) -> bool:
        """Return whether the supervised loop is live and recently synced."""
        task = self._sync_task
        if (
            not self.ready.is_set()
            or task is None
            or task.done()
            or self._last_sync_success_monotonic is None
        ):
            return False
        age = self._clock() - self._last_sync_success_monotonic
        return age <= self.config.sync_stale_after_seconds

    @property
    def supervisor_live(self) -> bool:
        """Return false only after a started supervisor has terminated."""
        return self._sync_task is None or not self._sync_task.done()

    async def register_user(
        self,
        *,
        username: str,
        password: SecretStr,
        admin: bool = False,
    ) -> dict[str, str | bool]:
        """Register through Matrix registration-token UIA with fallback."""
        registration_token = self.config.registration_token
        if registration_token is None:
            raise RuntimeError(
                "Matrix registration token is not configured",
            )

        async def register(http: Any) -> dict[str, str | bool]:
            if not admin:
                response = await http.post(
                    "/_matrix/client/v3/register",
                    json={
                        "username": username,
                        "password": password.get_secret_value(),
                        "auth": {
                            "type": "m.login.registration_token",
                            "token": registration_token.get_secret_value(),
                        },
                    },
                )
                if getattr(response, "status_code", 200) in {200, 201}:
                    user_id = response.json().get("user_id")
                    if not isinstance(user_id, str) or not user_id:
                        raise RuntimeError(
                            "Matrix registration returned no user ID",
                        )
                    return {"user_id": user_id, "admin": False}
                payload = response.json()
                fallback = (
                    getattr(response, "status_code", 0) in {404, 405}
                    or payload.get("errcode") == "M_EXCLUSIVE"
                )
                if not fallback:
                    response.raise_for_status()

            # Synapse-compatible homeservers additionally support creating an
            # admin account through their nonce/HMAC endpoint. Tuwunel does
            # not expose this path, so ordinary users always use the standard
            # Matrix registration-token flow above.
            nonce_response = await http.get(
                "/_synapse/admin/v1/register",
            )
            try:
                nonce_response.raise_for_status()
            except Exception as exc:
                if admin:
                    raise RuntimeError(
                        "this homeserver cannot create admin accounts "
                        "through the Manager",
                    ) from exc
                raise
            nonce = nonce_response.json().get("nonce")
            if not isinstance(nonce, str) or not nonce:
                raise RuntimeError("Matrix registration nonce is invalid")
            raw_password = password.get_secret_value()
            admin_marker = "admin" if admin else "notadmin"
            mac_payload = "\x00".join(
                (nonce, username, raw_password, admin_marker),
            ).encode()
            mac = hmac.new(
                registration_token.get_secret_value().encode(),
                mac_payload,
                hashlib.sha1,
            ).hexdigest()
            response = await http.post(
                "/_synapse/admin/v1/register",
                json={
                    "nonce": nonce,
                    "username": username,
                    "password": raw_password,
                    "admin": admin,
                    "mac": mac,
                },
            )
            response.raise_for_status()
            user_id = response.json().get("user_id")
            if not isinstance(user_id, str) or not user_id:
                raise RuntimeError(
                    "Matrix registration returned no user ID",
                )
            return {"user_id": user_id, "admin": admin}

        if self._registration_http is not None:
            return await register(self._registration_http)
        async with httpx.AsyncClient(
            base_url=self.config.homeserver,
            timeout=30,
        ) as http:
            return await register(http)

    async def start(self, handler: InboundHandler) -> None:
        """Prepare encryption state and start the owned sync loop."""
        self.bind_handler(handler)
        self._needs_full_state = True
        CryptoStore(self.config.crypto_store).prepare()
        self.config.media_dir.mkdir(parents=True, exist_ok=True)
        client = self._ensure_client()
        await self._prepare_crypto(client)
        self._client_prepared = True
        self._sync_task = asyncio.create_task(
            self._supervise_sync_loop(),
            name="matrix-sync-supervisor",
        )
        self._sync_task.add_done_callback(self._on_sync_task_done)

    async def wait_until_ready(self, *, timeout: float = 60) -> None:
        """Wait for the first durable sync or surface an exited sync task."""
        if self.ready.is_set():
            return
        if self._sync_task is None:
            raise RuntimeError("Matrix sync loop is not running")
        ready_wait = asyncio.create_task(self.ready.wait())
        try:
            done, _ = await asyncio.wait(
                (ready_wait, self._sync_task),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if ready_wait in done and self.ready.is_set():
                return
            if self._sync_task in done:
                await self._sync_task
            raise TimeoutError(
                "Matrix did not complete its initial sync",
            )
        finally:
            ready_wait.cancel()
            await asyncio.gather(ready_wait, return_exceptions=True)

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
                # Manager owns retries and client recreation. Leaving either
                # value as None lets matrix-nio wait forever inside _send(),
                # preventing the outer watchdog from observing the failure.
                max_timeouts=0,
                max_limit_exceeded=0,
            )
            self._client = AsyncClient(
                self.config.homeserver,
                self.config.user_id,
                device_id=None,
                store_path=str(self.config.crypto_store),
                config=nio_config,
            )
        if created or not getattr(self._client, "access_token", None):
            self._client.access_token = self._access_token
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
        self._client_prepared = False
        self.ready.clear()

    async def sync_once(self) -> None:
        """Run one resumable sync and durably advance its cursor."""
        client = await self._ensure_prepared_client()
        since = await self._state.get_value("matrix.sync_token")
        try:
            if self._needs_full_state and since is not None:
                hydration = await client.sync(
                    timeout=0,
                    since=None,
                    full_state=True,
                )
                if _is_unknown_token(hydration):
                    raise MatrixUnknownTokenError("M_UNKNOWN_TOKEN")
                if not hasattr(hydration, "next_batch"):
                    raise RuntimeError(
                        f"Matrix room hydration failed: {hydration}",
                    )
            response = await client.sync(
                timeout=self.config.sync_timeout_ms,
                since=since,
                # A fresh account has no cursor, so this first response also
                # hydrates nio's in-memory room cache. Restarts hydrate above
                # before safely resuming the durable incremental cursor.
                full_state=self._needs_full_state and since is None,
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
        self._needs_full_state = False
        self._last_sync_success_monotonic = self._clock()
        self.last_sync_success_at = datetime.now(UTC)
        self.ready.set()

    async def run_sync_loop(self) -> None:
        """Sync forever with bounded password-based token recovery."""
        refresh_attempts = 0
        delays = (5, 10, 20)
        while True:
            try:
                await asyncio.wait_for(
                    self.sync_once(),
                    timeout=self.config.sync_watchdog_timeout_seconds,
                )
                refresh_attempts = 0
                # A homeserver or test double may return immediately. Yield
                # so cancellation, readiness, and room-handler tasks cannot
                # be starved by a hot sync loop.
                await asyncio.sleep(0)
            except MatrixUnknownTokenError as exc:
                self.ready.clear()
                logger.warning(
                    "Matrix access token was rejected; attempting refresh",
                    extra={
                        "refresh_attempt": refresh_attempts + 1,
                        "refresh_limit": len(delays),
                    },
                )
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
                            "Matrix sync failed after three token refresh attempts",
                        ) from exc
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                self.ready.clear()
                logger.exception(
                    "Matrix sync watchdog expired; rebuilding transport",
                    extra={
                        "watchdog_seconds": (self.config.sync_watchdog_timeout_seconds),
                    },
                )
                await self._rebuild_client()
                await self._sleeper(
                    self.config.sync_retry_delay_seconds,
                )
            except Exception:
                self.ready.clear()
                logger.exception(
                    "Matrix sync failed; rebuilding transport",
                )
                await self._rebuild_client()
                await self._sleeper(
                    self.config.sync_retry_delay_seconds,
                )

    async def _supervise_sync_loop(self) -> None:
        """Restart a sync loop that exits outside the normal retry path."""
        while True:
            try:
                await self.run_sync_loop()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.ready.clear()
                logger.exception(
                    "Matrix sync loop exited unexpectedly; restarting",
                )
                await self._rebuild_client()
                await self._sleeper(
                    self.config.sync_retry_delay_seconds,
                )

    def _on_sync_task_done(self, task: asyncio.Task[None]) -> None:
        self.ready.clear()
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            logger.error("Matrix sync supervisor stopped unexpectedly")
            return
        logger.error(
            "Matrix sync supervisor crashed",
            exc_info=(type(error), error, error.__traceback__),
        )

    async def _ensure_prepared_client(self) -> Any:
        client = self._ensure_client()
        if not self._client_prepared:
            await self._prepare_crypto(client)
            self._client_prepared = True
        return client

    async def _rebuild_client(self) -> None:
        """Close a failed owned transport so the next sync starts cleanly."""
        if self._client_injected:
            return
        client = self._client
        self._client = None
        self._client_prepared = False
        self._needs_full_state = True
        close = getattr(client, "close", None)
        if close is None:
            return
        try:
            await asyncio.wait_for(close(), timeout=5)
        except Exception:
            logger.exception("Failed to close stale Matrix transport")

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
        client = self._ensure_client()
        self._access_token = access_token
        client.access_token = access_token
        user_id = getattr(response, "user_id", None)
        if user_id:
            client.user_id = user_id
            client.user = user_id
        device_id = getattr(response, "device_id", None)
        if device_id:
            client.device_id = device_id
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
                    self.history.append(inbound)
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
        event_type = (
            source.get("type", "m.room.message")
            if isinstance(source, dict)
            else "m.room.message"
        )
        relates_to = content.get("m.relates_to", {})
        relation_type = (
            relates_to.get("rel_type") if isinstance(relates_to, dict) else None
        )
        is_bot_acknowledgement = content.get("io.agentteams.acknowledgement") is True
        is_transient = content.get("io.agentteams.transient") is True
        if (
            event_type == "m.room.redaction"
            or relation_type == "m.replace"
            or is_bot_acknowledgement
            or is_transient
        ):
            return None
        body = getattr(event, "body", None) or content.get("body")
        if not isinstance(body, str):
            return None
        milliseconds = getattr(event, "server_timestamp", 0) or 0
        timestamp = datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
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
        media = self._media_references(content, body)
        return InboundEvent(
            room_id=room_id,
            event_id=event_id,
            sender=sender,
            body=body,
            timestamp=timestamp,
            is_direct=self._is_direct_room(room_id, sender),
            thread_id=thread_id,
            mentions=mentions,
            media=media,
            event_type=event_type,
            relation_type=relation_type,
            is_bot_acknowledgement=is_bot_acknowledgement,
        )

    def _is_direct_room(self, room_id: str, sender: str) -> bool:
        rooms = getattr(self._client, "rooms", {}) or {}
        room = rooms.get(room_id)
        users = getattr(room, "users", {}) if room is not None else {}
        user_ids = set(users)
        return (
            len(user_ids) == 2
            and self.config.user_id in user_ids
            and sender in user_ids
        )

    @staticmethod
    def _media_references(
        content: dict[str, Any],
        body: str,
    ) -> tuple[MediaReference, ...]:
        msgtype = content.get("msgtype")
        if msgtype not in {"m.image", "m.file", "m.audio", "m.video"}:
            return ()
        info = content.get("info", {})
        info = info if isinstance(info, dict) else {}
        encrypted_file = content.get("file")
        if isinstance(encrypted_file, dict):
            key = encrypted_file.get("key", {})
            hashes = encrypted_file.get("hashes", {})
            return (
                MediaReference(
                    mxc_uri=str(encrypted_file.get("url", "")),
                    media_type=str(
                        info.get("mimetype", "application/octet-stream"),
                    ),
                    filename=body,
                    size=info.get("size"),
                    encryption_key=(key.get("k") if isinstance(key, dict) else None),
                    encryption_hash=(
                        hashes.get("sha256") if isinstance(hashes, dict) else None
                    ),
                    encryption_iv=encrypted_file.get("iv"),
                ),
            )
        uri = content.get("url")
        if not isinstance(uri, str):
            return ()
        return (
            MediaReference(
                mxc_uri=uri,
                media_type=str(
                    info.get("mimetype", "application/octet-stream"),
                ),
                filename=body,
                size=info.get("size"),
            ),
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
        """Send one idempotent text event with structured relations."""
        content = self._text_content(
            text,
            thread_id=thread_id,
            mentions=mentions,
        )
        return await self._send_content(
            room_id,
            content,
            txn_id=txn_id,
        )

    async def set_typing(
        self,
        room_id: str,
        *,
        typing: bool,
        timeout_ms: int = 30_000,
    ) -> None:
        response = await self._ensure_client().room_typing(
            room_id,
            typing,
            timeout=timeout_ms,
        )
        _require_matrix_success(response, "set typing")

    async def mark_read(self, room_id: str, event_id: str) -> None:
        response = await self._ensure_client().room_read_markers(
            room_id,
            fully_read_event=event_id,
            read_event=event_id,
        )
        _require_matrix_success(response, "mark event read")

    async def joined_rooms(self) -> tuple[str, ...]:
        response = await self._ensure_client().joined_rooms()
        _require_matrix_success(response, "list joined rooms")
        rooms = getattr(response, "rooms", None)
        if not isinstance(rooms, list) or not all(
            isinstance(room_id, str) for room_id in rooms
        ):
            raise RuntimeError("Matrix joined rooms response is invalid")
        return tuple(sorted(set(rooms)))

    async def members(self, room_id: str) -> tuple[str, ...]:
        response = await self._ensure_client().joined_members(room_id)
        _require_matrix_success(response, "list room members")
        rows = getattr(response, "members", None)
        if not isinstance(rows, list):
            raise RuntimeError("Matrix member response is invalid")
        user_ids: set[str] = set()
        for row in rows:
            user_id = getattr(row, "user_id", None)
            if not isinstance(user_id, str) or not user_id:
                raise RuntimeError("Matrix member identity is invalid")
            user_ids.add(user_id)
        return tuple(sorted(user_ids))

    async def lookup_user(self, user_id: str) -> dict[str, str | None]:
        response = await self._ensure_client().get_profile(user_id)
        _require_matrix_success(response, "get user profile")
        display_name = getattr(response, "displayname", None)
        avatar_url = getattr(response, "avatar_url", None)
        if display_name is not None and not isinstance(display_name, str):
            raise RuntimeError("Matrix display name is invalid")
        if avatar_url is not None and not isinstance(avatar_url, str):
            raise RuntimeError("Matrix avatar URI is invalid")
        return {
            "user_id": user_id,
            "display_name": display_name,
            "avatar_url": avatar_url,
        }

    async def create_private_room(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        creation_marker: dict[str, str | int],
    ) -> str:
        response = await self._ensure_client().room_create(
            visibility=RoomVisibility.private,
            name=name,
            topic=topic,
            is_direct=False,
            preset=RoomPreset.private_chat,
            invite=invite,
            initial_state=(
                {
                    "type": "io.agentteams.creation",
                    "state_key": "",
                    "content": dict(creation_marker),
                },
            ),
        )
        _require_matrix_success(response, "create room")
        room_id = getattr(response, "room_id", None)
        if not isinstance(room_id, str) or not room_id:
            raise RuntimeError("Matrix room create returned no room ID")
        return room_id

    async def invite_user(self, room_id: str, user_id: str) -> None:
        response = await self._ensure_client().room_invite(
            room_id,
            user_id,
        )
        _require_matrix_success(response, "invite room member")

    async def kick_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None:
        response = await self._ensure_client().room_kick(
            room_id,
            user_id,
            reason,
        )
        _require_matrix_success(response, "kick room member")

    async def ban_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None:
        response = await self._ensure_client().room_ban(
            room_id,
            user_id,
            reason,
        )
        _require_matrix_success(response, "ban room member")

    async def unban_user(self, room_id: str, user_id: str) -> None:
        response = await self._ensure_client().room_unban(
            room_id,
            user_id,
        )
        _require_matrix_success(response, "unban room member")

    async def room_state(
        self,
        room_id: str,
    ) -> tuple[dict[str, Any], ...]:
        response = await self._ensure_client().room_get_state(room_id)
        _require_matrix_success(response, "get room state")
        events = getattr(response, "events", None)
        if not isinstance(events, list) or not all(
            isinstance(event, dict) for event in events
        ):
            raise RuntimeError("Matrix room state response is invalid")
        return tuple(dict(event) for event in events)

    async def upload_media(self, path: Path) -> str:
        return await MediaAdapter(self._ensure_client()).upload(path)

    async def download_media(
        self,
        reference: MediaReference,
    ) -> tuple[Any, ...]:
        return await MediaAdapter(self._ensure_client()).download(reference)

    async def edit_text(
        self,
        room_id: str,
        event_id: str,
        text: str,
        *,
        txn_id: str,
    ) -> str:
        """Replace a previously sent streaming text event."""
        final_content = self._text_content(text)
        content: dict[str, Any] = {
            **final_content,
            "m.new_content": final_content,
            "m.relates_to": ThreadProjector.replacement(event_id),
        }
        return await self._send_content(
            room_id,
            content,
            txn_id=txn_id,
        )

    def _text_content(
        self,
        text: str,
        *,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        content: dict[str, Any] = {
            "msgtype": "m.text",
            "body": text,
            "format": "org.matrix.custom.html",
            "formatted_body": markdown_to_matrix_html(text),
        }
        targets = list(dict.fromkeys(mentions))
        if targets:
            content["m.mentions"] = {"user_ids": targets}
            if self.config.mention_pill_in_body:
                pills = " ".join(
                    (
                        '<a href="https://matrix.to/#/'
                        f'{html.escape(user_id, quote=True)}">'
                        f"{html.escape(user_id)}</a>"
                    )
                    for user_id in targets
                )
                content["formatted_body"] = (
                    f"{pills} {content['formatted_body']}"
                ).strip()
        if thread_id:
            content["m.relates_to"] = ThreadProjector.relation(thread_id)
        return content

    async def _send_content(
        self,
        room_id: str,
        content: dict[str, Any],
        *,
        txn_id: str,
    ) -> str:
        state_key = f"matrix.txn.{txn_id}"
        await self._state.set_value(
            state_key,
            json.dumps(
                {
                    "room_id": room_id,
                    "txn_id": txn_id,
                    "status": "prepared",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        client = self._ensure_client()
        for attempt in range(2):
            try:
                response = await client.room_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content=content,
                    tx_id=txn_id,
                    ignore_unverified_devices=True,
                )
                break
            except TimeoutError:
                if attempt:
                    raise
                await self._sleeper(0)
        event_id = getattr(response, "event_id", None)
        if not isinstance(event_id, str) or not event_id:
            raise RuntimeError(f"Matrix send failed: {response}")
        await self._state.set_value(
            state_key,
            json.dumps(
                {
                    "room_id": room_id,
                    "txn_id": txn_id,
                    "event_id": event_id,
                    "status": "sent",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        return event_id


def _require_matrix_success(response: object, operation: str) -> None:
    if response is None or type(response).__name__.endswith("Error"):
        raise RuntimeError(f"Matrix {operation} failed: {response}")


def _is_unknown_token(value: object) -> bool:
    if isinstance(value, MatrixUnknownTokenError):
        return True
    if isinstance(value, ErrorResponse):
        error_code = value.status_code
        return error_code == "M_UNKNOWN_TOKEN" or (
            error_code in {"401", 401} and "token" in value.message.lower()
        )
    error_code = getattr(value, "errcode", None)
    if error_code == "M_UNKNOWN_TOKEN":
        return True
    http_status = getattr(value, "status", None)
    if http_status is None:
        http_status = getattr(value, "status_code", None)
    return http_status in {"401", 401}
