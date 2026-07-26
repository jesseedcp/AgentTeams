"""Matrix client ownership and transport boundary."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
from nio import (
    AsyncClient,
    AsyncClientConfig,
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
        self._registration_http = registration_http
        self._handler: InboundHandler | None = None
        self._sync_task: asyncio.Task[None] | None = None
        self._sleeper = sleeper
        self.ready = asyncio.Event()
        self.history = RoomHistory(limit=config.history_limit)

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
        CryptoStore(self.config.crypto_store).prepare()
        self.config.media_dir.mkdir(parents=True, exist_ok=True)
        client = self._ensure_client()
        await self._prepare_crypto(client)
        self._sync_task = asyncio.create_task(
            self.run_sync_loop(),
            name="matrix-sync",
        )

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
            relates_to.get("rel_type")
            if isinstance(relates_to, dict)
            else None
        )
        is_bot_acknowledgement = (
            content.get("io.agentteams.acknowledgement") is True
        )
        if (
            event_type == "m.room.redaction"
            or relation_type == "m.replace"
            or is_bot_acknowledgement
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
                    encryption_key=(
                        key.get("k") if isinstance(key, dict) else None
                    ),
                    encryption_hash=(
                        hashes.get("sha256")
                        if isinstance(hashes, dict)
                        else None
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
        user_ids = {
            getattr(row, "user_id", None)
            for row in rows
        }
        if None in user_ids or not all(
            isinstance(user_id, str) and user_id
            for user_id in user_ids
        ):
            raise RuntimeError("Matrix member identity is invalid")
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
    text = str(value)
    return "M_UNKNOWN_TOKEN" in text or (
        "401" in text and "token" in text.lower()
    )
