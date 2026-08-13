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
        # 逻辑说明：记录 Matrix 配置、持久状态接口及可注入客户端，缓存明文令牌并初始化同步任务、就绪事件、健康时间戳和房间历史；构造阶段不发起网络请求。
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
        # 逻辑说明：同时检查首次同步已就绪、监督任务仍运行且存在成功同步时间，再用单调时钟比较陈旧阈值；任一条件不满足便返回不健康且不修改状态。
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
        # 逻辑说明：先要求配置 registration token，再在共享 HTTP 客户端上下文中执行注册；普通账号优先采用 Matrix 标准 UIA，管理员或兼容性回退则由内层函数完成 Synapse nonce/HMAC 流程，任何服务器拒绝或无效 user_id 都直接报错，不留下伪成功结果。
        registration_token = self.config.registration_token
        if registration_token is None:
            raise RuntimeError(
                "Matrix registration token is not configured",
            )

        async def register(http: Any) -> dict[str, str | bool]:
            # 逻辑说明：普通账号先走 registration-token UIA，只有端点缺失或独占用户名时回退 Synapse nonce/HMAC；管理员直接走回退端点，HTTP 或 user_id 校验失败均向上抛错。
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
        # 逻辑说明：绑定入站处理器，准备加密库与媒体目录，验证客户端加密身份后创建唯一同步监督任务；任何前置步骤失败都会阻止任务句柄发布。
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
        # 逻辑说明：若尚未就绪，则并行等待 ready 事件或同步监督任务结束；监督任务异常原样传播，二者在期限内都未完成则抛超时，并始终取消临时等待任务。
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
        # 逻辑说明：替换后续 sync 事件要调用的规范化入站处理器；这里只保存引用，不启动同步或重放既有事件。
        self._handler = handler

    def _ensure_client(self) -> Any:
        # 逻辑说明：惰性创建禁用 nio 内部重试的 AsyncClient，并给新建或无令牌实例补充访问令牌和用户身份；注入实例会被复用，返回值始终保存在 self._client。
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
        # 逻辑说明：启用加密时先用 whoami 核对令牌用户和设备 ID，再按设备加载本地 crypto store 并执行密钥维护；身份不符或缺少设备 ID 会中止启动。
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
        # 逻辑说明：取消并等待同步监督任务，随后关闭当前客户端、清除 prepared 与 ready 状态；任务取消被正常吞掉，而客户端关闭错误仍会传播给调用方。
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
        # 逻辑说明：从持久 sync token 恢复同步，重启时先全量水合房间缓存，再加入邀请房间并顺序派发已加入房间事件；仅派发成功后保存 next_batch、维护密钥并标记就绪。
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
        # 逻辑说明：用 watchdog 反复执行单次同步；令牌失效最多按 5/10/20 秒退避用密码刷新，超时或其他异常则清空就绪状态、重建自有客户端并延迟重试，取消信号原样退出。
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
        # 逻辑说明：持续托管 run_sync_loop；若其非取消异常地退出，则记录崩溃、清除 ready、重建传输并按配置等待后重启，因此监督层本身只响应取消而结束。
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
        # 逻辑说明：同步监督任务完成时立即撤销就绪标志；取消无需告警，正常意外退出记 error，携带异常退出则保留 traceback 记录，但此回调不重启任务也不抛错。
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
        # 逻辑说明：取得现有或新建 nio 客户端，并在 prepared 标志为假时完成一次加密身份与密钥准备；只有 await 成功后才置位，失败时保留未准备状态供重试。
        client = self._ensure_client()
        if not self._client_prepared:
            await self._prepare_crypto(client)
            self._client_prepared = True
        return client

    async def _rebuild_client(self) -> None:
        """Close a failed owned transport so the next sync starts cleanly."""
        # 逻辑说明：注入客户端保持原样；自有客户端则先从实例状态摘除并要求下次全量水合，再限时五秒关闭旧连接，关闭失败只记录日志以便后续同步仍能重建。
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
        # 逻辑说明：有配置密码时调用 nio login 获取新凭据，并把 access token、可选用户与设备 ID 同步回缓存和客户端；无密码或响应缺少令牌返回 False，登录异常直接传播。
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
        # 逻辑说明：存在已绑定处理器时，按房间和 timeline 原顺序规范化事件；可用事件先追加历史再 await 上层处理，任一处理失败会中断派发并阻止本轮游标提交。
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
        # 逻辑说明：拒绝自身、缺标识、redaction、替换、确认及 transient 事件；从 Matrix source 提取正文、时间、线程/回复关系、mentions 和媒体，结合房间缓存判定私聊后构造 InboundEvent。
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
        # 逻辑说明：读取 nio 房间缓存中的成员键集合，仅当房间恰有 Manager 与当前 sender 两人时返回 True；缓存缺失或成员不匹配一律视为非私聊。
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
        # 逻辑说明：仅把四类媒体消息转换为单个 MediaReference；加密附件从 file/key/hashes/iv 取解密元数据，明文附件读取 url，缺少明文字符串 URI 时返回空元组。
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
        # 逻辑说明：把文本、可选线程关系和结构化 mentions 编成 Matrix 消息内容，再用调用方 txn_id 进入持久化幂等发送流程，返回 homeserver 分配的 event_id。
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
        # 逻辑说明：向指定房间发布或撤销本用户的 typing 状态并传入持续毫秒数；nio 返回空值或 Error 响应时统一抛出“set typing”运行时错误。
        response = await self._ensure_client().room_typing(
            room_id,
            typing,
            timeout=timeout_ms,
        )
        _require_matrix_success(response, "set typing")

    async def mark_read(self, room_id: str, event_id: str) -> None:
        # 逻辑说明：将同一 event_id 同时设置为房间 fully-read 与 read marker；远端拒绝或无响应由统一校验转换为“mark event read”异常，不产生返回值。
        response = await self._ensure_client().room_read_markers(
            room_id,
            fully_read_event=event_id,
            read_event=event_id,
        )
        _require_matrix_success(response, "mark event read")

    async def joined_rooms(self) -> tuple[str, ...]:
        # 逻辑说明：请求当前账号已加入房间，要求响应 rooms 为纯字符串列表，再去重排序成不可变元组；Matrix 错误或畸形载荷均抛 RuntimeError。
        response = await self._ensure_client().joined_rooms()
        _require_matrix_success(response, "list joined rooms")
        rooms = getattr(response, "rooms", None)
        if not isinstance(rooms, list) or not all(
            isinstance(room_id, str) for room_id in rooms
        ):
            raise RuntimeError("Matrix joined rooms response is invalid")
        return tuple(sorted(set(rooms)))

    async def members(self, room_id: str) -> tuple[str, ...]:
        # 逻辑说明：查询指定房间的已加入成员，逐行验证非空 user_id 后去重排序返回；任一成员对象无合法身份即拒绝整个响应，不返回部分名单。
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
        # 逻辑说明：按 user_id 获取 Matrix profile，允许展示名和头像 URI 为 None、否则必须是字符串，最后连同输入身份返回字典；远端或字段类型错误直接失败。
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
        # 逻辑说明：以 private_chat 预设创建非 direct 私密房间，同时发送名称、主题、邀请名单和 io.agentteams.creation 初始状态；仅接受非空 room_id 并将其返回。
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
        # 逻辑说明：调用 Matrix room_invite 将 user_id 邀入 room_id；成功时无返回数据，空响应或 Error 类型响应被转换为邀请成员失败异常。
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
        # 逻辑说明：把 room_id、user_id 和 reason 原样交给 room_kick 移除当前成员但不封禁；服务端错误经统一检查抛出，调用成功返回 None。
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
        # 逻辑说明：请求 room_ban 在指定房间封禁用户并记录 reason；仅 nio 非空且非 Error 响应视为成功，否则用操作名生成 RuntimeError。
        response = await self._ensure_client().room_ban(
            room_id,
            user_id,
            reason,
        )
        _require_matrix_success(response, "ban room member")

    async def unban_user(self, room_id: str, user_id: str) -> None:
        # 逻辑说明：请求 room_unban 撤销 room_id 中 user_id 的封禁，不附带原因；远端失败通过公共响应检查向调用方报告，成功无返回值。
        response = await self._ensure_client().room_unban(
            room_id,
            user_id,
        )
        _require_matrix_success(response, "unban room member")

    async def room_state(
        self,
        room_id: str,
    ) -> tuple[dict[str, Any], ...]:
        # 逻辑说明：拉取房间完整 state，要求 events 是全由字典组成的列表，并复制每个事件后以元组返回；错误响应或混入非字典元素时拒绝结果。
        response = await self._ensure_client().room_get_state(room_id)
        _require_matrix_success(response, "get room state")
        events = getattr(response, "events", None)
        if not isinstance(events, list) or not all(
            isinstance(event, dict) for event in events
        ):
            raise RuntimeError("Matrix room state response is invalid")
        return tuple(dict(event) for event in events)

    async def upload_media(self, path: Path) -> str:
        # 逻辑说明：用当前 nio 客户端构造短生命周期 MediaAdapter，把本地 Path 的存在性、MIME 推断与上传校验委托给它，并返回验证过的 mxc URI。
        return await MediaAdapter(self._ensure_client()).upload(path)

    async def download_media(
        self,
        reference: MediaReference,
    ) -> tuple[Any, ...]:
        # 逻辑说明：将单个 MediaReference 交给基于当前 nio 客户端的 MediaAdapter 下载；适配器负责 URI、大小、加密元数据和 MIME 校验，结果以 DataBlock 元组透传。
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
        # 逻辑说明：生成新文本内容并同时放入顶层与 m.new_content，再用 m.replace 关系指向原 event_id；以独立 txn_id 幂等发送并返回替换事件 ID。
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
        # 逻辑说明：构造同时含纯文本与 Markdown HTML 的 m.text 内容，按首次出现顺序去重 mentions，可选插入安全转义的 mention pills，并在有 thread_id 时附加线程关系。
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
        # 逻辑说明：先以 txn_id 持久化 prepared 记录，再最多重试一次 room_send 超时；取得非空 event_id 后覆盖为 sent 记录并返回，非超时异常或无事件 ID 直接失败。
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
    # 逻辑说明：把 None 或类名以 Error 结尾的 nio 响应视为失败，并把具体 operation 与响应写入 RuntimeError；其余对象仅表示调用成功，不返回数据。
    if response is None or type(response).__name__.endswith("Error"):
        raise RuntimeError(f"Matrix {operation} failed: {response}")


def _is_unknown_token(value: object) -> bool:
    # 逻辑说明：识别本地专用异常、nio ErrorResponse 的 M_UNKNOWN_TOKEN/含 token 的 401、通用 errcode 及 status/status_code 401；仅这些形态返回 True。
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
