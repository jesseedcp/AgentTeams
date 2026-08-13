# -*- coding: utf-8 -*-
"""AgentTeams-owned Matrix channel for QwenPaw 2."""

# 初学者导读：这个长连接是 QwenPaw Worker 与 Matrix homeserver 的传输边界。
# 入站链路为“同步事件 → 校验房间/发送者/提及 → 下载媒体 → 交给 QwenPaw Agent”；
# 出站链路为“Agent 内容 → 脱敏/线程/mention/长消息处理 → 写回原房间”。登录 token、
# sync token 与加密设备状态需要落盘，Pod 重启后才能从上次位置继续而不重复处理消息。
# Manager 也使用 Matrix，但其管理权限来自 AgentScope room policy，不由此 channel 提供。

from __future__ import annotations

import asyncio
import html
import importlib
import inspect
import io
import json
import logging
import mimetypes
import os
import random
import re
import time
import urllib.parse
from uuid import uuid4
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import httpx

from nio import (
    AsyncClient,
    AsyncClientConfig,
    KeysUploadResponse,
    LoginResponse,
    KeyVerificationCancel,
    KeyVerificationEvent,
    KeyVerificationKey,
    KeyVerificationMac,
    KeyVerificationStart,
    LocalProtocolError,
    MatrixRoom,
    MegolmEvent,
    RoomEncryptedAudio,
    RoomEncryptedFile,
    RoomEncryptedImage,
    RoomEncryptedVideo,
    RoomMessageAudio,
    RoomMessageFile,
    RoomMessageImage,
    RoomMessageText,
    RoomMessageVideo,
    SyncResponse,
    ToDeviceEvent,
    ToDeviceError,
    UploadResponse,
)
from nio.event_builders.direct_messages import ToDeviceMessage
from nio.events.to_device import RoomKeyRequest, RoomKeyRequestCancellation
from nio.responses import (
    JoinedMembersResponse,
    RoomGetStateEventResponse,
    RoomSendError,
    SyncError,
    WhoamiResponse,
)

from qwenpaw.schemas import (
    AudioContent,
    ContentType,
    FileContent,
    ImageContent,
    MessageType,
    RunStatus,
    TextContent,
    VideoContent,
)

from qwenpaw.app.channels.base import BaseChannel
from qwenpaw.app.channels.utils import file_url_to_local_path
from qwenpaw.constant import WORKING_DIR

logger = logging.getLogger("qwenpaw.channels.matrix")


CHANNEL_KEY = "agentteams_matrix"

# Tunables: sync / typing / DM membership cache TTL
SYNC_TIMEOUT_MS = 30000
TYPING_SERVER_TIMEOUT_MS = 30000
TYPING_RENEWAL_INTERVAL_S = 25
TYPING_MAX_DURATION_S = 120
DM_CACHE_TTL_MS = 30_000
TASK_ROOM_CACHE_TTL_MS = 30_000
MATRIX_EVENT_PROTOCOL_LIMIT_BYTES = 64 * 1024
MATRIX_TEXT_EVENT_SAFE_BYTES = (MATRIX_EVENT_PROTOCOL_LIMIT_BYTES * 3) // 4
MATRIX_TEXT_EVENT_FALLBACK_BUDGET_BYTES = MATRIX_TEXT_EVENT_SAFE_BYTES - 1024
MATRIX_LONG_MESSAGE_METADATA_KEY = "com.agentteams.long_message"
TEAMHARNESS_TRIGGER_CONTENT_KEY = "m.teamharness.trigger"
TEAMHARNESS_SELF_TRIGGER_TYPES = frozenset({"PROJECT_REQUESTED"})
TEAMHARNESS_TOOL_DISPLAY_RE = re.compile(
    r"^\s*(?:[^\n:]{1,80}:\s*)?🔧\s+(?:\*\*)?[A-Za-z0-9_.-]+(?:\*\*)?",
)
MATRIX_LONG_MESSAGE_MIMETYPE = "text/markdown; charset=utf-8"
MATRIX_ATTACHMENT_REL_TYPE = "com.agentteams.attachment"
MATRIX_ATTACHMENT_CONTEXT_FILE = "teamharness-matrix-context.json"
ATTACHMENT_PARENT_EVENT_KEYS = (
    "parentEventId",
    "parent_event_id",
    "attachmentParentEventId",
    "attachment_parent_event_id",
    "matrixAttachmentParentEventId",
    "matrix_attachment_parent_event_id",
)

# Known QwenPaw slash commands — used to decide whether to strip
# @mention prefix
_SLASH_COMMANDS = frozenset(
    {
        "message",
        "history",
        "compact_str",
        "compact",
        "new",
        "stop",
        "clear",
        "reset",
    },
)

# Aliases: map alternative command names to their canonical form.
_SLASH_ALIASES: dict[str, str] = {
    "reset": "clear",
}

# --- Thread / readiness / control constants (ported from CoPaw overlay) ---

_STOP_RESPONSE_RE = re.compile(
    r"Session\s+`matrix:[^`]+`:\s+(?P<status>[^.]+)\.",
    re.IGNORECASE,
)
_READINESS_REPLY_RE = re.compile(
    r"\breadiness\s+check\b.*\breply\s+with\s+the\s+exact\s+text\s+READY\b",
    re.IGNORECASE | re.DOTALL,
)

_THREAD_META_ROOT_KEY = "thread_root_event_id"
_MATRIX_THREAD_META_KEY = "matrix_thread_root_event_id"
_MATRIX_OWN_THREAD_ROOT_KEY = "matrix_own_thread_root_event_id"
_MATRIX_PENDING_THREAD_PARTS_KEY = "matrix_pending_thread_parts"
_MATRIX_PENDING_FINAL_MESSAGE_KEY = "matrix_pending_final_message"
_MATRIX_STREAMING_FINAL_TEXT_KEY = "matrix_streaming_final_text"
_MATRIX_FORCE_NOTICE_KEY = "matrix_force_notice"
_MATRIX_PLACEHOLDER_THREAD_ROOT_KEY = "matrix_placeholder_thread_root"

_TOOL_CALL_MESSAGE_TYPE_NAMES = frozenset(
    {"FUNCTION_CALL", "PLUGIN_CALL", "MCP_TOOL_CALL"},
)
_TOOL_OUTPUT_MESSAGE_TYPE_NAMES = frozenset(
    {"FUNCTION_CALL_OUTPUT", "PLUGIN_CALL_OUTPUT", "MCP_TOOL_CALL_OUTPUT"},
)
_MATRIX_STREAMING_REASONING_EVENT_ID_KEY = "matrix_streaming_reasoning_event_id"
_MATRIX_STREAMING_REASONING_LAST_EDIT_KEY = "matrix_streaming_reasoning_last_edit_at"
_MATRIX_STREAMING_REASONING_STREAM_ID_KEY = "matrix_streaming_reasoning_stream_id"
_MATRIX_TRANSIENT_META_KEY = "matrix_agentteams_transient"
_AGENTTEAMS_TRANSIENT_CONTENT_KEY = "io.agentteams.transient"
_AGENTTEAMS_FINAL_CONTENT_KEY = "io.agentteams.final"
_AGENTTEAMS_FINAL_EVENT_TYPE = "io.agentteams.response.final"
_MATRIX_CONTROL_EPOCH_KEY = "matrix_control_epoch"


def _clean_control_response_text(text: str) -> str:
    """Hide channel-internal session ids from user-facing control replies."""
    # 逻辑说明：`_clean_control_response_text` 接收 `text`，按既有分支组合输入并生成结果，并依次复用 `search`、`strip`，返回 `str`。
    # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    if not text:
        return text
    match = _STOP_RESPONSE_RE.search(text)
    if not match:
        return text
    status = match.group("status").strip()
    status = status[:1].upper() + status[1:] if status else "Task stopped"
    return _STOP_RESPONSE_RE.sub(status + ".", text)


def _ends_with_no_reply_control(text: str) -> bool:
    """Return true when the final non-empty output line is NO_REPLY."""
    # 逻辑说明：`_ends_with_no_reply_control` 接收 `text`，按既有分支组合输入并生成结果，并依次复用 `strip`、`splitlines`，返回 `bool`。
    # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    return bool(text) and text.rstrip().splitlines()[-1].strip() == "NO_REPLY"


def _is_teamharness_tool_display(text: str) -> bool:
    # 逻辑说明：`_is_teamharness_tool_display` 接收 `text`，按既有分支组合输入并生成结果，并依次复用 `match`，返回 `bool`。
    # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    return bool(text and TEAMHARNESS_TOOL_DISPLAY_RE.match(text))


def _readiness_probe_reply(text: str) -> str | None:
    """Return the direct reply for the Matrix runtime readiness probe."""
    # 逻辑说明：`_readiness_probe_reply` 接收 `text`，读取、筛选并规范化现有数据，并依次复用 `search`，返回 `str | None`。
    # 执行过程中包含外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    return "READY" if _READINESS_REPLY_RE.search(text or "") else None


def _enum_name(value: Any) -> str:
    """Return a stable enum-like name for runtime schemas."""
    # 逻辑说明：`_enum_name` 接收 `value`，按既有分支组合输入并生成结果，并依次复用 `upper`、`rsplit`，返回 `str`。
    # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    name = getattr(value, "name", None)
    if name:
        return str(name).upper()
    return str(value).rsplit(".", 1)[-1].upper() if value is not None else ""


def _dedupe_nonempty(values: List[str]) -> List[str]:
    # 逻辑说明：`_dedupe_nonempty` 接收 `values`，按既有分支组合输入并生成结果，并依次复用 `strip`、`append`，返回 `List[str]`。
    # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    result = []
    seen = set()
    for value in values:
        text = str(value).strip()
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _md_to_html(text: str) -> str:
    """Convert Markdown text to HTML for Matrix ``formatted_body``.

    Uses ``markdown-it-py`` (the Python port of markdown-it) with the same
    configuration as OpenClaw's Matrix extension so rendering is consistent
    across both runtimes:

    - html disabled (raw HTML is escaped)
    - linkify enabled (bare URLs become clickable links)
    - breaks enabled (single newlines become ``<br>``)
    - strikethrough enabled (``~~text~~``)

    Falls back to simple HTML-escape + ``<br>`` if the library is missing.
    """
    # 逻辑说明：`_md_to_html` 接收 Matrix 频道待发送的 Markdown 文本，优先用 markdown-it 渲染安全 formatted_body，缺少依赖时转为转义后的换行 HTML。
    # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    try:
        from markdown_it import MarkdownIt

        md = MarkdownIt(
            "commonmark",
            {
                "html": False,
                "linkify": True,
                "breaks": True,
                "typographer": False,
            },
        )
        md.enable("strikethrough")
        md.enable("table")

        # linkify support requires linkify-it-py
        try:
            from linkify_it import LinkifyIt

            md.linkify = LinkifyIt()
        except ImportError:
            logger.debug(
                "linkify-it-py not installed; bare URLs may not be linkified",
            )

        return md.render(text).rstrip("\n")
    except ImportError:
        logger.warning(
            "markdown-it-py not installed; formatted_body will be plain text",
        )
        return html.escape(text).replace("\n", "<br>\n")


def _edit_fallback_html(text: str) -> str:
    # 逻辑说明：`_edit_fallback_html` 接收 `text`，把输入转换为调用方需要的结构，并依次复用 `replace`、`escape`，返回 `str`。
    # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    escaped = html.escape(text).replace("\n", "<br>\n")
    return f"<p>* {escaped}</p>"


def _matrix_event_payload_size(content: Dict[str, Any]) -> int:
    # 逻辑说明：`_matrix_event_payload_size` 接收 `content`，构造协议数据并完成外部传输，并依次复用 `encode`、`dumps`，返回 `int`。
    # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    return len(
        json.dumps(
            content,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8"),
    )


def _long_message_metadata(
    path: Path,
    media_uri: Optional[str],
) -> Optional[Dict[str, Any]]:
    # 逻辑说明：`_long_message_metadata` 接收 `path`、`media_uri`，构造协议数据并完成外部传输，返回 `Optional[Dict[str, Any]]`。
    # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    if not media_uri:
        return None
    return {
        "version": 1,
        "url": media_uri,
        "filename": path.name,
        "mimetype": MATRIX_LONG_MESSAGE_MIMETYPE,
    }


def _attach_long_message_metadata(
    content: Dict[str, Any],
    metadata: Optional[Dict[str, Any]],
) -> None:
    # 逻辑说明：`_attach_long_message_metadata` 接收 `content`、`metadata`，构造协议数据并完成外部传输，不返回业务结果。
    # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
    if metadata:
        content[MATRIX_LONG_MESSAGE_METADATA_KEY] = dict(metadata)


# Markers that separate accumulated history from the triggering message,
# matching the convention used by OpenClaw so agents can parse uniformly.
HISTORY_CONTEXT_MARKER = "[Chat messages since your last reply - for context]"
CURRENT_MESSAGE_MARKER = "[Current message - respond to this]"
DEFAULT_HISTORY_LIMIT = 50


class QwenPawMatrixClient(AsyncClient):
    """Keep query-token auth for homeservers/proxies that drop auth headers.

    同时给 matrix-nio 客户端增加 AgentTeams 所需的请求重试与超时边界。
    """

    async def send(
        self,
        method: str,
        path: str,
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
        trace_context: Optional[Any] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        # 逻辑说明：`send` 接收 `method`、`path`、`data`、`headers`，构造协议数据并完成外部传输，并依次复用 `urlparse`、`parse_qs`，返回 `Any`。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self.access_token and "access_token=" not in path:
            url = urllib.parse.urlparse(path)
            query = urllib.parse.parse_qs(url.query)
            query["access_token"] = [self.access_token]
            path = urllib.parse.urlunparse(
                url._replace(
                    query=urllib.parse.urlencode(query, doseq=True),
                ),
            )
        return await super().send(
            method,
            path,
            data,
            headers,
            trace_context,
            timeout,
        )


@dataclass
class HistoryEntry:
    """A buffered room message that didn't mention the bot."""

    sender: str
    body: str
    timestamp: Optional[int] = None
    message_id: Optional[str] = None
    # Optional structured media parts (e.g. downloaded images for vision
    # models) to be included alongside the text history when the mention
    # arrives.
    media_parts: Optional[List[Any]] = None


class AgentTeamsMatrixChannel(BaseChannel):
    """维护 QwenPaw Worker 的 Matrix 登录、同步、房间策略与回复发送。

    一个 channel 实例对应一个 Matrix 身份，但可服务多个房间；session_id 必须按
    房间/发送者稳定计算，不能把不同房间历史混进同一模型上下文。停止时会取消
    后台任务并保存 sync token，以便重启恢复。

    QwenPaw channel that connects to a Matrix homeserver via matrix-nio.
    """

    channel = CHANNEL_KEY  # type: ignore[assignment]
    uses_manager_queue: bool = True

    def __init__(
        self,
        process: Callable,
        homeserver: str = "",
        matrix_user_id: str = "",
        access_token: str = "",
        password: str = "",
        device_name: str = "qwenpaw-worker",
        device_id: str = "",
        encryption: bool = False,
        dm_disabled: bool = False,
        group_disabled: bool = False,
        groups: Optional[Dict[str, Any]] = None,
        vision_enabled: bool = False,
        history_limit: int = DEFAULT_HISTORY_LIMIT,
        sync_timeout_ms: int = 30000,
        on_reply_sent: Optional[Callable] = None,
        display_config: Any = None,
        no_text_debounce: bool = True,
        show_thinking: bool = True,
        show_tool_calls: bool = True,
        show_tool_results: bool = True,
        streaming_enabled: bool = True,
        workspace_dir: Path | None = None,
        access_control_dm: bool = False,
        access_control_group: bool = False,
        enabled: bool = True,
        **_kwargs: Any,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `process`、`homeserver`、`matrix_user_id`、`access_token`，按既有分支组合输入并生成结果，并依次复用 `__init__`、`super`，不返回业务结果。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        super().__init__(
            process=process,
            on_reply_sent=on_reply_sent,
            display_config=display_config,
            no_text_debounce=no_text_debounce,
            streaming_enabled=streaming_enabled,
            access_control_dm=access_control_dm,
            access_control_group=access_control_group,
        )
        # Matrix connection
        self.homeserver: str = homeserver.rstrip("/")
        self.matrix_user_id: str = matrix_user_id
        self.access_token: str = access_token
        self.password: str = password
        self.device_name: str = device_name
        self.device_id: str = device_id
        self.encryption: bool = encryption
        self.enabled: bool = enabled
        # Channel-level mute
        self.dm_disabled: bool = dm_disabled
        self.group_disabled: bool = group_disabled
        # Per-room overrides
        self.groups: Dict[str, Any] = groups or {}
        # Media / history
        self.vision_enabled: bool = vision_enabled
        self.history_limit: int = max(0, history_limit)
        self.sync_timeout_ms: int = sync_timeout_ms

        self._workspace_dir = (
            Path(workspace_dir).expanduser() if workspace_dir else None
        )
        self._client: Optional[AsyncClient] = None
        self._user_id: Optional[str] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._typing_tasks: Dict[str, asyncio.Task] = {}
        self._room_histories: Dict[str, List[HistoryEntry]] = {}
        self._dm_room_cache: Dict[str, Dict[str, Any]] = {}
        self._teamharness_task_room_cache: Dict[str, Dict[str, Any]] = {}
        self._http_client: Optional[httpx.AsyncClient] = None
        self._handled_verification_requests: set[str] = set()
        self._verification_tx_peers: dict[str, tuple[str, str]] = {}
        self._sent_verification_done: set[str] = set()
        # Thread tracking (ported from CoPaw overlay)
        self._active_thread_roots: Dict[str, str] = {}
        self._proactive_send_state: Dict[str, Dict[str, Any]] = {}
        self._control_epochs: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Debounce key — serialize by room_id (avoid concurrent session access)
    # ------------------------------------------------------------------

    def get_debounce_key(self, payload: Any) -> str:
        # 逻辑说明：`get_debounce_key` 接收 `payload`，读取、筛选并规范化现有数据，并依次复用 `get`，返回 `str`。
        # 执行过程中包含外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if isinstance(payload, dict):
            meta = payload.get("meta") or {}
            room_id = meta.get("room_id")
            if room_id:
                return f"matrix:{room_id}"
            return payload.get("sender_id") or ""
        return getattr(payload, "session_id", "") or ""

    def _room_control_epoch(self, room_id: str) -> int:
        # 逻辑说明：`_room_control_epoch` 接收 `room_id`，按既有分支组合输入并生成结果，并依次复用 `get`，返回 `int`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        epochs = getattr(self, "_control_epochs", None)
        if epochs is None:
            epochs = {}
            self._control_epochs = epochs
        return int(epochs.get(room_id, 0))

    def _advance_room_control_epoch(self, room_id: str) -> int:
        # 逻辑说明：`_advance_room_control_epoch` 接收 `room_id`，计算目标值并更新持久或共享状态，并依次复用 `_room_control_epoch`，返回 `int`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        epoch = self._room_control_epoch(room_id) + 1
        self._control_epochs[room_id] = epoch
        return epoch

    def _response_is_stale(
        self,
        room_id: str,
        meta: Optional[Dict[str, Any]],
    ) -> bool:
        # 逻辑说明：`_response_is_stale` 接收 `room_id`、`meta`，按既有分支组合输入并生成结果，并依次复用 `get`、`_room_control_epoch`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not isinstance(meta, dict):
            return False
        raw_epoch = meta.get(_MATRIX_CONTROL_EPOCH_KEY)
        if raw_epoch is None:
            return False
        try:
            request_epoch = int(raw_epoch)
        except (TypeError, ValueError):
            return True
        return request_epoch < self._room_control_epoch(room_id)

    # ------------------------------------------------------------------
    # Factory — from_config / from_env
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        process: Callable,
        config: Any,
        on_reply_sent: Optional[Callable] = None,
        display_config: Any = None,
        no_text_debounce: bool = True,
        workspace_dir: Path | None = None,
    ) -> "AgentTeamsMatrixChannel":
        # Support pydantic model, dict, or SimpleNamespace
        # 逻辑说明：`from_config` 接收 `process`、`config`、`on_reply_sent`、`display_config`，按既有分支组合输入并生成结果，并依次复用 `hasattr`、`model_dump`，返回 `'AgentTeamsMatrixChannel'`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if isinstance(config, dict):
            raw = config
        else:
            raw = config.model_dump() if hasattr(config, "model_dump") else vars(config)
        return cls(
            process=process,
            homeserver=raw.get("homeserver", ""),
            matrix_user_id=raw.get("user_id", ""),
            access_token=raw.get("access_token", ""),
            password=raw.get("password", ""),
            device_name=raw.get("device_name", "qwenpaw-worker"),
            device_id=raw.get("device_id", ""),
            encryption=raw.get("encryption", False),
            dm_disabled=raw.get("dm_disabled", False),
            group_disabled=raw.get("group_disabled", False),
            groups=raw.get("groups"),
            vision_enabled=raw.get("vision_enabled", False),
            history_limit=raw.get("history_limit", DEFAULT_HISTORY_LIMIT),
            sync_timeout_ms=raw.get("sync_timeout_ms", 30000),
            on_reply_sent=on_reply_sent,
            display_config=display_config,
            no_text_debounce=no_text_debounce,
            show_thinking=bool(raw.get("show_thinking", True)),
            show_tool_calls=bool(raw.get("show_tool_calls", True)),
            show_tool_results=bool(raw.get("show_tool_results", True)),
            streaming_enabled=bool(raw.get("streaming_enabled", True)),
            workspace_dir=workspace_dir,
            access_control_dm=bool(raw.get("access_control_dm", False)),
            access_control_group=bool(raw.get("access_control_group", False)),
            enabled=raw.get("enabled", True),
        )

    @classmethod
    def from_env(
        cls,
        process: Callable,
        on_reply_sent=None,
    ) -> "AgentTeamsMatrixChannel":
        # 逻辑说明：`from_env` 接收 `process`、`on_reply_sent`，按既有分支组合输入并生成结果，并依次复用 `cls`、`get`，返回 `'AgentTeamsMatrixChannel'`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        return cls(
            process=process,
            homeserver=os.environ.get("AGENTTEAMS_MATRIX_SERVER", ""),
            access_token=os.environ.get("AGENTTEAMS_MATRIX_TOKEN", ""),
            on_reply_sent=on_reply_sent,
        )

    # ------------------------------------------------------------------
    # Lifecycle — client, login, event callbacks, _sync_loop
    # token + user_id/password login (§2); optional
    # E2EE client config + store; cleartext + encrypted event callbacks;
    # starts _sync_loop (§3).
    # ------------------------------------------------------------------

    def _build_client_config(
        self,
        encryption: bool = False,
    ) -> AsyncClientConfig:
        """Build an AsyncClientConfig with proper request timeout.

        The HTTP request timeout must exceed the sync long-poll timeout
        so the HTTP layer doesn't kill the connection while the
        homeserver is legitimately waiting for new events.
        """
        # 逻辑说明：`_build_client_config` 接收 `encryption`，把输入转换为调用方需要的结构，并依次复用 `AsyncClientConfig`，返回 `AsyncClientConfig`。
        # 执行过程中包含外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        sync_s = self.sync_timeout_ms / 1000
        request_timeout = max(sync_s + 30, 60)
        return AsyncClientConfig(
            store_sync_tokens=False,
            encryption_enabled=encryption,
            request_timeout=request_timeout,
        )

    @staticmethod
    def _derive_device_id_from_name(device_name: str) -> str:
        """Use configured device_name directly as fallback device_id."""
        return (device_name or "").strip()

    async def health_check(self) -> Dict[str, Any]:
        """Check Matrix client connection status."""
        # 逻辑说明：`health_check` 不接收业务参数，依据频道开关、homeserver、客户端与同步任务状态返回 disabled、unhealthy 或 healthy 的探针结果。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not getattr(self, "enabled", True) or not self.homeserver:
            return {
                "channel": self.channel,
                "status": "disabled",
                "detail": "Matrix homeserver not configured.",
            }
        if self._client is None:
            return {
                "channel": self.channel,
                "status": "unhealthy",
                "detail": "Matrix client not initialized.",
            }
        has_token = bool(self._client.access_token)
        if not has_token:
            return {
                "channel": self.channel,
                "status": "unhealthy",
                "detail": "Matrix client has no access token (not logged in).",
            }
        return {
            "channel": self.channel,
            "status": "healthy",
            "detail": "Matrix client is connected.",
        }

    def _restore_auth_state_before_start(
        self,
        *,
        has_password_creds: bool,
        has_token_cred: bool,
    ) -> None:
        # Auth source priority:
        # 1) Explicit user_id/password from config/UI
        # 2) Explicit access_token from config/UI
        # 3) Cached auth_state fallback
        #
        # When token or user_id/password is explicitly configured, do not
        # restore cached token, otherwise we may accidentally bypass the
        # intended auth path.
        # 逻辑说明：`_restore_auth_state_before_start` 接收 `has_password_creds`、`has_token_cred`，推进组件生命周期并同步运行状态，并依次复用 `_load_auth_state`，不返回业务结果。
        # 执行过程中包含外部 I/O；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        has_explicit_identity = (
            has_password_creds or has_token_cred or self.matrix_user_id
        )
        self._load_auth_state(
            restore_token=False,
            restore_identity=not has_explicit_identity,
        )

    def _preflight_e2ee_dependencies(self) -> None:
        """Probe olm before creating AsyncClientConfig;
        disable E2EE if absent."""
        # 逻辑说明：`_preflight_e2ee_dependencies` 在创建 nio 客户端前探测 olm；未启用加密时直接返回，缺少依赖时关闭 E2EE 并记录原因。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self.encryption:
            return
        try:
            importlib.import_module("olm")
        except ImportError:
            logger.error(
                "MatrixChannel: olm not installed — falling back to "
                "non-encrypted mode. "
                "To enable E2EE: pip install matrix-nio[e2e] && "
                "apt/dnf install libolm-dev",
            )
            self.encryption = False

    def _init_async_client(self, resolved_device_id: str) -> None:
        # E2EE: when encryption is enabled, provide store_path so matrix-nio
        # persists Olm/Megolm keys, and set config to auto-trust all devices
        # (appropriate for bot use cases where interactive verification is
        # impractical).
        # 逻辑说明：`_init_async_client` 接收 `resolved_device_id`，推进组件生命周期并同步运行状态，并依次复用 `_e2ee_store_path`、`mkdir`，不返回业务结果。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        store_path = None
        if self.encryption:
            store_path = self._e2ee_store_path()
            store_path.mkdir(parents=True, exist_ok=True)
        client_config = self._build_client_config(
            encryption=self.encryption,
        )
        self._client = QwenPawMatrixClient(
            self.homeserver,
            # Keep user neutral before auth; token/whoami or login response
            # will set the canonical MXID.
            user="",
            store_path=str(store_path) if store_path else "",
            config=client_config,
        )
        if resolved_device_id:
            self._client.device_id = resolved_device_id

    def _password_login_kwargs_for_nio(
        self,
        login_user: str,
        resolved_device_id: str,
    ) -> dict[str, Any]:
        # matrix-nio login() signature differs across versions. Build
        # kwargs from runtime signature to avoid argument collisions.
        # 逻辑说明：`_password_login_kwargs_for_nio` 接收 `login_user`、`resolved_device_id`，推进组件生命周期并同步运行状态，并依次复用 `signature`，返回 `dict[str, Any]`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        login_sig = inspect.signature(self._client.login)
        login_kwargs: dict[str, Any] = {}
        login_params = login_sig.parameters
        if "user" in login_params:
            login_kwargs["user"] = login_user
        elif "user_id" in login_params:
            login_kwargs["user_id"] = login_user
        if "password" in login_params:
            login_kwargs["password"] = self.password
        if "device_name" in login_params and self.device_name:
            login_kwargs["device_name"] = self.device_name
        if "device_id" in login_params:
            stable_device_id = self._client.device_id or resolved_device_id or ""
            if stable_device_id:
                login_kwargs["device_id"] = stable_device_id
        # For nio versions that derive username from client.user.
        if "user" not in login_params and "user_id" not in login_params:
            self._client.user = login_user
        return login_kwargs

    def _password_login_attempts(
        self,
        login_user: str,
        login_kwargs: dict[str, Any],
        resolved_device_id: str,
    ) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        # 逻辑说明：`_password_login_attempts` 接收 `login_user`、`login_kwargs`、`resolved_device_id`，推进组件生命周期并同步运行状态，并依次复用 `append`，返回 `list[tuple[tuple[Any, ...], dict[str, Any]]]`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        login_attempts: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        if login_kwargs:
            login_attempts.append(((), login_kwargs))
        login_attempts.append(
            (
                (self.password,),
                {
                    "device_name": self.device_name,
                    **({"device_id": resolved_device_id} if resolved_device_id else {}),
                },
            ),
        )
        login_attempts.append(
            (
                (login_user, self.password),
                {
                    "device_name": self.device_name,
                    **({"device_id": resolved_device_id} if resolved_device_id else {}),
                },
            ),
        )
        return login_attempts

    async def _try_password_login_variants(
        self,
        login_attempts: list[tuple[tuple[Any, ...], dict[str, Any]]],
    ) -> tuple[Any, Optional[TypeError]]:
        # 逻辑说明：`_try_password_login_variants` 接收 `login_attempts`，推进组件生命周期并同步运行状态，并依次复用 `login`，返回 `tuple[Any, Optional[TypeError]]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        last_exc: Optional[TypeError] = None
        for args, kwargs in login_attempts:
            try:
                resp = await self._client.login(*args, **kwargs)
                return resp, None
            except TypeError as exc:
                last_exc = exc
        return None, last_exc

    def _handle_password_login_success(self, resp: LoginResponse) -> None:
        # 逻辑说明：`_handle_password_login_success` 接收 `resp`，按请求类型分派并编排后续步骤，并依次复用 `info`、`_save_auth_state`，不返回业务结果。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        self._user_id = resp.user_id
        self._client.user_id = resp.user_id
        self._client.user = resp.user_id
        if getattr(resp, "device_id", None):
            self._client.device_id = resp.device_id
        if getattr(resp, "access_token", None):
            self._client.access_token = resp.access_token
        logger.info(
            "MatrixChannel: logged in component=matrix user_id=%s method=password device=%s "
            "device_name=%s",
            self._user_id,
            getattr(self._client, "device_id", ""),
            self.device_name,
        )
        self._save_auth_state()
        if self.encryption and self._client.store_path:
            if self._client.device_id:
                self._client.load_store()
                logger.info(
                    "MatrixChannel: crypto store loaded component=matrix store_path=%s",
                    self._client.store_path,
                )
            else:
                logger.warning(
                    "MatrixChannel: password login returned no device_id component=matrix; "
                    "E2EE store may not be reusable",
                )

    async def _login_with_password(
        self,
        login_user: str,
        resolved_device_id: str,
    ) -> bool:
        # 逻辑说明：`_login_with_password` 接收 `login_user`、`resolved_device_id`，推进组件生命周期并同步运行状态，并依次复用 `_password_login_kwargs_for_nio`、`_password_login_attempts`，返回 `bool`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        login_kwargs = self._password_login_kwargs_for_nio(
            login_user,
            resolved_device_id,
        )
        attempts = self._password_login_attempts(
            login_user,
            login_kwargs,
            resolved_device_id,
        )
        resp, last_exc = await self._try_password_login_variants(attempts)
        if last_exc is not None:
            raise last_exc
        if isinstance(resp, LoginResponse):
            self._handle_password_login_success(resp)
            return True
        logger.error(
            "MatrixChannel: password login failed component=matrix response_type=%s",
            type(resp).__name__,
        )
        return False

    async def _login_with_access_token(self) -> bool:
        # 逻辑说明：`_login_with_access_token` 把已配置令牌交给 nio 客户端并调用 whoami，校验实际用户与配置用户一致后补齐客户端身份，返回令牌是否可用。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        self._client.access_token = self.access_token
        whoami = await self._client.whoami()
        if isinstance(whoami, WhoamiResponse):
            if self.matrix_user_id and self.matrix_user_id != whoami.user_id:
                logger.error(
                    "MatrixChannel: configured user_id mismatch component=matrix configured_user_id=%s "
                    "token_owner=%s; refusing stale credentials",
                    self.matrix_user_id,
                    whoami.user_id,
                )
                return False
            self._user_id = whoami.user_id
            self._client.user_id = whoami.user_id
            self._client.user = whoami.user_id
            # E2EE requires device_id to associate Olm keys with this
            # device
            if whoami.device_id:
                self._client.device_id = whoami.device_id
            logger.info(
                "MatrixChannel: logged in component=matrix user_id=%s method=token device=%s",
                self._user_id,
                whoami.device_id,
            )
            self._save_auth_state()
            # Load crypto store after user_id and device_id are set
            if self.encryption and self._client.store_path:
                if self._client.device_id:
                    self._client.load_store()
                    logger.info(
                        "MatrixChannel: crypto store loaded component=matrix store_path=%s",
                        self._client.store_path,
                    )
                else:
                    logger.error(
                        "MatrixChannel: E2EE enabled but whoami returned no device_id component=matrix "
                        "encryption disabled; token may lack device scope",
                    )
                    self.encryption = False
            return True
        logger.error(
            "MatrixChannel: token login failed component=matrix response_type=%s",
            type(whoami).__name__,
        )
        return False

    def _register_plain_room_callbacks(self) -> None:
        # 逻辑说明：`_register_plain_room_callbacks` 在非加密模式下把文本与媒体事件类型分别绑定到频道处理器，使 nio 同步结果进入正确回调。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        self._client.add_event_callback(
            self._on_room_event,
            (RoomMessageText,),
        )
        self._client.add_event_callback(
            self._on_room_media_event,
            (
                RoomMessageImage,
                RoomMessageFile,
                RoomMessageAudio,
                RoomMessageVideo,
            ),
        )

    async def _setup_e2ee_after_login(self) -> bool:
        # 逻辑说明：`_setup_e2ee_after_login` 在登录成功后按 nio 标志上传设备密钥并启用加密房间回调；任一步响应异常都返回 false 阻止带病启动。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self.encryption:
            return True
        if self._client.should_upload_keys:
            resp = await self._client.keys_upload()
            if not isinstance(resp, KeysUploadResponse):
                logger.error(
                    "MatrixChannel: E2E keys upload failed after login component=matrix response_type=%s",
                    type(resp).__name__,
                )
                return False
            logger.info("MatrixChannel: E2E keys uploaded component=matrix")
        # Encrypted media events (decrypted by nio, delivered as
        # RoomEncrypted* types)
        self._client.add_event_callback(
            self._on_room_encrypted_media_event,
            (
                RoomEncryptedImage,
                RoomEncryptedAudio,
                RoomEncryptedVideo,
                RoomEncryptedFile,
            ),
        )
        # Undecryptable events (missing session key)
        self._client.add_event_callback(
            self._on_megolm_event,
            (MegolmEvent,),
        )
        self._client.add_to_device_callback(
            self._on_key_verification_event,
            (KeyVerificationEvent,),
        )
        self._client.add_to_device_callback(
            self._on_to_device_probe_event,
            (ToDeviceEvent,),
        )
        self._client.add_to_device_callback(
            self._on_room_key_request_event,
            (
                RoomKeyRequest,
                RoomKeyRequestCancellation,
            ),
        )
        logger.info(
            "MatrixChannel: key verification to-device callback registered component=matrix",
        )
        logger.info(
            "MatrixChannel: E2EE enabled, encrypted event handlers registered component=matrix",
        )
        return True

    async def start(self) -> None:
        # 逻辑说明：`start` 不接收业务参数，校验 homeserver/E2EE 依赖，创建并登录 Matrix 客户端，注册回调后启动唯一的后台同步任务。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        if not self.homeserver:
            logger.warning(
                "MatrixChannel: homeserver not configured, skipping component=matrix",
            )
            return
        self._preflight_e2ee_dependencies()
        login_user = (self.matrix_user_id or "").strip()
        has_password_creds = bool(login_user and self.password)
        has_token_cred = bool(self.access_token)
        self._restore_auth_state_before_start(
            has_password_creds=has_password_creds,
            has_token_cred=has_token_cred,
        )
        resolved_device_id = self.device_id or self._derive_device_id_from_name(
            self.device_name
        )
        self._init_async_client(resolved_device_id)

        if has_password_creds:
            if not await self._login_with_password(
                login_user,
                resolved_device_id,
            ):
                return
        elif self.access_token:
            if not await self._login_with_access_token():
                return
        else:
            logger.error("MatrixChannel: no credentials configured component=matrix")
            return

        self._register_plain_room_callbacks()
        if not await self._setup_e2ee_after_login():
            return

        self._http_client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=60,
        )

        self._sync_task = asyncio.create_task(self._sync_loop())
        logger.info("MatrixChannel: sync loop started component=matrix")

    async def stop(self) -> None:
        # 逻辑说明：`stop` 不接收业务参数，取消并等待同步任务退出，再关闭 nio 客户端并清空引用，确保重复停止不会泄露连接。
        # 执行过程中包含异步等待、外部 I/O 与实例状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                logger.debug(
                    "MatrixChannel: sync task cancelled during stop component=matrix"
                )
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        if self._client:
            await self._client.close()
        logger.info("MatrixChannel: stopped component=matrix")

    # ------------------------------------------------------------------
    # Sync loop — token persistence, catch-up, incremental sync, E2EE
    # maintenance
    # catch-up sync suppresses replay; incremental sync; E2EE maintenance
    # between syncs when encryption on.
    # ------------------------------------------------------------------

    @staticmethod
    def _sync_token_path() -> Optional[Path]:
        """Return the file path for persisting the Matrix sync token."""
        return WORKING_DIR / "matrix_sync_token"

    @staticmethod
    def _auth_state_path() -> Path:
        """Return the file path for persisted Matrix auth state."""
        return WORKING_DIR / "matrix_auth_state.json"

    @staticmethod
    def _ready_marker_path() -> Optional[Path]:
        # 逻辑说明：`_ready_marker_path` 是静态配置解析器，从 AGENTTEAMS_MATRIX_CHANNEL_READY_FILE 读取就绪标记路径；未配置时返回 None。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        marker = os.environ.get("AGENTTEAMS_MATRIX_CHANNEL_READY_FILE", "").strip()
        return Path(marker) if marker else None

    def _mark_channel_ready(self) -> None:
        """Mark Matrix sync ready for worker-level readiness probes."""
        # 逻辑说明：`_mark_channel_ready` 在 Matrix 首次同步成功后创建父目录并原子写入就绪标记，供容器 readiness probe 判断频道可用。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        path = self._ready_marker_path()
        if not path:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("ready\n", encoding="utf-8")
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to write ready marker: %s",
                exc,
            )

    def _load_auth_state(
        self,
        restore_token: bool = True,
        restore_identity: bool = True,
    ) -> None:
        """Best-effort load persisted access_token/user_id/device_id."""
        # 逻辑说明：`_load_auth_state` 接收 `restore_token`、`restore_identity`，读取、筛选并规范化现有数据，并依次复用 `_auth_state_path`、`exists`，不返回业务结果。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        path = self._auth_state_path()
        if not path.exists():
            return
        try:
            import json

            payload = json.loads(path.read_text())
            restored_any = False
            if restore_token and not self.access_token:
                self.access_token = str(payload.get("access_token", ""))
                restored_any = bool(self.access_token)
            if restore_identity:
                if not self.matrix_user_id:
                    self.matrix_user_id = str(payload.get("user_id", ""))
                    restored_any = restored_any or bool(self.matrix_user_id)
                if not self.device_id:
                    self.device_id = str(payload.get("device_id", ""))
                    restored_any = restored_any or bool(self.device_id)
            if restored_any:
                logger.info(
                    "MatrixChannel: restored auth state from %s "
                    "(token=%s, user=%s, device=%s)",
                    path,
                    bool(self.access_token),
                    self.matrix_user_id or "<unknown>",
                    self.device_id or "<unknown>",
                )
            else:
                logger.debug(
                    "MatrixChannel: auth state present at %s but not applied "
                    "(restore_token=%s restore_identity=%s)",
                    path,
                    restore_token,
                    restore_identity,
                )
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to load auth state from %s: %s",
                path,
                exc,
            )

    def _save_auth_state(self) -> None:
        """Persist access_token/user_id/device_id for stable restarts."""
        # 逻辑说明：`_save_auth_state` 从已登录 nio 客户端提取 access token、user ID 与 device ID，写入权限受限的认证状态文件以便重启复用。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return
        token = getattr(self._client, "access_token", "") or ""
        user_id = getattr(self._client, "user_id", "") or self._user_id or ""
        device_id = getattr(self._client, "device_id", "") or ""
        if not token or not user_id:
            return
        try:
            import json

            payload = {
                "access_token": token,
                "user_id": user_id,
                "device_id": device_id,
            }
            path = self._auth_state_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload))
            self.access_token = token
            self.matrix_user_id = user_id
            if device_id:
                self.device_id = device_id
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to persist auth state: %s",
                exc,
            )

    def _load_sync_token(self) -> Optional[str]:
        """Load persisted next_batch token from disk, or None.

        The token file is pulled from MinIO by FileSync.pull_all() during
        startup, so it's already on disk when this runs — even on a fresh
        container after destroy/recreate.
        """
        # 逻辑说明：`_load_sync_token` 从 FileSync 已恢复的 next_batch 文件读取 Matrix 增量同步游标，文件缺失、为空或读取失败时返回 None。
        # 执行过程中包含外部 I/O；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        path = self._sync_token_path()
        if path and path.exists():
            try:
                token = path.read_text().strip()
                if token:
                    logger.info(
                        "MatrixChannel: restored sync token from %s",
                        path,
                    )
                    return token
            except Exception as exc:
                logger.warning(
                    "MatrixChannel: failed to read sync token: %s",
                    exc,
                )
        return None

    def _save_sync_token(self, token: str) -> None:
        """Persist next_batch token to disk (push_loop uploads it to MinIO)."""
        # 逻辑说明：`_save_sync_token` 接收 `token`，计算目标值并更新持久或共享状态，并依次复用 `_sync_token_path`、`mkdir`，不返回业务结果。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        path = self._sync_token_path()
        if path:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(token)
            except Exception as exc:
                logger.warning(
                    "MatrixChannel: failed to save sync token: %s",
                    exc,
                )

    async def _e2ee_maintenance(self) -> None:
        """Perform E2EE key maintenance tasks after each sync.

        Mirrors what nio's sync_forever() does between syncs:
        - Upload device keys when needed
        - Query device keys for new/changed users
        - Claim one-time keys to establish Olm sessions
        - Send outgoing to-device messages (key shares, key requests)
        """
        # 逻辑说明：`_e2ee_maintenance` 在每次同步后按 nio 待办状态上传/查询/领取密钥并发送 to-device 消息，维持加密会话可解密、可分享。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self.encryption or not self._client or not self._client.olm:
            return
        try:
            if self._client.should_upload_keys:
                await self._client.keys_upload()
            if self._client.should_query_keys:
                await self._client.keys_query()
            if self._client.should_claim_keys:
                await self._client.keys_claim(
                    self._client.get_users_for_key_claiming(),
                )
            await self._client.send_to_device_messages()
        except Exception as exc:
            logger.warning("MatrixChannel: E2EE maintenance error: %s", exc)

    async def _on_key_verification_event(
        self,
        event: KeyVerificationEvent,
    ) -> None:
        """Complete the bot side of an Element SAS verification challenge."""
        # 逻辑说明：`_on_key_verification_event` 接收 `event`，按既有分支组合输入并生成结果，并依次复用 `info`、`type`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client or not self._client.olm:
            logger.info(
                "MatrixChannel: verification event received "
                "but olm is not ready (event=%s, tx=%s, sender=%s)",
                type(event).__name__,
                getattr(event, "transaction_id", ""),
                getattr(event, "sender", ""),
            )
            return

        try:
            logger.info(
                "MatrixChannel: verification event received "
                "(event=%s, tx=%s, sender=%s, from_device=%s)",
                type(event).__name__,
                getattr(event, "transaction_id", ""),
                getattr(event, "sender", ""),
                getattr(event, "from_device", ""),
            )
            if isinstance(event, KeyVerificationStart):
                await self._handle_key_verification_start(event)
            elif isinstance(event, KeyVerificationKey):
                await self._handle_key_verification_key(event)
            elif isinstance(event, KeyVerificationMac):
                sas = self._client.key_verifications.get(event.transaction_id)
                logger.info(
                    "MatrixChannel: key verification MAC received "
                    "(tx=%s, verified=%s, verified_devices=%s)",
                    event.transaction_id,
                    getattr(sas, "verified", False),
                    getattr(sas, "verified_devices", []),
                )
                if getattr(sas, "verified", False):
                    await self._send_verification_done(event.transaction_id)
            elif isinstance(event, KeyVerificationCancel):
                known_tx = event.transaction_id in getattr(
                    self._client,
                    "key_verifications",
                    {},
                )
                if known_tx:
                    logger.info(
                        "MatrixChannel: key verification cancelled by %s "
                        "(tx=%s, reason=%s)",
                        event.sender,
                        event.transaction_id,
                        getattr(event, "reason", ""),
                    )
                else:
                    logger.info(
                        "MatrixChannel: key verification cancelled for "
                        "unknown key verification tx=%s from %s (reason=%s)",
                        event.transaction_id,
                        event.sender,
                        getattr(event, "reason", ""),
                    )
            else:
                logger.info(
                    "MatrixChannel: unhandled verification event type=%s "
                    "(tx=%s, sender=%s)",
                    type(event).__name__,
                    getattr(event, "transaction_id", ""),
                    getattr(event, "sender", ""),
                )
        except Exception as exc:
            logger.warning(
                "MatrixChannel: key verification handling failed: %s",
                exc,
            )

    async def _on_to_device_probe_event(
        self,
        event: ToDeviceEvent,
    ) -> None:
        """Probe raw to-device verification event types for troubleshooting."""
        # 逻辑说明：`_on_to_device_probe_event` 接收 `event`，按既有分支组合输入并生成结果，并依次复用 `get`、`startswith`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        raw_type = getattr(event, "type", "")
        if not raw_type:
            raw_type = getattr(event, "source", {}).get("type", "")
        if not isinstance(raw_type, str) or not raw_type.startswith(
            "m.key.verification.",
        ):
            return
        logger.info(
            "MatrixChannel: raw to-device verification event "
            "(type=%s, parsed=%s, sender=%s)",
            raw_type,
            type(event).__name__,
            getattr(event, "sender", ""),
        )
        if raw_type in (
            "m.key.verification.request",
            "m.key.verification.ready",
        ):
            logger.warning(
                "MatrixChannel: homeserver sent %s but current matrix-nio "
                "cannot parse it into KeyVerificationEvent; handling via "
                "raw to-device compatibility path",
                raw_type,
            )
        if raw_type == "m.key.verification.request":
            await self._handle_unknown_key_verification_request(event)
        elif raw_type == "m.key.verification.done":
            await self._handle_unknown_key_verification_done(event)

    async def _on_room_key_request_event(
        self,
        event: RoomKeyRequest | RoomKeyRequestCancellation,
    ) -> None:
        """Log room-key request events that often require manual review."""
        # 逻辑说明：`_on_room_key_request_event` 接收 `event`，按既有分支组合输入并生成结果，并依次复用 `warning`、`info`，不返回业务结果。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if isinstance(event, RoomKeyRequest):
            logger.warning(
                "MatrixChannel: room key request received; other device may "
                "show 'Review' until trust decision is made "
                "(sender=%s device=%s request_id=%s room_id=%s session_id=%s)",
                event.sender,
                event.requesting_device_id,
                event.request_id,
                getattr(event, "room_id", ""),
                getattr(event, "session_id", ""),
            )
        else:
            logger.info(
                "MatrixChannel: room key request cancelled "
                "(sender=%s device=%s request_id=%s)",
                event.sender,
                event.requesting_device_id,
                event.request_id,
            )

    async def _handle_unknown_key_verification_request(
        self,
        event: ToDeviceEvent,
    ) -> None:
        """Compat path for m.key.verification.request on older matrix-nio."""
        # 逻辑说明：`_handle_unknown_key_verification_request` 接收 `event`，按请求类型分派并编排后续步骤，并依次复用 `get`、`debug`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client or not self._client.olm:
            return

        source = getattr(event, "source", {}) or {}
        content = source.get("content", {}) or {}
        sender = getattr(event, "sender", "")
        from_device = str(content.get("from_device", "") or "")
        transaction_id = str(content.get("transaction_id", "") or "")
        methods = content.get("methods", []) or []

        request_key = f"{sender}|{from_device}|{transaction_id}"
        if request_key in self._handled_verification_requests:
            logger.debug(
                "MatrixChannel: verification request already handled "
                "(sender=%s, device=%s, tx=%s)",
                sender,
                from_device,
                transaction_id,
            )
            return
        self._handled_verification_requests.add(request_key)

        if not sender or not from_device:
            logger.warning(
                "MatrixChannel: cannot handle verification request without "
                "sender/device (sender=%s, device=%s, tx=%s)",
                sender,
                from_device,
                transaction_id,
            )
            return

        self._verification_tx_peers[transaction_id] = (sender, from_device)

        our_device = getattr(self._client, "device_id", "") or ""
        if not our_device:
            logger.warning(
                "MatrixChannel: cannot reply verification request without "
                "local device_id (sender=%s, device=%s, tx=%s)",
                sender,
                from_device,
                transaction_id,
            )
            return

        ready_content = {
            "from_device": our_device,
            "methods": ["m.sas.v1"],
            "transaction_id": transaction_id,
        }
        ready_message = ToDeviceMessage(
            "m.key.verification.ready",
            sender,
            from_device,
            ready_content,
        )
        try:
            resp = await self._client.to_device(ready_message)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to send verification ready "
                "(sender=%s, device=%s, tx=%s): %s",
                sender,
                from_device,
                transaction_id,
                exc,
            )
            return

        if isinstance(resp, ToDeviceError):
            logger.warning(
                "MatrixChannel: homeserver rejected verification ready "
                "(sender=%s, device=%s, tx=%s): %s",
                sender,
                from_device,
                transaction_id,
                resp,
            )
            return

        logger.info(
            "MatrixChannel: sent verification ready for request "
            "(sender=%s, device=%s, tx=%s, methods=%s)",
            sender,
            from_device,
            transaction_id,
            methods,
        )

    async def _handle_unknown_key_verification_done(
        self,
        event: ToDeviceEvent,
    ) -> None:
        """Handle done event emitted as UnknownToDeviceEvent on older nio."""
        # 逻辑说明：`_handle_unknown_key_verification_done` 接收 `event`，按请求类型分派并编排后续步骤，并依次复用 `get`、`info`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        source = getattr(event, "source", {}) or {}
        content = source.get("content", {}) or {}
        tx = str(content.get("transaction_id", "") or "")
        if not tx:
            return
        logger.info(
            "MatrixChannel: received verification done from %s (tx=%s)",
            getattr(event, "sender", ""),
            tx,
        )
        if tx not in self._sent_verification_done:
            await self._send_verification_done(tx)

    async def _handle_key_verification_start(
        self,
        event: KeyVerificationStart,
    ) -> None:
        """Accept Element's SAS start, querying device keys if needed."""
        # 逻辑说明：`_handle_key_verification_start` 接收 `event`，按请求类型分派并编排后续步骤，并依次复用 `_recover_key_verification_start`、`warning`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client or not self._client.olm:
            return
        self._verification_tx_peers[event.transaction_id] = (
            event.sender,
            event.from_device,
        )

        if event.transaction_id not in self._client.key_verifications:
            await self._recover_key_verification_start(event)

        if event.transaction_id not in self._client.key_verifications:
            logger.warning(
                "MatrixChannel: cannot accept key verification from %s "
                "(device=%s, tx=%s) because no SAS state exists yet; "
                "retry verification after the next sync",
                event.sender,
                event.from_device,
                event.transaction_id,
            )
            return

        try:
            resp = await self._client.accept_key_verification(
                event.transaction_id,
            )
        except LocalProtocolError as exc:
            logger.warning(
                "MatrixChannel: accept_key_verification failed for tx=%s: %s",
                event.transaction_id,
                exc,
            )
            return

        if isinstance(resp, ToDeviceError):
            logger.warning(
                "MatrixChannel: accept_key_verification failed for tx=%s: %s",
                event.transaction_id,
                resp,
            )
            return

        logger.info(
            "MatrixChannel: accepted key verification from %s (device=%s, tx=%s)",
            event.sender,
            event.from_device,
            event.transaction_id,
        )

    async def _send_verification_done(self, transaction_id: str) -> None:
        """Send m.key.verification.done for runtimes lacking done helpers."""
        # 逻辑说明：`_send_verification_done` 接收 `transaction_id`，构造协议数据并完成外部传输，并依次复用 `get`、`warning`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if (
            not self._client
            or not transaction_id
            or transaction_id in self._sent_verification_done
        ):
            return

        peer = self._verification_tx_peers.get(transaction_id)
        if not peer:
            logger.warning(
                "MatrixChannel: cannot send verification done for tx=%s "
                "because peer device is unknown",
                transaction_id,
            )
            return

        sender, device_id = peer
        done_message = ToDeviceMessage(
            "m.key.verification.done",
            sender,
            device_id,
            {"transaction_id": transaction_id},
        )
        try:
            resp = await self._client.to_device(done_message)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to send verification done "
                "(tx=%s, sender=%s, device=%s): %s",
                transaction_id,
                sender,
                device_id,
                exc,
            )
            return

        if isinstance(resp, ToDeviceError):
            logger.warning(
                "MatrixChannel: homeserver rejected verification done "
                "(tx=%s, sender=%s, device=%s): %s",
                transaction_id,
                sender,
                device_id,
                resp,
            )
            return

        self._sent_verification_done.add(transaction_id)
        logger.info(
            "MatrixChannel: sent verification done (tx=%s, sender=%s, device=%s)",
            transaction_id,
            sender,
            device_id,
        )

    async def _recover_key_verification_start(
        self,
        event: KeyVerificationStart,
    ) -> None:
        """Re-process start event after matrix-nio queried unknown devices."""
        # 逻辑说明：`_recover_key_verification_start` 接收 `event`，推进组件生命周期并同步运行状态，并依次复用 `keys_query`、`warning`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        assert self._client is not None
        assert self._client.olm is not None

        try:
            if self._client.should_query_keys:
                await self._client.keys_query()
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to query keys for verification from %s: %s",
                event.sender,
                exc,
            )
            return

        try:
            self._client.olm.handle_key_verification(event)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to rebuild key verification state for tx=%s: %s",
                event.transaction_id,
                exc,
            )

    async def _handle_key_verification_key(
        self,
        event: KeyVerificationKey,
    ) -> None:
        """Log the SAS challenge and confirm the bot side."""
        # 逻辑说明：`_handle_key_verification_key` 接收 `event`，按请求类型分派并编排后续步骤，并依次复用 `get`、`warning`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return

        sas = self._client.key_verifications.get(event.transaction_id)
        if sas is None:
            logger.warning(
                "MatrixChannel: key verification key for unknown tx=%s "
                "from %s; ask Element to restart verification",
                event.transaction_id,
                event.sender,
            )
            return

        logger.warning(
            "MatrixChannel: Element key verification challenge from %s (tx=%s): %s",
            event.sender,
            event.transaction_id,
            self._format_sas_challenge(sas),
        )

        # Receiving Key queues share_key in nio; flush it before sending MAC.
        await self._client.send_to_device_messages()
        try:
            resp = await self._client.confirm_short_auth_string(
                event.transaction_id,
            )
        except LocalProtocolError as exc:
            logger.warning(
                "MatrixChannel: confirm_short_auth_string failed for tx=%s: %s",
                event.transaction_id,
                exc,
            )
            return

        if isinstance(resp, ToDeviceError):
            logger.warning(
                "MatrixChannel: confirm_short_auth_string failed for tx=%s: %s",
                event.transaction_id,
                resp,
            )
            return

        logger.info(
            "MatrixChannel: confirmed local SAS side for tx=%s; "
            "compare the challenge in Element and accept there if it matches",
            event.transaction_id,
        )

    @staticmethod
    def _format_sas_challenge(sas: Any) -> str:
        """Return a human-readable SAS challenge for logs."""
        # 逻辑说明：`_format_sas_challenge` 接收 `sas`，把输入转换为调用方需要的结构，并依次复用 `callable`、`get_emoji`，返回 `str`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        parts: list[str] = []

        get_emoji = getattr(sas, "get_emoji", None)
        if callable(get_emoji):
            try:
                emojis = get_emoji()
                if emojis:
                    parts.append(
                        "emoji="
                        + " ".join(
                            f"{symbol}({description})" for symbol, description in emojis
                        ),
                    )
            except Exception as exc:
                logger.debug("MatrixChannel: get_emoji failed: %s", exc)

        get_decimal = getattr(sas, "get_decimal", None)
        if callable(get_decimal):
            try:
                decimals = get_decimal()
                if decimals:
                    parts.append(
                        "decimal=" + " ".join(str(n) for n in decimals),
                    )
            except Exception as exc:
                logger.debug("MatrixChannel: get_decimal failed: %s", exc)

        return "; ".join(parts) if parts else "unavailable"

    # pylint: disable=too-many-branches,too-many-statements
    async def _sync_loop(self) -> None:
        # 逻辑说明：`_sync_loop` 加载持久化 next_batch；首次部署先静默捕获游标防止重放旧消息，随后循环增量同步、E2EE 维护并保存新游标。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        next_batch: Optional[str] = self._load_sync_token()

        # When no persisted token exists (old version upgrade or first
        # deploy), do an initial sync with callbacks suppressed — only capture
        # next_batch so subsequent syncs are incremental.  This prevents
        # replaying old messages when the token file doesn't exist yet.
        #
        # Use timeout=0 so startup does not long-poll while callbacks are
        # temporarily suppressed. A long-poll here can swallow fresh messages
        # sent during worker boot.
        #
        # To truly suppress callbacks, temporarily remove event callbacks
        # before the sync and restore them after, because nio's sync()
        # internally calls receive_response() which fires callbacks.
        if next_batch is None:
            logger.info(
                "MatrixChannel: no sync token found, "
                "performing catch-up sync component=matrix messages_suppressed=True",
            )
            try:
                saved_cbs = self._client.event_callbacks[:]
                self._client.event_callbacks.clear()
                try:
                    resp = await self._client.sync(
                        timeout=0,
                        full_state=True,
                    )
                finally:
                    self._client.event_callbacks.extend(saved_cbs)
                if isinstance(resp, SyncResponse):
                    next_batch = resp.next_batch
                    if next_batch is not None:
                        self._save_sync_token(next_batch)
                    # Still auto-join invited rooms during catch-up
                    for room_id in resp.rooms.invite:
                        logger.info(
                            "MatrixChannel: auto-joining component=matrix room_id=%s",
                            room_id,
                        )
                        await self._client.join(room_id)
                    await self._e2ee_maintenance()
                    logger.info(
                        "MatrixChannel: catch-up sync done, "
                        "will process messages from next sync component=matrix",
                    )
                else:
                    logger.warning(
                        "MatrixChannel: catch-up sync error component=matrix response_type=%s",
                        type(resp).__name__,
                    )
            except Exception as exc:
                logger.exception(
                    "MatrixChannel: catch-up sync exception component=matrix error_type=%s",
                    type(exc).__name__,
                )
        else:
            # Restored from token — do a full_state sync to populate room
            # member display names (nio needs full state for user_name()).
            # Event callbacks are already registered so any messages received
            # during the offline window will be processed normally.
            logger.info(
                "MatrixChannel: restored token, "
                "performing full-state sync component=matrix",
            )
            try:
                resp = await self._client.sync(
                    timeout=self.sync_timeout_ms,
                    since=next_batch,
                    full_state=True,
                )
                if isinstance(resp, SyncResponse):
                    next_batch = resp.next_batch
                    if next_batch is not None:
                        self._save_sync_token(next_batch)
                    for room_id in resp.rooms.invite:
                        logger.info(
                            "MatrixChannel: auto-joining component=matrix room_id=%s",
                            room_id,
                        )
                        await self._client.join(room_id)
                    await self._e2ee_maintenance()
                else:
                    logger.warning(
                        "MatrixChannel: full-state sync error component=matrix response_type=%s",
                        type(resp).__name__,
                    )
            except Exception as exc:
                logger.exception(
                    "MatrixChannel: full-state sync exception component=matrix error_type=%s",
                    type(exc).__name__,
                )

        self._mark_channel_ready()

        while True:
            try:
                resp = await self._client.sync(
                    timeout=self.sync_timeout_ms,
                    since=next_batch,
                    full_state=False,
                )
                if isinstance(resp, SyncResponse):
                    next_batch = resp.next_batch
                    if next_batch is not None:
                        self._save_sync_token(next_batch)
                    # Auto-join invited rooms
                    for room_id in resp.rooms.invite:
                        logger.info(
                            "MatrixChannel: auto-joining component=matrix room_id=%s",
                            room_id,
                        )
                        await self._client.join(room_id)
                    # E2EE: full key maintenance (upload, query, claim,
                    # to-device)
                    await self._e2ee_maintenance()
                else:
                    if isinstance(resp, SyncError) and getattr(
                        resp,
                        "status_code",
                        None,
                    ) in {"M_UNKNOWN_TOKEN", "M_MISSING_TOKEN"}:
                        logger.error(
                            "MatrixChannel: sync stopped due to "
                            "invalid/missing access token; please re-login "
                            "(password or fresh token) component=matrix",
                        )
                        if self._client:
                            self._client.access_token = ""
                        self.access_token = ""
                        return
                    logger.warning(
                        "MatrixChannel: sync error component=matrix response_type=%s",
                        type(resp).__name__,
                    )
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.debug("MatrixChannel: sync loop cancelled component=matrix")
                raise
            except Exception as exc:
                logger.exception(
                    "MatrixChannel: sync exception component=matrix error_type=%s",
                    type(exc).__name__,
                )
                await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Channel-level mute, per-room requireMention, strip @mention prefix
    # ------------------------------------------------------------------

    def _is_channel_disabled(
        self,
        sender_id: str,
        room_id: str,
        is_dm: bool,
    ) -> bool:
        """Return True if chat type is muted at channel level."""
        # 逻辑说明：`_is_channel_disabled` 接收 `sender_id`、`room_id`、`is_dm`，按既有分支组合输入并生成结果，并依次复用 `warning`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if is_dm and self.dm_disabled:
            logger.warning(
                "MatrixChannel: dropping DM message (dm_disabled) sender=%s room=%s",
                sender_id,
                room_id,
            )
            return True
        if not is_dm and self.group_disabled:
            logger.warning(
                "MatrixChannel: dropping group message (group_disabled) "
                "sender=%s room=%s",
                sender_id,
                room_id,
            )
            return True
        return False

    def _require_mention(self, room_id: str) -> bool:
        """Per-room config; default is require mention in group rooms."""
        # 逻辑说明：`_require_mention` 接收 `room_id`，校验并规范化输入，并依次复用 `get`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        room_cfg = self.groups.get(room_id) or self.groups.get("*")
        if room_cfg:
            if room_cfg.get("autoReply") is True:
                return False
            if "requireMention" in room_cfg:
                return bool(room_cfg["requireMention"])
        return True  # default: require mention in group rooms

    # pylint: disable=too-many-return-statements
    def _was_mentioned(self, event: Any, text: str) -> bool:
        # 逻辑说明：`_was_mentioned` 接收 `event`、`text`，按既有分支组合输入并生成结果，并依次复用 `get`、`escape`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        if not self._user_id:
            return False
        # 1. Check m.mentions (structured mention from Matrix spec)
        content = event.source.get("content", {})
        mentions = content.get("m.mentions", {})
        if self._user_id in mentions.get("user_ids", []):
            return True
        if mentions.get("room"):
            return True
        # 2. formatted_body: matrix.to mention links (Element HTML format)
        formatted_body = content.get("formatted_body", "")
        if formatted_body and self._user_id:
            escaped_uid = re.escape(self._user_id)
            if re.search(
                rf'href=["\']https://matrix\.to/#/{escaped_uid}["\']',
                formatted_body,
                re.IGNORECASE,
            ):
                return True
            encoded_uid = re.escape(urllib.parse.quote(self._user_id))
            if re.search(
                rf'href=["\']https://matrix\.to/#/{encoded_uid}["\']',
                formatted_body,
                re.IGNORECASE,
            ):
                return True
        # 3. Fallback: match full MXID in plain text
        if self._user_id and re.search(
            re.escape(self._user_id),
            text,
            re.IGNORECASE,
        ):
            return True
        return False

    def _teamharness_self_trigger(
        self, room_id: str, event: Any
    ) -> dict[str, Any] | None:
        # 逻辑说明：`_teamharness_self_trigger` 接收 `room_id`、`event`，按既有分支组合输入并生成结果，并依次复用 `get`、`strip`，返回 `dict[str, Any] | None`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        content = getattr(event, "source", {}).get("content", {})
        if not isinstance(content, dict):
            return None
        trigger = content.get(TEAMHARNESS_TRIGGER_CONTENT_KEY)
        if not isinstance(trigger, dict):
            return None
        if trigger.get("kind") != "self_cross_session":
            return None
        if trigger.get("type") not in TEAMHARNESS_SELF_TRIGGER_TYPES:
            return None
        target_room_id = str(trigger.get("targetRoomId") or "").strip()
        target_session = str(trigger.get("targetSession") or "").strip()
        if target_session.startswith("matrix:"):
            target_session = target_session[len("matrix:") :]
        if target_session.startswith("room:"):
            target_session = target_session[len("room:") :]
        if room_id not in {target_room_id, target_session}:
            return None
        return trigger

    def _strip_mention_prefix(self, text: str, room: Any = None) -> str:
        """Strip leading @mention prefix so slash commands can be detected.

        Handles MXID format (@user:server), room display name, and localpart.
        E.g. ``"@worker:hs.example /new"`` → ``"/new"``
             ``"math 💕: /clear"`` → ``"/clear"``.
        """
        # 逻辑说明：`_strip_mention_prefix` 接收 `text`、`room`，按既有分支组合输入并生成结果，并依次复用 `escape`、`sub`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._user_id:
            return text
        # 1. Strip MXID (@user:server) at start
        escaped = re.escape(self._user_id)
        result = re.sub(rf"^{escaped}\s*:?\s*", "", text, flags=re.IGNORECASE)
        if result != text:
            return result.strip()
        # 2. Strip room display name (e.g. "math 💕") at start — try before
        #    localpart so that "math 💕: /clear" is not partially matched by
        #    the shorter localpart "math".
        if room and self._user_id:
            display_name = self._get_display_name(room, self._user_id)
            logger.debug(
                "strip_mention_prefix: user_id=%s display_name=%r room_users=%d",
                self._user_id,
                display_name,
                len(getattr(room, "users", {})),
            )
            if display_name and display_name != self._user_id:
                result = re.sub(
                    rf"^{re.escape(display_name)}\s*:?\s*",
                    "",
                    text,
                    flags=re.IGNORECASE,
                )
                if result != text:
                    # Clean leftover decoration (e.g. emoji suffix) between
                    # the display name and the actual message content.
                    result = re.sub(r"^[^\w/]+", "", result)
                    return result.strip()
        # 3. Strip localpart (e.g. "math") at start — only if display name
        #    didn't match.
        localpart = self._user_id.split(":")[0].lstrip("@")
        if localpart:
            result = re.sub(
                rf"^@?{re.escape(localpart)}\s*:?\s*",
                "",
                text,
                flags=re.IGNORECASE,
            )
            if result != text:
                # After stripping localpart, there may be leftover decoration
                # from the display name (e.g. emoji suffix "💕: " from
                # "math 💕: /clear").  Strip non-alphanumeric prefix so the
                # slash command is exposed.
                result = re.sub(r"^[^\w/]+", "", result)
                return result.strip()
        return text

    def _control_command_text(self, text: str) -> str | None:
        """Return normalized runtime control command text, if any."""
        # 逻辑说明：`_control_command_text` 接收 `text`，按既有分支组合输入并生成结果，并依次复用 `strip`、`is_control_command`，返回 `str | None`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        registry = getattr(self, "_command_registry", None)
        if registry is None:
            return None

        stripped = (text or "").strip()
        if registry.is_control_command(stripped):
            return stripped

        if stripped.startswith("/"):
            normalized = "/" + stripped.lstrip("/")
            if normalized != stripped and registry.is_control_command(normalized):
                return normalized

        return None

    # ------------------------------------------------------------------
    # Display names & group history buffer (requireMention context)
    # display names from room / client.rooms (§5–§6);
    # per-room history buffer + history_limit; media_parts in buffer when
    # applicable; prefix merged into AgentRequest on mention (§6).
    # ------------------------------------------------------------------

    def _get_display_name(self, room: Any, user_id: str) -> str:
        """Best-effort human-readable name for a Matrix user in *room*.

        Tries the room object passed by nio first, then falls back to
        looking up the room in the nio client's rooms dict (which is
        populated by full_state sync at startup).
        """
        # 逻辑说明：`_get_display_name` 接收 `room`、`user_id`，读取、筛选并规范化现有数据，并依次复用 `user_name`、`debug`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        # 1. Try the room object directly (passed by nio callback)
        try:
            name = room.user_name(user_id)
            if name:
                return name
        except Exception as exc:
            logger.debug(
                "MatrixChannel: user_name failed for %s: %s",
                user_id,
                exc,
            )
        # 2. Fallback: look up from nio client's rooms dict
        if self._client:
            room_id = getattr(room, "room_id", None)
            if room_id:
                client_room = self._client.rooms.get(room_id)
                if client_room and client_room is not room:
                    try:
                        name = client_room.user_name(user_id)
                        if name:
                            logger.debug(
                                "display_name resolved via client.rooms "
                                "fallback: %s -> %r",
                                user_id,
                                name,
                            )
                            return name
                    except Exception as exc:
                        logger.debug(
                            "MatrixChannel: client_room user_name failed for %s: %s",
                            user_id,
                            exc,
                        )
        # 3. Fallback: localpart of MXID (e.g. "@alice:hs" → "alice")
        logger.debug(
            "display_name fallback to localpart for %s "
            "(room.users=%d, client_rooms=%d)",
            user_id,
            len(getattr(room, "users", {})),
            len(self._client.rooms) if self._client else 0,
        )
        return user_id.split(":")[0].lstrip("@") or user_id

    def _record_history(self, room_id: str, entry: HistoryEntry) -> None:
        """Append *entry* to the per-room history buffer (respect limit)."""
        # 逻辑说明：`_record_history` 接收 `room_id`、`entry`，计算目标值并更新持久或共享状态，并依次复用 `setdefault`、`append`，不返回业务结果。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        limit = self.history_limit
        if limit <= 0:
            return
        history = self._room_histories.setdefault(room_id, [])
        history.append(entry)
        while len(history) > limit:
            history.pop(0)

    def _build_history_prefix(self, room_id: str) -> str:
        """Format buffered history entries as a multi-line text block."""
        # 逻辑说明：`_build_history_prefix` 接收 `room_id`，把输入转换为调用方需要的结构，并依次复用 `get`、`append`，返回 `str`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        entries = self._room_histories.get(room_id, [])
        if not entries:
            return ""
        lines: list[str] = []
        for e in entries:
            line = f"{e.sender}: {e.body}"
            if e.message_id:
                line += f" [id:{e.message_id}]"
            lines.append(line)
        return "\n".join(lines)

    def _apply_history_to_parts(
        self,
        room_id: str,
        content_parts: list[Any],
    ) -> list[Any]:
        """Prepend accumulated history context to *content_parts*.

        If the first part is text, the history block is merged into it;
        otherwise a new text part is prepended.  Any media parts stored
        in history entries (e.g. downloaded images) are inserted between
        the history text block and the current message parts so that
        vision models can see them.

        Returns a (possibly new) list — the original is not mutated.
        """
        # 逻辑说明：`_apply_history_to_parts` 接收 `room_id`、`content_parts`，按既有分支组合输入并生成结果，并依次复用 `_build_history_prefix`、`get`，返回 `list[Any]`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self.history_limit <= 0:
            return content_parts
        history_text = self._build_history_prefix(room_id)
        if not history_text:
            return content_parts

        # Collect media content parts carried by history entries
        history_media: list[Any] = []
        for entry in self._room_histories.get(room_id, []):
            if entry.media_parts:
                history_media.extend(entry.media_parts)

        # Merge into the leading text part when possible
        first = content_parts[0] if content_parts else None
        if first and getattr(first, "type", None) == ContentType.TEXT:
            current_text = first.text or ""
            combined = (
                f"{HISTORY_CONTEXT_MARKER}\n{history_text}\n\n"
                f"{CURRENT_MESSAGE_MARKER}\n{current_text}"
            )
            return (
                [TextContent(type=ContentType.TEXT, text=combined)]
                + history_media
                + content_parts[1:]
            )
        # No leading text part (e.g. pure media) — prepend a dedicated block
        prefix_part = TextContent(
            type=ContentType.TEXT,
            text=(
                f"{HISTORY_CONTEXT_MARKER}\n{history_text}\n\n{CURRENT_MESSAGE_MARKER}"
            ),
        )
        return [prefix_part] + history_media + content_parts

    def _clear_history(self, room_id: str) -> None:
        """Drop the buffered history for *room_id*."""
        # 逻辑说明：`_clear_history` 接收 `room_id`，按既有分支组合输入并生成结果，并依次复用 `pop`，不返回业务结果。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        self._room_histories.pop(room_id, None)

    async def _record_media_history(
        self,
        room: Any,
        event: Any,
        sender_id: str,
        room_id: str,
    ) -> None:
        """Record a non-mentioned media message as a history entry.

        Produces a typed text description (e.g. ``[sent an image: photo.jpg]``)
        and, for images when vision is enabled, downloads the actual file so it
        can be included as an image content part later.
        """
        # 逻辑说明：`_record_media_history` 接收 `room`、`event`、`sender_id`、`room_id`，计算目标值并更新持久或共享状态，并依次复用 `lstrip`、`_download_mxc`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        body = event.body or ""
        media_parts: list[Any] = []

        if isinstance(event, RoomMessageImage):
            body_desc = f"[sent an image: {body}]" if body else "[sent an image]"
            if self.vision_enabled:
                mxc_url: str = getattr(event, "url", "") or ""
                if mxc_url:
                    eid = event.event_id[:8].lstrip("$")
                    filename = body or f"matrix_media_{eid}"
                    filename = f"{eid}_{filename}"
                    local_path = await self._download_mxc(mxc_url, filename)
                    if local_path:
                        media_parts.append(
                            ImageContent(
                                type=ContentType.IMAGE,
                                image_url=Path(local_path).as_uri(),
                            ),
                        )
        elif isinstance(event, RoomMessageFile):
            body_desc = f"[sent a file: {body}]" if body else "[sent a file]"
            mxc_url = getattr(event, "url", "") or ""
            if mxc_url:
                eid = event.event_id[:8].lstrip("$")
                filename = body or f"matrix_media_{eid}"
                filename = f"{eid}_{filename}"
                local_path = await self._download_mxc(mxc_url, filename)
                if local_path:
                    media_parts.append(
                        FileContent(
                            type=ContentType.FILE,
                            file_url=Path(local_path).as_uri(),
                            filename=body or filename,
                        ),
                    )
        elif isinstance(event, RoomMessageAudio):
            body_desc = f"[sent audio: {body}]" if body else "[sent audio]"
        elif isinstance(event, RoomMessageVideo):
            body_desc = f"[sent a video: {body}]" if body else "[sent a video]"
        else:
            body_desc = body or "[media]"

        self._record_history(
            room_id,
            HistoryEntry(
                sender=self._get_display_name(room, sender_id),
                body=body_desc,
                timestamp=getattr(event, "server_timestamp", None),
                message_id=event.event_id,
                media_parts=media_parts or None,
            ),
        )

    # ------------------------------------------------------------------
    # Media — local dirs, mxc download, E2EE decrypt, inbound handlers
    # local media dir; mxc fetch; AES decrypt for
    # encrypted attachments; cleartext + RoomEncrypted* inbound paths (§7).
    # ------------------------------------------------------------------

    def _media_dir(self) -> Path:
        """Return (and create) the local media storage directory."""
        # 逻辑说明：`_media_dir` 不接收参数，返回当前频道 workspace 下的 media 目录；没有专属 workspace 时退回全局工作目录。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._workspace_dir:
            return self._workspace_dir / "media"
        return WORKING_DIR / "media"

    def _mxc_to_http(self, mxc_url: str) -> str:
        """Convert an mxc:// URL to an HTTP download URL.

        Returns the original URL unchanged if it is not an mxc:// URL or if
        the format is invalid.
        """
        # 逻辑说明：`_mxc_to_http` 接收 `mxc_url`，按既有分支组合输入并生成结果，并依次复用 `startswith`、`split`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not mxc_url:
            return mxc_url
        if not mxc_url.startswith("mxc://"):
            return mxc_url
        rest = mxc_url[6:]  # strip "mxc://"
        if "/" not in rest:
            return mxc_url
        server, media_id = rest.split("/", 1)
        return f"{self.homeserver}/_matrix/media/v3/download/{server}/{media_id}"

    async def _download_mxc(
        self,
        mxc_url: str,
        filename: str,
    ) -> Optional[str]:
        """Download mxc:// to a local file; return path or None."""
        # 逻辑说明：`_download_mxc` 接收 `mxc_url`、`filename`，构造协议数据并完成外部传输，并依次复用 `startswith`、`split`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not mxc_url.startswith("mxc://"):
            return None
        try:
            rest = mxc_url[6:]  # strip "mxc://"
            server, media_id = rest.split("/", 1)
            url = f"{self.homeserver}/_matrix/media/v3/download/{server}/{media_id}"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            if not self._http_client:
                logger.warning("MatrixChannel: HTTP client not initialized")
                return None
            resp = await self._http_client.get(url, headers=headers)
            resp.raise_for_status()
            dest = self._media_dir() / filename
            dest.write_bytes(resp.content)
            logger.debug("MatrixChannel: downloaded %s → %s", mxc_url, dest)
            return str(dest)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to download %s: %s",
                mxc_url,
                exc,
            )
            return None

    def _e2ee_store_path(self) -> Path:
        """Return the directory for persisting Olm/Megolm crypto state."""
        return WORKING_DIR / "matrix_crypto_store"

    async def _download_encrypted_mxc(
        self,
        mxc_url: str,
        filename: str,
        key: dict,
        hashes: dict,
        iv: str,
    ) -> Optional[str]:
        """Download an encrypted mxc:// URI, decrypt it, and save locally."""
        # 逻辑说明：`_download_encrypted_mxc` 接收 `mxc_url`、`filename`、`key`、`hashes`，构造协议数据并完成外部传输，并依次复用 `startswith`、`split`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not mxc_url.startswith("mxc://") or not self._client:
            return None
        try:
            rest = mxc_url[6:]
            server, media_id = rest.split("/", 1)
            url = f"{self.homeserver}/_matrix/media/v3/download/{server}/{media_id}"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            if not self._http_client:
                logger.warning("MatrixChannel: HTTP client not initialized")
                return None
            resp = await self._http_client.get(url, headers=headers)
            resp.raise_for_status()

            from nio.crypto.attachments import decrypt_attachment

            jwk_key = key.get("k", "")
            sha256_hash = hashes.get("sha256", "")
            plaintext = decrypt_attachment(
                resp.content,
                jwk_key,
                sha256_hash,
                iv,
            )

            dest = self._media_dir() / filename
            dest.write_bytes(plaintext)
            logger.debug(
                "MatrixChannel: downloaded+decrypted %s → %s",
                mxc_url,
                dest,
            )
            return str(dest)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to download encrypted %s: %s",
                mxc_url,
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # Incoming E2EE — undecryptable log + decrypted media (§7)
    # MegolmEvent warning; RoomEncrypted* same allow/
    # history/vision path as cleartext media when nio decrypts (optional E2EE).
    # ------------------------------------------------------------------

    async def _on_megolm_event(
        self,
        room: MatrixRoom,
        event: MegolmEvent,
    ) -> None:
        """Handle undecryptable encrypted events (missing session key)."""
        # 逻辑说明：`_on_megolm_event` 接收 `room`、`event`，按既有分支组合输入并生成结果，并依次复用 `warning`，不返回业务结果。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        logger.warning(
            "MatrixChannel: could not decrypt event %s in %s (session_id=%s)",
            event.event_id,
            room.room_id,
            getattr(event, "session_id", "?"),
        )

    # pylint: disable=too-many-branches,too-many-statements
    async def _on_room_encrypted_media_event(
        self,
        room: MatrixRoom,
        event: Any,
    ) -> None:
        """Handle decrypted encrypted media (RoomEncryptedImage, etc.).

        Delivered by matrix-nio after Megolm decrypt. File bytes are still
        AES-encrypted; download + decrypt with key/iv/hashes from the event.
        """
        # 逻辑说明：`_on_room_encrypted_media_event` 接收 `room`、`event`，按既有分支组合输入并生成结果，并依次复用 `_is_dm_room`、`_is_channel_disabled`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        if event.sender == self._user_id:
            return

        sender_id = event.sender
        room_id = room.room_id
        # Use Matrix API for reliable DM detection (room.users unreliable
        # after token restore)
        is_dm = await self._is_dm_room(room_id, sender_id, room)

        if self._is_channel_disabled(sender_id, room_id, is_dm):
            return

        if not is_dm:
            if self._require_mention(room_id) and not self._was_mentioned(
                event,
                "",
            ):
                # Record as history (text description only)
                body = event.body or ""
                if isinstance(event, RoomEncryptedImage):
                    desc = (
                        f"[sent an encrypted image: {body}]"
                        if body
                        else "[sent an encrypted image]"
                    )
                elif isinstance(event, RoomEncryptedAudio):
                    desc = (
                        f"[sent encrypted audio: {body}]"
                        if body
                        else "[sent encrypted audio]"
                    )
                elif isinstance(event, RoomEncryptedVideo):
                    desc = (
                        f"[sent an encrypted video: {body}]"
                        if body
                        else "[sent an encrypted video]"
                    )
                else:
                    desc = (
                        f"[sent an encrypted file: {body}]"
                        if body
                        else "[sent an encrypted file]"
                    )
                self._record_history(
                    room_id,
                    HistoryEntry(
                        sender=self._get_display_name(room, sender_id),
                        body=desc,
                        timestamp=getattr(event, "server_timestamp", None),
                        message_id=event.event_id,
                    ),
                )
                return

        await self._send_read_receipt(room_id, event.event_id)
        await self._send_typing(room_id, True)

        body = event.body or ""
        mxc_url = getattr(event, "url", "") or ""
        key = getattr(event, "key", {}) or {}
        hashes = getattr(event, "hashes", {}) or {}
        iv = getattr(event, "iv", "") or ""

        content_parts: list[Any] = []

        if mxc_url and key and iv:
            eid = event.event_id[:8].lstrip("$")
            filename = body or f"matrix_media_{eid}"
            filename = f"{eid}_{filename}"
            local_path = await self._download_encrypted_mxc(
                mxc_url,
                filename,
                key,
                hashes,
                iv,
            )
            if local_path:
                file_uri = Path(local_path).as_uri()
                if isinstance(event, RoomEncryptedImage):
                    if self.vision_enabled:
                        content_parts.append(
                            ImageContent(
                                type=ContentType.IMAGE,
                                image_url=file_uri,
                            ),
                        )
                    else:
                        _no_vis = (
                            "[User sent an image (current model does not "
                            f"support image input): {body or filename}]"
                        )
                        content_parts.append(
                            TextContent(
                                type=ContentType.TEXT,
                                text=_no_vis,
                            ),
                        )
                elif isinstance(event, RoomEncryptedAudio):
                    content_parts.append(
                        AudioContent(
                            type=ContentType.AUDIO,
                            data=file_uri,
                        ),
                    )
                elif isinstance(event, RoomEncryptedVideo):
                    content_parts.append(
                        VideoContent(
                            type=ContentType.VIDEO,
                            video_url=file_uri,
                        ),
                    )
                else:
                    content_parts.append(
                        FileContent(
                            type=ContentType.FILE,
                            file_url=file_uri,
                            filename=body or filename,
                        ),
                    )
            else:
                content_parts.append(
                    TextContent(
                        type=ContentType.TEXT,
                        text=f"[Encrypted media unavailable: {body}]",
                    ),
                )

        if not content_parts:
            return

        if not is_dm:
            # Prefix sender identity so the LLM can distinguish participants
            sender_name = self._get_display_name(room, sender_id)
            first = content_parts[0] if content_parts else None
            if first and getattr(first, "type", None) == ContentType.TEXT:
                content_parts[0] = TextContent(
                    type=ContentType.TEXT,
                    text=f"{sender_name}: {first.text}",
                )
            else:
                content_parts.insert(
                    0,
                    TextContent(
                        type=ContentType.TEXT,
                        text=f"{sender_name}:",
                    ),
                )
            content_parts = self._apply_history_to_parts(
                room_id,
                content_parts,
            )

        worker_name = (self._user_id or "").split(":")[0].lstrip("@")
        payload = {
            "channel_id": CHANNEL_KEY,
            "sender_id": sender_id,
            "content_parts": content_parts,
            "acl_sender_id": sender_id,
            "meta": {
                "room_id": room_id,
                "is_dm": is_dm,
                "is_group": not is_dm,
                "worker_name": worker_name,
                "event_id": event.event_id,
                "sender_id": sender_id,
                _MATRIX_CONTROL_EPOCH_KEY: self._room_control_epoch(room_id),
            },
        }

        if self._enqueue:
            self._enqueue(payload)
            if not is_dm:
                self._clear_history(room_id)

    # ------------------------------------------------------------------
    # Media upload (local file → mxc://)
    # upload to homeserver media repo; shared by
    # send_media outbound path (same role as worker _upload_file).
    # ------------------------------------------------------------------

    async def _upload_file(self, file_ref: str) -> Optional[str]:
        """Upload a local file to Matrix; return mxc:// URI or None."""
        # 逻辑说明：`_upload_file` 接收 `file_ref`，构造协议数据并完成外部传输，并依次复用 `Path`、`file_url_to_local_path`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return None
        try:
            # file_ref may be a file:// URI or a plain path
            path = Path(file_url_to_local_path(file_ref) or file_ref)
            if not path.exists():
                logger.warning(
                    "MatrixChannel: upload source not found: %s",
                    file_ref,
                )
                return None
            mime_type, _ = mimetypes.guess_type(str(path))
            mime_type = mime_type or "application/octet-stream"
            data = path.read_bytes()
            resp, _ = await self._client.upload(
                io.BytesIO(data),
                content_type=mime_type,
                filename=path.name,
                filesize=len(data),
            )
            if isinstance(resp, UploadResponse):
                logger.debug(
                    "MatrixChannel: uploaded %s → %s",
                    path.name,
                    resp.content_uri,
                )
                return resp.content_uri
            logger.warning("MatrixChannel: upload failed: %s", resp)
            return None
        except Exception as exc:
            logger.warning(
                "MatrixChannel: upload error for %s: %s",
                file_ref,
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # DM room detection (joined_members API + short-lived cache)
    # reliable DM vs group after token restore (§8);
    # feeds allowlist / requireMention / history behavior.
    # ------------------------------------------------------------------

    def _is_dm_room_fallback(
        self,
        room: Optional[MatrixRoom],
        sender_id: str,
    ) -> bool:
        """Best-effort DM check when joined_members API is unavailable."""
        # 逻辑说明：`_is_dm_room_fallback` 接收 `room`、`sender_id`，按既有分支组合输入并生成结果，并依次复用 `_looks_like_teamharness_task_room`、`keys`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        room_id = str(getattr(room, "room_id", "") or "")
        if self._looks_like_teamharness_task_room(room_id, room):
            return False
        if not room or not self._user_id:
            return False
        try:
            users = list(getattr(room, "users", {}).keys())
            if users:
                return len(users) == 2 and self._user_id in users and sender_id in users
            member_count = int(getattr(room, "member_count", 0) or 0)
            return member_count == 2
        except Exception:
            return False

    def _room_has_teamharness_task_marker(
        self,
        room: Optional[MatrixRoom],
    ) -> bool:
        """Return True when room state looks like a TeamHarness task room."""
        # 逻辑说明：`_room_has_teamharness_task_marker` 接收 `room`，计算目标值并更新持久或共享状态，并依次复用 `callable`、`strip`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not room:
            return False
        for attr in ("topic", "name", "display_name"):
            value = getattr(room, attr, "")
            if callable(value):
                continue
            text = str(value or "").strip()
            if text.startswith("Task room for ") or "Task room for " in text:
                return True
        return False

    def _is_known_teamharness_task_room(self, room_id: str) -> bool:
        """Check local TeamHarness task metadata for an assignment room id."""
        # 逻辑说明：`_is_known_teamharness_task_room` 接收 `room_id`，按既有分支组合输入并生成结果，并依次复用 `time`、`get`，返回 `bool`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not room_id:
            return False
        now = int(time.time() * 1000)
        cache = getattr(self, "_teamharness_task_room_cache", None)
        if cache is None:
            cache = {}
            self._teamharness_task_room_cache = cache
        cached = cache.get(room_id)
        if cached and (now - cached["ts"]) < TASK_ROOM_CACHE_TTL_MS:
            return bool(cached["is_task_room"])

        is_task_room = False
        root = getattr(self, "_workspace_dir", None)
        workspace_dir = Path(root).expanduser() if root else Path(WORKING_DIR)
        tasks_dir = workspace_dir / "shared" / "tasks"
        if tasks_dir.is_dir():
            for meta_path in tasks_dir.glob("*/meta.json"):
                try:
                    task = json.loads(meta_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    logger.debug(
                        "MatrixChannel: failed to read task room metadata %s: %s",
                        meta_path,
                        exc,
                    )
                    continue
                task_room_id = str(
                    task.get("room_id") or task.get("roomId") or "",
                ).strip()
                if task_room_id == room_id:
                    is_task_room = True
                    break

        cache[room_id] = {"is_task_room": is_task_room, "ts": now}
        return is_task_room

    def _looks_like_teamharness_task_room(
        self,
        room_id: str,
        room: Optional[MatrixRoom] = None,
    ) -> bool:
        # 逻辑说明：`_looks_like_teamharness_task_room` 接收 `room_id`、`room`，按既有分支组合输入并生成结果，并依次复用 `_room_has_teamharness_task_marker`、`_is_known_teamharness_task_room`，返回 `bool`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._room_has_teamharness_task_marker(room):
            return True
        return self._is_known_teamharness_task_room(room_id)

    async def _is_dm_room(
        self,
        room_id: str,
        sender_id: str,
        room: Optional[MatrixRoom] = None,
    ) -> bool:
        """Check if a room is a DM (direct message) between self and sender.

        Uses Matrix API to get actual joined members, because nio's room.users
        can be unreliable after token restore.

        Args:
            room_id: The Matrix room ID
            sender_id: The sender's user ID

        Returns:
            True if the room has exactly 2 members (self and sender)
        """
        # 逻辑说明：`_is_dm_room` 接收 `room_id`、`sender_id`、`room`，按既有分支组合输入并生成结果，并依次复用 `_looks_like_teamharness_task_room`、`debug`，返回 `bool`。
        # 执行过程中包含异步等待/流式产出、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client or not self._user_id:
            return False

        if self._looks_like_teamharness_task_room(room_id, room):
            logger.debug(
                "MatrixChannel: room=%s is a TeamHarness task room; "
                "treating as group semantics",
                room_id,
            )
            return False

        now = int(time.time() * 1000)

        # Check cache
        cached = self._dm_room_cache.get(room_id)
        if cached and (now - cached["ts"]) < DM_CACHE_TTL_MS:
            members = cached["members"]
            is_dm = (
                len(members) == 2 and self._user_id in members and sender_id in members
            )
            logger.debug(
                "MatrixChannel: DM check (cached) room=%s members=%d is_dm=%s",
                room_id,
                len(members),
                is_dm,
            )
            return is_dm

        # Fetch from Matrix API
        try:
            resp = await self._client.joined_members(room_id)
            if isinstance(resp, JoinedMembersResponse):
                members = [m.user_id for m in resp.members]
                # Update cache
                self._dm_room_cache[room_id] = {"members": members, "ts": now}

                is_dm = (
                    len(members) == 2
                    and self._user_id in members
                    and sender_id in members
                )
                logger.debug(
                    "MatrixChannel: DM check (API) room=%s members=%d "
                    "is_dm=%s members=%s",
                    room_id,
                    len(members),
                    is_dm,
                    members,
                )
                return is_dm
            else:
                logger.warning(
                    "MatrixChannel: joined_members failed for %s: %s",
                    room_id,
                    resp,
                )
                fallback = self._is_dm_room_fallback(room, sender_id)
                logger.warning(
                    "MatrixChannel: joined_members fallback for %s -> is_dm=%s",
                    room_id,
                    fallback,
                )
                return fallback
        except Exception as exc:
            fallback = self._is_dm_room_fallback(room, sender_id)
            logger.warning(
                "MatrixChannel: joined_members error for %s: %s; fallback is_dm=%s",
                room_id,
                exc,
                fallback,
            )
            return fallback

    # ------------------------------------------------------------------
    # Incoming message handling — text
    # text receive; allowlist + per-room rules +
    # mention gating; history buffer when no mention; enqueue AgentRequest
    # (§9).
    # ------------------------------------------------------------------

    async def _on_room_event(
        self,
        room: MatrixRoom,
        event: RoomMessageText,
    ) -> None:
        # 逻辑说明：`_on_room_event` 接收 `room`、`event`，按既有分支组合输入并生成结果，并依次复用 `_teamharness_self_trigger`、`_is_dm_room`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        room_id = room.room_id
        sender_id = event.sender
        teamharness_self_trigger = None
        if sender_id == self._user_id:
            teamharness_self_trigger = self._teamharness_self_trigger(room_id, event)

        # Skip own messages early
        if sender_id == self._user_id and teamharness_self_trigger is None:
            return

        text = event.body or ""

        # Use Matrix API to reliably detect DM rooms
        # (nio's room.users is unreliable after token restore)
        is_dm = await self._is_dm_room(room_id, sender_id, room)

        logger.info(
            "_on_room_event: sender=%s room=%s body=%r is_dm=%s",
            event.sender,
            room_id,
            (event.body or "")[:80],
            is_dm,
        )

        if self._is_channel_disabled(sender_id, room_id, is_dm):
            return

        if teamharness_self_trigger is None and _is_teamharness_tool_display(text):
            logger.info(
                "MatrixChannel: skipping TeamHarness tool display event "
                "(room=%s sender=%s event_id=%s)",
                room_id,
                sender_id,
                event.event_id,
            )
            return

        is_thread_event = self._is_thread_event(event)

        # Readiness probe: reply directly without enqueuing to the agent
        readiness_reply = self._targeted_readiness_probe_reply(event, text)
        if readiness_reply:
            await self._send_plain_text(room_id, readiness_reply)
            return

        # Mention check for group rooms
        if not is_dm:
            if (
                teamharness_self_trigger is None
                and self._require_mention(room_id)
                and not self._was_mentioned(
                    event,
                    text,
                )
            ):
                # Thread events that don't mention us are silently ignored
                if is_thread_event:
                    return
                logger.info(
                    "MatrixChannel: group text not mentioned, cached to "
                    "history (room=%s sender=%s event_id=%s)",
                    room_id,
                    sender_id,
                    event.event_id,
                )
                self._record_history(
                    room_id,
                    HistoryEntry(
                        sender=self._get_display_name(room, sender_id),
                        body=text,
                        timestamp=getattr(event, "server_timestamp", None),
                        message_id=event.event_id,
                    ),
                )
                return

        # Mark as read + start typing immediately so the sender sees feedback
        await self._send_read_receipt(room_id, event.event_id)
        await self._send_typing(room_id, True)

        # Strip leading @mention so slash commands and NO_REPLY are detected
        # regardless of room type (group or DM).
        command_text = text
        stripped = self._strip_mention_prefix(text, room)

        # NO_REPLY protocol: the sender explicitly signals "nothing to say".
        # Drop it silently to prevent infinite ping-pong between agents.
        if stripped.strip() == "NO_REPLY":
            logger.info(
                "MatrixChannel: received NO_REPLY from %s in %s, ignoring",
                sender_id,
                room_id,
            )
            await self._send_typing(room_id, False)
            return

        control_text = self._control_command_text(stripped)
        cmd = ""
        if control_text is not None:
            command_text = control_text
        elif stripped.startswith("/"):
            command_text = "/" + stripped.lstrip("/")
            cmd = command_text.lstrip("/").split()[0]
        if control_text is None and cmd in _SLASH_COMMANDS:
            # Apply alias (e.g. /reset -> /clear)
            if cmd in _SLASH_ALIASES:
                canonical = _SLASH_ALIASES[cmd]
                command_text = command_text.replace(
                    f"/{cmd}",
                    f"/{canonical}",
                    1,
                )
            if stripped != text:
                logger.info(
                    "Stripped mention prefix for slash command: %r -> %r",
                    text,
                    command_text,
                )
        if command_text.strip().casefold().split(None, 1)[0] == "/stop":
            self._advance_room_control_epoch(room_id)

        # Build content parts, prepending accumulated history for group rooms.
        # Skip history prepend for slash commands — QwenPaw's command parser
        # requires the message to start with "/" to recognise it.
        content_parts: list[Any] = [
            TextContent(type=ContentType.TEXT, text=command_text),
        ]
        is_slash_cmd = command_text.startswith("/")
        if not is_dm and not is_slash_cmd:
            # Prefix sender identity so the LLM can distinguish participants
            sender_name = self._get_display_name(room, sender_id)
            content_parts[0] = TextContent(
                type=ContentType.TEXT,
                text=f"{sender_name}: {command_text}",
            )
            if not is_thread_event:
                content_parts = self._apply_history_to_parts(
                    room_id,
                    content_parts,
                )

        worker_name = (self._user_id or "").split(":")[0].lstrip("@")
        payload = {
            "channel_id": CHANNEL_KEY,
            "sender_id": sender_id,
            "content_parts": content_parts,
            "acl_sender_id": sender_id,
            "meta": {
                "room_id": room_id,
                "is_dm": is_dm,
                "is_group": not is_dm,
                "worker_name": worker_name,
                "event_id": event.event_id,
                "thread_root_event_id": event.event_id,
                "sender_id": sender_id,
                _MATRIX_CONTROL_EPOCH_KEY: self._room_control_epoch(room_id),
            },
        }
        if teamharness_self_trigger is not None:
            payload["meta"].update(
                {
                    "teamharness_trigger": True,
                    "teamharness_trigger_type": str(
                        teamharness_self_trigger.get("type") or ""
                    ),
                    "teamharness_trigger_kind": str(
                        teamharness_self_trigger.get("kind") or ""
                    ),
                },
            )

        if self._enqueue:
            self._enqueue(payload)
            if not is_dm and not is_thread_event:
                self._clear_history(room_id)

    # ------------------------------------------------------------------
    # Incoming message handling — media (image / file / audio / video)
    # media receive + mxc download; vision_enabled
    # gates image→model vs text downgrade; same allow/history path as text
    # (§9–§11).
    # ------------------------------------------------------------------

    # pylint: disable=too-many-branches,too-many-statements
    async def _on_room_media_event(self, room: MatrixRoom, event: Any) -> None:
        """Handle incoming media messages (image, file, audio, video)."""
        # 逻辑说明：`_on_room_media_event` 接收 `room`、`event`，按既有分支组合输入并生成结果，并依次复用 `_is_dm_room`、`_is_channel_disabled`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        if event.sender == self._user_id:
            return

        sender_id = event.sender
        room_id = room.room_id
        # Use Matrix API for reliable DM detection (room.users unreliable
        # after token restore)
        is_dm = await self._is_dm_room(room_id, sender_id, room)

        if self._is_channel_disabled(sender_id, room_id, is_dm):
            return

        is_thread_event = self._is_thread_event(event)

        # For group rooms, apply the same mention policy as text messages.
        if not is_dm:
            if self._require_mention(room_id) and not self._was_mentioned(
                event,
                "",
            ):
                if is_thread_event:
                    return
                logger.info(
                    "MatrixChannel: group media not mentioned, cached to "
                    "history (room=%s sender=%s event_id=%s)",
                    room_id,
                    sender_id,
                    event.event_id,
                )
                await self._record_media_history(
                    room,
                    event,
                    sender_id,
                    room_id,
                )
                return

        await self._send_read_receipt(room_id, event.event_id)
        await self._send_typing(room_id, True)

        mxc_url: str = getattr(event, "url", "") or ""
        body: str = event.body or ""  # filename or caption

        content_parts: list[Any] = []

        if mxc_url:
            # Use the body as filename, fall back to a safe default.
            # Strip leading '$' from Matrix event IDs to avoid URI encoding
            # issues ($→%24 breaks agentscope's image extension check).
            eid = event.event_id[:8].lstrip("$")
            filename = body or f"matrix_media_{eid}"
            filename = f"{eid}_{filename}"
            local_path = await self._download_mxc(mxc_url, filename)
            if local_path:
                file_uri = Path(local_path).as_uri()
                if isinstance(event, RoomMessageImage):
                    if self.vision_enabled:
                        content_parts.append(
                            ImageContent(
                                type=ContentType.IMAGE,
                                image_url=file_uri,
                            ),
                        )
                    else:
                        # No vision: downgrade image to text
                        _no_vis = (
                            "[User sent an image (current model does not "
                            f"support image input): {body or filename}]"
                        )
                        content_parts.append(
                            TextContent(
                                type=ContentType.TEXT,
                                text=_no_vis,
                            ),
                        )
                elif isinstance(event, RoomMessageAudio):
                    content_parts.append(
                        AudioContent(
                            type=ContentType.AUDIO,
                            data=file_uri,
                        ),
                    )
                elif isinstance(event, RoomMessageVideo):
                    content_parts.append(
                        VideoContent(
                            type=ContentType.VIDEO,
                            video_url=file_uri,
                        ),
                    )
                else:  # RoomMessageFile
                    content_parts.append(
                        FileContent(
                            type=ContentType.FILE,
                            file_url=file_uri,
                            filename=body or filename,
                        ),
                    )
            else:
                content_parts.append(
                    TextContent(
                        type=ContentType.TEXT,
                        text=f"[Media unavailable: {body}]",
                    ),
                )

        if not content_parts:
            return

        # Prepend accumulated history for group rooms
        if not is_dm:
            # Prefix sender identity so the LLM can distinguish participants
            sender_name = self._get_display_name(room, sender_id)
            first = content_parts[0] if content_parts else None
            if first and getattr(first, "type", None) == ContentType.TEXT:
                content_parts[0] = TextContent(
                    type=ContentType.TEXT,
                    text=f"{sender_name}: {first.text}",
                )
            else:
                content_parts.insert(
                    0,
                    TextContent(
                        type=ContentType.TEXT,
                        text=f"{sender_name}:",
                    ),
                )
            if not is_thread_event:
                content_parts = self._apply_history_to_parts(
                    room_id,
                    content_parts,
                )

        worker_name = (self._user_id or "").split(":")[0].lstrip("@")
        payload = {
            "channel_id": CHANNEL_KEY,
            "sender_id": sender_id,
            "content_parts": content_parts,
            "acl_sender_id": sender_id,
            "meta": {
                "room_id": room_id,
                "is_dm": is_dm,
                "is_group": not is_dm,
                "worker_name": worker_name,
                "event_id": event.event_id,
                "thread_root_event_id": event.event_id,
                "sender_id": sender_id,
                _MATRIX_CONTROL_EPOCH_KEY: self._room_control_epoch(room_id),
            },
        }

        if self._enqueue:
            self._enqueue(payload)
            if not is_dm and not is_thread_event:
                self._clear_history(room_id)

    # ------------------------------------------------------------------
    # Read receipt & typing indicator
    # read markers on handled messages; typing on/off
    # + renewal until cap (optional UX; §10).
    # ------------------------------------------------------------------

    async def _send_read_receipt(self, room_id: str, event_id: str) -> None:
        """Mark a message as read (sends both read receipt and read marker)."""
        # 逻辑说明：`_send_read_receipt` 接收 `room_id`、`event_id`，读取、筛选并规范化现有数据，并依次复用 `room_read_markers`、`debug`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client or not event_id:
            return
        try:
            await self._client.room_read_markers(
                room_id,
                fully_read_event=event_id,
                read_event=event_id,
            )
        except Exception as exc:
            logger.debug(
                "MatrixChannel: read receipt failed for %s: %s",
                event_id,
                exc,
            )

    async def _send_typing(
        self,
        room_id: str,
        typing: bool,
        timeout: int = TYPING_SERVER_TIMEOUT_MS,
    ) -> None:
        """Set typing indicator on/off for a room.

        When turning on, starts a background renewal task that re-sends the
        typing indicator periodically (see ``TYPING_RENEWAL_INTERVAL_S``)
        before the server timeout, up to ``TYPING_MAX_DURATION_S``.
        When turning off, cancels the renewal task.
        """
        # 逻辑说明：`_send_typing` 接收 `room_id`、`typing`、`timeout`，构造协议数据并完成外部传输，并依次复用 `pop`、`done`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return
        # Cancel any existing renewal task for this room
        existing = self._typing_tasks.pop(room_id, None)
        if existing and not existing.done():
            existing.cancel()
        try:
            await self._client.room_typing(
                room_id,
                typing_state=typing,
                timeout=timeout,
            )
        except Exception as exc:
            logger.debug(
                "MatrixChannel: typing indicator failed for %s: %s",
                room_id,
                exc,
            )
        # Start renewal loop if turning on
        if typing:
            self._typing_tasks[room_id] = asyncio.create_task(
                self._typing_renewal_loop(room_id, timeout),
            )

    async def _typing_renewal_loop(
        self,
        room_id: str,
        timeout: int = TYPING_SERVER_TIMEOUT_MS,
    ) -> None:
        """Re-send typing=true until cap or cancellation."""
        # 逻辑说明：`_typing_renewal_loop` 接收 `room_id`、`timeout`，按既有分支组合输入并生成结果，并依次复用 `sleep`、`room_typing`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        elapsed = 0
        try:
            while elapsed < TYPING_MAX_DURATION_S:
                await asyncio.sleep(TYPING_RENEWAL_INTERVAL_S)
                elapsed += TYPING_RENEWAL_INTERVAL_S
                if not self._client:
                    break
                await self._client.room_typing(
                    room_id,
                    typing_state=True,
                    timeout=timeout,
                )
        except asyncio.CancelledError:
            logger.debug(
                "MatrixChannel: typing renewal cancelled for %s",
                room_id,
            )
            raise
        except Exception as exc:
            logger.debug(
                "MatrixChannel: typing renewal failed for %s: %s",
                room_id,
                exc,
            )
        finally:
            # If we hit the cap, explicitly stop typing
            if elapsed >= TYPING_MAX_DURATION_S and self._client:
                try:
                    await self._client.room_typing(room_id, typing_state=False)
                except Exception as exc:
                    logger.debug(
                        "MatrixChannel: typing stop after cap failed for %s: %s",
                        room_id,
                        exc,
                    )
            self._typing_tasks.pop(room_id, None)

    # ------------------------------------------------------------------
    # build_agent_request_from_native (BaseChannel protocol)
    # native content_parts → QwenPaw Content; same
    # vision_enabled guard as inbound media for image parts (§11).
    # ------------------------------------------------------------------

    # pylint: disable=too-many-return-statements
    def _build_content_part(self, p: dict[str, Any]) -> Any:
        """Convert a native content-part dict to a QwenPaw Content object."""
        # 逻辑说明：`_build_content_part` 接收 `p`，把输入转换为调用方需要的结构，并依次复用 `get`、`TextContent`，返回 `Any`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        t = p.get("type")
        if t == "text" and p.get("text"):
            return TextContent(type=ContentType.TEXT, text=p["text"])
        if t == "image" and p.get("image_url"):
            if not self.vision_enabled:
                # Downgrade silently; _on_room_media_event should have already
                # converted this, but guard here for any code path that builds
                # content_parts directly.
                return TextContent(
                    type=ContentType.TEXT,
                    text=(
                        "[Image omitted: current model does not support image input]"
                    ),
                )
            return ImageContent(
                type=ContentType.IMAGE,
                image_url=p["image_url"],
            )
        if t == "file":
            return FileContent(
                type=ContentType.FILE,
                file_url=p.get("file_url", ""),
            )
        if t == "audio" and p.get("data"):
            return AudioContent(type=ContentType.AUDIO, data=p["data"])
        if t == "video" and p.get("video_url"):
            return VideoContent(
                type=ContentType.VIDEO,
                video_url=p["video_url"],
            )
        return None

    def build_agent_request_from_native(self, native_payload: Any) -> Any:
        # 逻辑说明：`build_agent_request_from_native` 接收 `native_payload`，把输入转换为调用方需要的结构，并依次复用 `get`、`TextContent`，返回 `Any`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        parts = native_payload.get("content_parts", [])
        meta = native_payload.get("meta", {})
        sender_id = native_payload.get("sender_id", "")
        room_id = meta.get("room_id", sender_id)
        session_id = f"matrix:{room_id}"

        # content_parts are already ContentType objects (from both
        # _on_room_event and _on_room_media_event); filter out None.
        content = [p for p in parts if p is not None]
        if not content:
            content = [TextContent(type=ContentType.TEXT, text="")]

        # Use room_id as the AgentRequest user_id so that all participants
        # in the same room share one session (QwenPaw keys session state on
        # both session_id AND user_id).  The real sender is preserved in
        # meta["sender_id"] for reply mentions.
        req = self.build_agent_request_from_user_content(
            channel_id=CHANNEL_KEY,
            sender_id=room_id,
            session_id=session_id,
            content_parts=content,
            channel_meta=meta,
        )
        req.channel_meta = meta  # type: ignore[attr-defined]
        return req

    def resolve_session_id(self, sender_id: str, channel_meta=None) -> str:
        # 逻辑说明：`resolve_session_id` 优先从 Matrix 元数据读取 room_id，并添加 `matrix:` 前缀作为会话键；它只生成标识，不发送消息或修改房间。
        room_id = (channel_meta or {}).get("room_id", sender_id)
        return f"matrix:{room_id}"

    def to_handle_from_target(self, *, user_id: str, session_id: str) -> str:
        """For Matrix, return room_id (session_id), not user_id.

        Matrix requires room_id to send messages, not user_id.
        Override BaseChannel's default implementation which returns user_id.
        The session_id carries a ``matrix:`` prefix added by
        :meth:`resolve_session_id`; strip it so the value is a raw
        Matrix room_id that can be passed directly to ``room_send``.
        """
        # 逻辑说明：`to_handle_from_target` 接收 `user_id`、`session_id`，按请求类型分派并编排后续步骤，并依次复用 `startswith`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if session_id.startswith("matrix:"):
            return session_id[len("matrix:") :]
        return session_id

    def get_to_handle_from_request(self, request: Any) -> str:
        # 逻辑说明：`get_to_handle_from_request` 接收 `request`，读取、筛选并规范化现有数据，并依次复用 `get`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        meta = getattr(request, "channel_meta", {}) or {}
        return meta.get("room_id", getattr(request, "user_id", ""))

    # ------------------------------------------------------------------
    # Mention helper — MSC3952 m.mentions from body text scan
    # ------------------------------------------------------------------

    # Regex to match Matrix user IDs: @localpart:domain (with optional port)
    _MATRIX_USER_ID_RE = re.compile(
        r"@[a-zA-Z0-9._=+/\-]+:[a-zA-Z0-9.\-]+(?::\d+)?",
    )

    def _extract_mentions_from_text(self, text: str) -> list[str]:
        """Extract all @user:domain Matrix IDs from message text."""
        # 逻辑说明：`_extract_mentions_from_text` 接收 `text`，按既有顺序执行资源生命周期步骤，并依次复用 `findall`、`fromkeys`，返回 `list[str]`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        matches = self._MATRIX_USER_ID_RE.findall(text)
        return list(dict.fromkeys(matches))  # dedupe, preserve order

    def _apply_mention(
        self,
        content: dict[str, Any],
        user_id: str,
        room_id: str,
        *,
        explicit_user_ids: Optional[List[str]] = None,
        fallback_user_id: Optional[str] = None,
    ) -> None:
        """Attach Matrix mentions in both visible and structured forms."""
        # 逻辑说明：`_apply_mention` 接收 `content`、`user_id`、`room_id`、`explicit_user_ids`，按既有分支组合输入并生成结果，并依次复用 `get`、`_extract_mentions_from_text`，不返回业务结果。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        body = content.get("body", "") or ""
        if isinstance(explicit_user_ids, str):
            targets = [explicit_user_ids]
        elif explicit_user_ids:
            targets = list(explicit_user_ids)
        else:
            targets = self._extract_mentions_from_text(body)
            if not targets and fallback_user_id:
                targets = [fallback_user_id]
        targets = [
            target for target in _dedupe_nonempty(targets) if target != self._user_id
        ]
        if not targets:
            return

        html_body = content.get("formatted_body", "") or ""
        if not html_body:
            html_body = html.escape(body).replace("\n", "<br>\n")

        for mxid in targets:
            display = self._resolve_display_name(mxid, room_id) or mxid
            if mxid in body:
                body = body.replace(mxid, display, 1)
            elif display not in body:
                body = f"{display} {body}" if body else display

            mxid_enc = urllib.parse.quote(mxid, safe="")
            anchor = (
                f'<a href="https://matrix.to/#/{mxid_enc}">{html.escape(display)}</a>'
            )
            if mxid in html_body:
                html_body = html_body.replace(mxid, anchor, 1)
            elif anchor not in html_body:
                html_body = f"{anchor} {html_body}" if html_body else anchor

        content["body"] = body
        content["format"] = "org.matrix.custom.html"
        content["formatted_body"] = html_body
        content["m.mentions"] = {"user_ids": targets}

    def _resolve_display_name(self, user_id: str, room_id: str) -> str:
        """Best-effort display name for *user_id* in *room_id*."""
        # 逻辑说明：`_resolve_display_name` 接收 `user_id`、`room_id`，校验并规范化输入，并依次复用 `get`、`user_name`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._client:
            room = self._client.rooms.get(room_id)
            if room:
                try:
                    name = room.user_name(user_id)
                    if name:
                        return name
                except Exception as exc:
                    logger.debug(
                        "MatrixChannel: resolve_display_name user_name failed "
                        "for %s: %s",
                        user_id,
                        exc,
                    )
        return user_id.split(":")[0].lstrip("@") or user_id

    def _mark_room_encrypted(
        self,
        room_id: str,
        room: Optional[MatrixRoom] = None,
    ) -> None:
        """Keep nio's encrypted room state consistent for outbound sends."""
        # 逻辑说明：`_mark_room_encrypted` 接收 `room_id`、`room`，计算目标值并更新持久或共享状态，并依次复用 `add`、`get`，不返回业务结果。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return
        encrypted_rooms = getattr(self._client, "encrypted_rooms", None)
        if encrypted_rooms is not None:
            encrypted_rooms.add(room_id)
        target_room = room
        if target_room is None:
            rooms = getattr(self._client, "rooms", {})
            target_room = rooms.get(room_id) if rooms else None
        if target_room is not None:
            try:
                target_room.encrypted = True
            except Exception as exc:
                logger.debug(
                    "MatrixChannel: failed to mark %s encrypted: %s",
                    room_id,
                    exc,
                )

    def _room_will_encrypt(self, room_id: str) -> bool:
        """Return whether matrix-nio will encrypt room_send for this room."""
        # 逻辑说明：`_room_will_encrypt` 接收 `room_id`，按既有分支组合输入并生成结果，并依次复用 `get`、`_mark_room_encrypted`，返回 `bool`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client or not getattr(self._client, "olm", None):
            return False
        rooms = getattr(self._client, "rooms", {})
        room = rooms.get(room_id) if rooms else None
        if room is not None and getattr(room, "encrypted", False) is True:
            return True
        encrypted_rooms = getattr(self._client, "encrypted_rooms", set()) or set()
        if room_id in encrypted_rooms:
            self._mark_room_encrypted(room_id, room)
            return True
        return False

    async def _prepare_room_send(self, room_id: str) -> None:
        """Align local encryption flags with the homeserver before
        ``room_send``.

        matrix-nio only wraps ``room_send`` as ``m.room.encrypted`` when
        ``client.rooms[room_id].encrypted`` is true. If incremental sync never
        applied ``m.room.encryption`` to that object (common after restoring
        tokens or partial state), sends are still plaintext and Element shows
        "not encrypted". Fetch room state once per send attempt when needed.
        """
        # 逻辑说明：`_prepare_room_send` 接收 `room_id`，构造协议数据并完成外部传输，并依次复用 `warning`、`_e2ee_maintenance`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self.encryption:
            return
        if not self._client or not getattr(self._client, "olm", None):
            logger.warning(
                "MatrixChannel: E2EE configured but Olm is not ready; "
                "outbound message to %s will not be encrypted",
                room_id,
            )
            return
        await self._e2ee_maintenance()
        if self._room_will_encrypt(room_id):
            return
        if room_id not in getattr(self._client, "rooms", {}):
            logger.warning(
                "MatrixChannel: room %s not in client cache; "
                "cannot confirm E2EE before send (wait for sync / join)",
                room_id,
            )
            return
        try:
            enc_state = await self._client.room_get_state_event(
                room_id,
                "m.room.encryption",
                "",
            )
        except Exception as exc:
            logger.warning(
                "MatrixChannel: failed to get m.room.encryption for %s: %s",
                room_id,
                exc,
            )
            enc_state = None

        if isinstance(enc_state, RoomGetStateEventResponse):
            algo = (enc_state.content or {}).get("algorithm")
            if algo:
                self._mark_room_encrypted(room_id)
                logger.debug(
                    "MatrixChannel: marked %s encrypted from server (%s)",
                    room_id,
                    algo,
                )

        if not self._room_will_encrypt(room_id):
            logger.warning(
                "MatrixChannel: E2EE on but room %s is not encrypted in "
                "client after state check; outbound send may be plaintext",
                room_id,
            )

    # ------------------------------------------------------------------
    # Thread detection and readiness (ported from CoPaw overlay)
    # ------------------------------------------------------------------

    def _is_thread_event(self, event: Any) -> bool:
        """Return True when an inbound Matrix event belongs to a thread."""
        # 逻辑说明：`_is_thread_event` 接收 `event`，按既有分支组合输入并生成结果，并依次复用 `get`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        source = getattr(event, "source", {}) or {}
        if not isinstance(source, dict):
            return False
        content = source.get("content", {}) or {}
        if not isinstance(content, dict):
            return False
        relates_to = content.get("m.relates_to", {}) or {}
        if not isinstance(relates_to, dict):
            return False
        return relates_to.get("rel_type") == "m.thread"

    def _targeted_readiness_probe_reply(self, event: Any, text: str) -> str | None:
        """Return a direct readiness reply if this event explicitly targets us."""
        # 逻辑说明：`_targeted_readiness_probe_reply` 接收 `event`、`text`，读取、筛选并规范化现有数据，并依次复用 `_readiness_probe_reply`、`get`，返回 `str | None`。
        # 执行过程中包含外部 I/O；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        reply = _readiness_probe_reply(text)
        if not reply or not self._user_id:
            return None
        content = getattr(event, "source", {}).get("content", {}) or {}
        mentions = content.get("m.mentions", {}) or {}
        if self._user_id in mentions.get("user_ids", []):
            return reply
        if re.search(re.escape(self._user_id), text, re.IGNORECASE):
            return reply
        return None

    async def _send_plain_text(self, room_id: str, text: str) -> None:
        """Send a plain Matrix text event without invoking the agent path."""
        # 逻辑说明：`_send_plain_text` 接收 `room_id`、`text`，构造协议数据并完成外部传输，并依次复用 `error`、`_room_send_with_retry`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            logger.error("MatrixChannel: direct send called but client not ready")
            return
        try:
            await self._room_send_with_retry(
                room_id,
                "m.room.message",
                {"msgtype": "m.text", "body": text},
            )
        except Exception as exc:
            logger.exception(
                "MatrixChannel: direct send failed to %s: %s",
                room_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Thread relation helpers (ported from CoPaw overlay)
    # ------------------------------------------------------------------

    def _with_thread_relation_meta(
        self,
        meta: Optional[Dict[str, Any]],
        thread_root_event_id: str,
    ) -> Dict[str, Any]:
        """Return send metadata pinned to a Matrix thread root."""
        # 逻辑说明：`_with_thread_relation_meta` 接收 `meta`、`thread_root_event_id`，按既有分支组合输入并生成结果，并依次复用 `setdefault`，返回 `Dict[str, Any]`。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        meta_dict = dict(meta or {})
        if thread_root_event_id:
            meta_dict[_MATRIX_THREAD_META_KEY] = thread_root_event_id
            meta_dict.setdefault(_THREAD_META_ROOT_KEY, thread_root_event_id)
        return meta_dict

    async def _send_or_queue_thread_parts(
        self,
        room_id: str,
        parts: List[Any],
        meta: Optional[Dict[str, Any]],
    ) -> bool:
        """Send follow-up parts in the current thread, or queue until rooted."""
        # 逻辑说明：`_send_or_queue_thread_parts` 接收 `room_id`、`parts`、`meta`，构造协议数据并完成外部传输，并依次复用 `_response_is_stale`、`get`，返回 `bool`。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        meta_dict = meta if isinstance(meta, dict) else {}
        if self._response_is_stale(room_id, meta_dict):
            return True
        thread_root = (
            meta_dict.get(_MATRIX_THREAD_META_KEY)
            or meta_dict.get(_MATRIX_OWN_THREAD_ROOT_KEY)
            or meta_dict.get(_THREAD_META_ROOT_KEY)
        )
        if not thread_root:
            meta_dict.setdefault(_MATRIX_PENDING_THREAD_PARTS_KEY, []).extend(
                parts,
            )
            return True
        thread_meta = self._with_thread_relation_meta(meta_dict, thread_root)
        await self.send_content_parts(room_id, parts, thread_meta)
        return True

    async def _flush_pending_thread_parts(
        self,
        room_id: str,
        meta: Optional[Dict[str, Any]],
    ) -> None:
        """Flush queued follow-up parts after the first event becomes root."""
        # 逻辑说明：`_flush_pending_thread_parts` 接收 `room_id`、`meta`，按既有分支组合输入并生成结果，并依次复用 `pop`、`_send_or_queue_thread_parts`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        meta_dict = meta if isinstance(meta, dict) else {}
        pending = meta_dict.pop(_MATRIX_PENDING_THREAD_PARTS_KEY, []) or []
        if pending:
            await self._send_or_queue_thread_parts(room_id, pending, meta_dict)
        await self._flush_pending_final_message_to_thread(room_id, meta_dict)

    async def _flush_pending_final_message_to_thread(
        self,
        room_id: str,
        meta: Optional[Dict[str, Any]],
    ) -> None:
        """Flush a queued final message event into the established thread."""
        # 逻辑说明：`_flush_pending_final_message_to_thread` 接收 `room_id`、`meta`，构造协议数据并完成外部传输，并依次复用 `pop`、`get`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        meta_dict = meta if isinstance(meta, dict) else {}
        pending = meta_dict.pop(_MATRIX_PENDING_FINAL_MESSAGE_KEY, None)
        if not pending:
            return
        thread_root = (
            meta_dict.get(_MATRIX_THREAD_META_KEY)
            or meta_dict.get(_MATRIX_OWN_THREAD_ROOT_KEY)
            or meta_dict.get(_THREAD_META_ROOT_KEY)
        )
        if not thread_root:
            meta_dict[_MATRIX_PENDING_FINAL_MESSAGE_KEY] = pending
            return
        thread_meta = self._with_thread_relation_meta(meta_dict, thread_root)
        # pending is the event object — use send_message_content to properly
        # render it through the renderer, not str() which produces repr.
        if isinstance(pending, str):
            await self.send(room_id, pending, thread_meta)
        else:
            await self.send_message_content(room_id, pending, thread_meta)

    async def _ensure_thread_root(
        self,
        to_handle: str,
        send_meta: Dict[str, Any],
    ) -> None:
        """Create a placeholder thread-root message if none exists yet."""
        # 逻辑说明：`_ensure_thread_root` 接收 `to_handle`、`send_meta`，按既有分支组合输入并生成结果，并依次复用 `_response_is_stale`、`get`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._response_is_stale(to_handle, send_meta):
            return
        if send_meta.get(_MATRIX_OWN_THREAD_ROOT_KEY):
            return
        if not self._client:
            return
        content: dict[str, Any] = {
            "msgtype": "m.notice",
            "body": "处理中...",
            _AGENTTEAMS_TRANSIENT_CONTENT_KEY: True,
        }
        try:
            resp = await self._room_send_with_retry(
                to_handle,
                "m.room.message",
                content,
            )
            event_id = getattr(resp, "event_id", None)
            if event_id:
                send_meta[_MATRIX_OWN_THREAD_ROOT_KEY] = event_id
                send_meta[_THREAD_META_ROOT_KEY] = event_id
                send_meta[_MATRIX_PLACEHOLDER_THREAD_ROOT_KEY] = True
                self._active_thread_roots[to_handle] = event_id
                self._write_attachment_context(to_handle, event_id)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: _ensure_thread_root failed for %s: %s",
                to_handle,
                exc,
            )

    async def _send_streaming_thread_text(
        self,
        to_handle: str,
        send_meta: Dict[str, Any],
        text: str,
    ) -> Optional[str]:
        # 逻辑说明：`_send_streaming_thread_text` 接收 `to_handle`、`send_meta`、`text`，构造协议数据并完成外部传输，并依次复用 `_response_is_stale`、`get`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._response_is_stale(to_handle, send_meta):
            return None
        thread_root = (
            send_meta.get(_MATRIX_THREAD_META_KEY)
            or send_meta.get(_MATRIX_OWN_THREAD_ROOT_KEY)
            or send_meta.get(_THREAD_META_ROOT_KEY)
        )
        if not thread_root or not self._client:
            return None
        content: dict[str, Any] = {
            "msgtype": "m.notice",
            "body": text,
            "format": "org.matrix.custom.html",
            "formatted_body": _md_to_html(text),
            _AGENTTEAMS_TRANSIENT_CONTENT_KEY: True,
        }
        self._apply_thread_relation(
            content,
            self._with_thread_relation_meta(send_meta, thread_root),
        )
        try:
            resp = await self._room_send_with_retry(
                to_handle,
                "m.room.message",
                content,
            )
            return getattr(resp, "event_id", None)
        except Exception as exc:
            logger.warning(
                "MatrixChannel: streaming thread send failed for %s: %s",
                to_handle,
                exc,
            )
            return None

    async def _edit_matrix_event(
        self,
        to_handle: str,
        event_id: str,
        text: str,
        *,
        msgtype: str = "m.notice",
        html: Optional[str] = None,
    ) -> None:
        # 逻辑说明：`_edit_matrix_event` 接收 `to_handle`、`event_id`、`text`、`msgtype`，构造协议数据并完成外部传输，并依次复用 `_edit_fallback_html`、`_matrix_event_payload_size`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not event_id or not self._client:
            return
        new_content: dict[str, Any] = {"msgtype": msgtype, "body": text}
        if html:
            new_content["format"] = "org.matrix.custom.html"
            new_content["formatted_body"] = html
        content: dict[str, Any] = {
            "msgtype": msgtype,
            "body": f"* {text}",
            "m.new_content": new_content,
            "m.relates_to": {"rel_type": "m.replace", "event_id": event_id},
        }
        if html:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = _edit_fallback_html(text)
        try:
            if _matrix_event_payload_size(content) > MATRIX_TEXT_EVENT_SAFE_BYTES:
                await self._send_long_text_fallback(
                    to_handle,
                    text,
                    {_MATRIX_OWN_THREAD_ROOT_KEY: event_id},
                    msgtype,
                    edit_event_id=event_id,
                )
                return
            await self._room_send_with_retry(
                to_handle,
                "m.room.message",
                content,
            )
        except Exception as exc:
            logger.warning(
                "MatrixChannel: event edit failed for %s: %s",
                to_handle,
                exc,
            )

    def _apply_thread_relation(
        self,
        content: Dict[str, Any],
        meta: Optional[Dict[str, Any]],
    ) -> None:
        """Attach Matrix thread relation metadata to an outgoing event.

        Only applies when an explicit thread context has been established
        (by _with_thread_relation_meta or _ensure_thread_root). The inbound
        _THREAD_META_ROOT_KEY alone is NOT sufficient — it is always present
        from the original sender's event_id and would incorrectly thread
        every reply under the sender's message.
        """
        # 逻辑说明：`_apply_thread_relation` 接收 `content`、`meta`，按既有分支组合输入并生成结果，并依次复用 `get`，不返回业务结果。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not isinstance(content, dict) or not isinstance(meta, dict):
            return
        thread_root = meta.get(_MATRIX_THREAD_META_KEY) or meta.get(
            _MATRIX_OWN_THREAD_ROOT_KEY
        )
        if not thread_root:
            return
        content["m.relates_to"] = {
            "rel_type": "m.thread",
            "event_id": thread_root,
            "is_falling_back": False,
        }

    def _attachment_parent_event_id(self, meta: Optional[Dict[str, Any]]) -> str:
        # 逻辑说明：`_attachment_parent_event_id` 接收 `meta`，按既有分支组合输入并生成结果，并依次复用 `strip`、`get`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not isinstance(meta, dict):
            return ""
        for key in ATTACHMENT_PARENT_EVENT_KEYS:
            value = str(meta.get(key) or "").strip()
            if value:
                return value
        return ""

    def _apply_attachment_relation(
        self,
        content: Dict[str, Any],
        meta: Optional[Dict[str, Any]],
    ) -> None:
        # 逻辑说明：`_apply_attachment_relation` 接收 `content`、`meta`，按既有分支组合输入并生成结果，并依次复用 `_attachment_parent_event_id`，不返回业务结果。
        # 执行过程中包含实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        parent_event_id = self._attachment_parent_event_id(meta)
        if not parent_event_id:
            return
        content["m.relates_to"] = {
            "rel_type": MATRIX_ATTACHMENT_REL_TYPE,
            "event_id": parent_event_id,
        }

    def _matrix_attachment_context_path(self) -> Optional[Path]:
        # 逻辑说明：`_matrix_attachment_context_path` 不接收参数，优先读取显式附件上下文文件，缺省时落到 QwenPaw 工作目录中的固定文件名；本函数不传输媒体。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        raw = os.environ.get("TEAMHARNESS_MATRIX_CONTEXT_FILE", "").strip()
        if raw:
            return Path(raw)
        qwenpaw_dir = os.environ.get("QWENPAW_WORKING_DIR", "").strip()
        if qwenpaw_dir:
            return Path(qwenpaw_dir) / MATRIX_ATTACHMENT_CONTEXT_FILE
        try:
            return Path(WORKING_DIR) / MATRIX_ATTACHMENT_CONTEXT_FILE
        except TypeError:
            return None

    def _write_attachment_context(self, room_id: str, event_id: str) -> None:
        # 逻辑说明：`_write_attachment_context` 接收 `room_id`、`event_id`，计算目标值并更新持久或共享状态，并依次复用 `_matrix_attachment_context_path`、`is_file`，不返回业务结果。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not room_id or not event_id:
            return
        path = self._matrix_attachment_context_path()
        if not path:
            return
        data: Dict[str, Any] = {}
        try:
            if path.is_file():
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data = loaded
        except (OSError, json.JSONDecodeError):
            data = {}
        rooms = data.get("rooms")
        if not isinstance(rooms, dict):
            rooms = {}
            data["rooms"] = rooms
        rooms[room_id] = {
            "attachmentParentEventId": event_id,
            "eventId": event_id,
            "updatedAt": time.time(),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
            tmp.write_text(
                json.dumps(data, ensure_ascii=False, sort_keys=True),
                encoding="utf-8",
            )
            tmp.replace(path)
        except OSError as exc:
            logger.debug(
                "MatrixChannel: attachment context write failed for %s: %s",
                room_id,
                exc,
            )

    def _matrix_media_event_content(
        self,
        file_ref: str,
        matrix_msgtype: str,
        mxc_uri: str,
    ) -> Dict[str, Any]:
        # 逻辑说明：`_matrix_media_event_content` 接收 `file_ref`、`matrix_msgtype`、`mxc_uri`，构造协议数据并完成外部传输，并依次复用 `file_url_to_local_path`、`basename`，返回 `Dict[str, Any]`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        path_str = file_url_to_local_path(file_ref) or file_ref
        filename = os.path.basename(path_str) or "file"
        mime_type, _ = mimetypes.guess_type(path_str)
        mime_type = mime_type or "application/octet-stream"
        try:
            file_size = os.path.getsize(path_str)
        except OSError:
            file_size = 0
        return {
            "msgtype": matrix_msgtype,
            "body": filename,
            "url": mxc_uri,
            "info": {
                "mimetype": mime_type,
                "size": file_size,
            },
        }

    async def _send_uploaded_media_event(
        self,
        room_id: str,
        file_ref: str,
        matrix_msgtype: str,
        mxc_uri: str,
        meta: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        # 逻辑说明：`_send_uploaded_media_event` 接收 `room_id`、`file_ref`、`matrix_msgtype`、`mxc_uri`，构造协议数据并完成外部传输，并依次复用 `_matrix_media_event_content`、`get`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        event_content = self._matrix_media_event_content(
            file_ref,
            matrix_msgtype,
            mxc_uri,
        )
        meta_dict = meta or {}
        sender_id = meta_dict.get("sender_id") or meta_dict.get("user_id")
        explicit_ids = meta_dict.get("mention_user_ids") or None
        if explicit_ids:
            self._apply_mention(
                event_content,
                sender_id or "",
                room_id,
                explicit_user_ids=explicit_ids,
            )
        self._apply_attachment_relation(event_content, meta_dict)

        resp = await self._room_send_with_retry(
            room_id,
            "m.room.message",
            event_content,
        )
        return getattr(resp, "event_id", None)

    def _matrix_text_content(
        self,
        room_id: str,
        text: str,
        meta: Optional[Dict[str, Any]],
        msgtype: str,
        *,
        html_body: Optional[str] = None,
    ) -> Dict[str, Any]:
        # 逻辑说明：`_matrix_text_content` 接收 `room_id`、`text`、`meta`、`msgtype`，构造协议数据并完成外部传输，并依次复用 `_md_to_html`、`get`，返回 `Dict[str, Any]`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有输入校验、范围限制与异常传播，避免失败或部分成功被误报为完整成功。
        content: dict[str, Any] = {
            "msgtype": msgtype,
            "body": text,
            "format": "org.matrix.custom.html",
            "formatted_body": html_body if html_body is not None else _md_to_html(text),
        }
        meta_dict = meta if isinstance(meta, dict) else {}
        if meta_dict.get(_MATRIX_TRANSIENT_META_KEY):
            content[_AGENTTEAMS_TRANSIENT_CONTENT_KEY] = True
        sender_id = meta_dict.get("sender_id") or meta_dict.get("user_id")
        explicit_ids = meta_dict.get("mention_user_ids") or None
        if explicit_ids or self._extract_mentions_from_text(text):
            self._apply_mention(
                content,
                sender_id or "",
                room_id,
                explicit_user_ids=explicit_ids,
            )
        self._apply_thread_relation(content, meta_dict)
        return content

    async def _send_agentteams_final_signal(
        self,
        room_id: str,
        text: str,
        meta: Optional[Dict[str, Any]],
    ) -> None:
        """Emit one bot-readable final event without duplicating visible text."""
        # 逻辑说明：`_send_agentteams_final_signal` 接收 `room_id`、`text`、`meta`，构造协议数据并完成外部传输，并依次复用 `_visible_final_text`、`_ends_with_no_reply_control`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return
        visible = self._visible_final_text(text)
        if not visible or _ends_with_no_reply_control(text):
            return
        upper = visible.upper()
        if not any(marker in upper for marker in ("TASK_COMPLETED:", "TASK_BLOCKED:")):
            return
        content = self._matrix_text_content(
            room_id,
            visible,
            meta,
            "m.text",
        )
        content[_AGENTTEAMS_FINAL_CONTENT_KEY] = True
        try:
            await self._room_send_with_retry(
                room_id,
                _AGENTTEAMS_FINAL_EVENT_TYPE,
                content,
            )
        except Exception as exc:
            logger.warning(
                "MatrixChannel: final signal send failed for %s: %s",
                room_id,
                exc,
            )

    def _write_long_message_file(self, text: str) -> Path:
        # 逻辑说明：`_write_long_message_file` 接收 `text`，计算目标值并更新持久或共享状态，并依次复用 `_media_dir`、`mkdir`，返回 `Path`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        directory = self._media_dir() / "long-messages"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"matrix-long-message-{time.time_ns()}.md"
        path.write_text(text, encoding="utf-8")
        return path

    def _long_message_summary(
        self,
        text: str,
        path: Path,
        media_uri: Optional[str],
        preview_chars: int,
    ) -> str:
        # 逻辑说明：`_long_message_summary` 接收 `text`、`path`、`media_uri`、`preview_chars`，构造协议数据并完成外部传输，并依次复用 `stat`、`rstrip`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        size = path.stat().st_size
        if media_uri:
            notice = (
                f"<单条 Matrix 消息已按安全长度截断，完整内容已缓存为 Matrix 附件："
                f"{path.name}，附件地址：{media_uri}。>"
            )
        else:
            notice = (
                f"<单条 Matrix 消息已按安全长度截断，完整内容已缓存为本地文件："
                f"{path.name}（{size} bytes），Matrix 附件上传失败。>"
            )
        prefix = (text or "")[:preview_chars].rstrip()
        if not prefix:
            return notice
        return f"{prefix}\n\n{notice}"

    def _bounded_long_message_content(
        self,
        text: str,
        path: Path,
        media_uri: Optional[str],
        build_content: Callable[[str], Dict[str, Any]],
    ) -> Dict[str, Any]:
        # 逻辑说明：`_bounded_long_message_content` 接收 `text`、`path`、`media_uri`、`build_content`，构造协议数据并完成外部传输，并依次复用 `_long_message_summary`、`build_content`，返回 `Dict[str, Any]`。
        # 执行过程中包含外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        low = 0
        high = len(text or "")
        best_content: Optional[Dict[str, Any]] = None
        while low <= high:
            preview_chars = (low + high) // 2
            summary = self._long_message_summary(
                text,
                path,
                media_uri,
                preview_chars,
            )
            content = build_content(summary)
            if (
                _matrix_event_payload_size(content)
                <= MATRIX_TEXT_EVENT_FALLBACK_BUDGET_BYTES
            ):
                best_content = content
                low = preview_chars + 1
            else:
                high = preview_chars - 1
        if best_content is not None:
            return best_content
        return build_content(self._long_message_summary(text, path, media_uri, 0))

    async def _send_long_text_fallback(
        self,
        room_id: str,
        text: str,
        meta: Optional[Dict[str, Any]],
        msgtype: str,
        *,
        edit_event_id: str = "",
    ) -> Optional[str]:
        # 逻辑说明：`_send_long_text_fallback` 接收 `room_id`、`text`、`meta`、`msgtype`，构造协议数据并完成外部传输，并依次复用 `_write_long_message_file`、`_upload_file`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return None
        path = self._write_long_message_file(text)
        file_ref = str(path)
        media_uri = await self._upload_file(file_ref)
        if not media_uri:
            logger.warning(
                "MatrixChannel: long text fallback upload failed for %s",
                file_ref,
            )
        long_message_metadata = _long_message_metadata(path, media_uri)

        def build_content(summary: str) -> Dict[str, Any]:
            # 逻辑说明：`build_content` 接收 `summary`，把输入转换为调用方需要的结构，并依次复用 `_md_to_html`、`_attach_long_message_metadata`，返回 `Dict[str, Any]`。
            # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
            if edit_event_id:
                html_body = _md_to_html(summary)
                new_content: dict[str, Any] = {
                    "msgtype": msgtype,
                    "body": summary,
                    "format": "org.matrix.custom.html",
                    "formatted_body": html_body,
                }
                _attach_long_message_metadata(new_content, long_message_metadata)
                content: Dict[str, Any] = {
                    "msgtype": msgtype,
                    "body": f"* {summary}",
                    "m.new_content": new_content,
                    "m.relates_to": {
                        "rel_type": "m.replace",
                        "event_id": edit_event_id,
                    },
                    "format": "org.matrix.custom.html",
                    "formatted_body": _edit_fallback_html(summary),
                }
                _attach_long_message_metadata(content, long_message_metadata)
                return content
            content = self._matrix_text_content(room_id, summary, meta, msgtype)
            _attach_long_message_metadata(content, long_message_metadata)
            return content

        content = self._bounded_long_message_content(
            text,
            path,
            media_uri,
            build_content,
        )
        resp = await self._room_send_with_retry(
            room_id,
            "m.room.message",
            content,
        )
        event_id = getattr(resp, "event_id", None)
        parent_event_id = edit_event_id or event_id
        if media_uri and parent_event_id:
            file_meta = dict(meta or {})
            file_meta["matrixAttachmentParentEventId"] = parent_event_id
            try:
                await self._send_uploaded_media_event(
                    room_id,
                    file_ref,
                    "m.file",
                    media_uri,
                    file_meta,
                )
            except Exception as exc:
                logger.warning(
                    "MatrixChannel: long text fallback media event failed for %s: %s",
                    room_id,
                    exc,
                )
        return event_id

    async def _edit_thread_root(
        self,
        to_handle: str,
        send_meta: Dict[str, Any],
        text: str,
        *,
        msgtype: str = "m.notice",
        html: Optional[str] = None,
    ) -> None:
        """Edit the thread-root placeholder message content."""
        # 逻辑说明：`_edit_thread_root` 接收 `to_handle`、`send_meta`、`text`、`msgtype`，按既有分支组合输入并生成结果，并依次复用 `get`、`_edit_matrix_event`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        root_id = send_meta.get(_MATRIX_OWN_THREAD_ROOT_KEY)
        await self._edit_matrix_event(
            to_handle,
            root_id,
            text,
            msgtype=msgtype,
            html=html,
        )

    # ------------------------------------------------------------------
    # Agent run lifecycle overrides (ported from CoPaw overlay)
    # ------------------------------------------------------------------

    def _is_completed_status(self, status: Any) -> bool:
        # 逻辑说明：`_is_completed_status` 接收 `status`，按既有分支组合输入并生成结果，并依次复用 `_enum_name`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if RunStatus is not None and status == RunStatus.Completed:
            return True
        return _enum_name(status) == "COMPLETED"

    def _is_in_progress_status(self, status: Any) -> bool:
        # 逻辑说明：`_is_in_progress_status` 接收 `status`，按既有分支组合输入并生成结果，并依次复用 `_enum_name`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if RunStatus is not None and status == RunStatus.InProgress:
            return True
        return _enum_name(status) == "INPROGRESS"

    def _is_reasoning_message(self, message_type: Any) -> bool:
        # 逻辑说明：`_is_reasoning_message` 接收 `message_type`，构造协议数据并完成外部传输，并依次复用 `_enum_name`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if MessageType is not None and message_type == MessageType.REASONING:
            return True
        return _enum_name(message_type) == "REASONING"

    def _is_tool_call_message(self, message_type: Any) -> bool:
        return _enum_name(message_type) in _TOOL_CALL_MESSAGE_TYPE_NAMES

    def _is_tool_output_message(self, message_type: Any) -> bool:
        return _enum_name(message_type) in _TOOL_OUTPUT_MESSAGE_TYPE_NAMES

    def _is_message_event(self, message_type: Any) -> bool:
        # 逻辑说明：`_is_message_event` 接收 `message_type`，构造协议数据并完成外部传输，并依次复用 `_enum_name`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if MessageType is not None and message_type == MessageType.MESSAGE:
            return True
        return _enum_name(message_type) == "MESSAGE"

    def _thread_content_parts(self, event: Any) -> List[Any]:
        """Render event for thread display with tool messages enabled."""
        # 逻辑说明：`_thread_content_parts` 接收 `event`，把输入转换为调用方需要的结构，并依次复用 `dc_replace`、`__class__`，返回 `List[Any]`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        from dataclasses import replace as dc_replace

        display_config = dc_replace(
            self._render_style.display_config,
            show_tool_calls=True,
            show_tool_results=True,
        )
        style = dc_replace(self._render_style, display_config=display_config)
        renderer = self._renderer.__class__(style)
        return renderer.message_to_parts(event)

    def _text_from_message_event(self, event: Any) -> str:
        # 逻辑说明：`_text_from_message_event` 接收 `event`，构造协议数据并完成外部传输，并依次复用 `_message_to_content_parts`、`strip`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        parts = self._message_to_content_parts(event)
        return "\n".join(
            getattr(p, "text", "") or getattr(p, "refusal", "") or ""
            for p in parts
            if getattr(p, "type", None) in (ContentType.TEXT, ContentType.REFUSAL)
        ).strip()

    def _visible_final_text(self, text: str) -> str:
        # 逻辑说明：`_visible_final_text` 接收 `text`，按既有分支组合输入并生成结果，并依次复用 `strip`、`_clean_control_response_text`，返回 `str`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        text = (text or "").strip()
        if not text:
            return ""
        return _clean_control_response_text(text).strip()

    def _tool_output_media_parts(self, event: Any) -> List[Any]:
        """Extract media-only parts from a tool output event."""
        # 逻辑说明：`_tool_output_media_parts` 接收 `event`，按既有分支组合输入并生成结果，并依次复用 `dc_replace`、`__class__`，返回 `List[Any]`。
        # 执行过程中包含外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        from dataclasses import replace as dc_replace

        display_config = dc_replace(
            self._render_style.display_config,
            show_tool_results=False,
        )
        style = dc_replace(self._render_style, display_config=display_config)
        renderer = self._renderer.__class__(style)
        parts = renderer.message_to_parts(event)
        return [p for p in parts if getattr(p, "type", None) != ContentType.TEXT]

    async def on_event_content(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: Dict[str, Any],
    ) -> bool:
        """Consume streaming tool progress without sending Matrix noise."""
        # 逻辑说明：`on_event_content` 接收 `request`、`to_handle`、`event`、`send_meta`，把输入转换为调用方需要的结构，并依次复用 `_response_is_stale`、`_is_in_progress_status`，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        del request
        if self._response_is_stale(to_handle, send_meta):
            return True
        if getattr(event, "type", None) != ContentType.DATA:
            return False
        if not self._is_in_progress_status(getattr(event, "status", None)):
            return False
        data = getattr(event, "data", None) or {}
        if not isinstance(data, dict) or "output" not in data:
            return False
        # Silently consume — return True so BaseChannel skips default send.
        # Tool output details are too noisy for Matrix; the completed
        # TOOL_CALL / TOOL_OUTPUT events carry the useful summary.
        return True

    async def on_event_message_completed(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: Dict[str, Any],
    ) -> None:
        """Route completed messages behind a processing root."""
        # 逻辑说明：`on_event_message_completed` 接收 `request`、`to_handle`、`event`、`send_meta`，构造协议数据并完成外部传输，并依次复用 `_response_is_stale`、`_is_reasoning_message`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        del request
        if self._response_is_stale(to_handle, send_meta):
            return
        message_type = getattr(event, "type", None)
        if self._is_reasoning_message(
            message_type,
        ) or self._is_tool_call_message(message_type):
            await self._ensure_thread_root(to_handle, send_meta)
            await self._flush_pending_final_message_to_thread(
                to_handle,
                send_meta,
            )
            parts = self._message_to_content_parts(event)
            if not parts:
                return
            if self._is_reasoning_message(message_type):
                send_meta[_MATRIX_FORCE_NOTICE_KEY] = True
            send_meta[_MATRIX_TRANSIENT_META_KEY] = True
            try:
                await self._send_or_queue_thread_parts(
                    to_handle,
                    parts,
                    send_meta,
                )
            finally:
                send_meta.pop(_MATRIX_FORCE_NOTICE_KEY, None)
                send_meta.pop(_MATRIX_TRANSIENT_META_KEY, None)
            return
        if self._is_tool_output_message(message_type):
            await self._flush_pending_final_message_to_thread(
                to_handle,
                send_meta,
            )
            parts = self._tool_output_media_parts(event)
            if parts:
                await self.send_content_parts(to_handle, parts, send_meta)
            return

        if self._is_message_event(message_type):
            await self._ensure_thread_root(to_handle, send_meta)
            await self._flush_pending_final_message_to_thread(
                to_handle,
                send_meta,
            )
            send_meta[_MATRIX_PENDING_FINAL_MESSAGE_KEY] = event
            return

        await self.send_message_content(to_handle, event, send_meta)

    async def on_streaming_start(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: Dict[str, Any],
        stream_type: str,
        accumulated_text: str = "",
    ) -> None:
        # 逻辑说明：`on_streaming_start` 接收 `request`、`to_handle`、`event`、`send_meta`，推进组件生命周期并同步运行状态，并依次复用 `_response_is_stale`、`pop`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        del request, accumulated_text
        if self._response_is_stale(to_handle, send_meta):
            return
        if stream_type == "reasoning":
            send_meta.pop(_MATRIX_STREAMING_REASONING_EVENT_ID_KEY, None)
            send_meta.pop(_MATRIX_STREAMING_REASONING_LAST_EDIT_KEY, None)
            stream_id = getattr(event, "id", None)
            if stream_id:
                send_meta[_MATRIX_STREAMING_REASONING_STREAM_ID_KEY] = stream_id
            await self._ensure_thread_root(to_handle, send_meta)
            return
        if stream_type == "message":
            await self._ensure_thread_root(to_handle, send_meta)

    async def on_streaming_delta(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: Dict[str, Any],
        stream_type: str,
        accumulated_text: str = "",
    ) -> None:
        # 逻辑说明：`on_streaming_delta` 接收一次流式增量及发送上下文；Matrix 频道不逐片编辑消息，因此显式丢弃这些参数，等待 `on_streaming_end` 统一发送最终文本。
        # 本函数不执行网络 I/O、状态写入或重试；保留异步钩子只是为了满足频道接口并避免重复 Matrix 事件。
        del request, to_handle, event, send_meta, stream_type, accumulated_text
        return

    async def on_streaming_end(
        self,
        request: Any,
        to_handle: str,
        event: Any,
        send_meta: Dict[str, Any],
        stream_type: str,
        accumulated_text: str = "",
    ) -> None:
        # 逻辑说明：`on_streaming_end` 接收 `request`、`to_handle`、`event`、`send_meta`，按既有分支组合输入并生成结果，并依次复用 `_response_is_stale`、`strip`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        del request
        if self._response_is_stale(to_handle, send_meta):
            return
        text = (accumulated_text or "").strip()
        if stream_type == "reasoning":
            await self._ensure_thread_root(to_handle, send_meta)
            if not text:
                text = self._text_from_message_event(event)
            if text:
                text = f"Thinking:\n\n{text}"
                await self._send_streaming_thread_text(
                    to_handle,
                    send_meta,
                    text,
                )
            send_meta.pop(_MATRIX_STREAMING_REASONING_EVENT_ID_KEY, None)
            send_meta.pop(_MATRIX_STREAMING_REASONING_LAST_EDIT_KEY, None)
            send_meta.pop(_MATRIX_STREAMING_REASONING_STREAM_ID_KEY, None)
            return

        if not text:
            text = self._text_from_message_event(event)
        if not text:
            return

        if stream_type == "message":
            send_meta[_MATRIX_STREAMING_FINAL_TEXT_KEY] = text

    async def send_event(
        self,
        *,
        user_id: str,
        session_id: str,
        event: Any,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Route proactive (cron) events through thread-aware logic.

        Uses shared per-room state so the thread root persists across events
        within one proactive send stream.
        """
        # 逻辑说明：`send_event` 接收 `user_id`、`session_id`、`event`、`meta`，构造协议数据并完成外部传输，并依次复用 `to_handle_from_target`、`get`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        obj = getattr(event, "object", None)
        status = getattr(event, "status", None)
        to_handle = self.to_handle_from_target(
            user_id=user_id,
            session_id=session_id,
        )

        # Get or create shared send_meta for this proactive send
        send_meta = self._proactive_send_state.get(to_handle)
        if send_meta is None:
            send_meta = dict(meta or {})
            self._proactive_send_state[to_handle] = send_meta

        if obj == "message" and self._is_completed_status(status):
            await self.on_event_message_completed(
                None,
                to_handle,
                event,
                send_meta,
            )
        elif obj == "response":
            # Stream completed — flush deferred final message and clean up
            await self._on_process_completed(None, to_handle, send_meta)
            self._proactive_send_state.pop(to_handle, None)

    async def _on_process_completed(
        self,
        request: Any,
        to_handle: str,
        send_meta: Dict[str, Any],
    ) -> None:
        """Edit thread root with final reply, or send directly if no thread."""
        # 逻辑说明：`_on_process_completed` 接收 `request`、`to_handle`、`send_meta`，按既有分支组合输入并生成结果，并依次复用 `_response_is_stale`、`pop`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if self._response_is_stale(to_handle, send_meta):
            self._active_thread_roots.pop(to_handle, None)
            await self._send_typing(to_handle, False)
            base_completed = getattr(super(), "_on_process_completed", None)
            if base_completed:
                await base_completed(request, to_handle, send_meta)
            return
        pending = send_meta.pop(_MATRIX_PENDING_FINAL_MESSAGE_KEY, None)
        streaming_final_text = send_meta.pop(
            _MATRIX_STREAMING_FINAL_TEXT_KEY,
            None,
        )
        is_placeholder = send_meta.pop(_MATRIX_PLACEHOLDER_THREAD_ROOT_KEY, False)
        final_signal_text = ""
        if is_placeholder:
            if streaming_final_text:
                raw_text = streaming_final_text.strip()
                if _ends_with_no_reply_control(raw_text):
                    await self._edit_thread_root(
                        to_handle,
                        send_meta,
                        "已处理",
                    )
                else:
                    text = self._visible_final_text(raw_text)
                    if not text:
                        await self._edit_thread_root(
                            to_handle,
                            send_meta,
                            "已完成",
                        )
                    else:
                        html_body = _md_to_html(text)
                        await self._edit_thread_root(
                            to_handle,
                            send_meta,
                            text,
                            msgtype="m.text",
                            html=html_body,
                        )
                        final_signal_text = text
            elif pending is not None:
                raw_text = self._text_from_message_event(pending)
                if _ends_with_no_reply_control(raw_text):
                    await self._edit_thread_root(
                        to_handle,
                        send_meta,
                        "已处理",
                    )
                else:
                    text = self._visible_final_text(raw_text)
                    if text:
                        html_body = _md_to_html(text)
                        await self._edit_thread_root(
                            to_handle,
                            send_meta,
                            text,
                            msgtype="m.text",
                            html=html_body,
                        )
                        final_signal_text = text
                    else:
                        await self._edit_thread_root(
                            to_handle,
                            send_meta,
                            "已完成",
                        )
            else:
                await self._edit_thread_root(
                    to_handle,
                    send_meta,
                    "已完成",
                )
            if final_signal_text:
                await self._send_agentteams_final_signal(
                    to_handle,
                    final_signal_text,
                    send_meta,
                )
            self._active_thread_roots.pop(to_handle, None)
        elif streaming_final_text:
            await self.send(to_handle, streaming_final_text.strip(), send_meta)
        elif pending is not None:
            await self.send_message_content(to_handle, pending, send_meta)
        await self._send_typing(to_handle, False)
        base_completed = getattr(super(), "_on_process_completed", None)
        try:
            if base_completed:
                await base_completed(request, to_handle, send_meta)
        finally:
            await self._send_typing(to_handle, False)

    async def _on_consume_error(
        self,
        request: Any,
        to_handle: str,
        err_text: str,
    ) -> None:
        """Edit thread root on error; suppress user-visible cancellation noise."""
        # 逻辑说明：`_on_consume_error` 接收 `request`、`to_handle`、`err_text`，按既有分支组合输入并生成结果，并依次复用 `pop`、`_edit_thread_root`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        root_id = self._active_thread_roots.pop(to_handle, None)
        if root_id:
            fallback_meta = {_MATRIX_OWN_THREAD_ROOT_KEY: root_id}
            status = (
                "已取消"
                if "Task has been cancelled" in (err_text or "")
                else "处理异常"
            )
            await self._edit_thread_root(to_handle, fallback_meta, status)
        if "Task has been cancelled" in (err_text or ""):
            logger.info(
                "MatrixChannel: suppressing cancellation error component=matrix handle=%s",
                to_handle,
            )
            await self._send_typing(to_handle, False)
            return
        await super()._on_consume_error(request, to_handle, err_text)

    # ------------------------------------------------------------------
    # Outgoing send — retry helper
    # Exponential backoff for transient room_send failures (5xx, 429,
    # network errors).  Deterministic errors (4xx except 429) are raised
    # immediately so callers can log without retry.
    # ------------------------------------------------------------------

    @staticmethod
    def _is_retryable_send_error(exc: Exception) -> bool:
        """Return True if *exc* is a transient error worth retrying."""
        # 逻辑说明：`_is_retryable_send_error` 接收 `exc`，构造协议数据并完成外部传输，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        # Network / transport layer errors — always retry
        if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
            return True
        if isinstance(exc, asyncio.TimeoutError):
            return True
        # httpx transport errors (httpx is already imported)
        if isinstance(exc, httpx.TransportError):
            return True
        return False

    # Matrix errcode strings that indicate a transient / retryable condition.
    _RETRYABLE_MATRIX_ERRCODES = frozenset(
        {
            "M_LIMIT_EXCEEDED",  # 429 rate-limited
            "M_UNKNOWN",  # generic server-side error
        }
    )

    @staticmethod
    def _is_retryable_room_send_response(resp: Any) -> bool:
        """Return True if a nio ``RoomSendError`` is transient.

        nio's ``ErrorResponse.status_code`` is a **Matrix errcode string**
        (e.g. ``"M_LIMIT_EXCEEDED"``), not an HTTP integer.  We therefore
        match on the errcode string for Matrix-level classification and
        fall back to the HTTP status code carried inside
        ``transport_response`` when available.
        """
        # 逻辑说明：`_is_retryable_room_send_response` 接收 `resp`，构造协议数据并完成外部传输，返回 `bool`。
        # 外部或状态副作用仅来自上述既有 helper 调用；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not isinstance(resp, RoomSendError):
            return False

        # 1. Check Matrix errcode (string)
        errcode = getattr(resp, "status_code", None)
        if errcode in AgentTeamsMatrixChannel._RETRYABLE_MATRIX_ERRCODES:
            return True

        # 2. Fall back to HTTP status via transport_response
        tr = getattr(resp, "transport_response", None)
        if tr is not None:
            http_status = getattr(tr, "status_code", None)
            if isinstance(http_status, int):
                if http_status == 429 or 500 <= http_status < 600:
                    return True

        # 3. No errcode and no transport info — assume transient
        if errcode is None and tr is None:
            return True

        return False

    async def _room_send_with_retry(
        self,
        room_id: str,
        message_type: str,
        content: dict,
        *,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ) -> Any:
        """Wrap ``_prepare_room_send`` + ``room_send`` with limited retries.

        Retryable errors (5xx, 429, network) trigger exponential back-off
        with a small random jitter.  Non-retryable errors propagate
        immediately.

        A single ``tx_id`` is generated for the entire retry sequence so that
        the Matrix homeserver can de-duplicate if a request was actually
        processed but the response was lost in transit (idempotency).

        Returns the successful ``RoomSendResponse``.
        """
        # 逻辑说明：`_room_send_with_retry` 接收 `room_id`、`message_type`、`content`、`max_retries`，构造协议数据并完成外部传输，并依次复用 `uuid4`、`range`，返回 `Any`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        tx_id = str(uuid4())
        last_exc: Optional[Exception] = None
        last_err_resp: Any = None

        for attempt in range(max_retries + 1):  # 0 = first try, 1-3 = retries
            try:
                await self._prepare_room_send(room_id)
                resp = await self._client.room_send(
                    room_id,
                    message_type,
                    content,
                    tx_id=tx_id,
                    ignore_unverified_devices=True,
                )

                # nio returns RoomSendError on failure instead of raising
                if isinstance(resp, RoomSendError):
                    if (
                        self._is_retryable_room_send_response(resp)
                        and attempt < max_retries
                    ):
                        # Prefer server-suggested delay for rate-limiting
                        retry_ms = getattr(resp, "retry_after_ms", None)
                        if (
                            retry_ms
                            and isinstance(retry_ms, (int, float))
                            and retry_ms > 0
                        ):
                            delay = retry_ms / 1000.0 + random.uniform(0, 0.5)
                        else:
                            delay = base_delay * (2**attempt) + random.uniform(0, 0.5)
                        logger.warning(
                            "MatrixChannel: room_send returned retryable error "
                            "(attempt %d/%d) room_id=%s status=%s message=%s — "
                            "retrying in %.1fs component=matrix",
                            attempt + 1,
                            max_retries,
                            room_id,
                            getattr(resp, "status_code", "?"),
                            getattr(resp, "message", ""),
                            delay,
                        )
                        last_err_resp = resp
                        await asyncio.sleep(delay)
                        continue
                    # Non-retryable or retries exhausted — let caller handle
                    if attempt >= max_retries and self._is_retryable_room_send_response(
                        resp
                    ):
                        logger.error(
                            "MatrixChannel: room_send retries exhausted "
                            "(attempts=%d) room_id=%s status=%s message=%s "
                            "component=matrix",
                            max_retries,
                            room_id,
                            getattr(resp, "status_code", "?"),
                            getattr(resp, "message", ""),
                        )
                    return resp

                # Success
                if attempt > 0:
                    logger.info(
                        "MatrixChannel: room_send succeeded after %d "
                        "retry(ies) room_id=%s component=matrix",
                        attempt,
                        room_id,
                    )
                return resp

            except Exception as exc:
                if self._is_retryable_send_error(exc) and attempt < max_retries:
                    delay = base_delay * (2**attempt) + random.uniform(0, 0.5)
                    logger.warning(
                        "MatrixChannel: room_send raised retryable exception "
                        "(attempt %d/%d) room_id=%s error=%s — "
                        "retrying in %.1fs component=matrix",
                        attempt + 1,
                        max_retries,
                        room_id,
                        exc,
                        delay,
                    )
                    last_exc = exc
                    await asyncio.sleep(delay)
                    continue
                # Non-retryable or retries exhausted
                if attempt >= max_retries and self._is_retryable_send_error(exc):
                    logger.error(
                        "MatrixChannel: room_send retries exhausted "
                        "(attempts=%d) room_id=%s last_error=%s "
                        "component=matrix",
                        max_retries,
                        room_id,
                        exc,
                    )
                raise

        # Should not reach here, but guard against it
        if last_exc is not None:
            raise last_exc
        return last_err_resp

    # ------------------------------------------------------------------
    # Outgoing send — text
    # Markdown→HTML (formatted_body); m.mentions when meta has sender_id.
    # ------------------------------------------------------------------

    async def send(
        self,
        to_handle: str,
        text: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        # 逻辑说明：`send` 接收 `to_handle`、`text`、`meta`，构造协议数据并完成外部传输，并依次复用 `error`、`get`，不返回业务结果。
        # 执行过程中包含异步等待/流式产出、外部 I/O、实例、文件或共享状态变更；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            logger.error(
                "MatrixChannel: send called but client not ready component=matrix"
            )
            return

        room_id = (meta or {}).get("room_id") or to_handle
        if self._response_is_stale(room_id, meta):
            logger.info(
                "MatrixChannel: suppressing stale response after /stop "
                "component=matrix room_id=%s",
                room_id,
            )
            await self._send_typing(room_id, False)
            return

        # NO_REPLY protocol: agent decided it has nothing to say.
        if _ends_with_no_reply_control(text):
            logger.info(
                "MatrixChannel: suppressing NO_REPLY send component=matrix room_id=%s",
                room_id,
            )
            await self._send_typing(room_id, False)
            return

        text = _clean_control_response_text(text)

        html_body = _md_to_html(text)
        meta_dict = meta if isinstance(meta, dict) else {}
        msgtype = (
            "m.notice" if meta_dict.pop(_MATRIX_FORCE_NOTICE_KEY, False) else "m.text"
        )
        content = self._matrix_text_content(
            room_id,
            text,
            meta_dict,
            msgtype,
            html_body=html_body,
        )
        logger.debug(
            "MatrixChannel (custom): sending message component=matrix with formatted_body "
            "text_len=%d html_len=%d",
            len(text),
            len(html_body),
        )

        try:
            if _matrix_event_payload_size(content) > MATRIX_TEXT_EVENT_SAFE_BYTES:
                event_id = await self._send_long_text_fallback(
                    room_id,
                    text,
                    meta_dict,
                    msgtype,
                )
                if (
                    event_id
                    and not meta_dict.get(_MATRIX_THREAD_META_KEY)
                    and not meta_dict.get(_MATRIX_OWN_THREAD_ROOT_KEY)
                ):
                    meta_dict[_MATRIX_OWN_THREAD_ROOT_KEY] = event_id
                    self._write_attachment_context(room_id, event_id)
                    await self._flush_pending_thread_parts(room_id, meta_dict)
                return
            resp = await self._room_send_with_retry(
                room_id,
                "m.room.message",
                content,
            )
            event_id = getattr(resp, "event_id", None)
            if (
                event_id
                and not meta_dict.get(_MATRIX_THREAD_META_KEY)
                and not meta_dict.get(_MATRIX_OWN_THREAD_ROOT_KEY)
                and not self._attachment_parent_event_id(meta_dict)
            ):
                meta_dict[_MATRIX_OWN_THREAD_ROOT_KEY] = event_id
                self._write_attachment_context(room_id, event_id)
                await self._flush_pending_thread_parts(room_id, meta_dict)
        except Exception as exc:
            logger.exception(
                "MatrixChannel: send failed to %s: %s",
                room_id,
                exc,
            )
        finally:
            await self._send_typing(room_id, False)

    # ------------------------------------------------------------------
    # Outgoing send — media
    # ------------------------------------------------------------------

    async def send_media(
        self,
        to_handle: str,
        part: Any,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Upload a local file to Matrix and send as m.image / m.file / etc."""
        # 逻辑说明：`send_media` 接收 `to_handle`、`part`、`meta`，构造协议数据并完成外部传输，并依次复用 `get`、`_response_is_stale`，返回 `Optional[str]`。
        # 执行过程中包含异步等待/流式产出、外部 I/O；保留现有异常和错误返回语义，避免失败或部分成功被误报为完整成功。
        if not self._client:
            return None

        room_id = (meta or {}).get("room_id") or to_handle
        if self._response_is_stale(room_id, meta):
            await self._send_typing(room_id, False)
            return None
        t = getattr(part, "type", None)

        # Extract the local file reference from the content part
        if t == ContentType.IMAGE:
            file_ref = getattr(part, "image_url", "")
            matrix_msgtype = "m.image"
        elif t == ContentType.VIDEO:
            file_ref = getattr(part, "video_url", "")
            matrix_msgtype = "m.video"
        elif t == ContentType.AUDIO:
            file_ref = getattr(part, "data", "")
            matrix_msgtype = "m.audio"
        elif t == ContentType.FILE:
            file_ref = getattr(part, "file_url", "") or getattr(
                part,
                "file_id",
                "",
            )
            matrix_msgtype = "m.file"
        else:
            return None

        if not file_ref:
            return None

        # Upload to Matrix media repository
        mxc_uri = await self._upload_file(file_ref)
        if not mxc_uri:
            logger.warning(
                "MatrixChannel: send_media upload failed for %s",
                file_ref,
            )
            return None

        # Build and send the Matrix room event
        try:
            await self._send_uploaded_media_event(
                room_id,
                file_ref,
                matrix_msgtype,
                mxc_uri,
                meta,
            )
            logger.debug(
                "MatrixChannel: sent %s %s to %s",
                matrix_msgtype,
                os.path.basename(file_url_to_local_path(file_ref) or file_ref)
                or "file",
                room_id,
            )
            return mxc_uri
        except Exception as exc:
            logger.exception(
                "MatrixChannel: send_media failed for %s: %s",
                room_id,
                exc,
            )
            return None
