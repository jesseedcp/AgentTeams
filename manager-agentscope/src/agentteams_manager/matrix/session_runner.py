"""Direct Matrix-to-AgentScope turn execution."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from agentscope.event import (
    ConfirmResult,
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
    UserConfirmResultEvent,
)
from agentscope.message import TextBlock, UserMsg
from agentscope.state import AgentState

from agentteams_manager.domain.errors import ConflictError, NotFoundError
from agentteams_manager.domain.ids import (
    matrix_transaction_id,
    operation_id_for,
)
from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.runtime.event_stream import EventStreamProjector
from agentteams_manager.runtime.messages import current_message_text
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.confirmations import (
    ConfirmationRequest,
    ConfirmationService,
)

from .media import MediaAdapter
from .threads import RoomHistory


class MatrixOutput(Protocol):
    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str: ...

    async def edit_text(
        self,
        room_id: str,
        event_id: str,
        text: str,
        *,
        txn_id: str,
    ) -> str: ...


class ConfirmationNotifications(Protocol):
    async def send_confirmation_request(
        self,
        *,
        confirmation_id: str,
        text: str,
    ) -> object: ...


class SessionMemory(Protocol):
    async def append_daily(
        self,
        *,
        room_id: str,
        content: str,
        source_event_id: str,
        now: datetime,
    ) -> object: ...

    async def curate_long_term(
        self,
        *,
        scope: str,
        category: str,
        content: str,
        importance: float,
        now: datetime,
    ) -> object: ...


class MatrixSessionRunner:
    """Drive one room-scoped Agent through its native streaming API."""

    def __init__(
        self,
        *,
        sessions: RoomSessionManager,
        matrix: MatrixOutput,
        admin_user_id: str,
        confirmations: ConfirmationService,
        admin_room_id: str = "!admin:local",
        confirmation_notifications: ConfirmationNotifications | None = None,
        history: RoomHistory | None = None,
        media: MediaAdapter | None = None,
        memory: SessionMemory | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
        confirmation_ttl: timedelta = timedelta(minutes=15),
        edit_interval_seconds: float = 0.5,
        metrics: Any | None = None,
    ) -> None:
        self._sessions = sessions
        self._matrix = matrix
        self._admin_user_id = admin_user_id
        self._admin_room_id = admin_room_id
        self._confirmations = confirmations
        self._confirmation_notifications = confirmation_notifications
        self._history = history or RoomHistory(limit=0)
        self._media = media
        self._memory = memory
        self._monotonic = monotonic
        self._now = now or (lambda: datetime.now(UTC))
        self._confirmation_ttl = confirmation_ttl
        self._edit_interval = edit_interval_seconds
        self._metrics = metrics

    async def handle(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        if self._metrics is not None:
            self._metrics.increment(
                "agentteams_manager_matrix_turns_total",
            )
            self._metrics.increment(
                "agentteams_manager_model_turns_total",
            )
        await self._mark_read(event.room_id, event.event_id)
        await self._set_typing(event.room_id, True)
        try:
            await self._expire_confirmations()
            pending = await self._confirmations.pending()
            await self._sessions.reset_due(
                self._now(),
                exclude_rooms=frozenset(
                    request.source_room_id for request in pending
                ),
            )
            if await self._handle_session_command(event, policy):
                return
            if await self._handle_global_confirmation(event, policy):
                return
            await self._run_user_turn(event, policy)
        finally:
            await self._set_typing(event.room_id, False)

    async def _run_user_turn(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        attachments = ()
        if event.media:
            if self._media is None:
                raise RuntimeError("Matrix media adapter is not configured")
            attachments = await self._media.download(event)
        history_projection = self._history.prefix(
            event.room_id,
            exclude_event_id=event.event_id,
        )
        message = UserMsg(
            name=event.sender_id,
            content=[
                TextBlock(text=current_message_text(event)),
                *attachments,
            ],
            id=event.event_id,
            created_at=event.timestamp.isoformat(),
            metadata={
                "room_id": event.room_id,
                "event_id": event.event_id,
                "sender_id": event.sender_id,
                "thread_id": event.thread_id,
            },
        )
        await self._run_and_project(
            event,
            policy,
            message,
            transient_context=history_projection,
        )

    async def _handle_global_confirmation(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> bool:
        parsed = _global_confirmation_command(event.body)
        if parsed is not None:
            action, confirmation_id = parsed
            self._require_confirmation_admin(event, policy)
            try:
                if action == "status":
                    await self._send_confirmation_status(event)
                    return True
                if confirmation_id is None:
                    confirmation_id = await self._single_pending_id(event)
                    if confirmation_id is None:
                        return True
                if action == "reset":
                    await self._cancel_confirmation(event, confirmation_id)
                    return True
                await self._resolve_global_confirmation(
                    event,
                    confirmation_id,
                    confirmed=action == "confirm",
                )
            except (ConflictError, NotFoundError, ValueError) as error:
                await self._send_confirmation_error(event, str(error))
            return True

        pending = await self._confirmations.pending_for_room(event.room_id)
        if pending is not None:
            await self._send_pending_confirmation_reminder(event, pending)
            return True
        return False

    async def _expire_confirmations(self) -> None:
        for expired in await self._confirmations.expire_due():
            await self._sessions.reset(expired.source_room_id)
            await self._send_confirmation_notice(
                expired.source_room_id,
                expired.confirmation_id,
                "审批请求已过期，原房间会话已解除等待状态。",
                sequence=3,
            )

    async def _handle_session_command(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> bool:
        try:
            parsed = _session_command(event.body)
        except ValueError as error:
            await self._send_session_command_result(
                event,
                f"无法执行会话命令：{error}",
                action="error",
            )
            return True
        if parsed is None:
            return False
        action, argument = parsed
        pending = await self._confirmations.pending()
        source_pending = tuple(
            request
            for request in pending
            if request.source_room_id == event.room_id
        )
        if (
            action == "reset"
            and pending
            and event.sender_id == self._admin_user_id
            and policy.kind is RoomKind.ADMIN_DM
        ):
            return False
        if action in {"new", "compact"} and source_pending:
            await self._send_session_command_result(
                event,
                "当前会话有操作等待审批，请先批准、拒绝或取消审批。",
                action=action,
            )
            return True
        if action == "new":
            status = await self._sessions.new(
                event.room_id,
                model_override=argument,
                now=self._now(),
            )
            model = status.model_override or "运行时默认模型"
            await self._send_session_command_result(
                event,
                f"已创建全新会话。模型：{model}",
                action=action,
            )
            return True
        if action == "reset":
            current = await self._sessions.status(event.room_id)
            await self._sessions.new(
                event.room_id,
                model_override=current.model_override,
                now=self._now(),
            )
            await self._send_session_command_result(
                event,
                "已清空当前会话上下文，房间模型设置保持不变。",
                action=action,
            )
            return True
        if action == "compact":
            status = await self._sessions.compact(event.room_id)
            summary = status.summary.strip() or (
                f"Session context contains "
                f"{status.context_messages} retained messages."
            )
            if self._memory is not None:
                now = self._now().astimezone(UTC)
                await self._memory.append_daily(
                    room_id=event.room_id,
                    content=summary,
                    source_event_id=event.event_id,
                    now=now,
                )
                await self._memory.curate_long_term(
                    scope=f"room:{event.room_id}",
                    category="session_summary",
                    content=summary,
                    importance=5,
                    now=now,
                )
            await self._send_session_command_result(
                event,
                "会话压缩完成："
                f"保留 {status.context_messages} 条上下文，"
                f"摘要 {status.summary_characters} 个字符。",
                action=action,
            )
            return True
        await self._send_session_status(event, policy, pending)
        return True

    async def _send_session_status(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        pending: tuple[ConfirmationRequest, ...],
    ) -> None:
        status = await self._sessions.status(event.room_id)
        model = status.model_override or "运行时默认模型"
        lines = [
            "会话状态：",
            f"- 房间类型：{policy.kind.value}",
            f"- 会话 ID：{status.session_id or '(尚未创建)'}",
            f"- 模型：{model}",
            f"- 上下文消息：{status.context_messages}",
            f"- 摘要字符：{status.summary_characters}",
            f"- 下次 04:00 重置：{status.next_reset_at.isoformat()}",
        ]
        if pending:
            lines.append(f"- 等待审批：{len(pending)}")
            lines.extend(
                f"  - {request.confirmation_id}: "
                f"{', '.join(call.name for call in request.tool_calls)}"
                for request in pending
            )
        else:
            lines.append("- 等待审批：0")
        await self._send_session_command_result(
            event,
            "\n".join(lines),
            action="status",
        )

    async def _send_session_command_result(
        self,
        event: InboundEvent,
        text: str,
        *,
        action: str,
    ) -> None:
        await self._matrix.send_text(
            event.room_id,
            text,
            txn_id=matrix_transaction_id(
                operation_id_for(
                    event.room_id,
                    event.event_id,
                    f"session-{action}",
                ),
                0,
            ),
            thread_id=event.thread_id,
        )

    async def _send_confirmation_error(
        self,
        event: InboundEvent,
        detail: str,
    ) -> None:
        await self._matrix.send_text(
            event.room_id,
            f"无法处理审批请求：{detail}",
            txn_id=matrix_transaction_id(
                operation_id_for(
                    event.room_id,
                    event.event_id,
                    "confirmation-error",
                ),
                0,
            ),
            thread_id=event.thread_id,
        )

    async def _single_pending_id(
        self,
        event: InboundEvent,
    ) -> str | None:
        pending = await self._confirmations.pending()
        if len(pending) == 1:
            return pending[0].confirmation_id
        if not pending:
            text = "当前没有等待审批的请求。"
        else:
            lines = [
                "当前有多个等待审批的请求，请使用明确的请求 ID：",
                *(
                    f"- {item.confirmation_id}: "
                    f"{', '.join(call.name for call in item.tool_calls)}"
                    for item in pending
                ),
            ]
            text = "\n".join(lines)
        await self._matrix.send_text(
            event.room_id,
            text,
            txn_id=matrix_transaction_id(
                operation_id_for(
                    event.room_id,
                    event.event_id,
                    "confirmation-selection",
                ),
                0,
            ),
            thread_id=event.thread_id,
        )
        return None

    async def _send_confirmation_status(
        self,
        event: InboundEvent,
    ) -> None:
        pending = await self._confirmations.pending()
        text = (
            "当前没有等待审批的请求。"
            if not pending
            else "\n".join(
                [
                    "等待审批：",
                    *(
                        f"- {item.confirmation_id} | "
                        f"房间 {item.source_room_id} | "
                        f"{', '.join(call.name for call in item.tool_calls)}"
                        for item in pending
                    ),
                ],
            )
        )
        await self._matrix.send_text(
            event.room_id,
            text,
            txn_id=matrix_transaction_id(
                operation_id_for(
                    event.room_id,
                    event.event_id,
                    "confirmation-status",
                ),
                0,
            ),
            thread_id=event.thread_id,
        )

    async def _cancel_confirmation(
        self,
        event: InboundEvent,
        confirmation_id: str,
    ) -> None:
        request = await self._confirmations.cancel(
            confirmation_id,
            admin_id=event.sender_id,
        )
        await self._sessions.reset(request.source_room_id)
        await self._send_confirmation_notice(
            request.source_room_id,
            confirmation_id,
            "管理员已取消该审批，原房间会话已重置。",
            sequence=2,
        )

    async def _resolve_global_confirmation(
        self,
        event: InboundEvent,
        confirmation_id: str,
        *,
        confirmed: bool,
    ) -> None:
        request = await self._confirmations.resolve(
            confirmation_id,
            admin_id=event.sender_id,
            decision=confirmed,
        )
        continuation = UserConfirmResultEvent(
            reply_id=request.source_reply_id,
            confirm_results=[
                ConfirmResult(
                    confirmed=confirmed,
                    tool_call=tool_call,
                )
                for tool_call in request.tool_calls
            ],
        )
        source_event = InboundEvent(
            room_id=request.source_room_id,
            event_id=event.event_id,
            sender=event.sender_id,
            body=event.body,
            timestamp=event.timestamp,
            is_direct=(
                request.source_policy.kind is RoomKind.ADMIN_DM
            ),
            thread_id=None,
        )
        await self._set_typing(request.source_room_id, True)
        try:
            await self._run_and_project(
                source_event,
                request.source_policy,
                continuation,
                tool_event_id=request.source_event_id,
            )
        finally:
            await self._set_typing(request.source_room_id, False)
        completed = await self._confirmations.complete(confirmation_id)
        decision_text = (
            "管理员已批准该操作。"
            if completed.decision
            else "管理员已拒绝该操作。"
        )
        await self._send_confirmation_notice(
            request.source_room_id,
            confirmation_id,
            decision_text,
            sequence=2,
        )

    def _require_confirmation_admin(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        if (
            event.sender_id != self._admin_user_id
            or policy.kind is not RoomKind.ADMIN_DM
        ):
            raise PermissionError(
                "only the admin may resolve Manager confirmations",
            )

    async def _send_pending_confirmation_reminder(
        self,
        event: InboundEvent,
        pending: ConfirmationRequest,
    ) -> None:
        tools = ", ".join(call.name for call in pending.tool_calls)
        confirmation_id = pending.confirmation_id
        prompt = (
            f"仍有操作等待管理员审批：{tools}\n"
            "这条消息尚未交给模型处理。请先在管理员私聊中处理：\n"
            f"/confirm {confirmation_id}\n"
            f"/deny {confirmation_id}"
        )
        reminder_operation = operation_id_for(
            event.room_id,
            event.event_id,
            confirmation_id,
        )
        await self._matrix.send_text(
            event.room_id,
            prompt,
            txn_id=matrix_transaction_id(reminder_operation, 0),
            thread_id=event.thread_id,
        )

    async def _run_and_project(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        inputs: Any,
        *,
        tool_event_id: str | None = None,
        transient_context: str = "",
    ) -> None:
        projector = EventStreamProjector()
        operation_id = operation_id_for(
            event.room_id,
            event.event_id,
            "matrix-reply",
        )
        sequence = 0
        sent_event_id: str | None = None
        last_sent_text = ""
        last_edit_at = 0.0
        pending: RequireUserConfirmEvent | None = None

        def remember_confirmation(
            agent_event: object,
            state: AgentState,
        ) -> None:
            nonlocal pending
            del state
            if not isinstance(agent_event, RequireUserConfirmEvent):
                return
            pending = agent_event

        async for agent_event in self._sessions.run_input(
            event,
            policy,
            inputs,
            tool_event_id=tool_event_id,
            on_event=remember_confirmation,
            transient_context=transient_context,
        ):
            projection = await projector.accept(agent_event)
            if isinstance(agent_event, TextBlockDeltaEvent) and projection.text:
                now = self._monotonic()
                if sent_event_id is None:
                    sent_event_id = await self._matrix.send_text(
                        event.room_id,
                        projection.text,
                        txn_id=matrix_transaction_id(
                            operation_id,
                            sequence,
                        ),
                        thread_id=event.thread_id,
                    )
                    sequence += 1
                    last_sent_text = projection.text
                    last_edit_at = now
                elif now - last_edit_at >= self._edit_interval:
                    await self._matrix.edit_text(
                        event.room_id,
                        sent_event_id,
                        projection.text,
                        txn_id=matrix_transaction_id(
                            operation_id,
                            sequence,
                        ),
                    )
                    sequence += 1
                    last_sent_text = projection.text
                    last_edit_at = now
        final = projector.snapshot().text
        if final and sent_event_id is None:
            sent_event_id = await self._matrix.send_text(
                event.room_id,
                final,
                txn_id=matrix_transaction_id(operation_id, sequence),
                thread_id=event.thread_id,
            )
            sequence += 1
        elif final and final != last_sent_text:
            await self._matrix.edit_text(
                event.room_id,
                sent_event_id,
                final,
                txn_id=matrix_transaction_id(operation_id, sequence),
            )
            sequence += 1

        if pending is not None:
            await self._create_global_confirmation(
                event,
                policy,
                pending,
            )

    async def _create_global_confirmation(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        pending: RequireUserConfirmEvent,
    ) -> None:
        now = self._now().astimezone(UTC)
        confirmation_id = operation_id_for(
            event.room_id,
            event.event_id,
            pending.reply_id,
        )
        request = await self._confirmations.create(
            ConfirmationRequest(
                confirmation_id=confirmation_id,
                source_room_id=event.room_id,
                source_event_id=event.event_id,
                source_reply_id=pending.reply_id,
                requester_id=event.sender_id,
                tool_calls=tuple(pending.tool_calls),
                source_policy=policy,
                created_at=now,
                expires_at=now + self._confirmation_ttl,
            ),
        )
        approval_text = _approval_prompt(request)
        if event.room_id == self._admin_room_id:
            await self._matrix.send_text(
                self._admin_room_id,
                approval_text,
                txn_id=matrix_transaction_id(confirmation_id, 0),
                thread_id=event.thread_id,
            )
            return
        await self._send_confirmation_notice(
            event.room_id,
            confirmation_id,
            "该操作需要管理员批准，已发送给管理员审批。",
            sequence=0,
        )
        if self._confirmation_notifications is not None:
            await self._confirmation_notifications.send_confirmation_request(
                confirmation_id=confirmation_id,
                text=approval_text,
            )
        else:
            await self._matrix.send_text(
                self._admin_room_id,
                approval_text,
                txn_id=matrix_transaction_id(confirmation_id, 1),
                mentions=(self._admin_user_id,),
            )

    async def _send_confirmation_notice(
        self,
        room_id: str,
        confirmation_id: str,
        text: str,
        *,
        sequence: int,
    ) -> None:
        await self._matrix.send_text(
            room_id,
            f"{text}\n请求 ID：{confirmation_id}",
            txn_id=matrix_transaction_id(confirmation_id, sequence),
        )

    async def _set_typing(self, room_id: str, enabled: bool) -> None:
        method = getattr(self._matrix, "set_typing", None)
        if method is None:
            return
        try:
            await method(room_id, typing=enabled)
        except Exception:
            return

    async def _mark_read(self, room_id: str, event_id: str) -> None:
        method = getattr(self._matrix, "mark_read", None)
        if method is None:
            return
        try:
            await method(room_id, event_id)
        except Exception:
            return


def _global_confirmation_command(
    body: str,
) -> tuple[str, str | None] | None:
    normalized = body.strip()
    if normalized in {"确认", "同意", "确认保存"}:
        return "confirm", None
    if normalized in {"拒绝", "取消"}:
        return "deny", None
    parts = normalized.split()
    if not parts:
        return None
    commands = {
        "/confirm": "confirm",
        "/deny": "deny",
        "/reset": "reset",
        "/status": "status",
    }
    action = commands.get(parts[0].lower())
    if action is None or len(parts) > 2:
        return None
    if action == "status" and len(parts) != 1:
        return None
    return action, parts[1] if len(parts) == 2 else None


_MODEL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


def _session_command(
    body: str,
) -> tuple[str, str | None] | None:
    parts = body.strip().split()
    if not parts:
        return None
    command = parts[0].lower()
    if command == "/new" and len(parts) <= 2:
        model = parts[1] if len(parts) == 2 else None
        if model is not None and _MODEL_NAME.fullmatch(model) is None:
            raise ValueError("invalid model name")
        return "new", model
    if command in {"/reset", "/compact", "/status"} and len(parts) == 1:
        return command[1:], None
    return None


def _approval_prompt(request: ConfirmationRequest) -> str:
    calls = "\n".join(
        f"- {call.name}: {_summarize_arguments(call.input)}"
        for call in request.tool_calls
    )
    return (
        "需要管理员批准\n"
        f"请求 ID：{request.confirmation_id}\n"
        f"来源房间：{request.source_room_id}\n"
        f"请求人：{request.requester_id}\n"
        f"操作与参数摘要：\n{calls}\n"
        f"批准：/confirm {request.confirmation_id}\n"
        f"拒绝：/deny {request.confirmation_id}\n"
        f"取消并重置：/reset {request.confirmation_id}"
    )


def _summarize_arguments(raw: object) -> str:
    value: object = raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return "[unparseable arguments]"
    redacted = _redact_secrets(value)
    summary = json.dumps(
        redacted,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return summary[:300]


def _redact_secrets(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): (
                "[REDACTED]"
                if _looks_secret(str(key))
                else _redact_secrets(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    return value


def _looks_secret(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(
        marker in normalized
        for marker in (
            "password",
            "secret",
            "token",
            "api_key",
            "access_key",
            "private_key",
        )
    )
