"""Direct Matrix-to-AgentScope turn execution.

把一个已授权 Matrix 事件执行成完整的 AgentScope turn。

真实链路是：解析斜杠命令或构造 ``UserMsg``，加载房间历史、媒体和受限记忆，运行
AgentScope ``reply_stream``，把公开 event 投影成 Matrix 消息。若 AgentScope 请求确认，
本模块会持久化 continuation 并在 Admin DM 发审批通知；稍后 ``/confirm`` 恢复的是原
tool call，而不是重新让模型生成一次。内部 reasoning 不会原样发布到房间。
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from agentscope.event import (
    ConfirmResult,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import TextBlock, UserMsg
from agentscope.state import AgentState

from agentteams_manager.domain.errors import (
    ConflictError,
    ManagerError,
    NotFoundError,
)
from agentteams_manager.domain.ids import (
    matrix_transaction_id,
    operation_id_for,
)
from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
    SessionCommandAction,
)
from agentteams_manager.runtime.event_stream import (
    EventStreamProjector,
    StreamProjection,
)
from agentteams_manager.runtime.messages import current_message_text
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.confirmations import (
    ConfirmationRequest,
    ConfirmationService,
)
from agentteams_manager.state.sessions import SessionSettings

from .commands import parse_session_command
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


class SessionMedia(Protocol):
    async def download(
        self,
        event: InboundEvent,
    ) -> tuple[Any, ...]: ...


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


class SessionMemoryProjection(Protocol):
    async def projection(
        self,
        *,
        room_id: str,
        include_private: bool,
        project_id: str | None = None,
    ) -> str: ...


class TaskProtocolReader(Protocol):
    async def get(self, task_id: str) -> Any | None: ...


class ProjectProtocolWorkflow(Protocol):
    async def complete_task(
        self,
        *,
        project_id: str,
        task_id: str,
        worker_event_id: str,
        sender_id: str,
        structured_result: dict[str, Any] | None = None,
    ) -> Any: ...

    async def report_blocked(
        self,
        *,
        project_id: str,
        task_id: str,
        sender_id: str,
        reason: str,
    ) -> Any: ...


class MatrixSessionRunner:
    """驱动一个 room-scoped Agent，并把 turn 结果安全写回 Matrix。

    输入的 event 已由 router claim、policy resolver 授权；输出端使用同一 source event
    派生 transaction ID。若流中出现 ``RequireUserConfirmEvent``，runner 会保存 AgentState
    和 tool call，发出审批请求并停止本 turn，确保确认后能从准确位置续跑。
    """

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
        media: SessionMedia | None = None,
        memory: SessionMemory | None = None,
        memory_service: SessionMemoryProjection | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
        confirmation_ttl: timedelta = timedelta(minutes=15),
        edit_interval_seconds: float = 0.5,
        metrics: Any | None = None,
        known_models: Mapping[str, bool] | None = None,
        default_model: str | None = None,
        known_models_provider: (
            Callable[[], Mapping[str, bool]] | None
        ) = None,
        default_model_provider: Callable[[], str | None] | None = None,
        task_reader: TaskProtocolReader | None = None,
        project_workflow: ProjectProtocolWorkflow | None = None,
    ) -> None:
        # 逻辑说明：保存会话、Matrix、审批、媒体和记忆依赖，并冻结模型目录及时间策略；构造阶段不发送消息，真实副作用从 handle/handle_control 开始。
        self._sessions = sessions
        self._matrix = matrix
        self._admin_user_id = admin_user_id
        self._admin_room_id = admin_room_id
        self._confirmations = confirmations
        self._confirmation_notifications = confirmation_notifications
        self._history = history or RoomHistory(limit=0)
        self._media = media
        self._memory = memory
        self._memory_service = memory_service
        self._monotonic = monotonic
        self._now = now or (lambda: datetime.now(UTC))
        self._confirmation_ttl = confirmation_ttl
        self._edit_interval = edit_interval_seconds
        self._metrics = metrics
        self._known_models = dict(known_models or {})
        self._default_model = (default_model or "").strip() or None
        self._known_models_provider = known_models_provider
        self._default_model_provider = default_model_provider
        self._task_reader = task_reader
        self._project_workflow = project_workflow

    async def handle(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        """处理一条普通入站事件，直到回复、审批暂停或确定性失败。"""
        # 逻辑说明：为普通事件记录指标和已读/输入状态，依次处理过期审批、定时重置、命令、审批命令和任务协议，全部未命中才启动模型 turn；finally 必定关闭输入提示。
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
            if await self._handle_task_protocol(event):
                return
            await self._run_user_turn(event, policy)
        finally:
            await self._set_typing(event.room_id, False)

    async def _handle_task_protocol(
        self,
        event: InboundEvent,
    ) -> bool:
        # 逻辑说明：识别 Worker 的 TASK_BLOCKED 协议并核对任务属于 Project，再通过工作流持久化阻塞原因；TASK_COMPLETED 只作为模型唤醒信号，不在此自动验收。
        completed = _parse_task_completed(event.body)
        if completed is not None:
            # A completion mention is a wake-up signal, not proof that the
            # result satisfies the task. Let the Agent inspect the durable
            # result and explicitly accept, revise, or block it with tools.
            return False

        blocked = _parse_task_blocked(event.body)
        if blocked is None:
            return False
        if (
            self._task_reader is None
            or self._project_workflow is None
        ):
            return False
        task_id, reason = blocked
        task = await self._task_reader.get(task_id)
        if task is None or not task.project_id:
            await self._send_task_protocol_result(
                event,
                f"无法记录任务 {task_id} 的 BLOCKED："
                "任务不存在或不属于 Project。",
                task_id=task_id,
            )
            return True
        project_id = str(task.project_id)
        if str(task.status) != "blocked":
            try:
                await self._project_workflow.report_blocked(
                    project_id=project_id,
                    task_id=task_id,
                    sender_id=event.sender_id,
                    reason=reason,
                )
            except ManagerError as error:
                await self._send_task_protocol_result(
                    event,
                    f"无法记录任务 {task_id} 的 BLOCKED：{error}",
                    task_id=task_id,
                )
                return True
        await self._send_task_protocol_result(
            event,
            f"已记录任务 {task_id} 为 blocked。\n"
            f"阻塞原因：{reason}",
            task_id=task_id,
        )
        return True

    async def _send_task_protocol_result(
        self,
        event: InboundEvent,
        text: str,
        *,
        task_id: str,
    ) -> None:
        # 逻辑说明：用来源事件和 task_id 派生稳定 transaction ID，把协议处理结果回发原线程并提及上报者；Matrix 发送失败由上层事件重试处理。
        operation_id = operation_id_for(
            event.room_id,
            event.event_id,
            f"task-protocol:{task_id}",
        )
        await self._matrix.send_text(
            event.room_id,
            text,
            txn_id=matrix_transaction_id(operation_id, 0),
            thread_id=event.thread_id,
            mentions=(event.sender_id,),
        )

    async def handle_control(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        """Handle queue-bypassing commands such as /stop."""
        # 逻辑说明：只允许已授权、非 silent 的精确 /stop 绕过普通房间队列；命中后先标已读再取消当前 turn，其余控制输入不产生副作用。
        if (
            policy.silent
            or event.sender_id not in policy.allowed_senders
        ):
            return
        parsed = parse_session_command(event.body)
        if (
            parsed is None
            or parsed.action is not SessionCommandAction.STOP
        ):
            return
        await self._mark_read(event.room_id, event.event_id)
        await self._send_stop_result(event)

    async def _run_user_turn(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        # 逻辑说明：下载当前事件附件，拼接排除自身的房间历史与权限允许的记忆投影，构造成带 Matrix 元数据的 UserMsg 后交给流式执行器。
        attachments: tuple[Any, ...] = ()
        if event.media:
            if self._media is None:
                raise RuntimeError("Matrix media adapter is not configured")
            attachments = await self._media.download(event)
        history_projection = self._history.prefix(
            event.room_id,
            exclude_event_id=event.event_id,
        )
        memory_projection = (
            await self._memory_service.projection(
                room_id=event.room_id,
                include_private=(
                    policy.kind is RoomKind.ADMIN_DM
                    and event.sender_id == self._admin_user_id
                ),
                project_id=policy.project_id,
            )
            if self._memory_service is not None
            else ""
        )
        transient_context = "\n\n".join(
            part
            for part in (memory_projection, history_projection)
            if part.strip()
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
            transient_context=transient_context,
        )

    async def _handle_global_confirmation(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> bool:
        # 逻辑说明：在管理员私聊解析 /confirm、/deny、/reset、/status 或自然语言确认；无命令但原房间有悬挂审批时发送提醒并阻止新消息进入模型。
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
        # 逻辑说明：批量领取到期审批，逐项重置来源房间会话并用固定序号通知过期；这样不会留下永远等待 continuation 的 Agent 状态。
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
        # 逻辑说明：确定性解析所有会话斜杠命令，阻止与悬挂审批冲突的 reset/new/compact，并分别查询或更新模型、思考、显示、审批、队列和上下文设置后回复。
        confirmation_command = _global_confirmation_command(event.body)
        if confirmation_command is not None:
            confirmation_action, confirmation_id = confirmation_command
            if (
                confirmation_action in {"confirm", "deny"}
                or (
                    confirmation_action == "reset"
                    and confirmation_id is not None
                )
            ):
                return False
        try:
            parsed = parse_session_command(event.body)
        except ValueError as error:
            await self._send_session_command_result(
                event,
                f"无法执行会话命令：{error}",
                action="error",
            )
            return True
        if parsed is None:
            return False
        action = parsed.action
        arguments = parsed.arguments
        pending = await self._confirmations.pending()
        source_pending = tuple(
            request
            for request in pending
            if request.source_room_id == event.room_id
        )
        if (
            action is SessionCommandAction.RESET
            and pending
            and event.sender_id == self._admin_user_id
            and policy.kind is RoomKind.ADMIN_DM
        ):
            return False
        if (
            action
            in {SessionCommandAction.NEW, SessionCommandAction.COMPACT}
            and source_pending
        ):
            await self._send_session_command_result(
                event,
                "当前会话有操作等待审批，请先批准、拒绝或取消审批。",
                action=action,
            )
            return True
        if action is SessionCommandAction.UNKNOWN:
            await self._send_session_command_result(
                event,
                f"未知命令：{parsed.source_name}\n"
                "使用 /help 查看可用命令。",
                action="unknown",
            )
            return True
        if action is SessionCommandAction.HELP:
            await self._send_session_command_result(
                event,
                _short_command_help(),
                action=action,
            )
            return True
        if action is SessionCommandAction.COMMANDS:
            await self._send_session_command_result(
                event,
                _command_catalog(),
                action=action,
            )
            return True
        if action is SessionCommandAction.MODELS:
            await self._send_session_command_result(
                event,
                self._model_catalog(),
                action=action,
            )
            return True
        if action is SessionCommandAction.MODEL:
            await self._handle_model_command(
                event,
                policy,
                arguments[0] if arguments else None,
            )
            return True
        if action is SessionCommandAction.THINK:
            await self._handle_think_command(
                event,
                arguments[0] if arguments else None,
            )
            return True
        if action is SessionCommandAction.REASONING:
            await self._handle_choice_command(
                event,
                field="reasoning_visibility",
                label="推理摘要显示",
                value=arguments[0] if arguments else None,
                choices=frozenset({"off", "on", "stream"}),
                action=action,
            )
            return True
        if action is SessionCommandAction.VERBOSE:
            await self._handle_choice_command(
                event,
                field="verbose_mode",
                label="工具摘要详细度",
                value=arguments[0] if arguments else None,
                choices=frozenset({"off", "on", "full"}),
                action=action,
            )
            return True
        if action is SessionCommandAction.ELEVATED:
            await self._handle_elevated_command(
                event,
                policy,
                arguments[0] if arguments else None,
            )
            return True
        if action is SessionCommandAction.QUEUE:
            await self._handle_queue_command(event, arguments)
            return True
        if action is SessionCommandAction.STOP:
            await self._send_stop_result(event)
            return True
        if action is SessionCommandAction.NEW:
            argument = arguments[0] if arguments else None
            status = await self._sessions.new(
                event.room_id,
                model_override=argument,
                now=self._now(),
            )
            model = self._effective_model(status.model_override)
            await self._send_session_command_result(
                event,
                f"已创建全新会话。模型：{model}",
                action=action,
            )
            return True
        if action is SessionCommandAction.RESET:
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
        if action is SessionCommandAction.COMPACT:
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

    async def _handle_model_command(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        argument: str | None,
    ) -> None:
        # 逻辑说明：处理 /model 的列表、状态和切换分支；模型参数先由目录/序号校验，再保留原上下文切换会话模型，非法输入仅回复错误不改设置。
        normalized = argument.lower() if argument is not None else "list"
        if normalized == "list":
            await self._send_session_command_result(
                event,
                self._model_catalog(),
                action="model-list",
            )
            return
        if normalized == "status":
            status = await self._sessions.status(event.room_id)
            await self._send_session_command_result(
                event,
                "当前会话模型："
                f"{self._effective_model(status.model_override)}",
                action="model-status",
            )
            return
        try:
            selected = self._resolve_model(argument)
        except ValueError as error:
            await self._send_session_command_result(
                event,
                f"无法切换模型：{error}",
                action="model-error",
            )
            return
        status = await self._sessions.switch_model(
            event.room_id,
            policy,
            selected,
        )
        await self._send_session_command_result(
            event,
            "已切换当前房间模型，原会话上下文已保留。模型："
            f"{self._effective_model(status.model_override)}",
            action="model",
        )

    async def _handle_think_command(
        self,
        event: InboundEvent,
        argument: str | None,
    ) -> None:
        # 逻辑说明：读取并校验 /think 等级，on 兼容映射为 medium；若当前已知模型不支持 reasoning 则拒绝更新，否则持久化房间思考设置。
        settings = await self._sessions.settings(event.room_id)
        if argument is None or argument.lower() == "status":
            await self._send_session_command_result(
                event,
                "当前思考级别："
                f"{settings.thinking_effort or 'default'}",
                action="think-status",
            )
            return
        normalized = argument.lower()
        if normalized == "on":
            normalized = "medium"
        selected = None if normalized == "default" else normalized
        allowed = {"off", "minimal", "low", "medium", "high", "xhigh"}
        if selected is not None and selected not in allowed:
            await self._send_session_command_result(
                event,
                "思考级别必须是 default、off、minimal、low、"
                "medium、high 或 xhigh。",
                action="think-error",
            )
            return
        model = self._effective_model(settings.model_override)
        known_models = self._current_known_models()
        if (
            selected not in {None, "off"}
            and model in known_models
            and not known_models[model]
        ):
            await self._send_session_command_result(
                event,
                f"模型 {model} 不支持思考模式，设置未改变。",
                action="think-error",
            )
            return
        updated = await self._sessions.update_settings(
            event.room_id,
            thinking_effort=selected,
        )
        await self._send_session_command_result(
            event,
            "思考级别已设置为："
            f"{updated.thinking_effort or 'default'}",
            action="think",
        )

    async def _handle_choice_command(
        self,
        event: InboundEvent,
        *,
        field: str,
        label: str,
        value: str | None,
        choices: frozenset[str],
        action: SessionCommandAction,
    ) -> None:
        # 逻辑说明：作为 reasoning/verbose/elevated 的通用枚举设置器，status 只查询，非法值只回复允许集合，合法值才调用 update_settings 并报告新值。
        settings = await self._sessions.settings(event.room_id)
        current = str(getattr(settings, field))
        if value is None or value.lower() == "status":
            await self._send_session_command_result(
                event,
                f"当前{label}：{current}",
                action=f"{action}-status",
            )
            return
        normalized = value.lower()
        if normalized not in choices:
            await self._send_session_command_result(
                event,
                f"{label}必须是：{', '.join(sorted(choices))}",
                action=f"{action}-error",
            )
            return
        updated = await self._sessions.update_settings(
            event.room_id,
            **{field: normalized},
        )
        await self._send_session_command_result(
            event,
            f"{label}已设置为：{getattr(updated, field)}",
            action=action,
        )

    async def _handle_elevated_command(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        value: str | None,
    ) -> None:
        # 逻辑说明：强制 /elevated 只能由管理员在管理员私聊修改，将旧别名 on 归一为 ask，再复用枚举设置器持久化 off/ask/full 策略。
        if (
            event.sender_id != self._admin_user_id
            or event.sender_id not in policy.allowed_senders
            or policy.kind is not RoomKind.ADMIN_DM
        ):
            await self._send_session_command_result(
                event,
                "仅管理员可以在管理员私聊修改 elevated 确认策略。",
                action="elevated-denied",
            )
            return
        normalized = value.lower() if value is not None else "status"
        if normalized == "on":
            normalized = "ask"
        await self._handle_choice_command(
            event,
            field="elevated_mode",
            label="elevated 确认策略",
            value=normalized,
            choices=frozenset({"off", "ask", "full"}),
            action=SessionCommandAction.ELEVATED,
        )

    async def _handle_queue_command(
        self,
        event: InboundEvent,
        arguments: tuple[str, ...],
    ) -> None:
        # 逻辑说明：查询或设置房间 followup/collect/interrupt 队列模式，把 queue 旧别名映射为 followup，并严格限制队列上限为 1..100。
        settings = await self._sessions.settings(event.room_id)
        if not arguments or arguments[0].lower() == "status":
            await self._send_session_command_result(
                event,
                "当前队列："
                f"{settings.queue_mode}，上限 {settings.queue_limit}",
                action="queue-status",
            )
            return
        mode = arguments[0].lower()
        if mode == "queue":
            mode = "followup"
        if mode not in {"followup", "collect", "interrupt"}:
            await self._send_session_command_result(
                event,
                "队列模式必须是 followup、collect 或 interrupt。",
                action="queue-error",
            )
            return
        try:
            limit = (
                int(arguments[1])
                if len(arguments) == 2
                else settings.queue_limit
            )
        except ValueError:
            limit = 0
        if not 1 <= limit <= 100:
            await self._send_session_command_result(
                event,
                "队列上限必须是 1 到 100 的整数。",
                action="queue-error",
            )
            return
        updated = await self._sessions.update_settings(
            event.room_id,
            queue_mode=mode,
            queue_limit=limit,
        )
        await self._send_session_command_result(
            event,
            f"队列已设置为 {updated.queue_mode}，"
            f"上限 {updated.queue_limit}。",
            action="queue",
        )

    async def _send_stop_result(self, event: InboundEvent) -> None:
        # 逻辑说明：请求 RoomSessionManager 取消本房间当前 task，并根据是否确有运行任务生成不同提示，再通过会话命令回执使用稳定 txn_id 发送。
        stopped = await self._sessions.cancel(event.room_id)
        text = (
            "已停止当前任务，未完成的回复不会写入会话历史。"
            if stopped
            else "当前没有正在运行的任务。"
        )
        await self._send_session_command_result(
            event,
            text,
            action="stop",
        )

    def _resolve_model(self, argument: str | None) -> str | None:
        # 逻辑说明：把 default 映射为无 override、数字映射为模型目录序号，并允许目录中的名称或显式 provider/model；缺失和越界输入抛 ValueError。
        if argument is None:
            raise ValueError("missing model")
        normalized = argument.lower()
        if normalized == "default":
            return None
        known_models = self._current_known_models()
        if argument.isdigit():
            index = int(argument)
            models = tuple(known_models)
            if not 1 <= index <= len(models):
                raise ValueError("model number is outside the list")
            return models[index - 1]
        if argument in known_models or "/" in argument:
            return argument
        raise ValueError(
            "model is not in the configured list; use /models",
        )

    def _model_catalog(self) -> str:
        # 逻辑说明：按当前动态模型目录生成带序号的用户可读列表，并标记有效默认模型；目录为空返回明确提示，不改变任何会话设置。
        known_models = self._current_known_models()
        if not known_models:
            return "当前没有已配置的已知模型。"
        default_model = self._effective_default_model()
        lines = ["可用模型："]
        lines.extend(
            f"{index}. {model}"
            + (
                "（当前默认）"
                if model == default_model
                else ""
            )
            for index, model in enumerate(known_models, start=1)
        )
        lines.append("使用 /model <序号|provider/model> 切换。")
        return "\n".join(lines)

    async def _send_session_status(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        pending: tuple[ConfirmationRequest, ...],
    ) -> None:
        # 逻辑说明：读取房间会话状态、有效模型和全局待审批项，汇总设置、上下文、重置时间及审批工具后发送 /status 回执，不修改会话。
        status = await self._sessions.status(event.room_id)
        model = self._effective_model(status.model_override)
        lines = [
            "会话状态：",
            f"- 房间类型：{policy.kind.value}",
            f"- 会话 ID：{status.session_id or '(尚未创建)'}",
            f"- 模型：{model}",
            f"- 思考级别：{status.thinking_effort or 'default'}",
            f"- 推理摘要：{status.reasoning_visibility}",
            f"- 工具摘要：{status.verbose_mode}",
            f"- elevated：{status.elevated_mode}",
            f"- 队列：{status.queue_mode}（上限 {status.queue_limit}）",
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

    def _effective_model(self, override: str | None) -> str:
        return (
            override
            or self._effective_default_model()
            or "运行时默认模型"
        )

    def _effective_default_model(self) -> str | None:
        # 逻辑说明：优先调用动态默认模型 provider 并忽略其空白结果，未提供有效动态值时回退构造时冻结的默认模型。
        if self._default_model_provider is not None:
            model = (self._default_model_provider() or "").strip()
            if model:
                return model
        return self._default_model

    def _current_known_models(self) -> dict[str, bool]:
        # 逻辑说明：从动态 provider 读取模型到“是否支持思考”的映射，清理空名称并复制返回；动态目录为空时回退构造时快照，避免外部修改内部字典。
        if self._known_models_provider is not None:
            models = {
                model.strip(): reasoning
                for model, reasoning in self._known_models_provider().items()
                if model.strip()
            }
            if models:
                return models
        return dict(self._known_models)

    async def _send_session_command_result(
        self,
        event: InboundEvent,
        text: str,
        *,
        action: str,
    ) -> None:
        # 逻辑说明：用来源房间、事件和命令 action 派生幂等 transaction ID，把命令结果发回原 thread；同一事件重试不会生成另一条独立回执。
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
        # 逻辑说明：把审批解析或状态冲突异常转换成管理员可见消息，并以 confirmation-error 派生稳定 txn_id；本函数不改变审批记录。
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
        # 逻辑说明：省略请求 ID 时仅在全局恰有一个待审批项才自动选择；零个或多个会列出原因/候选并返回 None，避免批准错对象。
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
        # 逻辑说明：读取所有 pending 审批并格式化请求 ID、来源房间和工具名，以稳定 status transaction 回管理员房间；这是只读查询。
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
        # 逻辑说明：以管理员身份原子取消指定审批，再重置来源房间以丢弃等待 continuation 的 Agent 状态，并通知原房间取消结果。
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
        # 逻辑说明：领取审批并构造与原 reply/tool call 对应的 UserConfirmResultEvent，在来源房间续跑；若重启导致 continuation 丢失则取消并重置，成功后才 complete 并通知决定。
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
            try:
                await self._run_and_project(
                    source_event,
                    request.source_policy,
                    continuation,
                    tool_event_id=request.source_event_id,
                )
            except ValueError as error:
                if "not waiting for user confirmation" not in str(error):
                    raise
                await self._confirmations.cancel(
                    confirmation_id,
                    admin_id=event.sender_id,
                )
                await self._sessions.reset(request.source_room_id)
                await self._send_confirmation_notice(
                    request.source_room_id,
                    confirmation_id,
                    "审批续跑状态在 Manager 重启后无法安全恢复；"
                    "系统已取消悬挂审批并重置原房间会话。"
                    "已执行的幂等操作不会回滚，请先查询资源实际状态；"
                    "如操作尚未完成，请重新发起。",
                    sequence=2,
                )
                return
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
        # 逻辑说明：同时核验发送者是配置管理员且当前策略属于 ADMIN_DM，不满足即抛 PermissionError，防止其他房间解析审批命令后越权执行。
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
        # 逻辑说明：来源房间仍有 pending 时列出工具和管理员应使用的 confirm/deny 命令，并按新消息派生 txn_id；该消息不会交给模型。
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
        # 逻辑说明：运行房间 Agent 的事件流，按节流间隔首次发送或编辑公开投影；收集同一 reply 的确认工具调用，流结束后创建 durable 审批而不自行执行工具。
        projector = EventStreamProjector()
        settings = await self._sessions.settings(event.room_id)
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
            # 逻辑说明：回调只捕获 RequireUserConfirmEvent，按 tool_call.id 合并同一 reply 的多批调用；若一次 run 出现不同 reply_id 立即报错，避免错误续跑。
            nonlocal pending
            del state
            if not isinstance(agent_event, RequireUserConfirmEvent):
                return
            if pending is None:
                pending = agent_event
                return
            if pending.reply_id != agent_event.reply_id:
                raise RuntimeError(
                    "one AgentScope run requested confirmation for "
                    "multiple replies",
                )
            tool_calls = {
                tool_call.id: tool_call
                for tool_call in pending.tool_calls
            }
            tool_calls.update(
                {
                    tool_call.id: tool_call
                    for tool_call in agent_event.tool_calls
                },
            )
            pending = pending.model_copy(
                update={"tool_calls": list(tool_calls.values())},
            )

        async for agent_event in self._sessions.run_input(
            event,
            policy,
            inputs,
            tool_event_id=tool_event_id,
            on_event=remember_confirmation,
            transient_context=transient_context,
        ):
            projection = await projector.accept(agent_event)
            visible = _stream_projection_text(projection, settings)
            if visible and visible != last_sent_text:
                now = self._monotonic()
                if sent_event_id is None:
                    sent_event_id = await self._matrix.send_text(
                        event.room_id,
                        visible,
                        txn_id=matrix_transaction_id(
                            operation_id,
                            sequence,
                        ),
                        thread_id=event.thread_id,
                    )
                    sequence += 1
                    last_sent_text = visible
                    last_edit_at = now
                elif now - last_edit_at >= self._edit_interval:
                    await self._matrix.edit_text(
                        event.room_id,
                        sent_event_id,
                        visible,
                        txn_id=matrix_transaction_id(
                            operation_id,
                            sequence,
                        ),
                    )
                    sequence += 1
                    last_sent_text = visible
                    last_edit_at = now
        final = _final_projection_text(projector.snapshot(), settings)
        if final and sent_event_id is None:
            sent_event_id = await self._matrix.send_text(
                event.room_id,
                final,
                txn_id=matrix_transaction_id(operation_id, sequence),
                thread_id=event.thread_id,
            )
            sequence += 1
        elif final and final != last_sent_text:
            assert sent_event_id is not None
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
        # 逻辑说明：由来源事件和 reply_id 生成稳定审批 ID，持久化工具调用、权限快照与到期时间；原房间先获通知，完整审批提示投递管理员渠道。
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
        # 逻辑说明：把审批状态和请求 ID 发给指定房间，transaction ID 由 confirmation_id 与阶段 sequence 派生，使创建、决定、过期通知可重试且互不覆盖。
        await self._matrix.send_text(
            room_id,
            f"{text}\n请求 ID：{confirmation_id}",
            txn_id=matrix_transaction_id(confirmation_id, sequence),
        )

    async def _set_typing(self, room_id: str, enabled: bool) -> None:
        # 逻辑说明：兼容可选 Matrix set_typing 能力并切换房间输入提示；适配器缺失或网络失败均静默忽略，因为辅助 UI 状态不能中断主 turn。
        method = getattr(self._matrix, "set_typing", None)
        if method is None:
            return
        try:
            await method(room_id, typing=enabled)
        except Exception:
            return

    async def _mark_read(self, room_id: str, event_id: str) -> None:
        # 逻辑说明：兼容可选 Matrix mark_read 能力并推进指定事件回执；适配器缺失或失败不向上抛，防止已读状态故障阻塞消息处理。
        method = getattr(self._matrix, "mark_read", None)
        if method is None:
            return
        try:
            await method(room_id, event_id)
        except Exception:
            return


def _parse_task_completed(body: str) -> tuple[str, str] | None:
    # 逻辑说明：逐行清理 Markdown 包装并完整匹配 TASK_COMPLETED、Task ID 与可选摘要；首个合法报告返回结构化结果，否则返回 None，不写任务状态。
    for raw_line in body.splitlines():
        line = raw_line.strip().replace("**", "").replace("`", "")
        line = re.sub(r"^[*-]\s+", "", line)
        task_match = re.fullmatch(
            r"(?:@[^\s]+\s+)?TASK_COMPLETED\s*[:：]\s*"
            r"(task-[A-Za-z0-9][A-Za-z0-9_-]*)"
            r"(?:\s*(?:[-—–]\s*)?(.+))?",
            line,
            flags=re.IGNORECASE,
        )
        if task_match is None:
            continue
        task_id = task_match.group(1)
        summary = (task_match.group(2) or "").strip(" *_`-—–")
        return task_id, summary or "Worker reported TASK_COMPLETED"
    return None


def _parse_task_blocked(body: str) -> tuple[str, str] | None:
    # 逻辑说明：从多行 Worker 文本提取 BLOCKED Task ID，并兼容同行或独立“阻塞原因/reason”；没有 Task ID 返回 None，缺原因使用明确默认说明。
    task_id: str | None = None
    reason: str | None = None
    for raw_line in body.splitlines():
        line = raw_line.strip().replace("**", "").replace("`", "")
        line = re.sub(r"^[*-]\s+", "", line)
        task_match = re.fullmatch(
            r"(?:@[^\s]+\s+)?(?:TASK_)?BLOCKED\s*[:：]\s*"
            r"(task-[A-Za-z0-9][A-Za-z0-9_-]*)"
            r"(?:\s*(?:[-—–]\s*)?(.+))?",
            line,
            flags=re.IGNORECASE,
        )
        if task_match is not None:
            task_id = task_match.group(1)
            inline_reason = (task_match.group(2) or "").strip(" *_`-—–")
            if inline_reason:
                reason = inline_reason
            continue
        reason_match = re.match(
            r"(?:阻塞原因|reason)\s*[:：]\s*(.+)",
            line,
            flags=re.IGNORECASE,
        )
        if reason_match is not None:
            reason = reason_match.group(1).strip(" *_`")
    if task_id is None:
        return None
    return task_id, reason or "Worker reported TASK_BLOCKED"


def _global_confirmation_command(
    body: str,
) -> tuple[str, str | None] | None:
    # 逻辑说明：把自然语言确认/拒绝及 /confirm、/deny、/reset、/status 解析为动作和可选 ID；参数个数不合规则返回 None，权限检查留给 runner。
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


def _short_command_help() -> str:
    return (
        "会话命令：\n"
        "/new [model] · /reset · /compact · /status\n"
        "/model [list|status|default|序号|provider/model] · /models\n"
        "/stop · /think · /reasoning · /verbose\n"
        "/elevated · /queue · /help · /commands"
    )


def _command_catalog() -> str:
    return (
        "完整命令目录：\n"
        "- /new [model]：新建空白会话\n"
        "- /reset：清空上下文并保留房间设置\n"
        "- /compact：压缩旧上下文\n"
        "- /status：查看会话状态\n"
        "- /model、/models：列出、查看或切换模型\n"
        "- /stop：立即取消当前运行\n"
        "- /think <default|off|minimal|low|medium|high|xhigh>\n"
        "- /reasoning <off|on|stream>：只显示安全推理状态\n"
        "- /verbose <off|on|full>：控制工具执行摘要\n"
        "- /elevated <off|ask|full>：off 仅高风险操作审批；"
        "ask 所有工具审批；full 完全免审批（仅管理员私聊）\n"
        "- /queue <followup|collect|interrupt> [1-100]\n"
        "- /help：简要帮助\n"
        "- /commands：本目录"
    )


def _stream_projection_text(
    projection: StreamProjection,
    settings: SessionSettings,
) -> str:
    # 逻辑说明：从流式投影选取当前可公开正文；仅在 stream 模式以固定安全文案表示正在推理，full 模式附加工具状态，绝不暴露模型内部思维。
    text = projection.text
    if (
        not text
        and projection.thinking_observed
        and settings.reasoning_visibility == "stream"
    ):
        text = "模型正在推理（内部思维内容不会公开）…"
    if settings.verbose_mode == "full" and projection.tool_calls:
        text = _append_public_section(
            text,
            _tool_summary(projection),
        )
    return text


def _final_projection_text(
    projection: StreamProjection,
    settings: SessionSettings,
) -> str:
    # 逻辑说明：turn 结束时组合最终正文、安全的“使用了推理”摘要和按 verbose 设置允许的工具概览，生成用于最后一次发送/编辑的文本。
    text = projection.text
    if (
        projection.thinking_observed
        and settings.reasoning_visibility in {"on", "stream"}
    ):
        text = _append_public_section(
            text,
            "推理摘要：模型使用了推理模式；内部思维内容未公开。",
        )
    if settings.verbose_mode in {"on", "full"} and projection.tool_calls:
        text = _append_public_section(text, _tool_summary(projection))
    return text


def _tool_summary(projection: StreamProjection) -> str:
    # 逻辑说明：只把 projector 已脱敏的工具名称与执行状态格式化为列表，不包含工具参数、返回数据或内部 reasoning。
    lines = ["工具执行："]
    lines.extend(
        f"- {tool.name} ({tool.state})"
        for tool in projection.tool_calls
    )
    return "\n".join(lines)


def _append_public_section(text: str, section: str) -> str:
    return f"{text}\n\n{section}" if text else section


def _approval_prompt(request: ConfirmationRequest) -> str:
    # 逻辑说明：生成管理员审批卡片，包含请求 ID、来源、请求人、逐个工具及脱敏参数，并列出三种精确命令；仅格式化，不更改请求状态。
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
    # 逻辑说明：字符串参数先按 JSON 解析，失败返回固定占位；结构化值递归脱敏后以稳定键序压缩成最多 300 字符，供审批提示展示。
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
    # 逻辑说明：递归复制字典和列表，疑似密钥字段整值替换为 [REDACTED]，其他标量原样返回；不修改原始 tool input。
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
    # 逻辑说明：先统一字段名大小写和连字符，再检查 password、token、api/access/private key 等标记，为递归脱敏提供保守判定。
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
