"""Direct Matrix-to-AgentScope turn execution."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Protocol

from agentscope.event import (
    ConfirmResult,
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
    UserConfirmResultEvent,
)
from agentscope.message import TextBlock, UserMsg
from agentscope.state import AgentState

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
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.sessions import (
    PendingConfirmation,
    clear_pending_confirmation,
    pending_confirmation,
    set_pending_confirmation,
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


class MatrixSessionRunner:
    """Drive one room-scoped Agent through its native streaming API."""

    def __init__(
        self,
        *,
        sessions: RoomSessionManager,
        matrix: MatrixOutput,
        admin_user_id: str,
        history: RoomHistory | None = None,
        media: MediaAdapter | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        edit_interval_seconds: float = 0.5,
    ) -> None:
        self._sessions = sessions
        self._matrix = matrix
        self._admin_user_id = admin_user_id
        self._history = history or RoomHistory(limit=0)
        self._media = media
        self._monotonic = monotonic
        self._edit_interval = edit_interval_seconds

    async def handle(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        command = _confirmation_command(event.body)
        if command is not None:
            await self._handle_confirmation(event, policy, *command)
            return

        attachments = ()
        if event.media:
            if self._media is None:
                raise RuntimeError("Matrix media adapter is not configured")
            attachments = await self._media.download(event)
        history_prefix = self._history.prefix(
            event.room_id,
            exclude_event_id=event.event_id,
        )
        message = UserMsg(
            name=event.sender_id,
            content=[
                TextBlock(text=history_prefix + event.body),
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
        await self._run_and_project(event, policy, message)

    async def _handle_confirmation(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        confirmed: bool,
        reply_id: str,
    ) -> None:
        if (
            event.sender_id != self._admin_user_id
            or policy.kind is not RoomKind.ADMIN_DM
        ):
            raise PermissionError(
                "only the admin may resolve Manager confirmations",
            )
        session = await self._sessions.get_or_create(
            event.room_id,
            policy,
        )
        pending = pending_confirmation(session.agent.state)
        if pending is None or pending.reply_id != reply_id:
            raise ValueError("confirmation does not match a pending reply")
        results = [
            ConfirmResult(confirmed=confirmed, tool_call=tool_call)
            for tool_call in pending.tool_calls
        ]
        continuation = UserConfirmResultEvent(
            reply_id=reply_id,
            confirm_results=results,
        )
        await self._run_and_project(
            event,
            policy,
            continuation,
            tool_event_id=pending.event_id,
        )
        clear_pending_confirmation(session.agent.state)
        await self._sessions.persist(event.room_id)

    async def _run_and_project(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        inputs: Any,
        *,
        tool_event_id: str | None = None,
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
        pending: PendingConfirmation | None = None

        def remember_confirmation(
            agent_event: object,
            state: AgentState,
        ) -> None:
            nonlocal pending
            if not isinstance(agent_event, RequireUserConfirmEvent):
                return
            pending = PendingConfirmation(
                reply_id=agent_event.reply_id,
                event_id=event.event_id,
                tool_calls=tuple(agent_event.tool_calls),
            )
            set_pending_confirmation(state, pending)

        async for agent_event in self._sessions.run_input(
            event,
            policy,
            inputs,
            tool_event_id=tool_event_id,
            on_event=remember_confirmation,
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
            tools = ", ".join(call.name for call in pending.tool_calls)
            prompt = (
                f"Confirmation required for: {tools}\n"
                f"/confirm {pending.reply_id}\n"
                f"/deny {pending.reply_id}"
            )
            confirmation_operation = operation_id_for(
                event.room_id,
                event.event_id,
                pending.reply_id,
            )
            await self._matrix.send_text(
                event.room_id,
                prompt,
                txn_id=matrix_transaction_id(
                    confirmation_operation,
                    0,
                ),
                thread_id=event.thread_id,
            )


def _confirmation_command(body: str) -> tuple[bool, str] | None:
    parts = body.strip().split()
    if len(parts) != 2:
        return None
    if parts[0] == "/confirm":
        return True, parts[1]
    if parts[0] == "/deny":
        return False, parts[1]
    return None
