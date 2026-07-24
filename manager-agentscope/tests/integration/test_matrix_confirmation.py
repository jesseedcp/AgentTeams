from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.event import (
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
    UserConfirmResultEvent,
)
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState

from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.session_runner import MatrixSessionRunner
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import (
    SessionRepository,
    pending_confirmation,
)
from tests.integration.test_matrix_agent_turn import RecordingMatrix


class ConfirmationAgent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self.confirmation_results: list[UserConfirmResultEvent] = []

    async def reply_stream(self, *, inputs: object):
        if isinstance(inputs, UserConfirmResultEvent):
            self.confirmation_results.append(inputs)
            yield TextBlockDeltaEvent(
                reply_id=inputs.reply_id,
                block_id="text",
                delta="Deleted alice.",
            )
            return
        yield RequireUserConfirmEvent(
            reply_id="reply-delete",
            tool_calls=[
                ToolCallBlock(
                    id="call-delete",
                    name="delete_worker",
                    input='{"name":"alice"}',
                ),
            ],
        )


class Factory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agent: ConfirmationAgent | None = None

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> ConfirmationAgent:
        del policy
        self.agent = ConfirmationAgent(room_id)
        if state is not None:
            self.agent.state = state
        return self.agent


def _event(body: str, event_id: str, sender: str = "@admin:local") -> InboundEvent:
    return InboundEvent(
        room_id="!admin:local",
        event_id=event_id,
        sender=sender,
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=True,
    )


def _policy(sender: str = "@admin:local") -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_senders=frozenset({sender}),
    )


@pytest.mark.asyncio
async def test_confirmation_continues_same_reply(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    factory = Factory()
    sessions = RoomSessionManager(factory=factory, sessions=repository)
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
    )

    await asyncio.wait_for(
        runner.handle(
            _event("delete alice", "$delete"),
            _policy(),
        ),
        timeout=1,
    )

    prompt = matrix.sent[-1]
    assert "/confirm reply-delete" in prompt.text
    stored = await repository.load("!admin:local")
    assert stored is not None
    assert pending_confirmation(stored.state).reply_id == "reply-delete"

    await runner.handle(
        _event("/confirm reply-delete", "$confirm"),
        _policy(),
    )

    assert factory.agent is not None
    result = factory.agent.confirmation_results[0]
    assert result.reply_id == "reply-delete"
    assert result.confirm_results[0].confirmed
    assert matrix.sent[-1].text == "Deleted alice."


@pytest.mark.asyncio
async def test_non_admin_cannot_resolve_confirmation(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    sessions = RoomSessionManager(
        factory=factory,
        sessions=SessionRepository(database),
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
    )
    await runner.handle(_event("delete alice", "$delete"), _policy())

    with pytest.raises(PermissionError, match="admin"):
        await runner.handle(
            _event(
                "/confirm reply-delete",
                "$intruder",
                sender="@intruder:local",
            ),
            _policy("@intruder:local"),
        )
