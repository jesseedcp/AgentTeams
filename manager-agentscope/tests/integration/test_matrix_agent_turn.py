from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from agentscope.event import TextBlockDeltaEvent
from agentscope.state import AgentState

from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.session_runner import MatrixSessionRunner
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.database import Database
from agentteams_manager.state.confirmations import (
    ConfirmationRepository,
    ConfirmationService,
)
from agentteams_manager.state.sessions import SessionRepository


class RecordingMatrix:
    def __init__(self) -> None:
        self.sent: list[SimpleNamespace] = []

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        self.sent.append(
            SimpleNamespace(
                kind="send",
                room_id=room_id,
                text=text,
                txn_id=txn_id,
                thread_id=thread_id,
                mentions=mentions,
            ),
        )
        return "$message"

    async def edit_text(
        self,
        room_id: str,
        event_id: str,
        text: str,
        *,
        txn_id: str,
    ) -> str:
        self.sent.append(
            SimpleNamespace(
                kind="edit",
                room_id=room_id,
                event_id=event_id,
                text=text,
                txn_id=txn_id,
            ),
        )
        return "$edit"


class ReplyAgent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self.reply_stream_inputs: list[object] = []

    async def reply_stream(self, *, inputs: object):
        self.reply_stream_inputs.append(inputs)
        yield TextBlockDeltaEvent(
            reply_id="reply",
            block_id="text",
            delta="There are ",
        )
        yield TextBlockDeltaEvent(
            reply_id="reply",
            block_id="text",
            delta="2 workers.",
        )


class Factory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agents: list[ReplyAgent] = []

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> ReplyAgent:
        del policy
        agent = ReplyAgent(room_id)
        if state is not None:
            agent.state = state
        self.agents.append(agent)
        return agent


def _event(body: str = "list workers") -> InboundEvent:
    return InboundEvent(
        room_id="!admin:local",
        event_id="$request",
        sender="@admin:local",
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=True,
    )


def _policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_senders=frozenset({"@admin:local"}),
    )


@pytest.mark.asyncio
async def test_runner_calls_reply_stream_directly(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    sessions = RoomSessionManager(
        factory=factory,
        sessions=SessionRepository(database),
    )
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
        monotonic=iter((0.0, 0.1)).__next__,
    )

    await runner.handle(_event(), _policy())

    message = factory.agents[0].reply_stream_inputs[0]
    assert message.name == "@admin:local"
    assert message.metadata["event_id"] == "$request"
    assert [item.kind for item in matrix.sent] == ["send", "edit"]
    assert matrix.sent[-1].text == "There are 2 workers."
    stored = await SessionRepository(database).load("!admin:local")
    assert stored is not None
    assert stored.last_event_id == "$request"
