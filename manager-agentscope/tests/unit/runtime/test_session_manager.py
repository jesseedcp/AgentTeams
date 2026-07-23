from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.state import AgentState

from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import SessionRepository


class FakeAgent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")

    async def reply_stream(self, message):
        self.state.summary = message.get_text_content() or ""
        if False:
            yield None


class FakeAgentFactory:
    async def create(self, room_id, policy, state=None):
        del policy
        agent = FakeAgent(room_id)
        if state is not None:
            agent.state = state
        return agent


def room_policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!room:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
    )


@pytest.mark.asyncio
async def test_session_id_is_matrix_room_id(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    manager = RoomSessionManager(
        factory=FakeAgentFactory(),
        sessions=SessionRepository(database),
    )

    session = await manager.get_or_create(
        "!room:example",
        room_policy(),
    )
    same = await manager.get_or_create(
        "!room:example",
        room_policy(),
    )

    assert session.agent.state.session_id == "matrix:!room:example"
    assert same is session


@pytest.mark.asyncio
async def test_completed_turn_persists_agent_state(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    sessions = SessionRepository(database)
    manager = RoomSessionManager(
        factory=FakeAgentFactory(),
        sessions=sessions,
    )
    event = InboundEvent(
        room_id="!room:example",
        event_id="$event",
        sender="@admin:example",
        body="remember this",
        timestamp=datetime.now(UTC),
    )

    assert [item async for item in manager.run(event, room_policy())] == []

    stored = await sessions.load("!room:example")
    assert stored is not None
    assert stored.state.summary == "remember this"
    assert stored.last_event_id == "$event"
