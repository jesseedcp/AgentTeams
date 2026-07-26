from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from agentscope.state import AgentState
from agentscope.message import UserMsg

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

    async def reply_stream(self, *, inputs):
        self.state.summary = inputs.get_text_content() or ""
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
    assert stored.state.summary.startswith("[Current message]")
    assert "Sender ID: @admin:example" in stored.state.summary
    assert stored.state.summary.endswith("remember this")
    assert stored.last_event_id == "$event"


class ModelFactory(FakeAgentFactory):
    runtime_revision = 1

    def __init__(self) -> None:
        self.models: list[str | None] = []

    async def create(
        self,
        room_id,
        policy,
        state=None,
        model_override=None,
    ):
        self.models.append(model_override)
        return await super().create(room_id, policy, state)


@pytest.mark.asyncio
async def test_new_session_uses_persisted_room_model(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    factory = ModelFactory()
    manager = RoomSessionManager(
        factory=factory,
        sessions=repository,
    )
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)

    await manager.new(
        "!room:example",
        model_override="qwen-custom",
        now=now,
    )
    await manager.get_or_create("!room:example", room_policy())

    assert factory.models == ["qwen-custom"]
    settings = await repository.settings("!room:example", now=now)
    assert settings.model_override == "qwen-custom"


@pytest.mark.asyncio
async def test_compact_bounds_context_and_daily_reset_drops_state(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    manager = RoomSessionManager(
        factory=ModelFactory(),
        sessions=repository,
        session_timezone="Asia/Shanghai",
    )
    session = await manager.get_or_create("!room:example", room_policy())
    session.agent.state.context = [
        UserMsg(name="admin", content=f"message-{number}")
        for number in range(10)
    ]
    await manager.persist("!room:example")

    status = await manager.compact(
        "!room:example",
        keep_messages=2,
        summary_limit=200,
    )

    assert status.context_messages == 2
    assert "message-0" in session.agent.state.summary
    due = status.next_reset_at + timedelta(seconds=1)
    assert await manager.reset_due(due) == ("!room:example",)
    assert await repository.load("!room:example") is None
    advanced = await repository.settings("!room:example", now=due)
    assert advanced.next_reset_at > due
