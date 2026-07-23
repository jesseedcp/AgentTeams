from pathlib import Path

import pytest
from agentscope.state import AgentState

from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import SessionRepository


@pytest.mark.asyncio
async def test_agent_state_round_trip(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    state = AgentState(session_id="matrix:!room:example")
    state.summary = "compressed context"

    await repository.save(
        room_id="!room:example",
        state=state,
        policy_revision=3,
        last_event_id="$event",
    )
    restored = await repository.load("!room:example")

    assert restored is not None
    assert restored.state.session_id == "matrix:!room:example"
    assert restored.state.summary == "compressed context"
    assert restored.policy_revision == 3
    assert restored.last_event_id == "$event"

