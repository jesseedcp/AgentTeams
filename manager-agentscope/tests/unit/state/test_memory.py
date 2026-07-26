from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agentteams_manager.state.database import Database
from agentteams_manager.state.memory import MemoryRepository


@pytest.mark.asyncio
async def test_memory_categories_are_persisted_and_bounded(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    memory = MemoryRepository(database, per_scope_limit=2)
    now = datetime(2026, 7, 26, 10, 0, tzinfo=UTC)

    for number in range(3):
        await memory.curate_long_term(
            scope="room:!admin:local",
            category="preference",
            content=f"preference-{number}",
            importance=number,
            now=now + timedelta(minutes=number),
        )
    await memory.record_project_decision(
        project_id="project-1",
        decision="Use SQLite",
        rationale="No Redis dependency",
        now=now,
    )
    await memory.assess_worker(
        worker_name="alice",
        capability="python",
        score=0.9,
        evidence="Passed project-1",
        now=now,
    )
    await memory.append_daily(
        room_id="!admin:local",
        content="Project completed",
        source_event_id="$done",
        now=now,
    )

    long_term = await memory.long_term("room:!admin:local")
    assert [item.content for item in long_term] == [
        "preference-2",
        "preference-1",
    ]
    assert (await memory.project_decisions("project-1"))[0].decision == (
        "Use SQLite"
    )
    assert (await memory.worker_assessments("alice"))[0].score == 0.9
    assert (await memory.daily("!admin:local", now.date()))[0].content == (
        "Project completed"
    )
