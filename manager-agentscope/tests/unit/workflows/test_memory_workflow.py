from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.state.database import Database
from agentteams_manager.state.memory import MemoryRepository
from agentteams_manager.workflows.memory import ManagerMemoryService


@pytest.mark.asyncio
async def test_private_memory_is_recalled_but_not_project_room_leaked(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = MemoryRepository(database)
    service = ManagerMemoryService(repository, now=lambda: now)

    await service.remember(
        room_id="!admin:local",
        source_event_id="$preference:tool",
        category="preference",
        content="Use Chinese for status reports.",
        importance=8,
    )
    await service.record_project_decision(
        room_id="!project:local",
        source_event_id="$decision:tool",
        project_id="project-20260730-120000-abc123",
        decision="Keep SQLite",
        rationale="Avoid another service dependency.",
        visibility="project",
    )
    await service.record_project_decision(
        room_id="!admin:local",
        source_event_id="$private-decision:tool",
        project_id="project-20260730-120000-abc123",
        decision="Private budget ceiling",
        rationale="Administrator-only financial context.",
    )
    await service.record_worker_assessment(
        room_id="!admin:local",
        source_event_id="$assessment:tool",
        worker_name="alice",
        capability="python",
        score=0.9,
        evidence="Accepted task-1 without revision.",
    )

    private = await service.recall(
        room_id="!admin:local",
        include_private=True,
        project_id="project-20260730-120000-abc123",
        worker_name="alice",
        limit=20,
    )
    assert {item.kind for item in private.entries} >= {
        "daily",
        "long_term",
        "project_decision",
        "worker_assessment",
    }

    project = await service.recall(
        room_id="!project:local",
        include_private=False,
        project_id="project-20260730-120000-abc123",
        limit=20,
    )
    assert {item.kind for item in project.entries} == {
        "daily",
        "project_decision",
    }
    project_text = "\n".join(item.content for item in project.entries)
    assert "Use Chinese" not in project_text
    assert "Accepted task-1" not in project_text
    assert "Private budget ceiling" not in project_text


@pytest.mark.asyncio
async def test_memory_survives_database_reopen_and_projects_cold_context(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    path = tmp_path / "manager.db"
    database = Database(path)
    await database.open()
    first = ManagerMemoryService(
        MemoryRepository(database),
        now=lambda: now,
    )
    await first.record_project_decision(
        room_id="!project:local",
        source_event_id="$decision:call",
        project_id="project-20260730-120000-abc123",
        decision="Approve plan revision 2",
        rationale="Acceptance tests cover the changed scope.",
        visibility="project",
    )
    await database.close()

    reopened = Database(path)
    await reopened.open()
    recovered = ManagerMemoryService(
        MemoryRepository(reopened),
        now=lambda: now,
    )
    projection = await recovered.projection(
        room_id="!project:local",
        include_private=False,
        project_id="project-20260730-120000-abc123",
    )

    assert "[Durable Manager memory]" in projection
    assert "Approve plan revision 2" in projection
    assert "Verify live resource state" in projection
