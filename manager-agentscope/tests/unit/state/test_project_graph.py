from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import TaskRecord
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import (
    ProjectGraphRepository,
    ProjectTaskState,
    TaskRepository,
)


def _task(task_id: str) -> TaskRecord:
    now = datetime(2026, 7, 26, 8, 0, tzinfo=UTC)
    return TaskRecord(
        task_id=task_id,
        task_type="finite",
        status=ProjectTaskState.PENDING,
        title=task_id,
        assigned_to="alice",
        room_id="!alice:local",
        project_id="project-1",
        metadata={},
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_project_dependency_cycle_is_rejected(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    tasks = TaskRepository(database)
    graph = ProjectGraphRepository(database)
    for task_id in ("task-a", "task-b", "task-c"):
        await tasks.create(_task(task_id))
    await graph.set_dependencies("task-b", ("task-a",))
    await graph.set_dependencies("task-c", ("task-b",))

    with pytest.raises(ConflictError, match="cycle"):
        await graph.set_dependencies("task-a", ("task-c",))

    assert await graph.dependencies("task-a") == ()


@pytest.mark.asyncio
async def test_completed_dependency_promotes_next_task_to_ready(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    tasks = TaskRepository(database)
    graph = ProjectGraphRepository(database)
    await tasks.create(_task("task-a"))
    await tasks.create(_task("task-b"))
    await graph.set_dependencies("task-b", ("task-a",))

    first = await graph.promote_ready("project-1")
    assert tuple(item.task_id for item in first) == ("task-a",)

    await graph.transition(
        "task-a",
        expected={ProjectTaskState.READY},
        target=ProjectTaskState.DISPATCHED,
        actor_id="@manager:local",
    )
    await graph.transition(
        "task-a",
        expected={ProjectTaskState.DISPATCHED},
        target=ProjectTaskState.IN_PROGRESS,
        actor_id="@alice:local",
    )
    await graph.transition(
        "task-a",
        expected={ProjectTaskState.IN_PROGRESS},
        target=ProjectTaskState.COMPLETED,
        actor_id="@alice:local",
    )

    second = await graph.promote_ready("project-1")

    assert tuple(item.task_id for item in second) == ("task-b",)
    transitions = await graph.transitions("task-a")
    assert tuple(item.to_status for item in transitions) == (
        ProjectTaskState.READY,
        ProjectTaskState.DISPATCHED,
        ProjectTaskState.IN_PROGRESS,
        ProjectTaskState.COMPLETED,
    )
