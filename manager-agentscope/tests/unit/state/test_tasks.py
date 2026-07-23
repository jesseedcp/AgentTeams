from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agentteams_manager.domain.models import TaskRecord
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import TaskRepository


def recurring_task(
    task_id: str,
    *,
    next_at: datetime,
    last_at: datetime | None = None,
) -> TaskRecord:
    now = datetime.now(UTC)
    return TaskRecord(
        task_id=task_id,
        task_type="recurring",
        status="active",
        title=task_id,
        assigned_to="alice",
        room_id="!alice:example",
        schedule="*/5 * * * *",
        timezone="UTC",
        last_executed_at=last_at,
        next_scheduled_at=next_at,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_due_schedules_exclude_future_and_executed_occurrences(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TaskRepository(database)
    now = datetime.now(UTC)
    due_at = now - timedelta(minutes=1)
    await repository.create(recurring_task("task-due", next_at=due_at))
    await repository.create(
        recurring_task(
            "task-done",
            next_at=due_at,
            last_at=due_at,
        ),
    )
    await repository.create(
        recurring_task(
            "task-future",
            next_at=now + timedelta(minutes=1),
        ),
    )

    due = await repository.due_schedules(now)

    assert [task.task_id for task in due] == ["task-due"]

