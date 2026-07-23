from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskService
from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import (
    OrderedTaskRepository,
    TaskController,
    TaskMatrix,
    TaskStorage,
    TaskSupervisor,
)


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 23, 12, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


@pytest.mark.asyncio
async def test_record_execution_does_not_dispatch_again(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = MutableClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix(order)
    service = TaskService(
        tasks=OrderedTaskRepository(TaskRepository(database), order),
        storage=TaskStorage(
            MinioClient(FakeS3(), bucket="agentteams"),
            order,
        ),
        controller=TaskController(),
        matrix=matrix,
        supervisor=supervisor,
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
    )
    created = await service.create_recurring(
        title="Monitor releases",
        spec="Check the release feed.",
        assigned_to="alice",
        schedule="0 * * * *",
        timezone="UTC",
        context=MutationContext(
            room_id="!admin:example",
            event_id="$request",
            tool_call_id="create-recurring",
        ),
    )
    send_count = len(matrix.attempts)

    clock.value = datetime(2026, 7, 23, 13, 1, tzinfo=UTC)
    execution = await service.record_execution(
        task_id=created.task_id,
        worker_event_id="$executed",
    )

    assert execution.status == "active"
    assert execution.next_scheduled_at == datetime(
        2026,
        7,
        23,
        14,
        tzinfo=UTC,
    )
    assert len(matrix.attempts) == send_count
