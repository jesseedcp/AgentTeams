from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.workflows.heartbeat import TaskHeartbeat
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
async def test_due_recurring_occurrence_is_visible_only_once(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TaskRepository(database)
    order: list[str] = []
    clock = MutableClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix(order)
    service = TaskService(
        tasks=OrderedTaskRepository(repository, order),
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
    clock.value = datetime(2026, 7, 23, 13, 31, tzinfo=UTC)
    heartbeat = TaskHeartbeat(tasks=repository, service=service)

    first = await heartbeat.dispatch_due(clock.value)
    second = await heartbeat.dispatch_due(clock.value)

    assert first.dispatched == (created.task_id,)
    assert first.late == (created.task_id,)
    assert second.already_dispatched == (created.task_id,)
    assert len(matrix.visible) == 1
