from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskService
from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import (
    FixedClock,
    OrderedTaskRepository,
    TaskController,
    TaskMatrix,
    TaskStorage,
    TaskSupervisor,
)


@pytest.mark.asyncio
async def test_restart_reuses_assignment_transaction_id(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix(order)
    matrix.timeout_once = True
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
    context = MutationContext(
        room_id="!admin:example",
        event_id="$request",
        tool_call_id="create-task",
    )

    with pytest.raises(TimeoutError):
        await service.create_finite(
            title="Fix login",
            spec="Acceptance: tests pass",
            assigned_to="alice",
            context=context,
        )
    recovered = await service.create_finite(
        title="Fix login",
        spec="Acceptance: tests pass",
        assigned_to="alice",
        context=context,
    )

    assert recovered.status == "assigned"
    assert len({attempt.txn_id for attempt in matrix.attempts}) == 1
    assert len(matrix.visible) == 1
