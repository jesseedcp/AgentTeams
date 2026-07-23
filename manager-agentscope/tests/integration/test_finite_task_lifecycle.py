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
async def test_finite_task_can_be_created_and_completed(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TaskRepository(database)
    order: list[str] = []
    s3 = FakeS3()
    clock = FixedClock()
    service = TaskService(
        tasks=OrderedTaskRepository(repository, order),
        storage=TaskStorage(
            MinioClient(s3, bucket="agentteams"),
            order,
        ),
        controller=TaskController(),
        matrix=TaskMatrix(order),
        supervisor=TaskSupervisor(clock),
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
    )
    created = await service.create_finite(
        title="Fix login",
        spec="Acceptance: tests pass",
        assigned_to="alice",
        context=MutationContext(
            room_id="!admin:example",
            event_id="$request",
            tool_call_id="create-task",
        ),
    )
    await service.storage.put_bytes_if_version(
        f"shared/tasks/{created.task_id}/result.md",
        b"All tests pass.",
        expected_etag=None,
        content_type="text/markdown",
    )

    completed = await service.record_completion(
        task_id=created.task_id,
        worker_event_id="$done",
    )

    stored = await repository.get(created.task_id)
    metadata = await service.storage.get_json(
        f"shared/tasks/{created.task_id}/meta.json",
    )
    assert completed.status == "completed"
    assert stored is not None and stored.status == "completed"
    assert metadata["status"] == "completed"
    assert completed.summary == "All tests pass."
