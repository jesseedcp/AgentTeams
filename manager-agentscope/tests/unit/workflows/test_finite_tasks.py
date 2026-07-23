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


def context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$request",
        tool_call_id="create-task",
    )


@pytest.mark.asyncio
async def test_artifacts_and_sqlite_exist_before_dispatch(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    service = TaskService(
        tasks=OrderedTaskRepository(TaskRepository(database), order),
        storage=TaskStorage(
            MinioClient(FakeS3(), bucket="agentteams"),
            order,
        ),
        controller=TaskController(),
        matrix=TaskMatrix(order),
        supervisor=TaskSupervisor(clock),
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
    )

    receipt = await service.create_finite(
        title="Fix login",
        spec="Acceptance: tests pass",
        assigned_to="alice",
        context=context(),
    )

    assert receipt.status == "assigned"
    assert order[:4] == [
        "sqlite.prepare",
        "minio.meta",
        "minio.spec",
        "matrix.assignment",
    ]

    second = await service.create_finite(
        title="Fix login",
        spec="Acceptance: tests pass",
        assigned_to="alice",
        context=context(),
    )

    assert second == receipt
    assert order.count("matrix.assignment") == 1


def test_assignment_message_preserves_upstream_worker_protocol() -> None:
    from agentteams_manager.workflows.tasks import TaskMessageFormatter

    text = TaskMessageFormatter.assignment(
        task_id="task-20260723-120000-abc123",
        title="Fix login",
        matrix_user_id="@worker-alice:example",
    )

    assert text.startswith(
        "@worker-alice:example New task "
        "[task-20260723-120000-abc123]: Fix login.",
    )
    assert "shared/tasks/task-20260723-120000-abc123/spec.md" in text
    assert "@mention me when complete." in text
