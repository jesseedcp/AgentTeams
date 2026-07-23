from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.notifications import NotificationRepository
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.workflows.matrix_resources import ChannelResolver
from agentteams_manager.workflows.notifications import (
    DailyMemory,
    NotificationService,
)
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


class Channels:
    async def primary_channel(self, user_id: str) -> str | None:
        del user_id
        return "!admin:example"

    async def trusted_channels(self, user_id: str) -> tuple[str, ...]:
        del user_id
        return ()


class Matrix(TaskMatrix):
    async def joined_rooms(self) -> tuple[str, ...]:
        return ("!admin:example", "!alice:example")

    async def members(self, room_id: str) -> tuple[str, ...]:
        del room_id
        return ("@admin:example", "@manager:example")


@pytest.mark.asyncio
async def test_crash_after_send_does_not_duplicate_completion(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    matrix = Matrix(order)
    s3 = FakeS3()
    storage = TaskStorage(
        MinioClient(s3, bucket="agentteams"),
        order,
    )
    notifications = NotificationService(
        notifications=NotificationRepository(database),
        resolver=ChannelResolver(
            channels=Channels(),
            matrix=matrix,
            manager_admin_room="!admin:example",
        ),
        matrix=matrix,
        supervisor=supervisor,
        memory=DailyMemory(storage=storage, clock=clock),
        clock=clock,
        admin_user_id="@admin:example",
    )
    service = TaskService(
        tasks=OrderedTaskRepository(TaskRepository(database), order),
        storage=storage,
        controller=TaskController(),
        matrix=matrix,
        supervisor=supervisor,
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
        notifications=notifications,
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
    await storage.put_bytes_if_version(
        f"shared/tasks/{created.task_id}/result.md",
        b"All tests pass.",
        expected_etag=None,
        content_type="text/markdown",
    )
    matrix.timeout_once = True

    with pytest.raises(TimeoutError):
        await service.record_completion(
            task_id=created.task_id,
            worker_event_id="$done",
        )
    completed = await service.record_completion(
        task_id=created.task_id,
        worker_event_id="$done",
    )

    notification_txns = {
        attempt.txn_id
        for attempt in matrix.attempts
        if "Task Completed" in attempt.text
    }
    memory = await storage.get_bytes("manager/memory/2026-07-23.md")
    assert completed.status == "completed"
    assert len(notification_txns) == 1
    assert len(matrix.visible) == 2  # assignment plus completion
    assert memory.count(created.task_id.encode()) == 1
