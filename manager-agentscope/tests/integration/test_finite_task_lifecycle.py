from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.models import TeamResource
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
        (
            "STATUS: SUCCESS\n"
            "SUMMARY: All tests pass.\n"
            "DELIVERABLES:\n"
            f"- shared/tasks/{created.task_id}/result.md\n"
        ).encode(),
        expected_etag=None,
        content_type="text/markdown",
    )
    submission = await service.inspect_result(task_id=created.task_id)

    completed = await service.record_completion(
        task_id=created.task_id,
        worker_event_id="$done",
        accepted=True,
        result_digest=submission.digest,
    )

    stored = await repository.get(created.task_id)
    metadata = await service.storage.get_json(
        f"shared/tasks/{created.task_id}/meta.json",
    )
    assert completed.status == "completed"
    assert stored is not None and stored.status == "completed"
    assert metadata["status"] == "completed"
    assert completed.summary == "All tests pass."


@pytest.mark.asyncio
async def test_teamharness_markdown_result_can_be_inspected_and_completed(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TaskRepository(database)
    order: list[str] = []
    s3 = FakeS3()
    clock = FixedClock()
    controller = TaskController()
    controller.teams["alpha"] = TeamResource(
        name="alpha",
        leader="alice",
        workers=("bob",),
        spec={
            "teamRoomID": "!team:example",
            "leaderDMRoomID": "!leader-admin:example",
        },
    )
    service = TaskService(
        tasks=OrderedTaskRepository(repository, order),
        storage=TaskStorage(
            MinioClient(s3, bucket="agentteams"),
            order,
        ),
        controller=controller,
        matrix=TaskMatrix(order),
        supervisor=TaskSupervisor(clock),
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
        manager_user_id="@manager:example",
    )
    created = await service.create_finite(
        title="Run TeamHarness acceptance",
        spec="Leader coordinates the team and reports the result.",
        assigned_to="alice",
        delegated_to_team="alpha",
        context=MutationContext(
            room_id="!admin:example",
            event_id="$request",
            tool_call_id="create-team-task",
        ),
    )
    result_path = (
        f"teams/alpha/shared/tasks/{created.task_id}/result.md"
    )
    await service.storage.put_bytes_if_version(
        result_path,
        (
            "# TeamHarness parent result\n\n"
            f"**Parent Task**: {created.task_id}\n"
            "**Status**: SUCCESS\n\n"
            "ROUTE-FIX-PASS\n\n"
            "## 验收结论\n"
            "Leader Room 路由和 Worker 子任务均已通过。\n\n"
            "## 子任务交付物\n"
            f"- `shared/tasks/{created.task_id}-01/result.md`\n\n"
            "**Deliverables**:\n"
            f"- `shared/tasks/{created.task_id}/result.md`\n"
        ).encode(),
        expected_etag=None,
        content_type="text/markdown",
    )

    submission = await service.inspect_result(task_id=created.task_id)
    completed = await service.record_completion(
        task_id=created.task_id,
        worker_event_id="$team-done",
        accepted=True,
        result_digest=submission.digest,
    )

    stored = await repository.get(created.task_id)
    metadata = await service.storage.get_json(
        f"teams/alpha/shared/tasks/{created.task_id}/meta.json",
    )
    assert submission.status == "SUCCESS"
    assert submission.deliverables == (
        f"shared/tasks/{created.task_id}/result.md",
    )
    assert completed.status == "completed"
    assert stored is not None and stored.status == "completed"
    assert metadata["status"] == "completed"
