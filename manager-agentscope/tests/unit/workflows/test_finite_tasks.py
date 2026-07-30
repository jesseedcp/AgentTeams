from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.models import TeamResource
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import (
    ProjectGraphRepository,
    TaskRepository,
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


@pytest.mark.asyncio
async def test_team_dispatch_uses_manager_leader_room_and_team_storage(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    repository = TaskRepository(database)
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
    matrix = TaskMatrix(order)
    s3 = FakeS3()
    storage = MinioClient(s3, bucket="agentteams")
    service = TaskService(
        tasks=OrderedTaskRepository(repository, order),
        storage=TaskStorage(
            storage,
            order,
        ),
        controller=controller,
        matrix=matrix,
        supervisor=TaskSupervisor(clock),
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
        manager_user_id="@manager:example",
    )

    receipt = await service.create_finite(
        title="Run acceptance",
        spec="Leader coordinates the team and reports the result.",
        assigned_to="alice",
        delegated_to_team="alpha",
        context=context(),
    )

    task = await repository.get(receipt.task_id)
    assert receipt.status == "assigned"
    assert matrix.attempts[0].room_id == "!alice:example"
    assert task is not None
    assert task.room_id == "!alice:example"
    assert task.delegated_to_team == "alpha"
    task_root = f"teams/alpha/shared/tasks/{task.task_id}"
    assert await storage.head(f"{task_root}/spec.md") is not None
    assert await storage.head(f"{task_root}/meta.json") is not None
    stored_spec = (
        await storage.get_bytes(f"{task_root}/spec.md")
    ).decode("utf-8")
    assert (
        f"shared/tasks/{task.task_id}/result.md"
        in stored_spec
    )
    assert (
        f"@manager:example TASK_COMPLETED: {task.task_id}"
        in stored_spec
    )
    assert "projectflow complete_project" in stored_spec
    assert "parentTaskCompletion.synced" in stored_spec
    assert "Do not send a duplicate completion reply" in stored_spec
    assert "send the result directly to the Admin room" in stored_spec
    assignment = matrix.attempts[0]
    assert (
        f"@manager:example TASK_COMPLETED: {task.task_id}"
        in assignment.text
    )
    assert "projectflow complete_project" in assignment.text
    assert "Do not send a duplicate completion reply" in assignment.text
    assert assignment.mentions == ("@worker-alice:example",)
    assert await storage.head(
        f"shared/tasks/{task.task_id}/spec.md",
    ) is None


@pytest.mark.asyncio
async def test_ready_project_team_task_dispatches_to_manager_leader_room(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    repository = TaskRepository(database)
    graph = ProjectGraphRepository(database)
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
    matrix = TaskMatrix(order)
    service = TaskService(
        tasks=OrderedTaskRepository(repository, order),
        storage=TaskStorage(
            MinioClient(FakeS3(), bucket="agentteams"),
            order,
        ),
        controller=controller,
        matrix=matrix,
        supervisor=TaskSupervisor(clock),
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
        manager_user_id="@manager:example",
        project_graph=graph,
    )

    prepared = await service.create_finite(
        title="Run project acceptance",
        spec="Leader coordinates the team and reports the result.",
        assigned_to="alice",
        delegated_to_team="alpha",
        project_id="project-20260723-120000-abc123",
        project_room_id="!project:example",
        defer_dispatch=True,
        context=context(),
    )
    await graph.set_dependencies(prepared.task_id, ())
    promoted = await graph.promote_ready(
        "project-20260723-120000-abc123",
    )
    assert tuple(item.task_id for item in promoted) == (prepared.task_id,)

    dispatched = await service.dispatch_ready(
        task_id=prepared.task_id,
        context=MutationContext(
            room_id="!project:example",
            event_id="$ready",
            tool_call_id="dispatch-ready",
        ),
    )

    task = await repository.get(prepared.task_id)
    assert dispatched.status == "dispatched"
    assert task is not None
    assert task.room_id == "!alice:example"
    assignment = matrix.attempts[-1]
    assert assignment.room_id == "!alice:example"
    assert (
        f"@manager:example TASK_COMPLETED: {task.task_id}"
        in assignment.text
    )
    assert assignment.mentions == ("@worker-alice:example",)


@pytest.mark.asyncio
async def test_direct_project_task_uses_project_room_and_assignee_team_storage(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    repository = TaskRepository(database)
    controller = TaskController()
    controller.workers["alice"] = controller.workers["alice"].model_copy(
        update={"team": "alpha", "role": "team_leader"},
    )
    s3 = FakeS3()
    storage = MinioClient(s3, bucket="agentteams")
    service = TaskService(
        tasks=OrderedTaskRepository(repository, order),
        storage=TaskStorage(storage, order),
        controller=controller,
        matrix=TaskMatrix(order),
        supervisor=TaskSupervisor(clock),
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
    )

    receipt = await service.create_finite(
        title="Draft requirements",
        spec="Report BLOCKED until the administrator supplies a color.",
        assigned_to="alice",
        project_id="project-20260723-120000-abc123",
        project_room_id="!project:example",
        context=context(),
    )

    task = await repository.get(receipt.task_id)
    assert task is not None
    assert task.delegated_to_team is None
    assert task.room_id == "!project:example"
    assert task.metadata["storage_team_name"] == "alpha"
    scoped_root = f"teams/alpha/shared/tasks/{task.task_id}"
    assert await storage.head(f"{scoped_root}/spec.md") is not None
    assert await storage.head(f"{scoped_root}/meta.json") is not None
    stored_metadata = await storage.get_json(f"{scoped_root}/meta.json")
    assert (
        stored_metadata["coordinator_matrix_user_id"]
        == "@manager:example"
    )
    assert stored_metadata["source_room_id"] == "!admin:example"
    assert await storage.head(
        f"shared/tasks/{task.task_id}/spec.md",
    ) is None


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
