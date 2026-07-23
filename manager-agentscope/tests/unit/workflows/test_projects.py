from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.projects import ProjectRepository
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.state.topology import TopologyRepository
from agentteams_manager.workflows.projects import ProjectService
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskService
from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.project_workflow import ProjectMatrix
from tests.fixtures.task_workflow import (
    FixedClock,
    OrderedTaskRepository,
    TaskController,
    TaskStorage,
    TaskSupervisor,
)


@pytest.mark.asyncio
async def test_project_room_contains_admin_and_selected_workers(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    controller = TaskController()
    controller.workers["bob"] = controller.workers["alice"].model_copy(
        update={
            "name": "bob",
            "room_id": "!bob:example",
            "matrix_user_id": "@worker-bob:example",
        },
    )
    matrix = ProjectMatrix(order)
    storage = TaskStorage(
        MinioClient(FakeS3(), bucket="agentteams"),
        order,
    )
    supervisor = TaskSupervisor(clock)
    task_service = TaskService(
        tasks=OrderedTaskRepository(TaskRepository(database), order),
        storage=storage,
        controller=controller,
        matrix=matrix,
        supervisor=supervisor,
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
    )
    service = ProjectService(
        projects=ProjectRepository(database),
        tasks=TaskRepository(database),
        task_service=task_service,
        storage=storage,
        controller=controller,
        matrix=matrix,
        topology=TopologyRepository(database),
        supervisor=supervisor,
        clock=clock,
        admin_user_id="@admin:example",
        manager_user_id="@manager:example",
    )

    project = await service.create(
        title="Release 2",
        description="Ship the new runtime",
        plan="Phase 1: implementation",
        participants=("alice", "bob"),
        context=MutationContext(
            room_id="!admin:example",
            event_id="$project",
            tool_call_id="create-project",
        ),
    )

    members = set(await matrix.members(project.room_id))
    assert members == {
        "@admin:example",
        "@manager:example",
        "@worker-alice:example",
        "@worker-bob:example",
    }
    assert order.index("minio.meta") < order.index("matrix.project_room")
    assert order.index("minio.spec") < order.index("matrix.project_room")
