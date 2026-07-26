from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import AmbiguousEffectError
from agentteams_manager.state.database import Database
from agentteams_manager.state.projects import ProjectRepository
from agentteams_manager.state.tasks import (
    ProjectGraphRepository,
    TaskRepository,
)
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
async def test_room_timeout_is_reconciled_by_project_marker(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    controller = TaskController()
    matrix = ProjectMatrix(order)
    matrix.timeout_after_create = True
    matrix.hide_after_timeout = True
    storage = TaskStorage(
        MinioClient(FakeS3(), bucket="agentteams"),
        order,
    )
    supervisor = TaskSupervisor(clock)
    tasks = TaskRepository(database)
    task_service = TaskService(
        tasks=OrderedTaskRepository(tasks, order),
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
        tasks=tasks,
        task_service=task_service,
        storage=storage,
        controller=controller,
        matrix=matrix,
        topology=TopologyRepository(database),
        graph=ProjectGraphRepository(database),
        supervisor=supervisor,
        clock=clock,
        admin_user_id="@admin:example",
        manager_user_id="@manager:example",
    )
    context = MutationContext(
        room_id="!admin:example",
        event_id="$project",
        tool_call_id="create-project",
    )

    with pytest.raises(AmbiguousEffectError):
        await service.create(
            title="Release 2",
            description="Ship it",
            plan="One phase",
            participants=("alice",),
            context=context,
        )
    recovered = await service.create(
        title="Release 2",
        description="Ship it",
        plan="One phase",
        participants=("alice",),
        context=context,
    )

    matching = [
        room
        for room in matrix.rooms.values()
        if room["marker"]["m.agentteams.project_id"]
        == recovered.project_id
    ]
    assert len(matching) == 1
