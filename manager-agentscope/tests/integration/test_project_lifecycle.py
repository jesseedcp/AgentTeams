from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import ConflictError
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
async def test_project_task_uses_worker_room_and_project_identity(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    controller = TaskController()
    matrix = ProjectMatrix(order)
    storage = TaskStorage(
        MinioClient(FakeS3(), bucket="agentteams"),
        order,
    )
    supervisor = TaskSupervisor(clock)
    tasks = TaskRepository(database)
    graph = ProjectGraphRepository(database)
    task_service = TaskService(
        tasks=OrderedTaskRepository(tasks, order),
        storage=storage,
        controller=controller,
        matrix=matrix,
        supervisor=supervisor,
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
        project_graph=graph,
    )
    service = ProjectService(
        projects=ProjectRepository(database),
        tasks=tasks,
        task_service=task_service,
        storage=storage,
        controller=controller,
        matrix=matrix,
        topology=TopologyRepository(database),
        graph=graph,
        supervisor=supervisor,
        clock=clock,
        admin_user_id="@admin:example",
        manager_user_id="@manager:example",
    )
    project = await service.create(
        title="Release 2",
        description="Ship it",
        plan="One phase",
        participants=("alice",),
        context=MutationContext(
            room_id="!admin:example",
            event_id="$project",
            tool_call_id="create-project",
        ),
    )

    task = await service.add_task(
        project_id=project.project_id,
        title="Build release",
        specification="Produce result.md",
        assigned_to="alice",
        context=MutationContext(
            room_id=project.room_id,
            event_id="$task",
            tool_call_id="add-task",
        ),
    )

    stored = await tasks.get(task.task_id)
    assert stored is not None
    assert stored.project_id == project.project_id
    assert stored.room_id == "!alice:example"
    assert matrix.messages[-1]["room_id"] == project.room_id

    dependent = await service.add_task(
        project_id=project.project_id,
        title="Publish release",
        specification="Publish after build",
        assigned_to="alice",
        dependencies=(task.task_id,),
        context=MutationContext(
            room_id=project.room_id,
            event_id="$dependent",
            tool_call_id="add-dependent-task",
        ),
    )
    waiting = await tasks.get(dependent.task_id)
    assert waiting is not None
    assert waiting.status == "pending"

    with pytest.raises(ConflictError, match="not task"):
        await service.complete_task(
            project_id=project.project_id,
            task_id=task.task_id,
            worker_event_id="$spoofed",
            sender_id="@worker-mallory:example",
            structured_result={"summary": "spoofed"},
        )

    await service.complete_task(
        project_id=project.project_id,
        task_id=task.task_id,
        worker_event_id="$completed",
        sender_id="@worker-alice:example",
        structured_result={"summary": "build ready"},
    )

    released = await tasks.get(dependent.task_id)
    assert released is not None
    assert released.status == "dispatched"

    await service.report_blocked(
        project_id=project.project_id,
        task_id=dependent.task_id,
        sender_id="@worker-alice:example",
        reason="release credentials are missing",
    )
    blocked = await tasks.get(dependent.task_id)
    assert blocked is not None
    assert blocked.status == "blocked"
