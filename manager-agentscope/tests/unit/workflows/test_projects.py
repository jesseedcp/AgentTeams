from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.state.database import Database
from agentteams_manager.state.memory import MemoryRepository
from agentteams_manager.state.projects import ProjectRepository
from agentteams_manager.state.tasks import (
    ProjectGraphRepository,
    TaskRepository,
)
from agentteams_manager.state.topology import TopologyRepository
from agentteams_manager.workflows.projects import ProjectService
from agentteams_manager.workflows.memory import ManagerMemoryService
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
    matrix.joined_members_only = True
    matrix.reject_duplicate_invites = True
    matrix.fail_invite_after_effect_once = "@worker-alice:example"
    storage = TaskStorage(
        MinioClient(FakeS3(), bucket="agentteams"),
        order,
    )
    supervisor = TaskSupervisor(clock)
    graph = ProjectGraphRepository(database)
    memory = MemoryRepository(database)
    task_service = TaskService(
        tasks=OrderedTaskRepository(TaskRepository(database), order),
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
        tasks=TaskRepository(database),
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
        memory=ManagerMemoryService(memory, now=clock.now),
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
    assert project.status == "planning"

    members = set(matrix.rooms[project.room_id]["members"])
    assert members == {
        "@admin:example",
        "@manager:example",
        "@worker-alice:example",
        "@worker-bob:example",
    }
    assert matrix.create_invites == [()]
    assert len(matrix.invite_attempts) == 3
    assert len(set(matrix.invite_attempts)) == 3
    assert order.index("minio.meta") < order.index("matrix.project_room")
    assert order.index("minio.spec") < order.index("matrix.project_room")
    assert await graph.participants(project.project_id) == ("alice", "bob")
    revisions = await graph.plan_revisions(project.project_id)
    assert len(revisions) == 1
    assert revisions[0].body == "Phase 1: implementation"
    with pytest.raises(ConflictError, match="not active"):
        await service.add_task(
            project_id=project.project_id,
            title="Must wait",
            specification="Do not dispatch before plan confirmation",
            assigned_to="alice",
            context=MutationContext(
                room_id=project.room_id,
                event_id="$too-early",
                tool_call_id="add-before-confirmation",
            ),
        )

    active = await service.confirm_plan(
        project_id=project.project_id,
        confirmed_by="@admin:example",
        context=MutationContext(
            room_id="!admin:example",
            event_id="$confirm",
            tool_call_id="confirm-project-plan",
        ),
    )
    assert active.status == "active"
    assert active.confirmed_by == "@admin:example"
    assert active.confirmed_at == clock.now()
    assert matrix.messages[-1]["text"].startswith(
        "[Project Plan Confirmed]",
    )
    decisions = await memory.project_decisions(project.project_id)
    assert len(decisions) == 1
    assert decisions[0].decision == "Confirmed project plan revision 1"
