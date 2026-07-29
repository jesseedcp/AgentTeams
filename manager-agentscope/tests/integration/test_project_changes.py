from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import OperationStatus
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


@dataclass
class ProjectHarness:
    service: ProjectService
    projects: ProjectRepository
    tasks: TaskRepository
    graph: ProjectGraphRepository
    matrix: ProjectMatrix
    storage: TaskStorage
    supervisor: TaskSupervisor

    def context(self, name: str, *, room_id: str = "!admin:example"):
        return MutationContext(
            room_id=room_id,
            event_id=f"${name}",
            tool_call_id=name,
        )


async def _harness(tmp_path: Path) -> ProjectHarness:
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
    projects = ProjectRepository(database)
    service = ProjectService(
        projects=projects,
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
    return ProjectHarness(
        service=service,
        projects=projects,
        tasks=tasks,
        graph=graph,
        matrix=matrix,
        storage=storage,
        supervisor=supervisor,
    )


@pytest.mark.asyncio
async def test_revision_creates_a_linked_task_and_holds_dependents(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build and publish a website",
        plan="Design, build, then publish",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    design = await harness.service.add_task(
        project_id=project.project_id,
        title="Design page",
        specification="Create the first design",
        assigned_to="alice",
        context=harness.context("design", room_id=project.room_id),
    )
    publish = await harness.service.add_task(
        project_id=project.project_id,
        title="Publish page",
        specification="Publish only after design approval",
        assigned_to="alice",
        dependencies=(design.task_id,),
        context=harness.context("publish", room_id=project.room_id),
    )

    revision = await harness.service.request_revision(
        project_id=project.project_id,
        task_id=design.task_id,
        feedback="Use the approved blue palette",
        assigned_to="bob",
        triggered_by_task_id=None,
        context=harness.context("revision", room_id=project.room_id),
    )

    original = await harness.tasks.get(design.task_id)
    stored_revision = await harness.tasks.get(revision.task_id)
    waiting = await harness.tasks.get(publish.task_id)
    assert original is not None
    assert original.status == "revision_needed"
    assert stored_revision is not None
    assert stored_revision.assigned_to == "bob"
    assert stored_revision.metadata["is_revision_for"] == design.task_id
    assert stored_revision.metadata["revision_feedback"] == (
        "Use the approved blue palette"
    )
    assert waiting is not None
    assert waiting.status == "pending"

    await harness.service.complete_task(
        project_id=project.project_id,
        task_id=revision.task_id,
        worker_event_id="$revision-completed",
        sender_id="@worker-bob:example",
        structured_result={"summary": "blue design ready"},
    )

    original = await harness.tasks.get(design.task_id)
    released = await harness.tasks.get(publish.task_id)
    stored_project = await harness.projects.get(project.project_id)
    assert original is not None
    assert original.status == "completed"
    assert released is not None
    assert released.status == "dispatched"
    assert stored_project is not None
    assert stored_project.metadata["task_statuses"][design.task_id] == (
        "completed"
    )
    assert stored_project.metadata["task_statuses"][revision.task_id] == (
        "completed"
    )


@pytest.mark.asyncio
async def test_reassignment_revokes_the_previous_assignee(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    task = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )

    reassigned = await harness.service.reassign_task(
        project_id=project.project_id,
        task_id=task.task_id,
        assigned_to="bob",
        reason="Alice is unavailable",
        context=harness.context("reassign", room_id=project.room_id),
    )

    assert reassigned.assigned_to == "bob"
    assert reassigned.room_id == "!bob:example"
    assert reassigned.status == "dispatched"
    with pytest.raises(ConflictError, match="not task"):
        await harness.service.complete_task(
            project_id=project.project_id,
            task_id=task.task_id,
            worker_event_id="$old-assignee",
            sender_id="@worker-alice:example",
            structured_result={"summary": "stale result"},
        )

    await harness.service.complete_task(
        project_id=project.project_id,
        task_id=task.task_id,
        worker_event_id="$new-assignee",
        sender_id="@worker-bob:example",
        structured_result={"summary": "new result"},
    )


@pytest.mark.asyncio
async def test_participant_changes_update_sqlite_and_matrix(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice",),
        context=harness.context("create"),
    )

    changed = await harness.service.update_participants(
        project_id=project.project_id,
        add=("bob",),
        remove=("alice",),
        reason="Move the project to Bob",
        context=harness.context("participants", room_id=project.room_id),
    )

    assert changed.participants == ("bob",)
    assert await harness.graph.participants(project.project_id) == ("bob",)
    members = await harness.matrix.members(project.room_id)
    assert "@worker-bob:example" in members
    assert "@worker-alice:example" not in members


@pytest.mark.asyncio
async def test_active_assignee_cannot_be_removed_from_project(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )

    with pytest.raises(ConflictError, match="active task"):
        await harness.service.update_participants(
            project_id=project.project_id,
            add=(),
            remove=("alice",),
            reason="Alice is unavailable",
            context=harness.context("remove", room_id=project.room_id),
        )


@pytest.mark.asyncio
async def test_plan_revision_is_versioned_and_exported(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Design then build",
        participants=("alice",),
        context=harness.context("create"),
    )

    changed = await harness.service.revise_plan(
        project_id=project.project_id,
        plan="Design, build, then run security review",
        change_kind="minor",
        reason="Clarify the final acceptance step",
        context=harness.context("plan", room_id=project.room_id),
    )

    assert changed.status == "active"
    revisions = await harness.graph.plan_revisions(project.project_id)
    assert tuple(item.change_kind for item in revisions) == (
        "initial",
        "minor",
    )
    stored = await harness.projects.get(project.project_id)
    assert stored is not None
    assert stored.metadata["plan"] == (
        "Design, build, then run security review"
    )
    exported = await harness.storage.get_bytes(
        f"shared/projects/{project.project_id}/plan.md",
    )
    assert b"security review" in exported


@pytest.mark.asyncio
async def test_last_task_completion_closes_project_and_notifies_admin(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice",),
        context=harness.context("create"),
    )
    task = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )

    await harness.service.complete_task(
        project_id=project.project_id,
        task_id=task.task_id,
        worker_event_id="$complete",
        sender_id="@worker-alice:example",
        structured_result={"summary": "website ready"},
    )

    stored = await harness.projects.get(project.project_id)
    assert stored is not None
    assert stored.status == "completed"
    completion_rooms = {
        message["room_id"]
        for message in harness.matrix.messages
        if "[Project Completed]" in message["text"]
    }
    assert completion_rooms == {project.room_id, "!admin:example"}


@pytest.mark.asyncio
async def test_revision_retry_reuses_the_linked_task(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    task = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )
    context = harness.context("revision", room_id=project.room_id)

    first = await harness.service.request_revision(
        project_id=project.project_id,
        task_id=task.task_id,
        feedback="Use blue",
        assigned_to="bob",
        triggered_by_task_id=None,
        context=context,
    )
    second = await harness.service.request_revision(
        project_id=project.project_id,
        task_id=task.task_id,
        feedback="Use blue",
        assigned_to="bob",
        triggered_by_task_id=None,
        context=context,
    )

    assert second.task_id == first.task_id
    project_tasks = await harness.tasks.list_by_project(project.project_id)
    assert len(project_tasks) == 2


@pytest.mark.asyncio
async def test_revision_returns_durable_receipt_when_announcement_is_ambiguous(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    task = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )
    harness.matrix.fail_once_for_text_prefix = "[Project Task Assigned]"
    context = harness.context("revision", room_id=project.room_id)

    revision = await harness.service.request_revision(
        project_id=project.project_id,
        task_id=task.task_id,
        feedback="Use blue",
        assigned_to="bob",
        triggered_by_task_id=None,
        context=context,
    )

    stored = await harness.tasks.get(revision.task_id)
    assert stored is not None
    assert stored.metadata["is_revision_for"] == task.task_id
    assert (
        harness.supervisor.operations[context.operation_id].status
        is OperationStatus.SUCCEEDED
    )


@pytest.mark.asyncio
async def test_task_transition_rebuilds_stale_project_task_index(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice",),
        context=harness.context("create"),
    )
    task = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )
    stored_project = await harness.projects.get(project.project_id)
    assert stored_project is not None
    changed = await harness.projects.update(
        project.project_id,
        expected={"active"},
        status="active",
        metadata={
            **stored_project.metadata,
            "task_ids": [*stored_project.metadata["task_ids"], "task-missing"],
            "task_statuses": {
                **stored_project.metadata["task_statuses"],
                "task-missing": "dispatched",
            },
        },
    )
    assert changed is not None

    await harness.service.reassign_task(
        project_id=project.project_id,
        task_id=task.task_id,
        assigned_to="alice",
        reason="Refresh task projection",
        context=harness.context("reassign", room_id=project.room_id),
    )

    rebuilt = await harness.projects.get(project.project_id)
    assert rebuilt is not None
    assert rebuilt.metadata["task_ids"] == [task.task_id]
    assert "task-missing" not in rebuilt.metadata["task_statuses"]
    assert "task-missing" not in rebuilt.metadata["task_assignments"]


@pytest.mark.asyncio
async def test_reassignment_retry_stays_dispatched(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    task = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Implement the page",
        assigned_to="alice",
        context=harness.context("task", room_id=project.room_id),
    )
    context = harness.context("reassign", room_id=project.room_id)

    first = await harness.service.reassign_task(
        project_id=project.project_id,
        task_id=task.task_id,
        assigned_to="bob",
        reason="Alice is unavailable",
        context=context,
    )
    second = await harness.service.reassign_task(
        project_id=project.project_id,
        task_id=task.task_id,
        assigned_to="bob",
        reason="Alice is unavailable",
        context=context,
    )

    assert first.status == second.status == "dispatched"
    stored = await harness.tasks.get(task.task_id)
    assert stored is not None
    assert stored.assigned_to == "bob"
    assert stored.status == "dispatched"


@pytest.mark.asyncio
async def test_participant_change_retry_is_idempotent(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice",),
        context=harness.context("create"),
    )
    context = harness.context("participants", room_id=project.room_id)

    first = await harness.service.update_participants(
        project_id=project.project_id,
        add=("bob",),
        remove=("alice",),
        reason="Move the project to Bob",
        context=context,
    )
    second = await harness.service.update_participants(
        project_id=project.project_id,
        add=("bob",),
        remove=("alice",),
        reason="Move the project to Bob",
        context=context,
    )

    assert first.participants == second.participants == ("bob",)
    revisions = await harness.graph.plan_revisions(project.project_id)
    assert tuple(item.change_kind for item in revisions) == (
        "initial",
        "major_participants",
    )


@pytest.mark.asyncio
async def test_plan_revision_retry_does_not_create_another_version(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice",),
        context=harness.context("create"),
    )
    context = harness.context("plan", room_id=project.room_id)

    first = await harness.service.revise_plan(
        project_id=project.project_id,
        plan="Build, then review",
        change_kind="minor",
        reason="Add acceptance review",
        context=context,
    )
    second = await harness.service.revise_plan(
        project_id=project.project_id,
        plan="Build, then review",
        change_kind="minor",
        reason="Add acceptance review",
        context=context,
    )

    assert first.status == second.status == "active"
    revisions = await harness.graph.plan_revisions(project.project_id)
    assert len(revisions) == 2


@pytest.mark.asyncio
async def test_resume_operation_recovers_participant_change(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.service.create(
        title="Website",
        description="Build a website",
        plan="Build it",
        participants=("alice",),
        context=harness.context("create"),
    )
    context = harness.context("participants", room_id=project.room_id)
    await harness.service.update_participants(
        project_id=project.project_id,
        add=("bob",),
        remove=("alice",),
        reason="Move the project to Bob",
        context=context,
    )
    operation = harness.supervisor.operations[context.operation_id].model_copy(
        update={"status": OperationStatus.RECONCILING},
    )
    harness.supervisor.operations[context.operation_id] = operation

    recovered = await harness.service.resume_operation(operation)

    assert recovered.participants == ("bob",)
    assert await harness.graph.participants(project.project_id) == ("bob",)
