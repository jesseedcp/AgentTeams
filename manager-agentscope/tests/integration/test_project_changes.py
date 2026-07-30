from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    OperationStatus,
    ProjectRecord,
    TeamResource,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.projects import ProjectRepository
from agentteams_manager.state.tasks import (
    ProjectGraphRepository,
    TaskRepository,
)
from agentteams_manager.state.topology import TopologyRepository
from agentteams_manager.workflows.projects import ProjectService
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import (
    TaskAcceptanceRequired,
    TaskResultInvalid,
    TaskService,
)
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
    task_service: TaskService
    projects: ProjectRepository
    tasks: TaskRepository
    graph: ProjectGraphRepository
    controller: TaskController
    matrix: ProjectMatrix
    storage: TaskStorage
    supervisor: TaskSupervisor

    def context(self, name: str, *, room_id: str = "!admin:example"):
        return MutationContext(
            room_id=room_id,
            event_id=f"${name}",
            tool_call_id=name,
        )

    async def create_project(self, **kwargs):
        context = kwargs["context"]
        prepared = await self.service.create(**kwargs)
        assert prepared.status == "planning"
        return await self.service.confirm_plan(
            project_id=prepared.project_id,
            confirmed_by="@admin:example",
            context=MutationContext(
                room_id=context.room_id,
                event_id=context.event_id,
                tool_call_id=f"{context.tool_call_id}:confirm-plan",
            ),
        )

    async def submit_result(
        self,
        *,
        task_id: str,
        status: str,
        summary: str,
    ):
        task = await self.tasks.get(task_id)
        assert task is not None
        team_name = str(task.metadata.get("storage_team_name") or "")
        root = (
            f"teams/{team_name}/shared/tasks/{task_id}"
            if team_name
            else f"shared/tasks/{task_id}"
        )
        result_path = f"shared/tasks/{task_id}/result.md"
        body = (
            f"STATUS: {status}\n"
            f"SUMMARY: {summary}\n"
            "DELIVERABLES:\n"
            f"- {result_path}\n"
        )
        await self.storage.put_bytes_if_version(
            f"{root}/result.md",
            body.encode(),
            expected_etag=None,
            content_type="text/markdown",
        )
        metadata_receipt = await self.storage.head(f"{root}/meta.json")
        assert metadata_receipt is not None
        metadata = await self.storage.get_json(f"{root}/meta.json")
        await self.storage.put_json_if_version(
            f"{root}/meta.json",
            {
                **metadata,
                "status": "submitted",
                "result_status": status,
                "summary": summary,
                "deliverables": [result_path],
                "result_path": result_path,
                "submitted_by_role": "worker",
            },
            expected_etag=metadata_receipt.etag,
        )
        return await self.task_service.inspect_result(task_id=task_id)

    async def accept_result(
        self,
        *,
        project_id: str,
        task_id: str,
        event_id: str,
        sender_id: str,
        summary: str,
    ):
        submission = await self.submit_result(
            task_id=task_id,
            status="SUCCESS",
            summary=summary,
        )
        return await self.service.complete_task(
            project_id=project_id,
            task_id=task_id,
            worker_event_id=event_id,
            sender_id=sender_id,
            accepted=True,
            result_digest=submission.digest,
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
        task_service=task_service,
        projects=projects,
        tasks=tasks,
        graph=graph,
        controller=controller,
        matrix=matrix,
        storage=storage,
        supervisor=supervisor,
    )


@pytest.mark.asyncio
async def test_success_requires_review_and_a_current_result_digest(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
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
    submission = await harness.submit_result(
        task_id=task.task_id,
        status="SUCCESS",
        summary="website ready",
    )

    with pytest.raises(TaskAcceptanceRequired):
        await harness.service.complete_task(
            project_id=project.project_id,
            task_id=task.task_id,
            worker_event_id="$not-reviewed",
            sender_id="@worker-alice:example",
            result_digest=submission.digest,
        )
    stored = await harness.tasks.get(task.task_id)
    assert stored is not None
    assert stored.status == "dispatched"

    result_key = f"shared/tasks/{task.task_id}/result.md"
    current = await harness.storage.head(result_key)
    assert current is not None
    await harness.storage.put_bytes_if_version(
        result_key,
        (
            "STATUS: SUCCESS\n"
            "SUMMARY: website ready\n"
            "DELIVERABLES:\n"
            f"- {result_key}\n"
            "\nChanged after review.\n"
        ).encode(),
        expected_etag=current.etag,
        content_type="text/markdown",
    )
    with pytest.raises(TaskResultInvalid, match="changed after inspection"):
        await harness.service.complete_task(
            project_id=project.project_id,
            task_id=task.task_id,
            worker_event_id="$stale-review",
            sender_id="@worker-alice:example",
            accepted=True,
            result_digest=submission.digest,
        )

    refreshed = await harness.task_service.inspect_result(
        task_id=task.task_id,
    )
    accepted = await harness.service.complete_task(
        project_id=project.project_id,
        task_id=task.task_id,
        worker_event_id="$accepted",
        sender_id="@worker-alice:example",
        accepted=True,
        result_digest=refreshed.digest,
    )
    assert accepted.status == "completed"
    assert accepted.result_status == "SUCCESS"


@pytest.mark.asyncio
async def test_revision_submission_creates_rework_and_holds_dependents(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
        title="Website",
        description="Build and publish a website",
        plan="Build then publish",
        participants=("alice", "bob"),
        context=harness.context("create"),
    )
    build = await harness.service.add_task(
        project_id=project.project_id,
        title="Build page",
        specification="Use the approved palette",
        assigned_to="alice",
        context=harness.context("build", room_id=project.room_id),
    )
    publish = await harness.service.add_task(
        project_id=project.project_id,
        title="Publish page",
        specification="Publish accepted output",
        assigned_to="bob",
        dependencies=(build.task_id,),
        context=harness.context("publish", room_id=project.room_id),
    )
    submission = await harness.submit_result(
        task_id=build.task_id,
        status="REVISION_NEEDED",
        summary="palette does not match the specification",
    )

    decided = await harness.service.complete_task(
        project_id=project.project_id,
        task_id=build.task_id,
        worker_event_id="$revision-result",
        sender_id="@worker-alice:example",
        result_digest=submission.digest,
    )

    stored_build = await harness.tasks.get(build.task_id)
    stored_publish = await harness.tasks.get(publish.task_id)
    project_tasks = await harness.tasks.list_by_project(project.project_id)
    revisions = tuple(
        item
        for item in project_tasks
        if item.metadata.get("is_revision_for") == build.task_id
    )
    assert decided.status == "revision_needed"
    assert decided.result_status == "REVISION_NEEDED"
    assert stored_build is not None
    assert stored_build.status == "revision_needed"
    assert stored_publish is not None
    assert stored_publish.status == "pending"
    assert len(revisions) == 1
    assert revisions[0].status == "dispatched"


@pytest.mark.asyncio
async def test_interrupted_submission_blocks_without_unlocking_dependents(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
        title="Release",
        description="Build and release",
        plan="Build then release",
        participants=("alice",),
        context=harness.context("create"),
    )
    build = await harness.service.add_task(
        project_id=project.project_id,
        title="Build",
        specification="Build the release",
        assigned_to="alice",
        context=harness.context("build", room_id=project.room_id),
    )
    release = await harness.service.add_task(
        project_id=project.project_id,
        title="Release",
        specification="Release accepted build",
        assigned_to="alice",
        dependencies=(build.task_id,),
        context=harness.context("release", room_id=project.room_id),
    )
    submission = await harness.submit_result(
        task_id=build.task_id,
        status="INTERRUPTED",
        summary="build host restarted",
    )

    decided = await harness.service.complete_task(
        project_id=project.project_id,
        task_id=build.task_id,
        worker_event_id="$interrupted",
        sender_id="@worker-alice:example",
        result_digest=submission.digest,
    )

    stored_release = await harness.tasks.get(release.task_id)
    project_tasks = await harness.tasks.list_by_project(project.project_id)
    assert decided.status == "blocked"
    assert decided.result_status == "INTERRUPTED"
    assert stored_release is not None
    assert stored_release.status == "pending"
    assert not any(
        item.metadata.get("is_revision_for") == build.task_id
        for item in project_tasks
    )


@pytest.mark.asyncio
async def test_revision_creates_a_linked_task_and_holds_dependents(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
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
    assert any(
        phase == "before"
        and request.get("operation")
        == "request_project_task_revision"
        for phase, _effect, request in harness.supervisor.events
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

    await harness.accept_result(
        project_id=project.project_id,
        task_id=revision.task_id,
        event_id="$revision-completed",
        sender_id="@worker-bob:example",
        summary="blue design ready",
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
    project = await harness.create_project(
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
    assert reassigned.room_id == project.room_id
    assert reassigned.status == "dispatched"
    reassignment_audit = next(
        message
        for message in reversed(harness.matrix.messages)
        if message["text"].startswith("[Project Task Reassigned]")
    )
    assert reassignment_audit["room_id"] == project.room_id
    assert reassignment_audit["mentions"] == ()
    with pytest.raises(ConflictError, match="not task"):
        await harness.service.complete_task(
            project_id=project.project_id,
            task_id=task.task_id,
            worker_event_id="$old-assignee",
            sender_id="@worker-alice:example",
            structured_result={"summary": "stale result"},
        )

    await harness.accept_result(
        project_id=project.project_id,
        task_id=task.task_id,
        event_id="$new-assignee",
        sender_id="@worker-bob:example",
        summary="new result",
    )


@pytest.mark.asyncio
async def test_reassignment_migrates_task_files_to_new_assignee_team(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
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
    global_root = f"shared/tasks/{task.task_id}"
    assert await harness.storage.head(f"{global_root}/spec.md") is not None
    harness.controller.workers["bob"] = (
        harness.controller.workers["bob"].model_copy(
            update={"team": "beta", "role": "worker"},
        )
    )

    reassigned = await harness.service.reassign_task(
        project_id=project.project_id,
        task_id=task.task_id,
        assigned_to="bob",
        reason="Move work to the beta Team",
        context=harness.context("reassign", room_id=project.room_id),
    )

    stored = await harness.tasks.get(task.task_id)
    assert reassigned.status == "dispatched"
    assert stored is not None
    assert stored.metadata["storage_team_name"] == "beta"
    scoped_root = f"teams/beta/shared/tasks/{task.task_id}"
    assert await harness.storage.head(f"{scoped_root}/spec.md") is not None
    assert await harness.storage.head(f"{scoped_root}/meta.json") is not None


@pytest.mark.asyncio
async def test_reassignment_from_team_routes_to_the_project_room(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    harness.controller.teams["alpha"] = TeamResource(
        name="alpha",
        leader="alice",
        workers=("bob",),
        spec={
            "teamRoomID": "!team:example",
            "leaderDMRoomID": "!leader-admin:example",
        },
    )
    harness.controller.workers["bob"] = (
        harness.controller.workers["bob"].model_copy(
            update={"team": "alpha", "role": "worker"},
        )
    )
    project = await harness.create_project(
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
        delegated_to_team="alpha",
        context=harness.context("task", room_id=project.room_id),
    )
    original = await harness.tasks.get(task.task_id)
    assert original is not None
    assert original.delegated_to_team == "alpha"
    assert task.room_id == "!alice:example"

    reassigned = await harness.service.reassign_task(
        project_id=project.project_id,
        task_id=task.task_id,
        assigned_to="bob",
        reason="Move the Team parent task to Bob directly",
        context=harness.context("reassign", room_id=project.room_id),
    )

    stored = await harness.tasks.get(task.task_id)
    assert reassigned.status == "dispatched"
    assert reassigned.assigned_to == "bob"
    assert reassigned.room_id == project.room_id
    assert stored is not None
    assert stored.delegated_to_team is None
    assert stored.metadata["matrix_user_id"] == "@worker-bob:example"
    assignment = next(
        message
        for message in reversed(harness.matrix.messages)
        if f"New task [{task.task_id}]" in message["text"]
    )
    assert assignment["room_id"] == project.room_id
    assert assignment["mentions"] == ("@worker-bob:example",)


@pytest.mark.asyncio
async def test_participant_changes_update_sqlite_and_matrix(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
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
    project = await harness.create_project(
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
    project = await harness.create_project(
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
    project = await harness.create_project(
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

    await harness.accept_result(
        project_id=project.project_id,
        task_id=task.task_id,
        event_id="$complete",
        sender_id="@worker-alice:example",
        summary="website ready",
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
    project = await harness.create_project(
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
    second_context = MutationContext(
        room_id=context.room_id,
        event_id="$revision-repeated-user-message",
        tool_call_id="revision-second-tool-call",
    )
    second = await harness.service.request_revision(
        project_id=project.project_id,
        task_id=task.task_id,
        feedback="Use blue",
        assigned_to="bob",
        triggered_by_task_id=None,
        context=second_context,
    )

    assert second.task_id == first.task_id
    project_tasks = await harness.tasks.list_by_project(project.project_id)
    assert len(project_tasks) == 2


@pytest.mark.asyncio
async def test_legacy_duplicate_revision_completion_reconciles_and_closes(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
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
    first_revision = await harness.service.request_revision(
        project_id=project.project_id,
        task_id=task.task_id,
        feedback="Use blue",
        assigned_to="bob",
        triggered_by_task_id=None,
        context=harness.context("revision", room_id=project.room_id),
    )
    duplicate_revision = await harness.service.add_task(
        project_id=project.project_id,
        title="Revision: Build page",
        specification="Legacy duplicate revision",
        assigned_to="bob",
        context=harness.context(
            "duplicate-revision",
            room_id=project.room_id,
        ),
        metadata={
            "is_revision_for": task.task_id,
            "revision_feedback": "Use blue",
        },
    )

    await harness.accept_result(
        project_id=project.project_id,
        task_id=first_revision.task_id,
        event_id="$first-revision-completed",
        sender_id="@worker-bob:example",
        summary="first revision ready",
    )
    await harness.accept_result(
        project_id=project.project_id,
        task_id=duplicate_revision.task_id,
        event_id="$duplicate-revision-completed",
        sender_id="@worker-bob:example",
        summary="duplicate revision ready",
    )

    original = await harness.tasks.get(task.task_id)
    stored_project = await harness.projects.get(project.project_id)
    assert original is not None
    assert original.status == "completed"
    assert stored_project is not None
    assert stored_project.status == "completed"
    assert set(stored_project.metadata["task_statuses"].values()) == {
        "completed",
    }


@pytest.mark.asyncio
async def test_revision_returns_durable_receipt_when_announcement_is_ambiguous(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    project = await harness.create_project(
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
    project = await harness.create_project(
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
    project = await harness.create_project(
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
    project = await harness.create_project(
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
    project = await harness.create_project(
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
    project = await harness.create_project(
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


@pytest.mark.asyncio
async def test_force_close_cancels_an_unstarted_planning_project(
    tmp_path: Path,
) -> None:
    harness = await _harness(tmp_path)
    now = datetime(2026, 7, 23, 12, tzinfo=UTC)
    project_id = "project-20260723-120000-cancel"
    await harness.projects.create(
        ProjectRecord(
            project_id=project_id,
            name="Abandoned draft",
            room_id="",
            status="planning",
            metadata={
                "description": "A draft that never created its room",
                "plan": "No work started",
                "participants": ["alice"],
                "worker_users": {"alice": "@worker-alice:example"},
                "requester_room_id": "!admin:example",
                "task_ids": [],
            },
            created_at=now,
            updated_at=now,
        ),
    )
    with pytest.raises(ConflictError, match="cannot close from planning"):
        await harness.service.close(
            project_id=project_id,
            force=False,
            context=harness.context("close-draft"),
        )
    context = harness.context("cancel-draft")

    receipt = await harness.service.close(
        project_id=project_id,
        force=True,
        context=context,
    )

    assert receipt.status == "cancelled"
    stored = await harness.projects.get(project_id)
    assert stored is not None
    assert stored.status == "cancelled"
    assert stored.metadata["forced_close"] is True
    assert stored.metadata["cancelled_at"] == now.isoformat()
    metadata = await harness.storage.get_json(
        f"shared/projects/{project_id}/meta.json",
    )
    assert metadata["status"] == "cancelled"
    assert metadata["room_id"] is None
    assert harness.matrix.messages[-1]["room_id"] == "!admin:example"
    assert harness.matrix.messages[-1]["text"].startswith(
        f"[Project Cancelled] {project_id}:",
    )

    repeated = await harness.service.close(
        project_id=project_id,
        force=True,
        context=context,
    )
    assert repeated.status == "cancelled"
