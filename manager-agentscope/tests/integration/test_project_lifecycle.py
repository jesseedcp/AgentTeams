from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.state.database import Database
from agentteams_manager.state.projects import ProjectRepository
from agentteams_manager.state.tasks import (
    ProjectGraphRepository,
    ProjectTaskState,
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
async def test_project_task_uses_project_room_for_work_and_progress(
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
    assert project.status == "planning"
    project = await service.confirm_plan(
        project_id=project.project_id,
        confirmed_by="@admin:example",
        context=MutationContext(
            room_id="!admin:example",
            event_id="$project-confirmed",
            tool_call_id="confirm-project-plan",
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
    assert stored.room_id == project.room_id
    assignment = next(
        item
        for item in matrix.messages
        if item["text"].startswith("@worker-alice:example New task")
    )
    assert assignment["room_id"] == project.room_id
    project_announcement = next(
        item
        for item in matrix.messages
        if item["text"].startswith("[Project Task Assigned]")
    )
    assert project_announcement["room_id"] == project.room_id

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
    retired_worker_room = "!retired-worker:example"
    waiting = await tasks.update_routing(
        dependent.task_id,
        room_id=retired_worker_room,
        metadata={
            **waiting.metadata,
            "project_room_id": project.room_id,
        },
    )
    legacy_metadata_key = (
        f"shared/tasks/{dependent.task_id}/meta.json"
    )
    legacy_metadata_receipt = await storage.head(legacy_metadata_key)
    assert legacy_metadata_receipt is not None
    legacy_metadata = await storage.get_json(legacy_metadata_key)
    await storage.put_json_if_version(
        legacy_metadata_key,
        {**legacy_metadata, "room_id": retired_worker_room},
        expected_etag=legacy_metadata_receipt.etag,
    )
    assert waiting.room_id == retired_worker_room

    await graph.transition(
        task.task_id,
        expected={ProjectTaskState.DISPATCHED},
        target=ProjectTaskState.COMPLETED,
        actor_id="@worker-alice:example",
        reason="simulate completion before dispatch recovery",
    )
    completed = await tasks.get(task.task_id)
    assert completed is not None
    await tasks.transition(
        task.task_id,
        expected={"completed"},
        target="completed",
        metadata={
            **completed.metadata,
            "completion_finalized": True,
        },
    )
    promoted = await graph.promote_ready(project.project_id)
    assert tuple(item.task_id for item in promoted) == (
        dependent.task_id,
    )
    controller.workers["alice"] = controller.workers["alice"].model_copy(
        update={"team": "release-team"},
    )

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
    assert released.room_id == project.room_id
    assert released.metadata["storage_team_name"] == "release-team"
    assert await storage.head(
        "teams/release-team/shared/tasks/"
        f"{dependent.task_id}/spec.md",
    ) is not None

    await service.report_blocked(
        project_id=project.project_id,
        task_id=dependent.task_id,
        sender_id="@worker-alice:example",
        reason="release credentials are missing",
    )
    blocked = await tasks.get(dependent.task_id)
    assert blocked is not None
    assert blocked.status == "blocked"

    result_path = f"shared/tasks/{dependent.task_id}/result.md"
    scoped_root = f"teams/release-team/shared/tasks/{dependent.task_id}"
    metadata_key = f"{scoped_root}/meta.json"
    metadata_receipt = await storage.head(metadata_key)
    assert metadata_receipt is not None
    worker_metadata = await storage.get_json(metadata_key)
    await storage.put_json_if_version(
        metadata_key,
        {
            **worker_metadata,
            "status": "submitted",
            "acknowledged_by_role": "worker",
            "assigned_at": worker_metadata["created_at"],
            "deliverables": [
                result_path,
            ],
            "result_path": result_path,
            "result_status": "SUCCESS",
            "spec_path": "",
            "submitted_by_role": "worker",
            "summary": "credentials arrived and release completed",
            "task_title": blocked.title,
        },
        expected_etag=metadata_receipt.etag,
    )
    await storage.put_bytes_if_version(
        f"{scoped_root}/result.md",
        (
            "STATUS: SUCCESS\n"
            "SUMMARY: credentials arrived and release completed\n"
            "DELIVERABLES:\n"
            f"- {result_path}\n"
        ).encode(),
        expected_etag=None,
        content_type="text/markdown",
    )
    submission = await task_service.inspect_result(
        task_id=dependent.task_id,
    )

    completed_after_unblock = await service.complete_task(
        project_id=project.project_id,
        task_id=dependent.task_id,
        worker_event_id="$completed-after-blocked",
        sender_id="@worker-alice:example",
        accepted=True,
        result_digest=submission.digest,
    )
    assert completed_after_unblock.status == "completed"
    completed = await tasks.get(dependent.task_id)
    assert completed is not None
    assert completed.status == "completed"
