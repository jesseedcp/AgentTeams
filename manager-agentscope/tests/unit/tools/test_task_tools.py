from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import (
    ProjectRecord,
    RoomKind,
    RoomPolicy,
    TaskRecord,
)
from agentteams_manager.tools.tasks import (
    TASK_TOOL_NAMES,
    TaskToolkit,
)
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.projects import ProjectReceipt


class Tasks:
    def __init__(self) -> None:
        self.completed: list[tuple[str, str]] = []
        self.created: list[dict[str, object]] = []

    async def list_all(self):
        return ()

    async def get(self, task_id: str):
        return TaskRecord(
            task_id=task_id,
            task_type="finite",
            status="assigned",
            title="Test",
            assigned_to="alice",
            room_id="!alice:example",
            created_at=datetime(2026, 7, 23, tzinfo=UTC),
            updated_at=datetime(2026, 7, 23, tzinfo=UTC),
        )

    async def record_completion(
        self,
        *,
        task_id: str,
        worker_event_id: str,
        structured_result=None,
        accepted: bool = False,
        result_digest: str | None = None,
    ):
        del structured_result, accepted, result_digest
        self.completed.append((task_id, worker_event_id))
        return {
            "operation_id": "a" * 32,
            "task_id": task_id,
            "status": "completed",
        }

    async def create_finite(self, **kwargs):
        self.created.append(kwargs)
        return {
            "operation_id": "a" * 32,
            "task_id": "task-standalone",
            "status": "dispatched",
        }

    async def inspect_result(self, *, task_id: str):
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "summary": "ready",
            "deliverables": [f"shared/tasks/{task_id}/result.md"],
            "result_path": f"shared/tasks/{task_id}/result.md",
            "digest": "b" * 64,
        }


class Projects:
    def __init__(self) -> None:
        self.added: list[dict[str, object]] = []
        self.created: list[dict[str, object]] = []
        self.confirmed: list[dict[str, object]] = []

    async def list_all(self):
        return ()

    async def get(self, project_id: str):
        return ProjectRecord(
            project_id=project_id,
            name="Project",
            room_id="!project:example",
            status="active",
            metadata={"participants": ["alice"]},
            created_at=datetime(2026, 7, 23, tzinfo=UTC),
            updated_at=datetime(2026, 7, 23, tzinfo=UTC),
        )

    async def add_task(self, **kwargs):
        self.added.append(kwargs)
        return {
            "operation_id": "b" * 32,
            "task_id": "task-project",
            "status": "dispatched",
        }

    async def create(self, **kwargs):
        self.created.append(kwargs)
        return ProjectReceipt(
            operation_id="c" * 32,
            project_id="project-20260723-120000-abc123",
            title="Project",
            status="planning",
            room_id="!project:example",
            participants=("alice",),
        )

    async def confirm_plan(self, **kwargs):
        self.confirmed.append(kwargs)
        return ProjectReceipt(
            operation_id="d" * 32,
            project_id=kwargs["project_id"],
            title="Project",
            status="active",
            room_id="!project:example",
            participants=("alice",),
        )


class FileSync:
    async def read_task_file(
        self,
        task_id: str,
        path: str,
        *,
        max_bytes: int,
    ):
        return {
            "task_id": task_id,
            "path": path,
            "content": "artifact",
            "bytes_read": len("artifact"),
            "max_bytes": max_bytes,
        }


class Git:
    pass


def _context() -> MutationContext:
    return MutationContext(
        room_id="!alice:example",
        event_id="$worker-complete",
        tool_call_id="call-1",
    )


def _policy(**changes: object) -> RoomPolicy:
    values: dict[str, object] = {
        "room_id": "!alice:example",
        "kind": RoomKind.WORKER_ROOM,
        "revision": 1,
        "allowed_tools": TASK_TOOL_NAMES,
        "confirm_tools": frozenset(),
        "resource_name": "alice",
    }
    values.update(changes)
    return RoomPolicy(**values)


def test_task_tools_have_closed_schemas_and_policy_filtering() -> None:
    toolkit = TaskToolkit(
        policy=_policy(
            allowed_tools=frozenset({"list_tasks", "get_task"}),
        ),
        tasks=Tasks(),
        projects=Projects(),
        task_service=Tasks(),
        project_service=Projects(),
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )

    assert {tool.name for tool in toolkit.tools} == {
        "list_tasks",
        "get_task",
    }
    for tool in toolkit.tools:
        assert tool.input_schema["type"] == "object"
        assert tool.input_schema["additionalProperties"] is False


def test_project_change_tools_are_registered_with_closed_schemas() -> None:
    expected = {
        "request_project_revision",
        "reassign_project_task",
        "report_project_blocked",
        "revise_project_plan",
        "revise_project_plan_major",
        "update_project_participants",
    }
    toolkit = TaskToolkit(
        policy=_policy(allowed_tools=frozenset(expected)),
        tasks=Tasks(),
        projects=Projects(),
        task_service=Tasks(),
        project_service=Projects(),
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )

    assert {tool.name for tool in toolkit.tools} == expected
    assert expected <= TASK_TOOL_NAMES
    for tool in toolkit.tools:
        assert tool.input_schema["type"] == "object"
        assert tool.input_schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_full_mode_auto_confirms_a_prepared_project() -> None:
    projects = Projects()
    toolkit = TaskToolkit(
        policy=_policy(
            kind=RoomKind.ADMIN_DM,
            allowed_tools=frozenset({"create_project"}),
            allowed_senders=frozenset({"@admin:example"}),
            confirmation_mode="full",
        ),
        tasks=Tasks(),
        projects=projects,
        task_service=Tasks(),
        project_service=projects,
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )

    chunk = await toolkit.tools[0].call(
        title="Project",
        description="Ship it",
        plan="Build then verify",
        participants=["alice"],
    )
    result = json.loads(chunk.content[0].text)

    assert result["status"] == "active"
    assert len(projects.created) == 1
    assert len(projects.confirmed) == 1
    assert projects.confirmed[0]["auto_confirmed"] is True
    assert (
        projects.confirmed[0]["confirmed_by"]
        == "@admin:example"
    )


@pytest.mark.asyncio
async def test_leader_room_can_read_direct_task_assigned_to_team_member() -> None:
    toolkit = TaskToolkit(
        policy=_policy(
            kind=RoomKind.LEADER_ROOM,
            allowed_tools=frozenset({"get_task"}),
            resource_name="manual-lead",
            team_name="manual-qa",
            allowed_worker_names=frozenset(
                {"manual-lead", "k8s-smoke", "alice"},
            ),
        ),
        tasks=Tasks(),
        projects=Projects(),
        task_service=Tasks(),
        project_service=Projects(),
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )

    chunk = await toolkit.tools[0].call(task_id="task-direct-project")

    assert json.loads(chunk.content[0].text)["item"]["assigned_to"] == "alice"


@pytest.mark.asyncio
async def test_read_task_file_is_policy_scoped_and_read_only() -> None:
    toolkit = TaskToolkit(
        policy=_policy(allowed_tools=frozenset({"read_task_file"})),
        tasks=Tasks(),
        projects=Projects(),
        task_service=Tasks(),
        project_service=Projects(),
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )

    tool = toolkit.tools[0]
    chunk = await tool.call(
        task_id="task-1",
        path="result.md",
        max_bytes=4096,
    )

    assert tool.name == "read_task_file"
    assert tool.is_read_only is True
    assert json.loads(chunk.content[0].text)["content"] == "artifact"


@pytest.mark.asyncio
async def test_completion_uses_bound_matrix_event_not_model_input() -> None:
    service = Tasks()
    toolkit = TaskToolkit(
        policy=_policy(allowed_tools=frozenset({"complete_task"})),
        tasks=service,
        projects=Projects(),
        task_service=service,
        project_service=Projects(),
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )
    tool = toolkit.tools[0]

    chunk = await tool.call(
        task_id="task-1",
        result={"ok": True},
        accepted=True,
        result_digest="a" * 64,
    )

    assert service.completed == [("task-1", "$worker-complete")]
    assert json.loads(chunk.content[0].text)["status"] == "completed"
    with pytest.raises(Exception):
        await tool.call(
            task_id="task-1",
            worker_event_id="$forged",
        )


@pytest.mark.asyncio
async def test_result_inspection_is_read_only_and_returns_digest() -> None:
    service = Tasks()
    toolkit = TaskToolkit(
        policy=_policy(
            allowed_tools=frozenset({"inspect_task_result"}),
        ),
        tasks=service,
        projects=Projects(),
        task_service=service,
        project_service=Projects(),
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )
    tool = toolkit.tools[0]

    chunk = await tool.call(task_id="task-1")
    result = json.loads(chunk.content[0].text)

    assert tool.name == "inspect_task_result"
    assert tool.is_read_only is True
    assert result["digest"] == "b" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "payload", "expected_team"),
    [
        (
            "create_task",
            {
                "title": "Project work",
                "specification": "Do the indexed work",
                "assigned_to": "alice",
                "project_id": "project-123",
                "project_room_id": "!spoofed:example",
            },
            None,
        ),
        (
            "delegate_team_task",
            {
                "title": "Team project work",
                "specification": "Delegate through the Leader",
                "leader": "alice",
                "team_name": "alpha",
                "project_id": "project-123",
                "project_room_id": "!spoofed:example",
            },
            "alpha",
        ),
    ],
)
async def test_generic_project_delegation_uses_project_service(
    tool_name: str,
    payload: dict[str, object],
    expected_team: str | None,
) -> None:
    tasks = Tasks()
    projects = Projects()
    toolkit = TaskToolkit(
        policy=_policy(
            allowed_tools=frozenset({tool_name}),
            resource_scope_all=True,
        ),
        tasks=tasks,
        projects=projects,
        task_service=tasks,
        project_service=projects,
        file_sync=FileSync(),
        git=Git(),
        context_provider=_context,
    )
    tool = next(item for item in toolkit.tools if item.name == tool_name)

    await tool.call(**payload)

    assert tasks.created == []
    assert len(projects.added) == 1
    assert projects.added[0]["project_id"] == "project-123"
    assert projects.added[0]["assigned_to"] == "alice"
    assert projects.added[0]["delegated_to_team"] == expected_team
    assert "project_room_id" not in projects.added[0]
