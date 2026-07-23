from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import (
    RoomKind,
    RoomPolicy,
    TaskRecord,
)
from agentteams_manager.tools.tasks import (
    TASK_TOOL_NAMES,
    TaskToolkit,
)
from agentteams_manager.workflows.resources import MutationContext


class Tasks:
    def __init__(self) -> None:
        self.completed: list[tuple[str, str]] = []

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
    ):
        del structured_result
        self.completed.append((task_id, worker_event_id))
        return {
            "operation_id": "a" * 32,
            "task_id": task_id,
            "status": "completed",
        }


class Projects:
    async def list_all(self):
        return ()

    async def get(self, project_id: str):
        del project_id
        return None


class FileSync:
    pass


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

    chunk = await tool.call(task_id="task-1", result={"ok": True})

    assert service.completed == [("task-1", "$worker-complete")]
    assert json.loads(chunk.content[0].text)["status"] == "completed"
    with pytest.raises(Exception):
        await tool.call(
            task_id="task-1",
            worker_event_id="$forged",
        )
