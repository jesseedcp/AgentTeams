from __future__ import annotations

import json

import pytest

from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import ToolInvocationContext
from agentteams_manager.tools.memory import (
    MEMORY_TOOL_NAMES,
    MemoryToolkit,
)


class MemoryService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def recall(self, **kwargs):
        self.calls.append(("recall", kwargs))
        return {"entries": []}

    async def remember(self, **kwargs):
        self.calls.append(("remember", kwargs))
        return {"memory_id": "memory-1"}

    async def record_project_decision(self, **kwargs):
        self.calls.append(("project", kwargs))
        return {"memory_id": "decision-1"}

    async def record_worker_assessment(self, **kwargs):
        self.calls.append(("worker", kwargs))
        return {"memory_id": "assessment-1"}


def _context() -> ToolInvocationContext:
    return ToolInvocationContext(
        room_id="!admin:local",
        event_id="$event",
        tool_call_id="call-1",
    )


def _policy(kind: RoomKind = RoomKind.ADMIN_DM) -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=kind,
        revision=1,
        allowed_tools=MEMORY_TOOL_NAMES,
        allowed_senders=frozenset({"@admin:local"}),
    )


def test_memory_tools_exist_only_in_admin_dm_with_closed_schemas() -> None:
    service = MemoryService()
    admin = MemoryToolkit(
        policy=_policy(),
        service=service,  # type: ignore[arg-type]
        context_provider=_context,
    )
    worker = MemoryToolkit(
        policy=_policy(RoomKind.WORKER_ROOM),
        service=service,  # type: ignore[arg-type]
        context_provider=_context,
    )

    assert {tool.name for tool in admin.tools} == MEMORY_TOOL_NAMES
    assert worker.tools == ()
    for tool in admin.tools:
        assert tool.input_schema["type"] == "object"
        assert tool.input_schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_memory_mutations_bind_room_event_and_tool_call() -> None:
    service = MemoryService()
    toolkit = MemoryToolkit(
        policy=_policy(),
        service=service,  # type: ignore[arg-type]
        context_provider=_context,
    )
    tools = {tool.name: tool for tool in toolkit.tools}

    result = await tools["remember_manager_memory"].call(
        category="preference",
        content="Use Chinese.",
        importance=8,
    )
    await tools["record_project_decision"].call(
        project_id="project-20260730-120000-abc123",
        decision="Approve plan",
        rationale="Tests passed.",
    )
    await tools["record_worker_assessment"].call(
        worker_name="alice",
        capability="python",
        score=0.9,
        evidence="Accepted task result.",
    )

    assert json.loads(result.content[0].text)["memory_id"] == "memory-1"
    assert [name for name, _ in service.calls] == [
        "remember",
        "project",
        "worker",
    ]
    for _, arguments in service.calls:
        assert arguments["room_id"] == "!admin:local"
        assert arguments["source_event_id"] == "$event:call-1"
