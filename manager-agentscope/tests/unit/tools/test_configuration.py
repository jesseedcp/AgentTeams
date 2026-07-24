from __future__ import annotations

import json

import pytest

from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.configuration import ConfigurationToolkit
from agentteams_manager.workflows.resources import MutationContext


class Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, MutationContext]] = []

    async def switch_worker_model(self, *, worker, request, context):
        self.calls.append((worker, request.model, context))
        return {"worker": worker, "model": request.model}


def _context() -> MutationContext:
    return MutationContext(
        room_id="!leader:example",
        event_id="$switch",
        tool_call_id="tool-1",
    )


@pytest.mark.asyncio
async def test_worker_model_tool_is_closed_and_scope_bound() -> None:
    policy = RoomPolicy(
        room_id="!leader:example",
        kind=RoomKind.LEADER_ROOM,
        revision=1,
        allowed_tools=frozenset({"switch_worker_model"}),
        allowed_worker_names=frozenset({"alice"}),
    )
    service = Service()
    toolkit = ConfigurationToolkit(
        policy=policy,
        service=service,
        context_provider=_context,
    )
    tool = toolkit.tools[0]

    chunk = await tool.call(worker="alice", model="new")

    assert tool.input_schema["additionalProperties"] is False
    assert json.loads(chunk.content[0].text) == {
        "model": "new",
        "worker": "alice",
    }
    assert service.calls == [("alice", "new", _context())]
    with pytest.raises(Exception):
        await tool.call(worker="bob", model="new")


def test_model_tools_are_room_kind_bound_even_if_policy_is_too_broad() -> None:
    service = Service()
    human_policy = RoomPolicy(
        room_id="!human:example",
        kind=RoomKind.HUMAN_OR_CHANNEL_ROOM,
        revision=1,
        allowed_tools=frozenset(
            {"switch_model", "switch_worker_model"},
        ),
        resource_scope_all=True,
    )
    leader_policy = RoomPolicy(
        room_id="!leader:example",
        kind=RoomKind.LEADER_ROOM,
        revision=1,
        allowed_tools=frozenset(
            {"switch_model", "switch_worker_model"},
        ),
        allowed_worker_names=frozenset({"alice"}),
    )

    assert not ConfigurationToolkit(
        policy=human_policy,
        service=service,
    ).tools
    assert [
        tool.name
        for tool in ConfigurationToolkit(
            policy=leader_policy,
            service=service,
        ).tools
    ] == ["switch_worker_model"]
