import pytest
from agentscope.permission import PermissionBehavior, PermissionContext

from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import ManagerTool


async def no_op_handler(**kwargs):
    return kwargs


@pytest.mark.asyncio
async def test_worker_room_cannot_create_worker() -> None:
    tool = ManagerTool(
        name="create_worker",
        description="Create a Worker",
        input_schema={
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": False,
        },
        policy=RoomPolicy(
            room_id="!worker:example",
            kind=RoomKind.WORKER_ROOM,
            revision=1,
            allowed_tools=frozenset({"get_task"}),
        ),
        handler=no_op_handler,
    )

    decision = await tool.check_permissions(
        {"name": "bob"},
        PermissionContext(),
    )

    assert decision.behavior is PermissionBehavior.DENY


@pytest.mark.asyncio
async def test_risky_admin_tool_requests_confirmation() -> None:
    policy = RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_tools=frozenset({"delete_worker"}),
        confirm_tools=frozenset({"delete_worker"}),
    )
    tool = ManagerTool(
        name="delete_worker",
        description="Delete a Worker",
        input_schema={
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": False,
        },
        policy=policy,
        handler=no_op_handler,
    )

    decision = await tool.check_permissions(
        {"name": "bob"},
        PermissionContext(),
    )

    assert decision.behavior is PermissionBehavior.ASK
    assert decision.bypass_immune

