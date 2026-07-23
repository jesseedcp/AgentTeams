from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.domain.models import (
    HumanResource,
    InboundEvent,
    RoomKind,
)
from agentteams_manager.matrix.policy import RoomPolicyResolver


def _event(
    *,
    room_id: str,
    sender_id: str,
    is_direct: bool,
) -> InboundEvent:
    return InboundEvent(
        room_id=room_id,
        event_id="$event",
        sender=sender_id,
        body="hello",
        timestamp=datetime.now(UTC),
        is_direct=is_direct,
    )


class FakeTopology:
    def __init__(
        self,
        bindings: dict[str, object] | None = None,
        humans: dict[str, HumanResource] | None = None,
    ) -> None:
        self.bindings = bindings or {}
        self.humans = humans or {}

    async def room_binding(self, room_id: str) -> object | None:
        return self.bindings.get(room_id)

    async def human_for_sender(
        self,
        matrix_user_id: str,
    ) -> HumanResource | None:
        return self.humans.get(matrix_user_id)


@pytest.mark.asyncio
async def test_unknown_group_sender_is_silent() -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!group:local",
            sender_id="@unknown:local",
            is_direct=False,
        ),
    )

    assert policy.silent
    assert not policy.allowed_tools


@pytest.mark.asyncio
async def test_admin_dm_gets_management_tools() -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!admin-dm:local",
            sender_id="@admin:local",
            is_direct=True,
        ),
    )

    assert policy.kind is RoomKind.ADMIN_DM
    assert "create_worker" in policy.allowed_tools


@pytest.mark.asyncio
async def test_worker_identity_gets_only_worker_room_tools() -> None:
    binding = SimpleNamespace(
        room_kind=RoomKind.WORKER_ROOM,
        room_id="!worker:local",
        resource_name="alice",
        matrix_user_id="@alice:local",
        payload={},
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology({"!worker:local": binding}),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!worker:local",
            sender_id="@alice:local",
            is_direct=False,
        ),
    )

    assert policy.kind is RoomKind.WORKER_ROOM
    assert policy.allowed_tools == frozenset(
        {"delegate_task", "complete_task", "sync_files", "git_result"},
    )
    assert policy.resource_name == "alice"


@pytest.mark.asyncio
async def test_scoped_human_cannot_gain_resource_admin_tools() -> None:
    human = HumanResource(
        name="reviewer",
        matrix_user_id="@reviewer:local",
        permission_level=2,
        allowed_rooms=("!worker:local",),
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology(humans={"@reviewer:local": human}),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!worker:local",
            sender_id="@reviewer:local",
            is_direct=False,
        ),
    )

    assert not policy.silent
    assert "list_workers" in policy.allowed_tools
    assert "create_worker" not in policy.allowed_tools
    assert "switch_model" not in policy.allowed_tools
