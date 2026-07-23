from __future__ import annotations

import pytest

from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import HumanResource
from agentteams_manager.tools.resources import (
    ChannelResolver,
    authorize_resource_target,
    human_room_policy,
)


def scoped_human(level: int = 2) -> HumanResource:
    return HumanResource(
        name="reviewer",
        matrix_user_id="@reviewer:example",
        permission_level=level,
        allowed_rooms=(
            "!selected-team:example",
            "!selected-worker:example",
        ),
        spec={
            "accessibleTeams": ["alpha"],
            "accessibleWorkers": ["alpha-dev"],
        },
    )


def test_level_two_human_is_read_only_and_resource_scoped() -> None:
    policy = human_room_policy(
        scoped_human(),
        room_id="!selected-team:example",
        revision=4,
    )

    assert "list_workers" in policy.allowed_tools
    assert "create_worker" not in policy.allowed_tools
    assert "delete_worker" not in policy.allowed_tools
    assert "send_notification" in policy.confirm_tools
    assert policy.allowed_team_names == frozenset({"alpha"})
    assert policy.allowed_worker_names == frozenset({"alpha-dev"})
    authorize_resource_target(policy, resource_type="team", name="alpha")
    with pytest.raises(PermissionDeniedError):
        authorize_resource_target(
            policy,
            resource_type="worker",
            name="other",
        )


def test_level_one_human_mutations_still_require_confirmation() -> None:
    policy = human_room_policy(
        scoped_human(level=1),
        room_id="!any:example",
        revision=5,
    )

    assert policy.resource_scope_all
    assert "create_worker" in policy.allowed_tools
    assert "create_worker" in policy.confirm_tools
    assert "upload_matrix_media" in policy.confirm_tools


class Channels:
    def __init__(
        self,
        *,
        primary: str | None,
        trusted: tuple[str, ...] = (),
    ) -> None:
        self.primary = primary
        self.trusted = trusted

    async def primary_channel(self, user_id: str) -> str | None:
        del user_id
        return self.primary

    async def trusted_channels(self, user_id: str) -> tuple[str, ...]:
        del user_id
        return self.trusted


class Matrix:
    def __init__(
        self,
        joined: tuple[str, ...],
        members: dict[str, tuple[str, ...]],
    ) -> None:
        self.joined = joined
        self.member_rows = members

    async def joined_rooms(self) -> tuple[str, ...]:
        return self.joined

    async def members(self, room_id: str) -> tuple[str, ...]:
        return self.member_rows.get(room_id, ())


@pytest.mark.asyncio
async def test_missing_primary_room_falls_back_to_admin_room() -> None:
    resolver = ChannelResolver(
        channels=Channels(primary="!gone:example"),
        matrix=Matrix(joined=("!admin:example",), members={}),
        manager_admin_room="!admin:example",
    )

    room_id = await resolver.notification_room(
        recipient="@reviewer:example",
    )

    assert room_id == "!admin:example"


@pytest.mark.asyncio
async def test_shared_trusted_room_precedes_admin_fallback() -> None:
    resolver = ChannelResolver(
        channels=Channels(
            primary=None,
            trusted=("!trusted:example",),
        ),
        matrix=Matrix(
            joined=("!trusted:example", "!admin:example"),
            members={
                "!trusted:example": (
                    "@manager:example",
                    "@reviewer:example",
                ),
            },
        ),
        manager_admin_room="!admin:example",
    )

    room_id = await resolver.notification_room(
        recipient="@reviewer:example",
    )

    assert room_id == "!trusted:example"
