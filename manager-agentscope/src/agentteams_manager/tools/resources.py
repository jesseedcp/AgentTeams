"""Resource authorization and deterministic Matrix channel selection."""

from __future__ import annotations

from typing import Protocol

from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import (
    HumanResource,
    RoomPolicy,
)
from agentteams_manager.matrix.policy import (
    policy_for_human,
)

def human_room_policy(
    human: HumanResource,
    *,
    room_id: str,
    revision: int,
) -> RoomPolicy:
    """Map one Controller Human permission tier to immutable authority."""
    return policy_for_human(
        human,
        room_id=room_id,
        revision=revision,
    )


def authorize_resource_target(
    policy: RoomPolicy,
    *,
    resource_type: str,
    name: str,
) -> None:
    """Reject a target outside the Human's Controller-declared scope."""
    if policy.resource_scope_all:
        return
    if resource_type == "team":
        allowed = policy.allowed_team_names
    elif resource_type == "worker":
        allowed = policy.allowed_worker_names
    else:
        raise PermissionDeniedError(
            f"resource type {resource_type!r} is not scopeable",
        )
    if name not in allowed:
        raise PermissionDeniedError(
            f"{resource_type}/{name} is outside this room's scope",
        )


class ChannelTopology(Protocol):
    async def primary_channel(self, user_id: str) -> str | None: ...

    async def trusted_channels(
        self,
        user_id: str,
    ) -> tuple[str, ...]: ...


class ChannelMatrix(Protocol):
    async def joined_rooms(self) -> tuple[str, ...]: ...

    async def members(self, room_id: str) -> tuple[str, ...]: ...


class ChannelResolver:
    """Choose only explicit notification channels in stable order."""

    def __init__(
        self,
        *,
        channels: ChannelTopology,
        matrix: ChannelMatrix,
        manager_admin_room: str,
    ) -> None:
        self._channels = channels
        self._matrix = matrix
        self._manager_admin_room = manager_admin_room

    async def notification_room(self, *, recipient: str) -> str:
        joined = frozenset(await self._matrix.joined_rooms())
        primary = await self._channels.primary_channel(recipient)
        if primary and await self._usable(
            primary,
            recipient=recipient,
            joined=joined,
        ):
            return primary
        for room_id in await self._channels.trusted_channels(recipient):
            if await self._usable(
                room_id,
                recipient=recipient,
                joined=joined,
            ):
                return room_id
        return self._manager_admin_room

    async def _usable(
        self,
        room_id: str,
        *,
        recipient: str,
        joined: frozenset[str],
    ) -> bool:
        if room_id not in joined:
            return False
        return recipient in await self._matrix.members(room_id)
