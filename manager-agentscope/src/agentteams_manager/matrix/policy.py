"""Hard room and sender authorization for Matrix events."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection
from typing import Protocol

from agentteams_manager.domain.models import (
    HumanResource,
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.state.topology import TopologyBinding

ALL_MANAGER_TOOLS = frozenset(
    {
        "find_worker",
        "list_workers",
        "get_worker",
        "create_worker",
        "update_worker",
        "delete_worker",
        "list_teams",
        "get_team",
        "create_team",
        "update_team",
        "delete_team",
        "list_humans",
        "get_human",
        "create_human",
        "update_human",
        "delete_human",
        "list_tasks",
        "get_task",
        "create_task",
        "update_task",
        "delete_task",
        "delegate_task",
        "delegate_team_task",
        "complete_task",
        "schedule_task",
        "create_project",
        "list_projects",
        "get_project",
        "update_project",
        "delete_project",
        "sync_files",
        "git_delegate",
        "git_result",
        "list_channels",
        "create_channel",
        "update_channel",
        "delete_channel",
        "send_notification",
        "list_matrix_rooms",
        "invite_matrix_user",
        "kick_matrix_user",
        "ban_matrix_user",
        "unban_matrix_user",
        "list_mcp_servers",
        "configure_mcp",
        "remove_mcp",
        "switch_model",
        "switch_worker_model",
        "publish_service",
    },
)

WORKER_TOOLS = frozenset(
    {"delegate_task", "complete_task", "sync_files", "git_result"},
)
LEADER_TOOLS = frozenset(
    {"delegate_team_task", "complete_task", "sync_files"},
)
HUMAN_TOOLS = frozenset({"list_workers", "list_tasks", "sync_files"})
TRUSTED_TOOLS = frozenset({"list_workers", "list_tasks"})
UNKNOWN_TOOLS: frozenset[str] = frozenset()

_CONFIRM_TOOLS = frozenset(
    tool
    for tool in ALL_MANAGER_TOOLS
    if tool.startswith(
        (
            "create_",
            "update_",
            "delete_",
            "configure_",
            "remove_",
            "switch_",
            "publish_",
            "kick_",
            "ban_",
            "unban_",
            "invite_",
        ),
    )
)


class TopologyReader(Protocol):
    async def room_binding(
        self,
        room_id: str,
    ) -> TopologyBinding | None: ...

    async def human_for_sender(
        self,
        matrix_user_id: str,
    ) -> HumanResource | None: ...


RevisionProvider = Callable[[], int | Awaitable[int]]


class RoomPolicyResolver:
    """Resolve immutable turn authority from Controller-backed topology."""

    def __init__(
        self,
        *,
        topology: TopologyReader,
        admin_user_id: str,
        trusted_contacts: Collection[str] = (),
        revision: int | RevisionProvider = 1,
    ) -> None:
        self._topology = topology
        self._admin_user_id = admin_user_id
        self._trusted_contacts = frozenset(trusted_contacts)
        self._revision = revision

    async def resolve(self, event: InboundEvent) -> RoomPolicy:
        revision = await self._current_revision()
        if event.is_direct and event.sender_id == self._admin_user_id:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=revision,
                allowed_tools=ALL_MANAGER_TOOLS,
                confirm_tools=_CONFIRM_TOOLS,
                allowed_senders=frozenset({event.sender_id}),
            )

        binding = await self._topology.room_binding(event.room_id)
        human = await self._topology.human_for_sender(event.sender_id)
        if binding is not None:
            return self._resolve_bound_room(
                event=event,
                binding=binding,
                human=human,
                revision=revision,
            )

        if human is not None and self._human_can_access(human, event.room_id):
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.HUMAN_OR_CHANNEL_ROOM,
                revision=revision,
                allowed_tools=HUMAN_TOOLS,
                allowed_senders=frozenset({event.sender_id}),
                resource_name=human.name,
            )
        if event.sender_id in self._trusted_contacts:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.HUMAN_OR_CHANNEL_ROOM,
                revision=revision,
                allowed_tools=TRUSTED_TOOLS,
                allowed_senders=frozenset({event.sender_id}),
            )
        return self._unknown(event, revision)

    def _resolve_bound_room(
        self,
        *,
        event: InboundEvent,
        binding: TopologyBinding,
        human: HumanResource | None,
        revision: int,
    ) -> RoomPolicy:
        if binding.room_kind is RoomKind.TEAM_ROOM:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.TEAM_ROOM,
                revision=revision,
                silent=True,
            )

        known_actor = event.sender_id in {
            self._admin_user_id,
            binding.matrix_user_id,
        }
        if known_actor:
            tools = _tools_for_kind(binding.room_kind)
        elif human is not None and self._human_can_access(
            human,
            event.room_id,
        ):
            tools = HUMAN_TOOLS
        elif event.sender_id in self._trusted_contacts:
            tools = TRUSTED_TOOLS
        else:
            return self._unknown(event, revision, force_silent=True)

        return RoomPolicy(
            room_id=event.room_id,
            kind=binding.room_kind,
            revision=revision,
            allowed_tools=tools,
            allowed_senders=frozenset({event.sender_id}),
            resource_name=binding.resource_name,
            team_name=(
                binding.resource_name
                if binding.room_kind
                in {RoomKind.LEADER_ROOM, RoomKind.TEAM_ROOM}
                else None
            ),
        )

    @staticmethod
    def _human_can_access(human: HumanResource, room_id: str) -> bool:
        return human.permission_level == 1 or room_id in human.allowed_rooms

    @staticmethod
    def _unknown(
        event: InboundEvent,
        revision: int,
        *,
        force_silent: bool = False,
    ) -> RoomPolicy:
        return RoomPolicy(
            room_id=event.room_id,
            kind=RoomKind.UNKNOWN,
            revision=revision,
            allowed_tools=UNKNOWN_TOOLS,
            silent=force_silent or not event.is_direct,
        )

    async def _current_revision(self) -> int:
        if isinstance(self._revision, int):
            return self._revision
        value = self._revision()
        if hasattr(value, "__await__"):
            return int(await value)
        return int(value)


def _tools_for_kind(kind: RoomKind) -> frozenset[str]:
    if kind is RoomKind.WORKER_ROOM:
        return WORKER_TOOLS
    if kind is RoomKind.LEADER_ROOM:
        return LEADER_TOOLS
    return UNKNOWN_TOOLS
