"""Hard room and sender authorization for Matrix events."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from agentteams_manager.domain.models import (
    HumanResource,
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.state.topology import (
    Actor,
    ActorKind,
    TopologyBinding,
)

ALL_MANAGER_TOOLS = frozenset(
    {
        "find_worker",
        "import_worker",
        "list_workers",
        "get_worker",
        "create_worker",
        "update_worker",
        "sleep_worker",
        "wake_worker",
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
        "inspect_git_request",
        "git_delegate",
        "git_delegate_high_risk",
        "list_channels",
        "create_channel",
        "update_channel",
        "delete_channel",
        "send_notification",
        "list_matrix_rooms",
        "list_matrix_members",
        "lookup_matrix_user",
        "get_matrix_room_state",
        "upload_matrix_media",
        "download_matrix_media",
        "invite_matrix_user",
        "kick_matrix_user",
        "ban_matrix_user",
        "unban_matrix_user",
        "list_mcp_servers",
        "configure_mcp",
        "remove_mcp",
        "switch_model",
        "switch_worker_model",
        "update_manager_identity",
        "publish_service",
    },
)

WORKER_TOOLS = frozenset(
    {
        "delegate_task",
        "complete_task",
        "sync_files",
        "inspect_git_request",
        "git_delegate",
        "git_delegate_high_risk",
    },
)
LEADER_TOOLS = frozenset(
    {
        "delegate_team_task",
        "complete_task",
        "sync_files",
        "switch_worker_model",
    },
)
PROJECT_TOOLS = frozenset(
    {
        "list_tasks",
        "get_task",
        "complete_task",
        "list_projects",
        "get_project",
        "update_project",
        "delete_project",
        "sync_files",
    },
)
PROJECT_PARTICIPANT_TOOLS = frozenset(
    {
        "list_tasks",
        "get_task",
        "complete_task",
        "get_project",
        "sync_files",
    },
)
HUMAN_TOOLS = frozenset({"list_workers", "list_tasks", "sync_files"})
TEAM_SCOPED_HUMAN_TOOLS = frozenset(
    {
        "list_workers",
        "get_worker",
        "list_teams",
        "get_team",
        "list_tasks",
        "get_task",
        "sync_files",
        "send_notification",
    },
)
WORKER_SCOPED_HUMAN_TOOLS = frozenset(
    {
        "list_workers",
        "get_worker",
        "list_tasks",
        "get_task",
        "sync_files",
        "send_notification",
    },
)
TRUSTED_TOOLS = frozenset({"list_workers", "list_tasks"})
UNKNOWN_TOOLS: frozenset[str] = frozenset()

CONFIRM_TOOLS = frozenset(
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
) | frozenset(
    {
        "import_worker",
        "sleep_worker",
        "wake_worker",
        "send_notification",
        "upload_matrix_media",
        "git_delegate_high_risk",
    },
)
READ_ONLY_RESOURCE_TOOLS = frozenset(
    {
        "list_workers",
        "get_worker",
        "list_teams",
        "get_team",
        "list_humans",
        "get_human",
    },
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

    async def actor_for_sender(
        self,
        matrix_user_id: str,
    ) -> Actor | None: ...


RevisionProvider = Callable[[], int | Awaitable[int]]


class RoomPolicyResolver:
    """Resolve immutable turn authority from Controller-backed topology."""

    def __init__(
        self,
        *,
        topology: TopologyReader,
        admin_user_id: str,
        manager_user_id: str = "@manager:local",
        revision: int | RevisionProvider = 1,
    ) -> None:
        self._topology = topology
        self._admin_user_id = admin_user_id
        self._manager_user_id = manager_user_id
        self._revision = revision

    async def resolve(self, event: InboundEvent) -> RoomPolicy:
        revision = await self._current_revision()
        if not self._is_actionable(event):
            return self._unknown(
                event,
                revision,
                force_silent=True,
            )
        if event.sender_id == self._manager_user_id:
            return self._unknown(
                event,
                revision,
                force_silent=True,
            )
        if event.is_direct and event.sender_id == self._admin_user_id:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=revision,
                allowed_tools=ALL_MANAGER_TOOLS,
                confirm_tools=CONFIRM_TOOLS,
                allowed_senders=frozenset({event.sender_id}),
            )

        binding = await self._topology.room_binding(event.room_id)
        actor = await self._topology.actor_for_sender(event.sender_id)
        human = await self._topology.human_for_sender(event.sender_id)
        if binding is not None:
            policy = self._resolve_bound_room(
                event=event,
                binding=binding,
                actor=actor,
                human=human,
                revision=revision,
            )
            if not self.should_wake(event, binding, actor):
                return policy.model_copy(update={"silent": True})
            return policy

        if human is not None and self._human_can_access(human, event.room_id):
            policy = policy_for_human(
                human,
                room_id=event.room_id,
                revision=revision,
            )
        elif actor is not None and actor.kind is ActorKind.TRUSTED_CONTACT:
            policy = RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.HUMAN_OR_CHANNEL_ROOM,
                revision=revision,
                allowed_tools=TRUSTED_TOOLS,
                allowed_senders=frozenset({event.sender_id}),
            )
        else:
            return self._unknown(event, revision)
        if not self.should_wake(event, binding, actor):
            return policy.model_copy(update={"silent": True})
        return policy

    def _resolve_bound_room(
        self,
        *,
        event: InboundEvent,
        binding: TopologyBinding,
        actor: Actor | None,
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

        project_participant = (
            binding.room_kind is RoomKind.PROJECT_ROOM
            and actor is not None
            and actor.resource_name
            in _scope_names(binding.payload.get("participants"))
            and actor.kind
            in {
                ActorKind.WORKER,
                ActorKind.TEAM_LEADER,
                ActorKind.TEAM_WORKER,
            }
        )
        known_actor = (
            event.sender_id == self._admin_user_id
            or event.sender_id == binding.matrix_user_id
            or project_participant
        )
        if known_actor:
            tools = (
                PROJECT_PARTICIPANT_TOOLS
                if project_participant
                else _tools_for_kind(binding.room_kind)
            )
        elif human is not None and self._human_can_access(
            human,
            event.room_id,
        ):
            return policy_for_human(
                human,
                room_id=event.room_id,
                revision=revision,
            )
        elif actor is not None and actor.kind is ActorKind.TRUSTED_CONTACT:
            tools = TRUSTED_TOOLS
        else:
            return self._unknown(event, revision, force_silent=True)

        return RoomPolicy(
            room_id=event.room_id,
            kind=binding.room_kind,
            revision=revision,
            allowed_tools=tools,
            confirm_tools=tools & CONFIRM_TOOLS,
            allowed_senders=frozenset({event.sender_id}),
            resource_name=binding.resource_name,
            team_name=(
                binding.resource_name
                if binding.room_kind
                in {RoomKind.LEADER_ROOM, RoomKind.TEAM_ROOM}
                else None
            ),
            allowed_mcp_names=_mcp_names(binding.payload),
            allowed_worker_names=(
                team_member_names(binding.payload)
                if binding.room_kind is RoomKind.LEADER_ROOM
                else frozenset()
            ),
            project_id=(
                binding.resource_name
                if binding.room_kind is RoomKind.PROJECT_ROOM
                else None
            ),
        )

    def should_wake(
        self,
        event: InboundEvent,
        binding: TopologyBinding | None,
        actor: Actor | None,
    ) -> bool:
        del actor
        if event.sender_id == self._manager_user_id:
            return False
        if binding is not None and binding.room_kind is RoomKind.TEAM_ROOM:
            return False
        if binding is not None and binding.room_kind in {
            RoomKind.LEADER_ROOM,
            RoomKind.PROJECT_ROOM,
        }:
            return self._manager_user_id in event.mentions
        if event.is_direct:
            return True
        return self._manager_user_id in event.mentions

    @staticmethod
    def _is_actionable(event: InboundEvent) -> bool:
        return (
            event.event_type != "m.room.redaction"
            and event.relation_type != "m.replace"
            and not event.is_bot_acknowledgement
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
    if kind is RoomKind.PROJECT_ROOM:
        return PROJECT_TOOLS
    return UNKNOWN_TOOLS


def policy_for_human(
    human: HumanResource,
    *,
    room_id: str,
    revision: int,
) -> RoomPolicy:
    """Map Controller Human tiers to tools and named resource scopes."""
    if human.permission_level == 1:
        tools = ALL_MANAGER_TOOLS
        confirmations = CONFIRM_TOOLS
        scope_all = True
    elif human.permission_level == 2:
        tools = TEAM_SCOPED_HUMAN_TOOLS
        confirmations = tools & CONFIRM_TOOLS
        scope_all = False
    else:
        tools = WORKER_SCOPED_HUMAN_TOOLS
        confirmations = tools & CONFIRM_TOOLS
        scope_all = False
    return RoomPolicy(
        room_id=room_id,
        kind=RoomKind.HUMAN_OR_CHANNEL_ROOM,
        revision=revision,
        allowed_tools=tools,
        confirm_tools=confirmations,
        allowed_senders=frozenset({human.matrix_user_id}),
        resource_name=human.name,
        resource_scope_all=scope_all,
        allowed_team_names=_scope_names(
            human.spec.get("accessibleTeams"),
        ),
        allowed_worker_names=_scope_names(
            human.spec.get("accessibleWorkers"),
        ),
    )


def _scope_names(value: object) -> frozenset[str]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(
        item
        for item in value
        if isinstance(item, str) and item
    )


def _mcp_names(payload: dict[str, object]) -> frozenset[str]:
    spec = payload.get("spec")
    sources = (
        spec.get("mcpServers")
        if isinstance(spec, dict)
        else None
    )
    if sources is None:
        sources = payload.get("mcpServers")
    if not isinstance(sources, (list, tuple)):
        return frozenset()
    return frozenset(
        name
        for item in sources
        if isinstance(item, dict)
        and isinstance((name := item.get("name")), str)
        and name
    )


def team_member_names(
    payload: dict[str, object],
) -> frozenset[str]:
    """Return only Controller-materialized Team member names."""
    leader = payload.get("leader")
    workers = payload.get("workers")
    names = {
        item
        for item in (
            leader,
            *(workers if isinstance(workers, (list, tuple)) else ()),
        )
        if isinstance(item, str) and item
    }
    return frozenset(names)
