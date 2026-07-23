"""Controller-fact reconciliation and Matrix topology materialization."""

from __future__ import annotations

import asyncio
from collections.abc import Collection
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    HumanResource,
    OperationKind,
    OperationRecord,
    RoomKind,
    RoomPolicy,
    TeamResource,
    TopologySnapshot,
    WorkerResource,
)
from agentteams_manager.matrix.policy import (
    ALL_MANAGER_TOOLS,
    CONFIRM_TOOLS,
    HUMAN_TOOLS,
    LEADER_TOOLS,
    READ_ONLY_RESOURCE_TOOLS,
    TRUSTED_TOOLS,
    WORKER_TOOLS,
)
from agentteams_manager.state.topology import TopologyRepository


class ReconcileDisposition(StrEnum):
    """What Controller facts prove about an ambiguous operation."""

    EFFECT_ABSENT = "effect_absent"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ReconcileResult(BaseModel):
    """A transport-neutral proof returned by a resource reconciler."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: ReconcileDisposition
    receipt: dict[str, Any] = Field(default_factory=dict)
    message: str = ""

    @classmethod
    def effect_absent(cls) -> ReconcileResult:
        return cls(disposition=ReconcileDisposition.EFFECT_ABSENT)

    @classmethod
    def pending(
        cls,
        *,
        receipt: dict[str, Any] | None = None,
        message: str = "",
    ) -> ReconcileResult:
        return cls(
            disposition=ReconcileDisposition.PENDING,
            receipt=receipt or {},
            message=message,
        )

    @classmethod
    def succeeded(
        cls,
        *,
        receipt: dict[str, Any],
    ) -> ReconcileResult:
        return cls(
            disposition=ReconcileDisposition.SUCCEEDED,
            receipt=receipt,
        )

    @classmethod
    def failed(cls, message: str) -> ReconcileResult:
        return cls(
            disposition=ReconcileDisposition.FAILED,
            message=message,
        )


class ReconciliationController(Protocol):
    async def get_worker(self, name: str) -> WorkerResource | None: ...

    async def get_team(self, name: str) -> TeamResource | None: ...

    async def get_human(self, name: str) -> HumanResource | None: ...


class ResourceReconciler:
    """Resolve uncertain mutations from current Controller resources."""

    _CREATE_KINDS = {
        OperationKind.CREATE_WORKER: "worker",
        OperationKind.CREATE_TEAM: "team",
        OperationKind.CREATE_HUMAN: "human",
    }
    _UPDATE_KINDS = {
        OperationKind.UPDATE_WORKER: "worker",
        OperationKind.UPDATE_TEAM: "team",
        OperationKind.UPDATE_HUMAN: "human",
    }
    _DELETE_KINDS = {
        OperationKind.DELETE_WORKER: "worker",
        OperationKind.DELETE_TEAM: "team",
        OperationKind.DELETE_HUMAN: "human",
    }

    def __init__(self, controller: ReconciliationController) -> None:
        self._controller = controller

    async def reconcile(
        self,
        operation: OperationRecord,
    ) -> ReconcileResult:
        resource_type = (
            self._CREATE_KINDS.get(operation.kind)
            or self._UPDATE_KINDS.get(operation.kind)
            or self._DELETE_KINDS.get(operation.kind)
        )
        if resource_type is None:
            raise ValueError(
                f"operation kind {operation.kind.value!r} is not a "
                "resource mutation",
            )
        name = _target_name(operation.target_key, resource_type)
        resource = await self._get(resource_type, name)

        if operation.kind in self._DELETE_KINDS:
            if resource is None:
                return ReconcileResult.succeeded(
                    receipt={"name": name, "deleted": True},
                )
            return ReconcileResult.pending(
                receipt=_resource_receipt(resource),
                message=f"{resource_type}/{name} still exists",
            )

        if resource is None:
            return ReconcileResult.effect_absent()
        phase = _resource_phase(resource).casefold()
        message = _resource_message(resource)
        if phase in {"failed", "error", "deleting"}:
            return ReconcileResult.failed(
                message or f"{resource_type}/{name} entered {phase}",
            )
        receipt = _resource_receipt(resource)
        if phase in {"pending", "creating", "provisioning"}:
            return ReconcileResult.pending(
                receipt=receipt,
                message=message,
            )
        return ReconcileResult.succeeded(receipt=receipt)

    async def _get(
        self,
        resource_type: str,
        name: str,
    ) -> WorkerResource | TeamResource | HumanResource | None:
        if resource_type == "worker":
            return await self._controller.get_worker(name)
        if resource_type == "team":
            return await self._controller.get_team(name)
        return await self._controller.get_human(name)


class TopologyController(Protocol):
    async def list_workers(self) -> tuple[WorkerResource, ...]: ...

    async def list_teams(self) -> tuple[TeamResource, ...]: ...

    async def list_humans(self) -> tuple[HumanResource, ...]: ...


class TopologyMatrix(Protocol):
    async def joined_rooms(self) -> tuple[str, ...]: ...

    async def members(self, room_id: str) -> tuple[str, ...]: ...


class TopologyResolver:
    """Build policy facts only after Controller and Matrix agree."""

    def __init__(
        self,
        *,
        controller: TopologyController,
        matrix: TopologyMatrix,
        topology: TopologyRepository,
        manager_user_id: str,
        admin_user_id: str,
        admin_room_id: str,
        trusted_contacts: Collection[str] = (),
    ) -> None:
        self._controller = controller
        self._matrix = matrix
        self._topology = topology
        self._manager_user_id = manager_user_id
        self._admin_user_id = admin_user_id
        self._admin_room_id = admin_room_id
        self._trusted_contacts = frozenset(trusted_contacts)
        self._refresh_lock = asyncio.Lock()

    async def refresh(self) -> TopologySnapshot:
        async with self._refresh_lock:
            return await self._refresh()

    async def _refresh(self) -> TopologySnapshot:
        workers, teams, humans, joined_rooms = await asyncio.gather(
            self._controller.list_workers(),
            self._controller.list_teams(),
            self._controller.list_humans(),
            self._matrix.joined_rooms(),
        )
        workers_by_name = _unique_by_name(workers, "Worker")
        _unique_by_name(teams, "Team")
        _unique_by_name(humans, "Human")

        team_member_names: set[str] = set()
        join_targets: set[str] = set()
        forbidden_rooms: set[str] = set()
        resolved_teams: list[TeamResource] = []
        for team in teams:
            leader = workers_by_name.get(team.leader)
            if leader is None:
                raise ConflictError(
                    f"team/{team.name} leader worker/{team.leader} "
                    "does not exist",
                )
            team_member_names.add(team.leader)
            for worker_name in team.workers:
                if worker_name not in workers_by_name:
                    raise ConflictError(
                        f"team/{team.name} worker/{worker_name} "
                        "does not exist",
                    )
                team_member_names.add(worker_name)
            if leader.room_id:
                join_targets.add(leader.room_id)
            resolved_teams.append(
                team.model_copy(update={"room_id": leader.room_id}),
            )
            _add_string_room(forbidden_rooms, team.spec.get("teamRoomID"))
            _add_string_room(
                forbidden_rooms,
                team.spec.get("leaderDMRoomID"),
            )
            for worker_name in team.workers:
                room_id = workers_by_name[worker_name].room_id
                if room_id:
                    forbidden_rooms.add(room_id)

        for worker in workers:
            if worker.name not in team_member_names and worker.room_id:
                join_targets.add(worker.room_id)

        joined_room_set = frozenset(joined_rooms)
        rooms_to_inspect = (
            join_targets
            | {
                room_id
                for human in humans
                for room_id in human.allowed_rooms
            }
            | {
                worker.room_id
                for worker in workers
                if worker.room_id is not None
            }
        ) & joined_room_set
        inspected_room_ids = tuple(sorted(rooms_to_inspect))
        membership_rows = await asyncio.gather(
            *(
                self._matrix.members(room_id)
                for room_id in inspected_room_ids
            ),
        )
        memberships = {
            room_id: frozenset(members)
            for room_id, members in zip(
                inspected_room_ids,
                membership_rows,
                strict=True,
            )
        }
        self._validate_memberships(
            workers=workers,
            humans=humans,
            join_targets=join_targets,
            forbidden_rooms=forbidden_rooms,
            joined_rooms=joined_room_set,
            memberships=memberships,
        )
        revision = await self._topology.revision() + 1
        snapshot = TopologySnapshot(
            revision=revision,
            workers=workers,
            teams=tuple(resolved_teams),
            humans=humans,
            manager_join_targets=tuple(sorted(join_targets)),
            forbidden_rooms=tuple(sorted(forbidden_rooms)),
            refreshed_at=datetime.now(UTC),
        )
        await self._topology.replace_snapshot(snapshot)
        return snapshot

    async def policy_for(
        self,
        room_id: str,
        sender_id: str,
    ) -> RoomPolicy:
        revision = await self._topology.revision()
        if room_id == self._admin_room_id:
            if sender_id == self._admin_user_id:
                return RoomPolicy(
                    room_id=room_id,
                    kind=RoomKind.ADMIN_DM,
                    revision=revision,
                    allowed_tools=ALL_MANAGER_TOOLS,
                    confirm_tools=CONFIRM_TOOLS,
                    allowed_senders=frozenset({sender_id}),
                )
            return RoomPolicy(
                room_id=room_id,
                kind=RoomKind.ADMIN_DM,
                revision=revision,
                allowed_tools=READ_ONLY_RESOURCE_TOOLS,
                allowed_senders=frozenset({sender_id}),
            )

        binding = await self._topology.room_binding(room_id)
        if binding is None:
            return _deny_policy(room_id, revision)
        if binding.room_kind is RoomKind.TEAM_ROOM:
            return _deny_policy(room_id, revision, silent=True)

        if sender_id in {
            self._admin_user_id,
            binding.matrix_user_id,
        }:
            tools = (
                LEADER_TOOLS
                if binding.room_kind is RoomKind.LEADER_ROOM
                else WORKER_TOOLS
            )
        else:
            human = await self._topology.human_for_sender(sender_id)
            if human is not None and (
                human.permission_level == 1
                or room_id in human.allowed_rooms
            ):
                tools = HUMAN_TOOLS
            elif sender_id in self._trusted_contacts:
                tools = TRUSTED_TOOLS
            else:
                return _deny_policy(room_id, revision, silent=True)
        return RoomPolicy(
            room_id=room_id,
            kind=binding.room_kind,
            revision=revision,
            allowed_tools=tools,
            allowed_senders=frozenset({sender_id}),
            resource_name=binding.resource_name,
            team_name=(
                binding.resource_name
                if binding.room_kind is RoomKind.LEADER_ROOM
                else None
            ),
        )

    def _validate_memberships(
        self,
        *,
        workers: tuple[WorkerResource, ...],
        humans: tuple[HumanResource, ...],
        join_targets: set[str],
        forbidden_rooms: set[str],
        joined_rooms: frozenset[str],
        memberships: dict[str, frozenset[str]],
    ) -> None:
        for room_id in join_targets:
            if (
                room_id not in joined_rooms
                or self._manager_user_id not in memberships[room_id]
            ):
                raise ConflictError(
                    f"Manager is not joined to required room {room_id}",
                )
        for room_id in forbidden_rooms:
            if room_id in joined_rooms:
                raise ConflictError(
                    f"Manager is present in private Team room {room_id}",
                )
        for worker in workers:
            if (
                worker.room_id
                and worker.matrix_user_id
                and worker.room_id in memberships
                and worker.matrix_user_id
                not in memberships[worker.room_id]
            ):
                raise ConflictError(
                    f"worker/{worker.name} Matrix identity does not match "
                    f"room {worker.room_id}",
                )
        for human in humans:
            for room_id in human.allowed_rooms:
                if (
                    room_id in memberships
                    and human.matrix_user_id not in memberships[room_id]
                ):
                    raise ConflictError(
                        f"human/{human.name} Matrix identity does not "
                        f"match room {room_id}",
                    )


def _target_name(target_key: str, resource_type: str) -> str:
    prefix = f"{resource_type}/"
    if not target_key.startswith(prefix) or len(target_key) == len(prefix):
        raise ValueError(
            f"target {target_key!r} is not a {resource_type} key",
        )
    return target_key[len(prefix) :]


def _resource_receipt(
    resource: WorkerResource | TeamResource | HumanResource,
) -> dict[str, Any]:
    return resource.model_dump(mode="json")


def _resource_phase(
    resource: WorkerResource | TeamResource | HumanResource,
) -> str:
    if isinstance(resource, (WorkerResource, TeamResource)):
        return resource.phase or ""
    value = resource.status.get("phase")
    return str(value) if value is not None else ""


def _resource_message(
    resource: WorkerResource | TeamResource | HumanResource,
) -> str:
    value = resource.status.get("message")
    return str(value) if value is not None else ""


def _unique_by_name(
    resources: tuple[WorkerResource, ...]
    | tuple[TeamResource, ...]
    | tuple[HumanResource, ...],
    label: str,
) -> dict[str, Any]:
    indexed: dict[str, Any] = {}
    for resource in resources:
        if resource.name in indexed:
            raise ConflictError(
                f"duplicate {label} resource {resource.name!r}",
            )
        indexed[resource.name] = resource
    return indexed


def _add_string_room(rooms: set[str], value: object) -> None:
    if isinstance(value, str) and value:
        rooms.add(value)


def _deny_policy(
    room_id: str,
    revision: int,
    *,
    silent: bool = False,
) -> RoomPolicy:
    return RoomPolicy(
        room_id=room_id,
        kind=RoomKind.UNKNOWN,
        revision=revision,
        silent=silent,
    )
