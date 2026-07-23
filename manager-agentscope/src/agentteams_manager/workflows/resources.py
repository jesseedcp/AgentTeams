"""Controller-fact reconciliation and Matrix topology materialization."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.clients.agt import (
    AgtProtocolError,
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from agentteams_manager.clients.process import ProcessTimeout
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
)
from agentteams_manager.domain.ids import (
    matrix_transaction_id,
    operation_id_for,
)
from agentteams_manager.domain.models import (
    ExternalEffect,
    HumanResource,
    OperationKind,
    OperationRecord,
    OperationStatus,
    RoomKind,
    RoomPolicy,
    TeamResource,
    TopologySnapshot,
    WorkerResource,
)
from agentteams_manager.domain.ports import MatrixPort
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


class MutationContext(BaseModel):
    """Stable Matrix/tool identity for a resource mutation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    room_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    tool_call_id: str = Field(min_length=1)

    @property
    def operation_id(self) -> str:
        return operation_id_for(
            self.room_id,
            self.event_id,
            self.tool_call_id,
        )


class ResourceController(ReconciliationController, Protocol):
    async def list_workers(self) -> tuple[WorkerResource, ...]: ...

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource | None: ...

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
    ) -> WorkerResource: ...

    async def sleep_worker(self, name: str) -> WorkerResource: ...

    async def wake_worker(self, name: str) -> WorkerResource: ...

    async def delete_worker(self, name: str) -> None: ...


class ResourceSupervisor(Protocol):
    async def begin(
        self,
        *,
        operation_id: str,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> OperationRecord: ...

    async def before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> object: ...

    async def effect_acknowledged(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...

    async def effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...

    async def effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord: ...

    async def effect_failed(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord: ...


class TopologyRefresher(Protocol):
    async def refresh(self) -> object: ...


Sleeper = Callable[[float], Awaitable[None]]


class ResourceService:
    """Run Worker mutations as durable, fact-proved workflows."""

    def __init__(
        self,
        *,
        controller: ResourceController,
        supervisor: ResourceSupervisor,
        topology: TopologyRefresher,
        matrix: MatrixPort,
        sleeper: Sleeper = asyncio.sleep,
        worker_poll_delays: tuple[float, ...] = (0.25, 0.5, 1, 2, 4),
        greeting: str = (
            "Hello. I am the AgentTeams Manager. "
            "This room is your direct coordination channel."
        ),
    ) -> None:
        if not worker_poll_delays:
            raise ValueError("worker_poll_delays cannot be empty")
        if any(delay < 0 for delay in worker_poll_delays):
            raise ValueError("worker poll delays cannot be negative")
        self._controller = controller
        self._supervisor = supervisor
        self._topology = topology
        self._matrix = matrix
        self._sleeper = sleeper
        self._worker_poll_delays = worker_poll_delays
        self._greeting = greeting

    async def get_worker(self, name: str) -> WorkerResource | None:
        return await self._controller.get_worker(name)

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        return await self._controller.list_workers()

    async def create_worker(
        self,
        request: WorkerCreateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CREATE_WORKER,
            target_key=f"worker/{request.name}",
            request=request.model_dump(mode="json"),
        )
        return await self.resume_worker_create(operation)

    async def resume_worker_create(
        self,
        operation: OperationRecord,
    ) -> WorkerResource:
        if operation.kind is not OperationKind.CREATE_WORKER:
            raise ValueError("operation is not a Worker create")
        request = WorkerCreateRequest.model_validate(operation.request)
        if operation.target_key != f"worker/{request.name}":
            raise ConflictError(
                "Worker create operation target does not match request",
            )
        if operation.status is OperationStatus.SUCCEEDED:
            return await self._require_worker(request.name)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"create worker/{request.name} previously failed",
            )

        if operation.status is OperationStatus.PLANNED:
            existing = await self._controller.get_worker(request.name)
            if existing is not None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource already exists",
                )
                raise ConflictError(
                    f"worker/{request.name} already exists",
                )
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "create_worker",
                    "name": request.name,
                    "runtime": request.runtime,
                    "model": request.model,
                },
            )
            try:
                await self._controller.create_worker(request)
            except Exception as exc:
                worker = await self._handle_ambiguous_worker_effect(
                    operation_id=operation.operation_id,
                    name=request.name,
                    effect=ExternalEffect.CONTROLLER,
                    exc=exc,
                )
            else:
                worker = None
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {"name": request.name, "accepted": True},
            )
        else:
            worker = await self._controller.get_worker(request.name)
            if worker is None:
                raise AmbiguousEffectError(
                    f"create worker/{request.name} has no Controller proof",
                )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                _resource_receipt(worker),
            )

        ready = await self._wait_for_worker_room(
            operation_id=operation.operation_id,
            name=request.name,
            initial=worker,
        )
        try:
            await self._topology.refresh()
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.MATRIX,
                f"topology not converged: {type(exc).__name__}",
            )
            raise

        transaction_id = matrix_transaction_id(
            operation.operation_id,
            0,
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "greet_worker",
                "room_id": ready.room_id or "",
                "txn_id": transaction_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                ready.room_id or "",
                self._greeting,
                txn_id=transaction_id,
            )
        except Exception as exc:
            if _ambiguous_exception(exc):
                await self._supervisor.effect_ambiguous(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    type(exc).__name__,
                )
            else:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    _safe_reason(exc),
                )
            raise
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                **_resource_receipt(ready),
                "greeting_event_id": event_id,
                "greeting_txn_id": transaction_id,
            },
        )
        return ready

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        await self._require_worker(request.name)

        async def mutate() -> None:
            await self._controller.update_worker(request)

        async def prove() -> WorkerResource | None:
            worker = await self._controller.get_worker(request.name)
            if worker is None or not _matches_worker_update(worker, request):
                return None
            return worker

        result = await self._run_worker_mutation(
            context=context,
            kind=OperationKind.UPDATE_WORKER,
            name=request.name,
            request={
                "action": "update",
                **request.model_dump(mode="json"),
            },
            mutate=mutate,
            prove=prove,
        )
        if result is None:
            raise NotFoundError(f"worker/{request.name} disappeared")
        return result

    async def sleep_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        await self._require_worker(name)

        async def mutate() -> None:
            await self._controller.sleep_worker(name)

        async def prove() -> WorkerResource | None:
            worker = await self._controller.get_worker(name)
            if worker is None:
                return None
            state = str(worker.status.get("containerState", "")).casefold()
            return worker if state in {"stopped", "sleeping", "exited"} else None

        result = await self._run_worker_mutation(
            context=context,
            kind=OperationKind.UPDATE_WORKER,
            name=name,
            request={"name": name, "action": "sleep"},
            mutate=mutate,
            prove=prove,
        )
        if result is None:
            raise NotFoundError(f"worker/{name} disappeared")
        return result

    async def wake_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        await self._require_worker(name)

        async def mutate() -> None:
            await self._controller.wake_worker(name)

        async def prove() -> WorkerResource | None:
            worker = await self._controller.get_worker(name)
            if worker is None:
                return None
            state = str(worker.status.get("containerState", "")).casefold()
            return worker if state in {"running", "ready"} else None

        result = await self._run_worker_mutation(
            context=context,
            kind=OperationKind.UPDATE_WORKER,
            name=name,
            request={"name": name, "action": "wake"},
            mutate=mutate,
            prove=prove,
        )
        if result is None:
            raise NotFoundError(f"worker/{name} disappeared")
        return result

    async def delete_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> None:
        await self._require_worker(name)

        async def mutate() -> None:
            await self._controller.delete_worker(name)

        async def prove() -> WorkerResource | None | bool:
            worker = await self._controller.get_worker(name)
            return True if worker is None else None

        await self._run_worker_mutation(
            context=context,
            kind=OperationKind.DELETE_WORKER,
            name=name,
            request={"name": name, "action": "delete"},
            mutate=mutate,
            prove=prove,
        )

    async def _run_worker_mutation(
        self,
        *,
        context: MutationContext,
        kind: OperationKind,
        name: str,
        request: dict[str, object],
        mutate: Callable[[], Awaitable[None]],
        prove: Callable[
            [],
            Awaitable[WorkerResource | None | bool],
        ],
    ) -> WorkerResource | None:
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=kind,
            target_key=f"worker/{name}",
            request=request,
        )
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"{request['action']} worker/{name} previously failed",
            )
        if operation.status is OperationStatus.SUCCEEDED:
            proof = await prove()
            return proof if isinstance(proof, WorkerResource) else None

        if operation.status is OperationStatus.PLANNED:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": f"{request['action']}_worker",
                    "name": name,
                },
            )
            try:
                await mutate()
            except Exception as exc:
                if _ambiguous_exception(exc):
                    await self._supervisor.effect_ambiguous(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        type(exc).__name__,
                    )
                else:
                    await self._supervisor.effect_failed(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        _safe_reason(exc),
                    )
                    raise

        proof = await prove()
        if proof is None:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller state has not converged",
            )
            raise AmbiguousEffectError(
                f"{request['action']} worker/{name} is not proven",
            )
        receipt = (
            _resource_receipt(proof)
            if isinstance(proof, WorkerResource)
            else {"name": name, "deleted": True}
        )
        try:
            await self._topology.refresh()
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.MATRIX,
                f"topology not converged: {type(exc).__name__}",
            )
            raise
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt,
        )
        if isinstance(proof, WorkerResource):
            return proof
        return None

    async def _wait_for_worker_room(
        self,
        *,
        operation_id: str,
        name: str,
        initial: WorkerResource | None,
    ) -> WorkerResource:
        worker = initial
        attempts = len(self._worker_poll_delays) + 1
        for index in range(attempts):
            if worker is None or index > 0 or initial is None:
                worker = await self._controller.get_worker(name)
            if worker is not None:
                phase = (worker.phase or "").casefold()
                if phase in {"failed", "error", "deleting"}:
                    message = _resource_message(worker)
                    await self._supervisor.effect_failed(
                        operation_id,
                        ExternalEffect.CONTROLLER,
                        message or f"worker/{name} entered {phase}",
                    )
                    raise ConflictError(
                        message or f"worker/{name} entered {phase}",
                    )
                if worker.room_id and phase not in {
                    "pending",
                    "creating",
                    "provisioning",
                }:
                    return worker
            if index < len(self._worker_poll_delays):
                await self._sleeper(self._worker_poll_delays[index])
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.CONTROLLER,
            "Worker readiness did not converge",
        )
        raise AmbiguousEffectError(
            f"worker/{name} has no ready Controller room",
        )

    async def _handle_ambiguous_worker_effect(
        self,
        *,
        operation_id: str,
        name: str,
        effect: ExternalEffect,
        exc: Exception,
    ) -> WorkerResource:
        if not _ambiguous_exception(exc):
            await self._supervisor.effect_failed(
                operation_id,
                effect,
                _safe_reason(exc),
            )
            raise exc
        await self._supervisor.effect_ambiguous(
            operation_id,
            effect,
            type(exc).__name__,
        )
        worker = await self._controller.get_worker(name)
        if worker is None:
            raise AmbiguousEffectError(
                f"worker/{name} create result is unknown",
            ) from exc
        return worker

    async def _require_worker(self, name: str) -> WorkerResource:
        worker = await self._controller.get_worker(name)
        if worker is None:
            raise NotFoundError(f"worker/{name} does not exist")
        return worker


class RecoverableOperationReader(Protocol):
    async def list_recoverable(self) -> tuple[OperationRecord, ...]: ...


class WorkerRecoveryReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()


class ResourceHeartbeat:
    """Resume pending Worker creates without issuing another create."""

    def __init__(
        self,
        *,
        operations: RecoverableOperationReader,
        resources: ResourceService,
    ) -> None:
        self._operations = operations
        self._resources = resources

    async def reconcile_pending_workers(self) -> WorkerRecoveryReport:
        operations = tuple(
            operation
            for operation in await self._operations.list_recoverable()
            if operation.kind is OperationKind.CREATE_WORKER
        )
        reconciled: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        for operation in operations:
            try:
                await self._resources.resume_worker_create(operation)
            except AmbiguousEffectError:
                pending.append(operation.operation_id)
            except Exception:
                failed.append(operation.operation_id)
            else:
                reconciled.append(operation.operation_id)
        return WorkerRecoveryReport(
            inspected=len(operations),
            reconciled=tuple(reconciled),
            pending=tuple(pending),
            failed=tuple(failed),
        )


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


def _matches_worker_update(
    worker: WorkerResource,
    request: WorkerUpdateRequest,
) -> bool:
    if request.model is not None and worker.model != request.model:
        return False
    if request.runtime is not None and worker.runtime != request.runtime:
        return False
    if (
        request.image is not None
        and worker.spec.get("image") != request.image
    ):
        return False
    return True


def _ambiguous_exception(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            AgtProtocolError,
            ConnectionError,
            ProcessTimeout,
            TimeoutError,
        ),
    )


def _safe_reason(exc: BaseException) -> str:
    return type(exc).__name__


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
