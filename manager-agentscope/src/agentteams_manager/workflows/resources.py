"""Controller-fact reconciliation and Matrix topology materialization."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import secrets
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from agentteams_manager.clients.agt import (
    AgtProtocolError,
    HumanCreateRequest,
    HumanUpdateRequest,
    ResourceName,
    TeamCreateRequest,
    WorkerCreateRequest,
    WorkerRuntime,
    WorkerUpdateRequest,
)
from agentteams_manager.clients.nacos import NacosWorker
from agentteams_manager.clients.process import ProcessTimeout
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
    RecoveryError,
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
    LEADER_TOOLS,
    READ_ONLY_RESOURCE_TOOLS,
    TRUSTED_TOOLS,
    WORKER_TOOLS,
    policy_for_human,
    team_member_names,
)
from agentteams_manager.state.topology import ActorKind, TopologyRepository


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
        OperationKind.IMPORT_WORKER: "worker",
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

    async def apply_worker_package(
        self,
        *,
        name: str,
        package_uri: str,
        expected_digest: str,
        runtime: WorkerRuntime,
    ) -> WorkerResource: ...

    async def sleep_worker(self, name: str) -> WorkerResource: ...

    async def wake_worker(self, name: str) -> WorkerResource: ...

    async def delete_worker(self, name: str) -> None: ...

    async def list_teams(self) -> tuple[TeamResource, ...]: ...

    async def create_team(self, request: TeamCreateRequest) -> None: ...

    async def apply_team(
        self,
        name: str,
        document: bytes,
    ) -> TeamResource: ...

    async def delete_team(self, name: str) -> None: ...

    async def list_humans(self) -> tuple[HumanResource, ...]: ...

    async def create_human(self, request: HumanCreateRequest) -> None: ...

    async def update_human(
        self,
        request: HumanUpdateRequest,
    ) -> HumanResource: ...

    async def delete_human(self, name: str) -> None: ...


class ResourceSupervisor(Protocol):
    async def get(self, operation_id: str) -> OperationRecord | None: ...

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


class NacosDiscovery(Protocol):
    async def search_workers(
        self,
        query: str,
    ) -> tuple[NacosWorker, ...]: ...

    async def verify_worker(self, candidate: NacosWorker) -> None: ...

    async def inspect_worker_uri(
        self,
        package_uri: str,
    ) -> NacosWorker: ...


class WorkerImportError(ConflictError):
    """A confirmed Nacos import failed without a generic fallback."""


class WorkerDiscovery(BaseModel):
    """Read-only discovery result requiring a separate import confirmation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    query: str = Field(min_length=1)
    candidates: tuple[NacosWorker, ...]
    requires_confirmation: bool = True
    discovery_token: str = Field(pattern=r"^[0-9a-f]{64}$")


class WorkerImportConfirmation(BaseModel):
    """Tamper-evident binding between a candidate and local Worker name."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate: NacosWorker
    worker_name: ResourceName
    confirmation_token: str = Field(pattern=r"^[0-9a-f]{64}$")


Sleeper = Callable[[float], Awaitable[None]]


class TeamSpec(BaseModel):
    """Typed subset of the current AgentTeams v1beta1 Team contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: ResourceName
    leader_name: ResourceName
    worker_names: tuple[ResourceName, ...] = ()
    team_name: ResourceName | None = None
    description: str = ""
    heartbeat_every: str | None = "30m"
    admin_name: ResourceName | None = None
    admin_matrix_id: str | None = None
    peer_mentions: bool = True

    @model_validator(mode="after")
    def validate_roster(self) -> TeamSpec:
        names = [self.leader_name, *self.worker_names]
        if len(names) != len(set(names)):
            raise ValueError("Team member names must be unique")
        if self.admin_matrix_id and not self.admin_name:
            raise ValueError(
                "admin_name is required when admin_matrix_id is set",
            )
        return self

    @property
    def is_simple_create(self) -> bool:
        return True

    @property
    def member_names(self) -> tuple[ResourceName, ...]:
        return (self.leader_name, *self.worker_names)

    def to_create_request(self) -> TeamCreateRequest:
        return TeamCreateRequest(
            name=self.name,
            leader_name=self.leader_name,
            worker_names=self.worker_names,
            team_name=self.team_name,
            description=self.description or None,
            heartbeat_every=self.heartbeat_every,
            admin_name=self.admin_name,
            admin_matrix_id=self.admin_matrix_id,
            peer_mentions=self.peer_mentions,
        )

    def to_apply_document(self) -> bytes:
        spec: dict[str, object] = {
            "teamName": self.team_name or self.name,
            "workerMembers": [
                {"name": self.leader_name, "role": "team_leader"},
                *[
                    {"name": name, "role": "worker"}
                    for name in self.worker_names
                ],
            ],
            "peerMentions": self.peer_mentions,
        }
        if self.description:
            spec["description"] = self.description
        if self.heartbeat_every:
            spec["heartbeatEvery"] = self.heartbeat_every
        if self.admin_name:
            spec["admin"] = {
                "name": self.admin_name,
                "matrixUserId": self.admin_matrix_id or "",
            }
        return json.dumps(
            {
                "apiVersion": "agentteams.io/v1beta1",
                "kind": "Team",
                "metadata": {"name": self.name},
                "spec": spec,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")


class ResourceService:
    """Run Worker mutations as durable, fact-proved workflows."""

    def __init__(
        self,
        *,
        controller: ResourceController,
        supervisor: ResourceSupervisor,
        topology: TopologyRefresher,
        matrix: MatrixPort,
        nacos: NacosDiscovery | None = None,
        confirmation_key: bytes | None = None,
        sleeper: Sleeper = asyncio.sleep,
        worker_poll_delays: tuple[float, ...] = (0.25, 0.5, 1, 2, 4),
        team_poll_delays: tuple[float, ...] = (0.5, 1, 2, 4, 8),
        human_poll_delays: tuple[float, ...] = (0.5, 1, 2, 4, 8),
        greeting: str = (
            "Hello. I am the AgentTeams Manager. "
            "This room is your direct coordination channel."
        ),
    ) -> None:
        if not worker_poll_delays:
            raise ValueError("worker_poll_delays cannot be empty")
        if any(delay < 0 for delay in worker_poll_delays):
            raise ValueError("worker poll delays cannot be negative")
        if not team_poll_delays:
            raise ValueError("team_poll_delays cannot be empty")
        if any(delay < 0 for delay in team_poll_delays):
            raise ValueError("team poll delays cannot be negative")
        if not human_poll_delays:
            raise ValueError("human_poll_delays cannot be empty")
        if any(delay < 0 for delay in human_poll_delays):
            raise ValueError("human poll delays cannot be negative")
        self._controller = controller
        self._supervisor = supervisor
        self._topology = topology
        self._matrix = matrix
        self._nacos = nacos
        self._confirmation_key = confirmation_key or secrets.token_bytes(32)
        if len(self._confirmation_key) < 32:
            raise ValueError("confirmation_key must be at least 32 bytes")
        self._sleeper = sleeper
        self._worker_poll_delays = worker_poll_delays
        self._team_poll_delays = team_poll_delays
        self._human_poll_delays = human_poll_delays
        self._greeting = greeting

    async def get_worker(self, name: str) -> WorkerResource | None:
        return await self._controller.get_worker(name)

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        return await self._controller.list_workers()

    async def get_team(self, name: str) -> TeamResource | None:
        return await self._controller.get_team(name)

    async def list_teams(self) -> tuple[TeamResource, ...]:
        return await self._controller.list_teams()

    async def get_human(self, name: str) -> HumanResource | None:
        return await self._controller.get_human(name)

    async def list_humans(self) -> tuple[HumanResource, ...]:
        return await self._controller.list_humans()

    async def find_worker(self, query: str) -> WorkerDiscovery:
        """Search Nacos without performing any Controller mutation."""
        if self._nacos is None:
            raise WorkerImportError("Nacos discovery is not configured")
        normalized = " ".join(query.split())
        if not normalized:
            raise ValueError("Worker search query cannot be empty")
        candidates = await self._nacos.search_workers(normalized)
        unsigned = {
            "query": normalized,
            "candidates": [
                item.model_dump(mode="json")
                for item in candidates
            ],
        }
        return WorkerDiscovery(
            query=normalized,
            candidates=candidates,
            discovery_token=self._confirmation_signature(
                "discovery",
                unsigned,
            ),
        )

    def confirm_import(
        self,
        discovery: WorkerDiscovery,
        *,
        candidate_name: str,
        worker_name: str,
    ) -> WorkerImportConfirmation:
        """Bind an explicit candidate choice to its local Worker name."""
        unsigned_discovery = {
            "query": discovery.query,
            "candidates": [
                item.model_dump(mode="json")
                for item in discovery.candidates
            ],
        }
        expected = self._confirmation_signature(
            "discovery",
            unsigned_discovery,
        )
        if not hmac.compare_digest(expected, discovery.discovery_token):
            raise WorkerImportError("Worker discovery confirmation is invalid")
        candidate = next(
            (
                item
                for item in discovery.candidates
                if item.name == candidate_name
            ),
            None,
        )
        if candidate is None:
            raise NotFoundError(
                f"Nacos candidate {candidate_name!r} was not discovered",
            )
        if re.fullmatch(r"[a-z0-9][a-z0-9-]*", worker_name) is None:
            raise ValueError(f"invalid resource name {worker_name!r}")
        payload = {
            "candidate": candidate.model_dump(mode="json"),
            "worker_name": worker_name,
        }
        return WorkerImportConfirmation(
            candidate=candidate,
            worker_name=worker_name,
            confirmation_token=self._confirmation_signature(
                "import",
                payload,
            ),
        )

    async def confirm_direct_import(
        self,
        *,
        package_uri: str,
        worker_name: str,
    ) -> WorkerImportConfirmation:
        """Inspect but do not search one explicit URI, then bind its import."""
        if self._nacos is None:
            raise WorkerImportError("Nacos discovery is not configured")
        candidate = await self._nacos.inspect_worker_uri(package_uri)
        query = f"direct:{candidate.package_uri}"
        candidates = (candidate,)
        discovery = WorkerDiscovery(
            query=query,
            candidates=candidates,
            discovery_token=self._confirmation_signature(
                "discovery",
                {
                    "query": query,
                    "candidates": [
                        candidate.model_dump(mode="json"),
                    ],
                },
            ),
        )
        return self.confirm_import(
            discovery,
            candidate_name=candidate.name,
            worker_name=worker_name,
        )

    async def import_worker(
        self,
        confirmation: WorkerImportConfirmation,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        """Import exactly the confirmed package, with no generic fallback."""
        self._validate_import_confirmation(confirmation)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.IMPORT_WORKER,
            target_key=f"worker/{confirmation.worker_name}",
            request=confirmation.model_dump(mode="json"),
        )
        return await self.resume_worker_import(operation)

    async def resume_worker_import(
        self,
        operation: OperationRecord,
    ) -> WorkerResource:
        if operation.kind is not OperationKind.IMPORT_WORKER:
            raise ValueError("operation is not a Worker import")
        confirmation = WorkerImportConfirmation.model_validate(
            operation.request,
        )
        self._validate_import_confirmation(confirmation)
        name = confirmation.worker_name
        candidate = confirmation.candidate
        if operation.target_key != f"worker/{name}":
            raise ConflictError(
                "Worker import operation target does not match confirmation",
            )
        if operation.status is OperationStatus.SUCCEEDED:
            return await self._require_worker(name)
        if operation.status is OperationStatus.FAILED:
            raise WorkerImportError(
                f"import worker/{name} previously failed",
            )

        worker: WorkerResource | None = None
        if operation.status is OperationStatus.PLANNED:
            if await self._controller.get_worker(name) is not None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource already exists",
                )
                raise ConflictError(f"worker/{name} already exists")
            if self._nacos is None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "Nacos discovery is not configured",
                )
                raise WorkerImportError("Nacos discovery is not configured")
            try:
                await self._nacos.verify_worker(candidate)
            except Exception as exc:
                reason = _safe_import_reason(exc)
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    reason,
                )
                raise WorkerImportError(reason) from exc
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "apply_worker_package",
                    "name": name,
                    "package_uri": candidate.package_uri,
                    "expected_digest": candidate.digest,
                    "runtime": candidate.runtime,
                },
            )
            try:
                worker = await self._controller.apply_worker_package(
                    name=name,
                    package_uri=candidate.package_uri,
                    expected_digest=candidate.digest,
                    runtime=candidate.runtime,
                )
            except Exception as exc:
                if _ambiguous_exception(exc):
                    await self._supervisor.effect_ambiguous(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        type(exc).__name__,
                    )
                    worker = await self._controller.get_worker(name)
                    if worker is None:
                        raise AmbiguousEffectError(
                            f"import worker/{name} result is unknown",
                        ) from exc
                else:
                    reason = _safe_import_reason(exc)
                    await self._supervisor.effect_failed(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        reason,
                    )
                    raise WorkerImportError(reason) from exc
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "name": name,
                    "package_uri": candidate.package_uri,
                    "digest": candidate.digest,
                    "accepted": True,
                },
            )
        else:
            worker = await self._controller.get_worker(name)
            if worker is None:
                raise AmbiguousEffectError(
                    f"import worker/{name} has no Controller proof",
                )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                _resource_receipt(worker),
            )

        ready = await self._wait_for_worker_room(
            operation_id=operation.operation_id,
            name=name,
            initial=worker,
        )
        if ready.runtime != candidate.runtime:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Imported Worker runtime does not match confirmation",
            )
            raise AmbiguousEffectError(
                f"import worker/{name} did not converge to confirmed runtime",
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
                "package_uri": candidate.package_uri,
                "digest": candidate.digest,
                "greeting_event_id": event_id,
                "greeting_txn_id": transaction_id,
            },
        )
        return ready

    def _validate_import_confirmation(
        self,
        confirmation: WorkerImportConfirmation,
    ) -> None:
        payload = {
            "candidate": confirmation.candidate.model_dump(mode="json"),
            "worker_name": confirmation.worker_name,
        }
        expected = self._confirmation_signature("import", payload)
        if not hmac.compare_digest(
            expected,
            confirmation.confirmation_token,
        ):
            raise WorkerImportError("Worker import confirmation is invalid")

    def _confirmation_signature(
        self,
        purpose: str,
        payload: Mapping[str, object],
    ) -> str:
        encoded = json.dumps(
            {"purpose": purpose, "payload": payload},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hmac.new(
            self._confirmation_key,
            encoded,
            hashlib.sha256,
        ).hexdigest()

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

        worker: WorkerResource | None
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
        if not _matches_worker_create(ready, request):
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller Worker does not match create request",
            )
            raise AmbiguousEffectError(
                f"create worker/{request.name} is not proven",
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

    async def reset_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        """Recreate one Worker from a journaled copy of its desired state."""
        current = await self._require_worker(name)
        desired = _worker_create_from_resource(current)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_WORKER,
            target_key=f"worker/{name}",
            request={
                "action": "reset",
                "desired": desired.model_dump(mode="json"),
            },
        )
        return await self._resume_worker_reset(operation)

    async def delete_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> None:
        if await self._supervisor.get(context.operation_id) is None:
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

    async def create_team(
        self,
        spec: TeamSpec,
        *,
        context: MutationContext,
    ) -> TeamResource:
        return await self._run_team_upsert(
            spec=spec,
            context=context,
            kind=OperationKind.CREATE_TEAM,
            force_apply=not spec.is_simple_create,
            reject_existing=True,
        )

    async def apply_team(
        self,
        spec: TeamSpec,
        *,
        context: MutationContext,
    ) -> TeamResource:
        return await self._run_team_upsert(
            spec=spec,
            context=context,
            kind=OperationKind.UPDATE_TEAM,
            force_apply=True,
            reject_existing=False,
        )

    async def delete_team(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> tuple[str, ...]:
        recorded_operation = await self._supervisor.get(
            context.operation_id,
        )
        existing = await self._controller.get_team(name)
        current_members = (
            (existing.leader, *existing.workers)
            if existing is not None
            else ()
        )
        request: dict[str, object]
        if recorded_operation is not None:
            request = recorded_operation.request
        else:
            request = {
                "name": name,
                "action": "delete",
                "preserved_workers": list(current_members),
            }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.DELETE_TEAM,
            target_key=f"team/{name}",
            request=request,
        )
        recorded_members = operation.request.get("preserved_workers", ())
        preserved_workers = tuple(
            str(worker)
            for worker in recorded_members
            if str(worker)
        )
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(f"delete team/{name} previously failed")
        if operation.status is OperationStatus.SUCCEEDED:
            if await self._controller.get_team(name) is not None:
                raise ConflictError(
                    f"team/{name} exists after a completed delete",
                )
            return preserved_workers
        if operation.status is OperationStatus.PLANNED:
            if existing is None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource does not exist",
                )
                raise NotFoundError(f"team/{name} does not exist")
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {"operation": "delete_team", "name": name},
            )
            try:
                await self._controller.delete_team(name)
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
        if await self._controller.get_team(name) is not None:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller still reports the Team",
            )
            raise AmbiguousEffectError(
                f"delete team/{name} is not proven",
            )
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {
                "name": name,
                "deleted": True,
                "preserved_workers": list(preserved_workers),
            },
        )
        return preserved_workers

    async def create_human(
        self,
        request: HumanCreateRequest,
        *,
        context: MutationContext,
    ) -> HumanResource:
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CREATE_HUMAN,
            target_key=f"human/{request.name}",
            request=request.model_dump(mode="json"),
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return await self._require_human(request.name)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"create human/{request.name} previously failed",
            )

        human: HumanResource | None = None
        if operation.status is OperationStatus.PLANNED:
            if await self._controller.get_human(request.name) is not None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource already exists",
                )
                raise ConflictError(f"human/{request.name} already exists")
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "create_human",
                    "name": request.name,
                    "permission_level": request.permission_level,
                },
            )
            try:
                await self._controller.create_human(request)
            except Exception as exc:
                if _ambiguous_exception(exc):
                    await self._supervisor.effect_ambiguous(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        type(exc).__name__,
                    )
                    human = await self._controller.get_human(request.name)
                    if human is None:
                        raise AmbiguousEffectError(
                            f"create human/{request.name} result is unknown",
                        ) from exc
                else:
                    await self._supervisor.effect_failed(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        _safe_reason(exc),
                    )
                    raise
        else:
            human = await self._controller.get_human(request.name)
            if human is None:
                raise AmbiguousEffectError(
                    f"create human/{request.name} has no Controller proof",
                )

        ready = await self._wait_for_human(
            operation_id=operation.operation_id,
            name=request.name,
            initial=human,
        )
        if not _matches_human_create(ready, request):
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller Human does not match create request",
            )
            raise AmbiguousEffectError(
                f"create human/{request.name} is not proven",
            )
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            _resource_receipt(ready),
        )
        return ready

    async def delete_human(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> None:
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.DELETE_HUMAN,
            target_key=f"human/{name}",
            request={"name": name, "action": "delete"},
        )
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(f"delete human/{name} previously failed")
        if operation.status is OperationStatus.SUCCEEDED:
            if await self._controller.get_human(name) is not None:
                raise ConflictError(
                    f"human/{name} exists after a completed delete",
                )
            return
        if operation.status is OperationStatus.PLANNED:
            if await self._controller.get_human(name) is None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource does not exist",
                )
                raise NotFoundError(f"human/{name} does not exist")
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {"operation": "delete_human", "name": name},
            )
            try:
                await self._controller.delete_human(name)
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
        if await self._controller.get_human(name) is not None:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller still reports the Human",
            )
            raise AmbiguousEffectError(
                f"delete human/{name} is not proven",
            )
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {"name": name, "deleted": True},
        )

    async def update_human(
        self,
        request: HumanUpdateRequest,
        *,
        context: MutationContext,
    ) -> HumanResource:
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_HUMAN,
            target_key=f"human/{request.name}",
            request=request.model_dump(mode="json"),
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return await self._require_human(request.name)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"update human/{request.name} previously failed",
            )

        human: HumanResource | None = None
        if operation.status is OperationStatus.PLANNED:
            if await self._controller.get_human(request.name) is None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource does not exist",
                )
                raise NotFoundError(
                    f"human/{request.name} does not exist",
                )
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "update_human",
                    "name": request.name,
                },
            )
            try:
                human = await self._controller.update_human(request)
            except Exception as exc:
                if _ambiguous_exception(exc):
                    await self._supervisor.effect_ambiguous(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        type(exc).__name__,
                    )
                    human = await self._controller.get_human(request.name)
                    if human is None or not _matches_human_update(
                        human,
                        request,
                    ):
                        raise AmbiguousEffectError(
                            f"update human/{request.name} result is unknown",
                        ) from exc
                else:
                    await self._supervisor.effect_failed(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        _safe_reason(exc),
                    )
                    raise
        else:
            human = await self._controller.get_human(request.name)
            if human is None or not _matches_human_update(human, request):
                raise AmbiguousEffectError(
                    f"update human/{request.name} has no Controller proof",
                )

        ready = await self._wait_for_human(
            operation_id=operation.operation_id,
            name=request.name,
            initial=human,
        )
        if not _matches_human_update(ready, request):
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller has not converged to the Human update",
            )
            raise AmbiguousEffectError(
                f"update human/{request.name} is not proven",
            )
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            _resource_receipt(ready),
        )
        return ready

    async def _wait_for_human(
        self,
        *,
        operation_id: str,
        name: str,
        initial: HumanResource | None,
    ) -> HumanResource:
        human = initial
        attempts = len(self._human_poll_delays) + 1
        for index in range(attempts):
            if human is None or index > 0 or initial is None:
                human = await self._controller.get_human(name)
            if human is not None:
                phase = _resource_phase(human).casefold()
                if phase in {"failed", "error", "deleting"}:
                    message = _resource_message(human)
                    await self._supervisor.effect_failed(
                        operation_id,
                        ExternalEffect.CONTROLLER,
                        message or f"human/{name} entered {phase}",
                    )
                    raise ConflictError(
                        message or f"human/{name} entered {phase}",
                    )
                if (
                    human.matrix_user_id
                    and phase in {"active", "ready", "running"}
                ):
                    return human
            if index < len(self._human_poll_delays):
                await self._sleeper(self._human_poll_delays[index])
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.CONTROLLER,
            "Human provisioning did not converge",
        )
        raise AmbiguousEffectError(f"human/{name} is not ready")

    async def _require_human(self, name: str) -> HumanResource:
        human = await self._controller.get_human(name)
        if human is None:
            raise NotFoundError(f"human/{name} does not exist")
        return human

    async def _run_team_upsert(
        self,
        *,
        spec: TeamSpec,
        context: MutationContext,
        kind: OperationKind,
        force_apply: bool,
        reject_existing: bool,
    ) -> TeamResource:
        mode = "apply" if force_apply else "create"
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=kind,
            target_key=f"team/{spec.name}",
            request={
                "mode": mode,
                "spec": spec.model_dump(mode="json"),
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return await self._require_team(spec.name)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"{mode} team/{spec.name} previously failed",
            )

        team: TeamResource | None = None
        if operation.status is OperationStatus.PLANNED:
            missing_workers: list[str] = []
            for worker_name in spec.member_names:
                if await self._controller.get_worker(worker_name) is None:
                    missing_workers.append(worker_name)
            if missing_workers:
                reason = (
                    "Team references missing Workers: "
                    + ", ".join(missing_workers)
                )
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    reason,
                )
                raise NotFoundError(reason)
            existing = await self._controller.get_team(spec.name)
            if reject_existing and existing is not None:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    "resource already exists",
                )
                raise ConflictError(f"team/{spec.name} already exists")
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": f"{mode}_team",
                    "name": spec.name,
                },
            )
            try:
                if force_apply:
                    team = await self._controller.apply_team(
                        spec.name,
                        spec.to_apply_document(),
                    )
                else:
                    await self._controller.create_team(
                        spec.to_create_request(),
                    )
            except Exception as exc:
                if _ambiguous_exception(exc):
                    await self._supervisor.effect_ambiguous(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        type(exc).__name__,
                    )
                    team = await self._controller.get_team(spec.name)
                    if team is None:
                        raise AmbiguousEffectError(
                            f"{mode} team/{spec.name} result is unknown",
                        ) from exc
                else:
                    await self._supervisor.effect_failed(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        _safe_reason(exc),
                    )
                    raise
        else:
            team = await self._controller.get_team(spec.name)
            if team is None:
                raise AmbiguousEffectError(
                    f"{mode} team/{spec.name} has no Controller proof",
                )

        ready = await self._wait_for_team(
            operation_id=operation.operation_id,
            spec=spec,
            initial=team,
        )
        if (
            ready.leader != spec.leader_name
            or ready.workers != spec.worker_names
        ):
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "Controller Team does not match desired members",
            )
            raise AmbiguousEffectError(
                f"{mode} team/{spec.name} is not proven",
            )
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            _resource_receipt(ready),
        )
        return ready

    async def _wait_for_team(
        self,
        *,
        operation_id: str,
        spec: TeamSpec,
        initial: TeamResource | None,
    ) -> TeamResource:
        team = initial
        attempts = len(self._team_poll_delays) + 1
        for index in range(attempts):
            if team is None or index > 0 or initial is None:
                team = await self._controller.get_team(spec.name)
            if team is not None:
                phase = (team.phase or "").casefold()
                if phase in {"failed", "error", "deleting"}:
                    message = _resource_message(team)
                    await self._supervisor.effect_failed(
                        operation_id,
                        ExternalEffect.CONTROLLER,
                        message or f"team/{spec.name} entered {phase}",
                    )
                    raise ConflictError(
                        message or f"team/{spec.name} entered {phase}",
                    )
                if _team_is_ready(
                    team,
                    expected_workers=len(spec.worker_names),
                ):
                    return team
            if index < len(self._team_poll_delays):
                await self._sleeper(self._team_poll_delays[index])
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.CONTROLLER,
            "Team readiness did not converge",
        )
        raise AmbiguousEffectError(
            f"team/{spec.name} is not ready",
        )

    async def _refresh_topology_or_reconcile(
        self,
        operation_id: str,
    ) -> None:
        try:
            await self._topology.refresh()
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                operation_id,
                ExternalEffect.MATRIX,
                f"topology not converged: {type(exc).__name__}",
            )
            raise

    async def _require_team(self, name: str) -> TeamResource:
        team = await self._controller.get_team(name)
        if team is None:
            raise NotFoundError(f"team/{name} does not exist")
        return team

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
        worker: WorkerResource | None = await self._controller.get_worker(name)
        if worker is None:
            raise AmbiguousEffectError(
                f"worker/{name} create result is unknown",
            ) from exc
        return worker

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> WorkerResource | TeamResource | HumanResource | None:
        """Prove and finish one already-dispatched resource operation."""
        if operation.kind is OperationKind.CREATE_WORKER:
            return await self.resume_worker_create(operation)
        if operation.kind is OperationKind.IMPORT_WORKER:
            return await self.resume_worker_import(operation)
        if operation.kind is OperationKind.UPDATE_WORKER:
            return await self._resume_worker_mutation(operation)
        if operation.kind is OperationKind.DELETE_WORKER:
            await self._resume_resource_delete(
                operation,
                resource_type="worker",
            )
            return None
        if operation.kind in {
            OperationKind.CREATE_TEAM,
            OperationKind.UPDATE_TEAM,
        }:
            return await self._resume_team_upsert(operation)
        if operation.kind is OperationKind.DELETE_TEAM:
            await self._resume_resource_delete(
                operation,
                resource_type="team",
            )
            return None
        if operation.kind is OperationKind.CREATE_HUMAN:
            return await self._resume_human_create(operation)
        if operation.kind is OperationKind.UPDATE_HUMAN:
            return await self._resume_human_update(operation)
        if operation.kind is OperationKind.DELETE_HUMAN:
            await self._resume_resource_delete(
                operation,
                resource_type="human",
            )
            return None
        raise ValueError("operation is not a Controller resource mutation")

    async def _resume_worker_mutation(
        self,
        operation: OperationRecord,
    ) -> WorkerResource:
        name = _target_name(operation.target_key, "worker")
        action = str(operation.request.get("action", ""))
        if action == "reset":
            return await self._resume_worker_reset(operation)
        worker: WorkerResource | None = await self._controller.get_worker(name)
        if worker is None:
            raise AmbiguousEffectError(
                f"worker/{name} disappeared during recovery",
            )
        proven = False
        if action == "update":
            request = WorkerUpdateRequest.model_validate(
                {
                    key: value
                    for key, value in operation.request.items()
                    if key != "action"
                },
            )
            proven = _matches_worker_update(worker, request)
        elif action == "sleep":
            proven = (
                str(worker.status.get("containerState", "")).casefold()
                in {"stopped", "sleeping", "exited"}
            )
        elif action == "wake":
            proven = (
                str(worker.status.get("containerState", "")).casefold()
                in {"running", "ready"}
            )
        if not proven:
            raise AmbiguousEffectError(
                f"{action or 'update'} worker/{name} is not proven",
            )
        await self._finish_recovered_resource(operation, worker)
        return worker

    async def _resume_worker_reset(
        self,
        operation: OperationRecord,
    ) -> WorkerResource:
        name = _target_name(operation.target_key, "worker")
        raw_desired = operation.request.get("desired")
        if not isinstance(raw_desired, dict):
            raise RecoveryError("Worker reset has no desired-state copy")
        desired = WorkerCreateRequest.model_validate(raw_desired)
        if desired.name != name:
            raise RecoveryError("Worker reset target does not match desired")
        if operation.status is OperationStatus.SUCCEEDED:
            succeeded_worker = await self._require_worker(name)
            if not _matches_worker_create(succeeded_worker, desired):
                raise RecoveryError(
                    f"succeeded reset worker/{name} changed desired state",
                )
            return succeeded_worker
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(f"reset worker/{name} previously failed")

        if operation.result.get("reset_stage") != "deleted":
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "delete_worker_for_reset",
                    "name": name,
                },
            )
            existing = await self._controller.get_worker(name)
            if existing is not None:
                await self._controller.delete_worker(name)
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "name": name,
                    "reset_stage": "deleted",
                    "desired": desired.model_dump(mode="json"),
                },
            )

        worker: WorkerResource | None = await self._controller.get_worker(name)
        if worker is None:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "recreate_worker_from_saved_state",
                    "name": name,
                    "runtime": desired.runtime,
                    "model": desired.model,
                },
            )
            try:
                worker = await self._controller.create_worker(desired)
            except Exception as exc:
                worker = await self._handle_ambiguous_worker_effect(
                    operation_id=operation.operation_id,
                    name=name,
                    effect=ExternalEffect.CONTROLLER,
                    exc=exc,
                )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "name": name,
                    "reset_stage": "recreated",
                },
            )
        ready = await self._wait_for_worker_room(
            operation_id=operation.operation_id,
            name=name,
            initial=worker,
        )
        if not _matches_worker_create(ready, desired):
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                "recreated Worker does not match saved desired state",
            )
            raise AmbiguousEffectError(
                f"reset worker/{name} is not proven",
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
            {
                **_resource_receipt(ready),
                "reset": True,
                "desired": desired.model_dump(mode="json"),
            },
        )
        return ready

    async def _resume_team_upsert(
        self,
        operation: OperationRecord,
    ) -> TeamResource:
        raw_spec = operation.request.get("spec")
        if not isinstance(raw_spec, dict):
            raise ConflictError("Team recovery request has no typed spec")
        spec = TeamSpec.model_validate(raw_spec)
        team = await self._controller.get_team(spec.name)
        if team is None:
            raise AmbiguousEffectError(
                f"team/{spec.name} has no Controller proof",
            )
        ready = await self._wait_for_team(
            operation_id=operation.operation_id,
            spec=spec,
            initial=team,
        )
        expected_workers = spec.worker_names
        if (
            ready.leader != spec.leader_name
            or ready.workers != expected_workers
        ):
            raise AmbiguousEffectError(
                f"team/{spec.name} does not match its desired members",
            )
        await self._finish_recovered_resource(operation, ready)
        return ready

    async def _resume_human_create(
        self,
        operation: OperationRecord,
    ) -> HumanResource:
        request = HumanCreateRequest.model_validate(operation.request)
        human = await self._controller.get_human(request.name)
        if human is None:
            raise AmbiguousEffectError(
                f"human/{request.name} has no Controller proof",
            )
        ready = await self._wait_for_human(
            operation_id=operation.operation_id,
            name=request.name,
            initial=human,
        )
        if not _matches_human_create(ready, request):
            raise AmbiguousEffectError(
                f"human/{request.name} does not match its desired scope",
            )
        await self._finish_recovered_resource(operation, ready)
        return ready

    async def _resume_human_update(
        self,
        operation: OperationRecord,
    ) -> HumanResource:
        request = HumanUpdateRequest.model_validate(operation.request)
        human = await self._controller.get_human(request.name)
        if human is None or not _matches_human_update(human, request):
            raise AmbiguousEffectError(
                f"update human/{request.name} is not proven",
            )
        await self._finish_recovered_resource(operation, human)
        return human

    async def _resume_resource_delete(
        self,
        operation: OperationRecord,
        *,
        resource_type: str,
    ) -> None:
        name = _target_name(operation.target_key, resource_type)
        getter = getattr(self._controller, f"get_{resource_type}")
        if await getter(name) is not None:
            raise AmbiguousEffectError(
                f"delete {resource_type}/{name} is not proven",
            )
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {"name": name, "deleted": True},
        )

    async def _finish_recovered_resource(
        self,
        operation: OperationRecord,
        resource: WorkerResource | TeamResource | HumanResource,
    ) -> None:
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            _resource_receipt(resource),
        )

    async def _require_worker(self, name: str) -> WorkerResource:
        worker = await self._controller.get_worker(name)
        if worker is None:
            raise NotFoundError(f"worker/{name} does not exist")
        return worker


class RecoverableOperationReader(Protocol):
    async def list_recoverable(self) -> tuple[OperationRecord, ...]: ...


class AuxiliaryOperationResumer(Protocol):
    async def resume(self, operation: OperationRecord) -> object: ...


class ResourceRecoveryReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()


class ResourceHeartbeat:
    """Resume pending resources without repeating unproven mutations."""

    def __init__(
        self,
        *,
        operations: RecoverableOperationReader,
        resources: ResourceService,
        matrix_resources: AuxiliaryOperationResumer | None = None,
    ) -> None:
        self._operations = operations
        self._resources = resources
        self._matrix_resources = matrix_resources

    async def reconcile_pending_resources(self) -> ResourceRecoveryReport:
        resource_kinds = {
            OperationKind.CREATE_WORKER,
            OperationKind.IMPORT_WORKER,
            OperationKind.UPDATE_WORKER,
            OperationKind.DELETE_WORKER,
            OperationKind.CREATE_TEAM,
            OperationKind.UPDATE_TEAM,
            OperationKind.DELETE_TEAM,
            OperationKind.CREATE_HUMAN,
            OperationKind.UPDATE_HUMAN,
            OperationKind.DELETE_HUMAN,
        }
        if self._matrix_resources is not None:
            resource_kinds.update(
                {
                    OperationKind.MATRIX_MUTATION,
                    OperationKind.CHANNEL_MUTATION,
                },
            )
        operations = tuple(
            operation
            for operation in await self._operations.list_recoverable()
            if operation.kind in resource_kinds
        )
        reconciled: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        for operation in operations:
            try:
                if operation.kind in {
                    OperationKind.MATRIX_MUTATION,
                    OperationKind.CHANNEL_MUTATION,
                }:
                    if self._matrix_resources is None:
                        raise RuntimeError(
                            "Matrix resource recovery is not configured",
                        )
                    await self._matrix_resources.resume(operation)
                else:
                    await self._resources.resume_operation(operation)
            except AmbiguousEffectError:
                pending.append(operation.operation_id)
            except Exception:
                failed.append(operation.operation_id)
            else:
                reconciled.append(operation.operation_id)
        return ResourceRecoveryReport(
            inspected=len(operations),
            reconciled=tuple(reconciled),
            pending=tuple(pending),
            failed=tuple(failed),
        )

    async def reconcile_pending_workers(self) -> ResourceRecoveryReport:
        """Compatibility alias for the original Worker-only heartbeat API."""
        return await self.reconcile_pending_resources()


WorkerRecoveryReport = ResourceRecoveryReport


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
    ) -> None:
        self._controller = controller
        self._matrix = matrix
        self._topology = topology
        self._manager_user_id = manager_user_id
        self._admin_user_id = admin_user_id
        self._admin_room_id = admin_room_id
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
                return policy_for_human(
                    human,
                    room_id=room_id,
                    revision=revision,
                )
            elif (
                (actor := await self._topology.actor_for_sender(sender_id))
                is not None
                and actor.kind is ActorKind.TRUSTED_CONTACT
            ):
                tools = TRUSTED_TOOLS
            else:
                return _deny_policy(room_id, revision, silent=True)
        return RoomPolicy(
            room_id=room_id,
            kind=binding.room_kind,
            revision=revision,
            allowed_tools=tools,
            confirm_tools=tools & CONFIRM_TOOLS,
            allowed_senders=frozenset({sender_id}),
            resource_name=binding.resource_name,
            team_name=(
                binding.resource_name
                if binding.room_kind is RoomKind.LEADER_ROOM
                else None
            ),
            allowed_worker_names=(
                team_member_names(binding.payload)
                if binding.room_kind is RoomKind.LEADER_ROOM
                else frozenset()
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


def _matches_worker_create(
    worker: WorkerResource,
    request: WorkerCreateRequest,
) -> bool:
    console_enabled, console_port = _worker_console_state(worker)
    return (
        worker.name == request.name
        and worker.runtime == request.runtime
        and worker.model == request.model
        and (
            request.image is None
            or worker.spec.get("image") == request.image
        )
        and (
            request.identity is None
            or worker.spec.get("identity") == request.identity
        )
        and (
            request.soul is None
            or worker.spec.get("soul") == request.soul
        )
        and (
            not request.skills
            or worker.skills == request.skills
        )
        and (
            request.package_uri is None
            or worker.spec.get("package") == request.package_uri
        )
        and (
            not request.expose
            or tuple(worker.spec.get("expose", ())) == request.expose
        )
        and console_enabled is request.console_enabled
        and (
            not request.console_enabled
            or console_port == request.console_port
        )
    )


def _worker_create_from_resource(
    worker: WorkerResource,
) -> WorkerCreateRequest:
    if not worker.model:
        raise ConflictError(
            f"worker/{worker.name} has no desired model to preserve",
        )
    if worker.runtime not in {"openclaw", "copaw", "hermes", "qwenpaw"}:
        raise ConflictError(
            f"worker/{worker.name} has unsupported runtime {worker.runtime!r}",
        )
    spec = worker.spec
    return WorkerCreateRequest(
        name=worker.name,
        runtime=cast(WorkerRuntime, worker.runtime),
        model=worker.model,
        image=str(spec.get("image") or "") or None,
        identity=str(spec.get("identity") or "") or None,
        soul=str(spec.get("soul") or "") or None,
        skills=worker.skills,
        package_uri=str(spec.get("package") or "") or None,
        expose=tuple(int(port) for port in spec.get("expose", ())),
        console_enabled=_worker_console_state(worker)[0],
        console_port=_worker_console_state(worker)[1],
    )


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
    console_enabled, console_port = _worker_console_state(worker)
    if (
        request.console_enabled is not None
        and console_enabled is not request.console_enabled
    ):
        return False
    if request.console_port is not None and (
        not console_enabled or console_port != request.console_port
    ):
        return False
    fields: tuple[tuple[object | None, object], ...] = (
        (request.identity, worker.spec.get("identity", "")),
        (request.soul, worker.spec.get("soul", "")),
        (
            request.skills,
            tuple(worker.skills),
        ),
        (
            request.package_uri,
            worker.spec.get("package", ""),
        ),
        (
            request.expose,
            tuple(worker.spec.get("expose", ())),
        ),
    )
    if any(
        expected is not None and expected != actual
        for expected, actual in fields
    ):
        return False
    return True


def _worker_console_state(worker: WorkerResource) -> tuple[bool, int]:
    console = worker.spec.get("console")
    if not isinstance(console, dict):
        return False, 8088
    enabled = bool(console.get("enabled", False))
    raw_port = console.get("port", 8088)
    try:
        port = int(raw_port)
    except (TypeError, ValueError):
        port = 8088
    return enabled, port


def _matches_human_create(
    human: HumanResource,
    request: HumanCreateRequest,
) -> bool:
    return (
        human.permission_level == request.permission_level
        and human.spec.get("displayName") == request.display_name
        and human.spec.get("email", "") == (request.email or "")
        and tuple(human.spec.get("accessibleTeams", ()))
        == request.accessible_teams
        and tuple(human.spec.get("accessibleWorkers", ()))
        == request.accessible_workers
        and human.spec.get("note", "") == (request.note or "")
    )


def _matches_human_update(
    human: HumanResource,
    request: HumanUpdateRequest,
) -> bool:
    fields: tuple[tuple[object | None, object], ...] = (
        (request.display_name, human.spec.get("displayName")),
        (request.email, human.spec.get("email", "")),
        (request.permission_level, human.permission_level),
        (
            request.accessible_teams,
            tuple(human.spec.get("accessibleTeams", ())),
        ),
        (
            request.accessible_workers,
            tuple(human.spec.get("accessibleWorkers", ())),
        ),
        (request.note, human.spec.get("note", "")),
    )
    return all(
        expected is None or expected == actual
        for expected, actual in fields
    )


def _document_value(
    document: dict[str, object],
    key: str,
    value: object | None,
) -> None:
    if value is not None and value != "":
        document[key] = value


def _team_is_ready(
    team: TeamResource,
    *,
    expected_workers: int,
) -> bool:
    if (team.phase or "").casefold() not in {"active", "ready", "running"}:
        return False
    if not bool(team.status.get("leaderReady")):
        return False
    try:
        ready = int(team.status.get("readyWorkers", 0))
        reported_total = int(team.status.get("totalWorkers", 0))
    except (TypeError, ValueError):
        return False
    return ready >= max(expected_workers, reported_total)


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


def _safe_import_reason(exc: BaseException) -> str:
    message = str(exc).strip() or type(exc).__name__
    message = re.sub(
        r"(?i)(nacos://)[^/@\s]+:[^/@\s]+@",
        r"\1[REDACTED]@",
        message,
    )
    message = re.sub(
        r"(?i)(token|password|secret|authorization|api[_-]?key)"
        r"(\s*[=:]\s*)([^,\s\"'}]+)",
        r"\1\2[REDACTED]",
        message,
    )
    return message[:1000]


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
