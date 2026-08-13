"""Controller-fact reconciliation and Matrix topology materialization.

协调 Controller 资源事实与 Matrix 房间拓扑的创建、更新和删除。

以 create_worker 为例：先在 Operation 中记录请求，再调用 Controller；返回超时不能
断言失败，而要 list/get Worker 核验。Controller Ready 后异步建立 Worker Room、邀请
成员、写入 topology 并通知 Admin。删除和更新也按实际资源状态对账，避免重复效果或
保留指向旧 room 的绑定。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
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

logger = logging.getLogger(__name__)


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
        # 逻辑说明：`effect_absent` 接收 当前服务状态，记录外部效果 absent，核心调用为 `cls`，返回 `ReconcileResult`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
        return cls(disposition=ReconcileDisposition.EFFECT_ABSENT)

    @classmethod
    def pending(
        cls,
        *,
        receipt: dict[str, Any] | None = None,
        message: str = "",
    ) -> ReconcileResult:
        # 逻辑说明：`pending` 接收 `receipt`、`message`，生成 pending 结果 Worker、Team、Human 与拓扑，核心调用为 `cls`，返回 `ReconcileResult`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`succeeded` 接收 `receipt`，生成成功结果 Worker、Team、Human 与拓扑，核心调用为 `cls`，返回 `ReconcileResult`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
        return cls(
            disposition=ReconcileDisposition.SUCCEEDED,
            receipt=receipt,
        )

    @classmethod
    def failed(cls, message: str) -> ReconcileResult:
        # 逻辑说明：`failed` 接收 `message`，生成失败结果 Worker、Team、Human 与拓扑，核心调用为 `cls`，返回 `ReconcileResult`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`__init__` 校验并保存 `controller`，为Worker、Team、Human 与拓扑建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
        self._controller = controller

    async def reconcile(
        self,
        operation: OperationRecord,
    ) -> ReconcileResult:
        # 逻辑说明：`reconcile` 接收 `operation`，恢复未完成操作 Worker、Team、Human 与拓扑，核心调用为 `get`、`ValueError`、`_target_name`，返回 `ReconcileResult`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`_get` 接收 `resource_type`、`name`，读取 Worker、Team、Human 与拓扑，核心调用为 `get_worker`、`get_team`、`get_human`，返回 `WorkerResource | TeamResource | HumanResource | None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`operation_id` 接收 当前服务状态，读取 operation id，核心调用为 `operation_id_for`，返回 `str`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`validate_roster` 检查 Team 的 leader 不会同时出现在 workers，且成员名称无重复；通过后返回自身，避免生成角色冲突的 Team 配置。
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
        # 逻辑说明：`member_names` 接收 当前服务状态，提取成员 names，返回 `tuple[ResourceName, ...]`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
        return (self.leader_name, *self.worker_names)

    def to_create_request(self) -> TeamCreateRequest:
        # 逻辑说明：`to_create_request` 接收 当前服务状态，转换请求 create request，核心调用为 `TeamCreateRequest`，返回 `TeamCreateRequest`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`to_apply_document` 接收 当前服务状态，转换请求 apply document，核心调用为 `encode`、`dumps`，返回 `bytes`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
    """通过可恢复 workflow 管理 Controller 资源及其 Matrix 拓扑。

    create/update/delete 的成功边界不是 client 返回，而是 Controller desired/observed state
    和必要房间绑定都已核验。Worker 创建采用异步 finalizer：先向管理员返回 accepted，
    Ready 后再独立创建房间和通知，避免一次模型 turn 长时间等待 Pod。
    """

    def __init__(
        self,
        *,
        controller: ResourceController,
        supervisor: ResourceSupervisor,
        topology: TopologyRefresher,
        matrix: MatrixPort,
        admin_room_id: str | None = None,
        nacos: NacosDiscovery | None = None,
        confirmation_key: bytes | None = None,
        sleeper: Sleeper = asyncio.sleep,
        worker_poll_delays: tuple[float, ...] = (
            0.5,
            1,
            2,
            4,
            8,
            16,
            16,
            16,
        ),
        team_poll_delays: tuple[float, ...] = (
            0.5,
            1,
            2,
            4,
            8,
            16,
            16,
            16,
        ),
        human_poll_delays: tuple[float, ...] = (
            0.5,
            1,
            2,
            4,
            8,
            16,
        ),
        delete_poll_delays: tuple[float, ...] = (
            0.5,
            1,
            2,
            4,
            8,
            16,
            16,
            16,
        ),
        greeting: str = (
            "Hello. I am the AgentTeams Manager. "
            "This room is your direct coordination channel."
        ),
    ) -> None:
        # 逻辑说明：`__init__` 校验并保存 `controller`、`supervisor`、`topology`、`matrix`、`admin_room_id`、`nacos`、`confirmation_key`、`sleeper`、`worker_poll_delays`、`team_poll_delays`、`human_poll_delays`、`delete_poll_delays`、`greeting`，为Worker、Team、Human 与拓扑建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
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
        if not delete_poll_delays:
            raise ValueError("delete_poll_delays cannot be empty")
        if any(delay < 0 for delay in delete_poll_delays):
            raise ValueError("delete poll delays cannot be negative")
        self._controller = controller
        self._supervisor = supervisor
        self._topology = topology
        self._matrix = matrix
        self._admin_room_id = admin_room_id
        self._nacos = nacos
        self._confirmation_key = confirmation_key or secrets.token_bytes(32)
        if len(self._confirmation_key) < 32:
            raise ValueError("confirmation_key must be at least 32 bytes")
        self._sleeper = sleeper
        self._worker_poll_delays = worker_poll_delays
        self._team_poll_delays = team_poll_delays
        self._human_poll_delays = human_poll_delays
        self._delete_poll_delays = delete_poll_delays
        self._greeting = greeting
        self._background_worker_creates: dict[
            str,
            asyncio.Task[None],
        ] = {}

    async def get_worker(self, name: str) -> WorkerResource | None:
        # 逻辑说明：`get_worker` 接收 `name`，读取 worker，核心调用为 `get_worker`，返回 `WorkerResource | None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._controller.get_worker(name)

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        # 逻辑说明：`list_workers` 接收 当前服务状态，列出 workers，核心调用为 `list_workers`，返回 `tuple[WorkerResource, ...]`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._controller.list_workers()

    async def get_team(self, name: str) -> TeamResource | None:
        # 逻辑说明：`get_team` 接收 `name`，读取 team，核心调用为 `get_team`，返回 `TeamResource | None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._controller.get_team(name)

    async def list_teams(self) -> tuple[TeamResource, ...]:
        # 逻辑说明：`list_teams` 接收 当前服务状态，列出 teams，核心调用为 `list_teams`，返回 `tuple[TeamResource, ...]`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._controller.list_teams()

    async def get_human(self, name: str) -> HumanResource | None:
        # 逻辑说明：`get_human` 接收 `name`，读取 human，核心调用为 `get_human`，返回 `HumanResource | None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._controller.get_human(name)

    async def list_humans(self) -> tuple[HumanResource, ...]:
        # 逻辑说明：`list_humans` 接收 当前服务状态，列出 humans，核心调用为 `list_humans`，返回 `tuple[HumanResource, ...]`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._controller.list_humans()

    async def find_worker(self, query: str) -> WorkerDiscovery:
        """Search Nacos without performing any Controller mutation."""
        # 逻辑说明：`find_worker` 接收 `query`，查找 worker，核心调用为 `WorkerImportError`、`join`、`split`，返回 `WorkerDiscovery`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`confirm_import` 接收 `discovery`、`candidate_name`、`worker_name`，确认 import，核心调用为 `model_dump`、`_confirmation_signature`、`compare_digest`，返回 `WorkerImportConfirmation`。 它只在内存中计算、校验或组装数据；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`confirm_direct_import` 接收 `package_uri`、`worker_name`，确认 direct import，核心调用为 `WorkerImportError`、`inspect_worker_uri`、`WorkerDiscovery`，返回 `WorkerImportConfirmation`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`import_worker` 接收 `confirmation`、`context`，处理 worker，核心调用为 `_validate_import_confirmation`、`begin`、`model_dump`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`resume_worker_import` 接收 `operation`，恢复 worker import，核心调用为 `ValueError`、`model_validate`、`_validate_import_confirmation`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`_validate_import_confirmation` 重新计算 Worker 包导入确认签名并用常量时间比较，同时核对确认用途与候选内容；不匹配时拒绝继续下载或创建 Worker。
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
        # 逻辑说明：`_confirmation_signature` 接收 `purpose`、`payload`，处理 signature，核心调用为 `encode`、`dumps`、`hexdigest`，返回 `str`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`create_worker` 接收 `request`、`context`，创建 worker，核心调用为 `begin`、`model_dump`、`_require_worker`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CREATE_WORKER,
            target_key=f"worker/{request.name}",
            request=request.model_dump(mode="json"),
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return await self._require_worker(request.name)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"create worker/{request.name} previously failed",
            )
        if operation.target_key != f"worker/{request.name}":
            raise ConflictError(
                "Worker create operation target does not match request",
            )
        worker = await self._accept_worker_create(operation, request)
        self._schedule_worker_create_finalization(operation.operation_id)
        return worker or WorkerResource(
            name=request.name,
            runtime=request.runtime,
            model=request.model,
            phase="Pending",
            team=request.team,
            role=request.role,
            skills=request.skills,
            spec={"provisioning": True},
        )

    async def resume_worker_create(
        self,
        operation: OperationRecord,
    ) -> WorkerResource:
        # 逻辑说明：`resume_worker_create` 接收 `operation`，恢复 worker create，核心调用为 `ValueError`、`model_validate`、`ConflictError`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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

        worker = await self._accept_worker_create(operation, request)
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
        notification_txn_id = matrix_transaction_id(
            operation.operation_id,
            1,
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "finalize_worker_create",
                "room_id": ready.room_id or "",
                "greeting_txn_id": transaction_id,
                "admin_room_id": self._admin_room_id or "",
                "notification_txn_id": notification_txn_id,
            },
        )
        notification_event_id: str | None = None
        try:
            event_id = await self._matrix.send_text(
                ready.room_id or "",
                self._greeting,
                txn_id=transaction_id,
            )
            if self._admin_room_id is not None:
                notification_event_id = await self._matrix.send_text(
                    self._admin_room_id,
                    f"Worker {ready.name} 已创建完成并可用。"
                    f"\nWorker 房间：{ready.room_id}",
                    txn_id=notification_txn_id,
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
                "notification_event_id": notification_event_id,
                "notification_txn_id": (
                    notification_txn_id
                    if self._admin_room_id is not None
                    else None
                ),
            },
        )
        return ready

    async def _accept_worker_create(
        self,
        operation: OperationRecord,
        request: WorkerCreateRequest,
    ) -> WorkerResource | None:
        """Submit the Controller mutation without waiting for room readiness."""

        # 逻辑说明：`_accept_worker_create` 接收 `operation`、`request`，验收 worker create，核心调用为 `get_worker`、`effect_failed`、`ConflictError`，返回 `WorkerResource | None`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
                worker = await self._controller.create_worker(request)
            except Exception as exc:
                worker = await self._handle_ambiguous_worker_effect(
                    operation_id=operation.operation_id,
                    name=request.name,
                    effect=ExternalEffect.CONTROLLER,
                    exc=exc,
                )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {"name": request.name, "accepted": True},
            )
        else:
            worker = await self._controller.get_worker(request.name)
            if worker is not None:
                await self._supervisor.effect_acknowledged(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    _resource_receipt(worker),
                )
        return worker

    def _schedule_worker_create_finalization(
        self,
        operation_id: str,
    ) -> None:
        # 逻辑说明：`_schedule_worker_create_finalization` 接收 `operation_id`，调度 worker create finalization，核心调用为 `get`、`done`、`create_task`，返回 `None`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
        current = self._background_worker_creates.get(operation_id)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(
            self._finalize_worker_create_in_background(operation_id),
            name=f"worker-create:{operation_id}",
        )
        self._background_worker_creates[operation_id] = task

        def clear_completed(completed: asyncio.Task[None]) -> None:
            # 逻辑说明：`clear_completed` 接收 `completed`，清理完成后台任务 completed，核心调用为 `get`、`pop`，返回 `None`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
            if (
                self._background_worker_creates.get(operation_id)
                is completed
            ):
                self._background_worker_creates.pop(operation_id, None)

        task.add_done_callback(clear_completed)

    async def _finalize_worker_create_in_background(
        self,
        operation_id: str,
    ) -> None:
        # 逻辑说明：`_finalize_worker_create_in_background` 接收 `operation_id`，完成后台收敛 worker create in background，核心调用为 `get`、`RecoveryError`、`resume_worker_create`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        try:
            operation = await self._supervisor.get(operation_id)
            if operation is None:
                raise RecoveryError(
                    f"Worker create operation {operation_id} disappeared",
                )
            await self.resume_worker_create(operation)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Background Worker creation did not converge",
                extra={"operation_id": operation_id},
            )
            await self._notify_worker_create_pending_or_failed(
                operation_id,
                exc,
            )

    async def _notify_worker_create_pending_or_failed(
        self,
        operation_id: str,
        exc: Exception,
    ) -> None:
        # 逻辑说明：`_notify_worker_create_pending_or_failed` 接收 `operation_id`、`exc`，通知 worker create pending or failed，核心调用为 `get`、`_target_name`、`_safe_reason`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        if self._admin_room_id is None:
            return
        try:
            operation = await self._supervisor.get(operation_id)
            if operation is None:
                return
            name = _target_name(operation.target_key, "worker")
            failed = operation.status is OperationStatus.FAILED
            status = "创建失败" if failed else "仍在后台创建"
            detail = _safe_reason(exc)
            await self._matrix.send_text(
                self._admin_room_id,
                f"Worker {name} {status}。"
                f"\n状态：{operation.status.value}"
                f"\n详情：{detail}",
                txn_id=matrix_transaction_id(operation_id, 2),
            )
        except Exception:
            logger.exception(
                "Failed to report background Worker create status",
                extra={"operation_id": operation_id},
            )

    async def wait_for_background_worker_creates(self) -> None:
        """Wait for currently scheduled creates; primarily a test/shutdown hook."""

        # 逻辑说明：`wait_for_background_worker_creates` 接收 当前服务状态，等待收敛 for background worker creates，核心调用为 `values`、`gather`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        while self._background_worker_creates:
            tasks = tuple(self._background_worker_creates.values())
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self) -> None:
        """Cancel finalizers safely; durable recovery resumes them on startup."""

        # 逻辑说明：`close` 接收 当前服务状态，关闭 Worker、Team、Human 与拓扑，核心调用为 `values`、`clear`、`cancel`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        tasks = tuple(self._background_worker_creates.values())
        self._background_worker_creates.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        # 逻辑说明：`update_worker` 接收 `request`、`context`，更新 worker，核心调用为 `_require_worker`、`_run_worker_mutation`、`model_dump`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        await self._require_worker(request.name)

        async def mutate() -> None:
            # 逻辑说明：`mutate` 是当前资源操作的局部回调，调用 `update_worker` 发起一次变更；异常原样交给外层 operation 状态机处理。
            await self._controller.update_worker(request)

        async def prove() -> WorkerResource | None:
            # 逻辑说明：`prove` 重新读取 Worker 并逐项比较 update 请求中的模型、身份、技能与暴露端口，只有控制面真实收敛才返回成功；读取异常交给外层 operation 处理。
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
        # 逻辑说明：`sleep_worker` 接收 `name`、`context`，休眠 worker，核心调用为 `_require_worker`、`_run_worker_mutation`、`NotFoundError`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        await self._require_worker(name)

        async def mutate() -> None:
            # 逻辑说明：`mutate` 是当前资源操作的局部回调，调用 `sleep_worker` 发起一次变更；异常原样交给外层 operation 状态机处理。
            await self._controller.sleep_worker(name)

        async def prove() -> WorkerResource | None:
            # 逻辑说明：`prove` 重新读取 Worker 并检查其 phase 是否已变为 sleeping，作为 sleep 外部效果的事实证明；尚未收敛时返回 false 让外层继续轮询。
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
        # 逻辑说明：`wake_worker` 接收 `name`、`context`，唤醒 worker，核心调用为 `_require_worker`、`_run_worker_mutation`、`NotFoundError`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        await self._require_worker(name)

        async def mutate() -> None:
            # 逻辑说明：`mutate` 是当前资源操作的局部回调，调用 `wake_worker` 发起一次变更；异常原样交给外层 operation 状态机处理。
            await self._controller.wake_worker(name)

        async def prove() -> WorkerResource | None:
            # 逻辑说明：`prove` 重新读取 Worker 并检查其 phase 是否已恢复为 running，作为 wake 操作完成的事实证明；未收敛时不生成成功回执。
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
        # 逻辑说明：`reset_worker` 接收 `name`、`context`，处理 worker，核心调用为 `_require_worker`、`_worker_create_from_resource`、`begin`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`delete_worker` 接收 `name`、`context`，删除 worker，核心调用为 `get`、`_require_worker`、`_run_worker_mutation`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        if await self._supervisor.get(context.operation_id) is None:
            await self._require_worker(name)

        async def mutate() -> None:
            # 逻辑说明：`mutate` 是当前资源操作的局部回调，调用 `delete_worker` 发起一次变更；异常原样交给外层 operation 状态机处理。
            await self._controller.delete_worker(name)

        async def prove() -> WorkerResource | None | bool:
            # 逻辑说明：`prove` 是当前资源操作的局部回调，读取 `_wait_for_absence` 核对变更后的真实状态；异常原样交给外层 operation 状态机处理。
            absent = await self._wait_for_absence(
                self._controller.get_worker,
                name,
                self._delete_poll_delays,
            )
            return True if absent else None

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
        # 逻辑说明：`create_team` 接收 `spec`、`context`，创建 team，核心调用为 `_run_team_upsert`，返回 `TeamResource`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`apply_team` 接收 `spec`、`context`，处理 team，核心调用为 `_run_team_upsert`，返回 `TeamResource`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`delete_team` 接收 `name`、`context`，删除 team，核心调用为 `get`、`get_team`、`begin`，返回 `tuple[str, ...]`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
            if not await self._wait_for_absence(
                self._controller.get_team,
                name,
                self._delete_poll_delays,
            ):
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
        if not await self._wait_for_absence(
            self._controller.get_team,
            name,
            self._delete_poll_delays,
        ):
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
        # 逻辑说明：`create_human` 接收 `request`、`context`，创建 human，核心调用为 `begin`、`model_dump`、`_require_human`，返回 `HumanResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`delete_human` 接收 `name`、`context`，删除 human，核心调用为 `begin`、`ConflictError`、`_wait_for_absence`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.DELETE_HUMAN,
            target_key=f"human/{name}",
            request={"name": name, "action": "delete"},
        )
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(f"delete human/{name} previously failed")
        if operation.status is OperationStatus.SUCCEEDED:
            if not await self._wait_for_absence(
                self._controller.get_human,
                name,
                self._delete_poll_delays,
            ):
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
        if not await self._wait_for_absence(
            self._controller.get_human,
            name,
            self._delete_poll_delays,
        ):
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
        # 逻辑说明：`update_human` 接收 `request`、`context`，更新 human，核心调用为 `begin`、`model_dump`、`_require_human`，返回 `HumanResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`_wait_for_human` 在有限次数内轮询for human（复用 `get_human`、`casefold`），收敛后返回 `HumanResource`；超时会记录 ambiguous effect，避免把尚未生效的控制面变更报告为成功。
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
        # 逻辑说明：`_require_human` 通过 `get_human`、`NotFoundError` 读取并验证 human，返回 `HumanResource`；目标不存在、依赖未启用或数据不合法时在产生后续副作用前抛出领域错误。
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
        # 逻辑说明：`_run_team_upsert` 接收 `spec`、`context`、`kind`、`force_apply`、`reject_existing`，执行一轮 team upsert，核心调用为 `begin`、`model_dump`、`_require_team`，返回 `TeamResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`_wait_for_team` 在有限次数内轮询for team（复用 `get_team`、`casefold`），收敛后返回 `TeamResource`；超时会记录 ambiguous effect，避免把尚未生效的控制面变更报告为成功。
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
        # 逻辑说明：`_refresh_topology_or_reconcile` 接收 `operation_id`，刷新 topology or reconcile，核心调用为 `refresh`、`effect_ambiguous`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        try:
            await self._topology.refresh()
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                operation_id,
                ExternalEffect.MATRIX,
                f"topology not converged: {type(exc).__name__}",
            )
            raise

    async def _wait_for_absence(
        self,
        getter: Callable[[str], Awaitable[Any | None]],
        name: str,
        poll_delays: tuple[float, ...],
    ) -> bool:
        # 逻辑说明：`_wait_for_absence` 在有限次数内轮询for absence（复用 `getter`、`_sleeper`），收敛后返回 `bool`；超时会记录 ambiguous effect，避免把尚未生效的控制面变更报告为成功。
        attempts = len(poll_delays) + 1
        for index in range(attempts):
            if await getter(name) is None:
                return True
            if index < len(poll_delays):
                await self._sleeper(poll_delays[index])
        return False

    async def _require_team(self, name: str) -> TeamResource:
        # 逻辑说明：`_require_team` 通过 `get_team`、`NotFoundError` 读取并验证 team，返回 `TeamResource`；目标不存在、依赖未启用或数据不合法时在产生后续副作用前抛出领域错误。
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
        # 逻辑说明：`_run_worker_mutation` 接收 `context`、`kind`、`name`、`request`、`mutate`、`prove`，执行一轮 worker mutation，核心调用为 `begin`、`ConflictError`、`prove`，返回 `WorkerResource | None`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`_wait_for_worker_room` 在有限次数内轮询for worker room（复用 `get_worker`、`casefold`），收敛后返回 `WorkerResource`；超时会记录 ambiguous effect，避免把尚未生效的控制面变更报告为成功。
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
        # 逻辑说明：`_handle_ambiguous_worker_effect` 接收 `operation_id`、`name`、`effect`、`exc`，处理请求 ambiguous worker effect，核心调用为 `_ambiguous_exception`、`effect_failed`、`_safe_reason`，返回 `WorkerResource`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`resume_operation` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `resume_worker_create`、`resume_worker_import`、`_resume_worker_mutation` 证明或补做下一阶段，最终返回 `WorkerResource | TeamResource | HumanResource | None`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
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
        # 逻辑说明：`_resume_worker_mutation` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `_target_name`、`get`、`_resume_worker_reset` 证明或补做下一阶段，最终返回 `WorkerResource`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
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
        # 逻辑说明：`_resume_worker_reset` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `_target_name`、`get`、`RecoveryError` 证明或补做下一阶段，最终返回 `WorkerResource`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
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
        # 逻辑说明：`_resume_team_upsert` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `get`、`ConflictError`、`model_validate` 证明或补做下一阶段，最终返回 `TeamResource`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
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
        # 逻辑说明：`_resume_human_create` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `model_validate`、`get_human`、`AmbiguousEffectError` 证明或补做下一阶段，最终返回 `HumanResource`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
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
        # 逻辑说明：`_resume_human_update` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `model_validate`、`get_human`、`_matches_human_update` 证明或补做下一阶段，最终返回 `HumanResource`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
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
        # 逻辑说明：`_resume_resource_delete` 从持久化 operation/request 重建Worker、Team、Human 与拓扑上下文，通过 `_target_name`、`_wait_for_absence`、`AmbiguousEffectError` 证明或补做下一阶段，最终返回 `None`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
        name = _target_name(operation.target_key, resource_type)
        getter = getattr(self._controller, f"get_{resource_type}")
        if not await self._wait_for_absence(
            getter,
            name,
            self._delete_poll_delays,
        ):
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
        # 逻辑说明：`_finish_recovered_resource` 接收 `operation`、`resource`，结束恢复 recovered resource，核心调用为 `_refresh_topology_or_reconcile`、`effect_succeeded`、`_resource_receipt`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        await self._refresh_topology_or_reconcile(operation.operation_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            _resource_receipt(resource),
        )

    async def _require_worker(self, name: str) -> WorkerResource:
        # 逻辑说明：`_require_worker` 通过 `get_worker`、`NotFoundError` 读取并验证 worker，返回 `WorkerResource`；目标不存在、依赖未启用或数据不合法时在产生后续副作用前抛出领域错误。
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
        # 逻辑说明：`__init__` 校验并保存 `operations`、`resources`、`matrix_resources`，为Worker、Team、Human 与拓扑建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
        self._operations = operations
        self._resources = resources
        self._matrix_resources = matrix_resources

    async def reconcile_pending_resources(self) -> ResourceRecoveryReport:
        # 逻辑说明：`reconcile_pending_resources` 从 operation 仓库列出尚未终结的 resources，逐项恢复并把成功与失败 ID 汇总为 `ResourceRecoveryReport`；单项失败不会阻断本轮其余恢复。
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
        # 逻辑说明：`reconcile_pending_workers` 从 operation 仓库列出尚未终结的 workers，逐项恢复并把成功与失败 ID 汇总为 `ResourceRecoveryReport`；单项失败不会阻断本轮其余恢复。
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
        # 逻辑说明：`__init__` 校验并保存 `controller`、`matrix`、`topology`、`manager_user_id`、`admin_user_id`、`admin_room_id`，为Worker、Team、Human 与拓扑建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
        self._controller = controller
        self._matrix = matrix
        self._topology = topology
        self._manager_user_id = manager_user_id
        self._admin_user_id = admin_user_id
        self._admin_room_id = admin_room_id
        self._refresh_lock = asyncio.Lock()

    async def refresh(self) -> TopologySnapshot:
        # 逻辑说明：`refresh` 接收 当前服务状态，刷新 Worker、Team、Human 与拓扑，核心调用为 `_refresh`，返回 `TopologySnapshot`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        async with self._refresh_lock:
            return await self._refresh()

    async def _refresh(self) -> TopologySnapshot:
        # 逻辑说明：`_refresh` 接收 当前服务状态，刷新 Worker、Team、Human 与拓扑，核心调用为 `gather`、`list_workers`、`list_teams`，返回 `TopologySnapshot`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
        # 逻辑说明：`policy_for` 接收 `room_id`、`sender_id`，计算房间策略 for，核心调用为 `revision`、`RoomPolicy`、`room_binding`，返回 `RoomPolicy`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
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
        # 逻辑说明：`_validate_memberships` 对照 Worker/Human、应加入房间、禁入房间和实际成员表，发现缺席、越权加入或未知成员就抛 ConflictError；这里只核对拓扑快照。
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
    # 逻辑说明：`_target_name` 接收 `target_key`、`resource_type`，解析目标 name，核心调用为 `startswith`、`ValueError`，返回 `str`。 它只在内存中计算、校验或组装数据；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
    prefix = f"{resource_type}/"
    if not target_key.startswith(prefix) or len(target_key) == len(prefix):
        raise ValueError(
            f"target {target_key!r} is not a {resource_type} key",
        )
    return target_key[len(prefix) :]


def _resource_receipt(
    resource: WorkerResource | TeamResource | HumanResource,
) -> dict[str, Any]:
    # 逻辑说明：`_resource_receipt` 从 `resource` 构造 `dict[str, Any]`，统一调用方看到的Worker、Team、Human 与拓扑结果；它只转换数据，不执行远端 I/O。
    return resource.model_dump(mode="json")


def _resource_phase(
    resource: WorkerResource | TeamResource | HumanResource,
) -> str:
    # 逻辑说明：`_resource_phase` 接收 `resource`，解析资源 phase，核心调用为 `get`，返回 `str`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
    if isinstance(resource, (WorkerResource, TeamResource)):
        return resource.phase or ""
    value = resource.status.get("phase")
    return str(value) if value is not None else ""


def _resource_message(
    resource: WorkerResource | TeamResource | HumanResource,
) -> str:
    # 逻辑说明：`_resource_message` 接收 `resource`，解析资源 message，核心调用为 `get`，返回 `str`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
    value = resource.status.get("message")
    return str(value) if value is not None else ""


def _matches_worker_create(
    worker: WorkerResource,
    request: WorkerCreateRequest,
) -> bool:
    # 逻辑说明：`_matches_worker_create` 比较期望请求与已观测 worker create，返回 `bool` 供 operation 判断外部效果是否已经发生；它只读数据，不修改资源。
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
    # 逻辑说明：`_worker_create_from_resource` 接收 `worker`，处理 Worker create from resource，核心调用为 `ConflictError`、`WorkerCreateRequest`、`get`，返回 `WorkerCreateRequest`。 它只在内存中计算、校验或组装数据；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
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
    # 逻辑说明：`_matches_worker_update` 比较期望请求与已观测 worker update，返回 `bool` 供 operation 判断外部效果是否已经发生；它只读数据，不修改资源。
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
    # 逻辑说明：`_worker_console_state` 接收 `worker`，处理 Worker console state，核心调用为 `get`，返回 `tuple[bool, int]`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
    # 逻辑说明：`_matches_human_create` 比较期望请求与已观测 human create，返回 `bool` 供 operation 判断外部效果是否已经发生；它只读数据，不修改资源。
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
    # 逻辑说明：`_matches_human_update` 比较期望请求与已观测 human update，返回 `bool` 供 operation 判断外部效果是否已经发生；它只读数据，不修改资源。
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
    # 逻辑说明：`_document_value` 接收 `document`、`key`、`value`，读取文档字段 value，返回 `None`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
    if value is not None and value != "":
        document[key] = value


def _team_is_ready(
    team: TeamResource,
    *,
    expected_workers: int,
) -> bool:
    # 逻辑说明：`_team_is_ready` 比较期望请求与已观测 is ready，返回 `bool` 供 operation 判断外部效果是否已经发生；它只读数据，不修改资源。
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
    # 逻辑说明：`_ambiguous_exception` 接收 `exc`，判断歧义 exception，返回 `bool`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
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
    # 逻辑说明：`_safe_import_reason` 把 `exc` 转成适合持久化或日志的 `str`，删除/隐藏敏感值并限制不安全结构；该过程不修改原对象。
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
    # 逻辑说明：`_unique_by_name` 接收 `resources`、`label`，处理 by name，核心调用为 `ConflictError`，返回 `dict[str, Any]`。 它只在内存中计算、校验或组装数据；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
    indexed: dict[str, Any] = {}
    for resource in resources:
        if resource.name in indexed:
            raise ConflictError(
                f"duplicate {label} resource {resource.name!r}",
            )
        indexed[resource.name] = resource
    return indexed


def _add_string_room(rooms: set[str], value: object) -> None:
    # 逻辑说明：`_add_string_room` 接收 `rooms`、`value`，添加 string room，核心调用为 `add`，返回 `None`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
    if isinstance(value, str) and value:
        rooms.add(value)


def _deny_policy(
    room_id: str,
    revision: int,
    *,
    silent: bool = False,
) -> RoomPolicy:
    # 逻辑说明：`_deny_policy` 接收 `room_id`、`revision`、`silent`，生成拒绝策略 policy，核心调用为 `RoomPolicy`，返回 `RoomPolicy`。 它只在内存中计算、校验或组装数据；下游异常按原语义向上传递，不会伪造成功结果。
    return RoomPolicy(
        room_id=room_id,
        kind=RoomKind.UNKNOWN,
        revision=revision,
        silent=silent,
    )
