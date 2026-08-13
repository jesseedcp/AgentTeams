"""Cross-system model, MCP, and service integration workflows.

协调 Controller、Higress、模型 gateway 与 Worker 的集成变更。

例如切换模型需要先 preflight，更新 Higress/Controller 期望状态，等待 runtime revision
发布，再通知受影响 Worker。每个跨系统效果之前都写 journal，超时后按实际状态对账。
这使“一条工具成功”代表所有必要系统最终一致，而不是某个 API 暂时返回 200。
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections.abc import Awaitable, Callable
from typing import Literal, Protocol, cast
from urllib.parse import urlsplit

from agentscope.tool import ToolBase
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_validator,
    model_validator,
)

from agentteams_manager.clients.agt import (
    AgtClient,
    WorkerUpdateRequest,
)
from agentteams_manager.clients.higress import (
    HigressTransportError,
    MCPServerState,
    ProxyMCPRequest,
    RestMCPRequest,
)
from agentteams_manager.clients.model_gateway import (
    ModelCapabilities,
    ModelGatewayClient,
    ModelSpec,
)
from agentteams_manager.clients.process import ProcessTimeout
from agentteams_manager.config import MCPServerDocument
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    RecoveryError,
)
from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationRecord,
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.domain.ports import Clock
from agentteams_manager.runtime.config_watcher import (
    ConfigWatcher,
    RuntimeRegistry,
)
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskSupervisorPort


class ModelSwitchRequest(ModelSpec):
    """Closed request contract shared by Manager and Worker switching."""


class ModelSwitchReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    target: str
    model: str
    context_window: int = Field(gt=0)
    max_tokens: int = Field(gt=0)
    reasoning: bool
    input_modalities: tuple[str, ...]
    phase: str
    runtime_revision: int | None = Field(default=None, ge=0)
    active_turns_preserved: bool = True


class ManagerIdentityRequest(BaseModel):
    """Administrator-confirmed identity preferences."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=80)
    communication_style: str = Field(min_length=1, max_length=1000)
    behavior_guidelines: tuple[str, ...] = Field(
        default=(),
        max_length=20,
    )
    default_language: str = Field(min_length=1, max_length=80)

    @field_validator(
        "name",
        "communication_style",
        "default_language",
    )
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("identity field must not be blank")
        return normalized

    @field_validator("behavior_guidelines")
    @classmethod
    def normalize_guidelines(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        normalized = tuple(" ".join(item.split()) for item in value)
        if any(not item for item in normalized):
            raise ValueError("behavior guidelines must not be blank")
        return normalized


class ManagerIdentityReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    manager: str
    name: str
    communication_style: str
    behavior_guidelines: tuple[str, ...]
    default_language: str
    phase: str
    runtime_revision: int = Field(ge=0)
    active_turns_preserved: bool = True


class RuntimeWatcherPort(Protocol):
    async def poll_once(self) -> object | None: ...


class HigressMCPPort(Protocol):
    async def list_mcp_servers(self) -> tuple[MCPServerState, ...]: ...

    async def get_consumers(self, name: str) -> frozenset[str]: ...

    async def upsert_rest_server(
        self,
        request: RestMCPRequest,
    ) -> MCPServerDocument: ...

    async def upsert_proxy(
        self,
        request: ProxyMCPRequest,
    ) -> MCPServerDocument: ...

    async def replace_consumers(
        self,
        name: str,
        consumers: set[str] | frozenset[str],
    ) -> frozenset[str]: ...

    async def delete_server(self, name: str) -> None: ...

    def descriptor(self, name: str) -> MCPServerDocument: ...


class MCPVerificationPort(Protocol):
    async def list_server_tools(
        self,
        server_name: str,
        *,
        revision: int,
    ) -> tuple[ToolBase, ...] | list[ToolBase]: ...

    async def call_server_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: dict[str, object],
        *,
        revision: int,
    ) -> object: ...


class WorkerNotificationPort(Protocol):
    async def notify_worker(
        self,
        worker: str,
        text: str,
        *,
        source_operation_id: str,
    ) -> None: ...


class CloudMCPManagementUnsupported(RuntimeError):
    """Cloud MCP definitions must be changed in the cloud console."""


class MCPIntegrationUnavailable(RuntimeError):
    """The Manager process has no local MCP administration dependencies."""


class MCPVerificationError(RuntimeError):
    """The configured MCP did not pass native AgentScope verification."""


class MCPConfiguration(BaseModel):
    """One credential-bearing request that is never persisted wholesale."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    server: RestMCPRequest | ProxyMCPRequest
    workers: tuple[str, ...] = ()
    verification_tool: str = Field(
        min_length=1,
        pattern=r"^mcp__[a-z0-9][a-z0-9-]*__[A-Za-z0-9_.:-]+$",
    )
    verification_arguments: dict[str, JsonValue] = Field(
        default_factory=dict,
        repr=False,
    )

    @field_validator("workers")
    @classmethod
    def validate_workers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("MCP Worker names must be unique")
        return value

    @field_validator("verification_arguments")
    @classmethod
    def reject_sensitive_verification_arguments(
        cls,
        value: dict[str, JsonValue],
    ) -> dict[str, JsonValue]:
        _validate_safe_verification_arguments(value)
        return value

    @model_validator(mode="after")
    def tool_belongs_to_server(self) -> MCPConfiguration:
        prefix = f"mcp__{self.server.name}__"
        if not self.verification_tool.startswith(prefix):
            raise ValueError(
                "verification tool must belong to the configured MCP",
            )
        return self


class MCPManagementReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    action: Literal["configure", "grant", "revoke", "delete"]
    name: str
    descriptor: MCPServerDocument | None = None
    consumers: frozenset[str] = frozenset()
    workers: tuple[str, ...] = ()
    verified: bool = False
    verification_tool: str | None = None
    runtime_revision: int | None = Field(default=None, ge=0)


class PublishedRoute(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    port: int = Field(ge=1, le=65535)
    domain: str = Field(min_length=1)


class ServicePublishingReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    action: Literal["publish", "unpublish"]
    worker: str
    ports: tuple[int, ...]
    routes: tuple[PublishedRoute, ...] = ()
    domains: tuple[str, ...] = ()
    supported: bool = True
    public: bool = True
    phase: str
    message: str = ""

    @model_validator(mode="after")
    def routes_match_domains(self) -> ServicePublishingReceipt:
        if self.domains != tuple(route.domain for route in self.routes):
            raise ValueError("service route domains must match routes")
        if not self.supported and (self.routes or self.domains):
            raise ValueError("unsupported publishing cannot report routes")
        return self


Sleep = Callable[[float], None | Awaitable[None]]
ServicePublishingAction = Literal["publish", "unpublish"]


class IntegrationService:
    """Validate live routes before changing Controller desired state."""

    def __init__(
        self,
        *,
        agt: AgtClient,
        gateway: ModelGatewayClient,
        supervisor: TaskSupervisorPort,
        clock: Clock,
        manager_name: str,
        registry: RuntimeRegistry,
        watcher: ConfigWatcher | RuntimeWatcherPort,
        sleep: Sleep,
        poll_attempts: int = 30,
        poll_interval: float = 1,
        higress: HigressMCPPort | None = None,
        mcp_verifier: MCPVerificationPort | None = None,
        worker_notifications: WorkerNotificationPort | None = None,
        runtime_mode: str = "local",
        mcp_propagation_timeout: float = 30,
    ) -> None:
        if not manager_name:
            raise ValueError("manager_name must not be empty")
        if poll_attempts < 1 or poll_interval < 0:
            raise ValueError("invalid integration polling bounds")
        if mcp_propagation_timeout <= 0:
            raise ValueError("MCP propagation timeout must be positive")
        self._agt = agt
        self._gateway = gateway
        self._supervisor = supervisor
        self._clock = clock
        self._manager_name = manager_name
        self._registry = registry
        self._watcher = watcher
        self._sleep = sleep
        self._poll_attempts = poll_attempts
        self._poll_interval = poll_interval
        self._higress = higress
        self._mcp_verifier = mcp_verifier
        self._worker_notifications = worker_notifications
        self._runtime_mode = runtime_mode.casefold()
        self._mcp_propagation_timeout = mcp_propagation_timeout

    async def switch_manager_model(
        self,
        request: ModelSwitchRequest,
        *,
        context: MutationContext,
    ) -> ModelSwitchReceipt:
        capabilities = await self._gateway.preflight(request)
        baseline_revision = self._registry.revision
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.SWITCH_MODEL,
            target_key=f"manager/{self._manager_name}/model",
            request={
                "target": "manager",
                "name": self._manager_name,
                "model": request.model,
                "capabilities": capabilities.model_dump(mode="json"),
                "baseline_revision": baseline_revision,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return ModelSwitchReceipt.model_validate(operation.result)
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "update_manager_model",
                "manager": self._manager_name,
                "model": request.model,
            },
        )
        try:
            manager = await self._agt.update_manager_model(
                self._manager_name,
                request.model,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {
                "manager": manager.name,
                "phase": manager.phase,
                "model": manager.model,
            },
        )
        for _ in range(self._poll_attempts):
            await self._watcher.poll_once()
            generation = getattr(self._registry, "current", None)
            document = (
                getattr(generation, "document", None)
                if generation is not None
                else None
            )
            if (
                self._registry.revision > baseline_revision
                and document is not None
                and document.model == request.model
            ):
                receipt = _model_receipt(
                    operation_id=operation.operation_id,
                    target=f"manager/{self._manager_name}",
                    capabilities=capabilities,
                    phase=manager.phase,
                    runtime_revision=self._registry.revision,
                )
                await self._supervisor.effect_succeeded(
                    operation.operation_id,
                    ExternalEffect.STORAGE,
                    receipt.model_dump(mode="json"),
                )
                return receipt
            await self._wait()
        await self._supervisor.effect_ambiguous(
            operation.operation_id,
            ExternalEffect.STORAGE,
            "runtime_revision_not_observed",
        )
        raise AmbiguousEffectError(
            "Controller model update is not visible in a higher runtime "
            "document revision",
        )

    async def switch_worker_model(
        self,
        *,
        worker: str,
        request: ModelSwitchRequest,
        context: MutationContext,
    ) -> ModelSwitchReceipt:
        capabilities = await self._gateway.preflight(request)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.SWITCH_MODEL,
            target_key=f"worker/{worker}/model",
            request={
                "target": "worker",
                "name": worker,
                "model": request.model,
                "capabilities": capabilities.model_dump(mode="json"),
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return ModelSwitchReceipt.model_validate(operation.result)
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "update_worker_model",
                "worker": worker,
                "model": request.model,
            },
        )
        try:
            await self._agt.update_worker(
                WorkerUpdateRequest(name=worker, model=request.model),
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {"worker": worker, "model": request.model},
        )
        observed: WorkerResource | None = None
        for _ in range(self._poll_attempts):
            observed = await self._agt.get_worker(worker)
            if observed is not None:
                phase = (observed.phase or "").casefold()
                if phase in {"failed", "error"}:
                    await self._supervisor.effect_failed(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        "worker entered a failed phase",
                    )
                    raise ConflictError(
                        f"worker/{worker} entered {observed.phase}",
                    )
                if observed.model == request.model and phase:
                    receipt = _model_receipt(
                        operation_id=operation.operation_id,
                        target=f"worker/{worker}",
                        capabilities=capabilities,
                        phase=observed.phase or "Unknown",
                    )
                    await self._supervisor.effect_succeeded(
                        operation.operation_id,
                        ExternalEffect.CONTROLLER,
                        receipt.model_dump(mode="json"),
                    )
                    return receipt
            await self._wait()
        await self._supervisor.effect_ambiguous(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            "worker_model_not_observed",
        )
        raise AmbiguousEffectError(
            f"worker/{worker} model did not converge",
        )

    async def update_manager_identity(
        self,
        request: ManagerIdentityRequest,
        *,
        context: MutationContext,
    ) -> ManagerIdentityReceipt:
        identity = _render_manager_identity(request)
        baseline_revision = self._registry.revision
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_MANAGER_IDENTITY,
            target_key=f"manager/{self._manager_name}/identity",
            request={
                **request.model_dump(mode="json"),
                "identity": identity,
                "baseline_revision": baseline_revision,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return ManagerIdentityReceipt.model_validate(operation.result)
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "update_manager_identity",
                "manager": self._manager_name,
            },
        )
        try:
            manager = await self._agt.update_manager_identity(
                self._manager_name,
                identity,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            {
                "manager": manager.name,
                "phase": manager.phase,
            },
        )
        runtime_revision = await self._wait_for_new_runtime_revision(
            operation.operation_id,
            baseline_revision=baseline_revision,
        )
        receipt = _identity_receipt(
            operation_id=operation.operation_id,
            manager=self._manager_name,
            request=request,
            phase=manager.phase,
            runtime_revision=runtime_revision,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def configure_mcp(
        self,
        request: MCPConfiguration,
        *,
        context: MutationContext,
    ) -> MCPManagementReceipt:
        self._require_local_mcp()
        higress, verifier = self._require_mcp_dependencies(
            require_verifier=True,
        )
        assert verifier is not None
        baseline_revision = self._registry.revision
        safe_request = _safe_configuration_request(
            request,
            baseline_revision=baseline_revision,
        )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CONFIGURE_MCP,
            target_key=f"mcp/{request.server.name}",
            request=safe_request,
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return MCPManagementReceipt.model_validate(operation.result)

        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "operation": "upsert_mcp",
                "name": request.server.name,
                "kind": safe_request["kind"],
            },
        )
        try:
            if isinstance(request.server, RestMCPRequest):
                descriptor = await higress.upsert_rest_server(request.server)
            else:
                descriptor = await higress.upsert_proxy(request.server)
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "operation": "upsert_mcp",
                "name": request.server.name,
            },
        )

        existing_consumers = await higress.get_consumers(
            request.server.name,
        )
        intended_consumers = {
            *existing_consumers,
            "manager",
            *(_worker_consumer(name) for name in request.workers),
        }
        consumers = await self._converge_consumers(
            operation.operation_id,
            request.server.name,
            observed=existing_consumers,
            intended=intended_consumers,
            higress=higress,
        )
        await self._install_descriptor(
            operation.operation_id,
            descriptor,
            workers=request.workers,
        )
        runtime_revision = await self._wait_for_runtime_descriptor(
            operation.operation_id,
            descriptor,
            baseline_revision=baseline_revision,
        )
        await self._wait_for_verification_tool(
            operation.operation_id,
            request.server.name,
            request.verification_tool,
            revision=runtime_revision,
            verifier=verifier,
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.PROCESS,
            {
                "operation": "verify_mcp_tool",
                "name": request.server.name,
                "tool": request.verification_tool,
                "argument_names": sorted(
                    request.verification_arguments,
                ),
            },
        )
        try:
            await verifier.call_server_tool(
                request.server.name,
                request.verification_tool,
                cast(
                    dict[str, object],
                    request.verification_arguments,
                ),
                revision=runtime_revision,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.PROCESS,
                exc,
            )
            raise MCPVerificationError(
                "native AgentScope MCP verification call failed "
                f"({type(exc).__name__})",
            ) from None
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.PROCESS,
            {
                "operation": "verify_mcp_tool",
                "name": request.server.name,
                "tool": request.verification_tool,
            },
        )
        await self._notify_mcp_workers(
            operation.operation_id,
            request.server.name,
            request.workers,
        )
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="configure",
            name=request.server.name,
            descriptor=descriptor,
            consumers=consumers,
            workers=request.workers,
            verified=True,
            verification_tool=request.verification_tool,
            runtime_revision=runtime_revision,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            (
                ExternalEffect.MATRIX
                if request.workers
                else ExternalEffect.PROCESS
            ),
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def list_mcp_servers(self) -> tuple[MCPServerState, ...]:
        higress, _ = self._require_mcp_dependencies()
        return await higress.list_mcp_servers()

    async def grant_mcp(
        self,
        name: str,
        *,
        workers: tuple[str, ...],
        context: MutationContext,
    ) -> MCPManagementReceipt:
        self._require_local_mcp()
        higress, _ = self._require_mcp_dependencies()
        _validate_worker_names(workers)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CONFIGURE_MCP,
            target_key=f"mcp/{name}",
            request={
                "action": "grant",
                "name": name,
                "workers": list(workers),
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return MCPManagementReceipt.model_validate(operation.result)
        existing = await higress.get_consumers(name)
        intended = {
            *existing,
            "manager",
            *(_worker_consumer(worker) for worker in workers),
        }
        consumers = await self._converge_consumers(
            operation.operation_id,
            name,
            observed=existing,
            intended=intended,
            higress=higress,
        )
        descriptor = higress.descriptor(name)
        await self._install_descriptor(
            operation.operation_id,
            descriptor,
            workers=workers,
        )
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="grant",
            name=name,
            descriptor=descriptor,
            consumers=consumers,
            workers=workers,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def revoke_mcp(
        self,
        name: str,
        *,
        workers: tuple[str, ...],
        context: MutationContext,
    ) -> MCPManagementReceipt:
        self._require_local_mcp()
        higress, _ = self._require_mcp_dependencies()
        _validate_worker_names(workers)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CONFIGURE_MCP,
            target_key=f"mcp/{name}",
            request={
                "action": "revoke",
                "name": name,
                "workers": list(workers),
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return MCPManagementReceipt.model_validate(operation.result)
        revoked = {_worker_consumer(worker) for worker in workers}
        existing = await higress.get_consumers(name)
        consumers = await self._converge_consumers(
            operation.operation_id,
            name,
            observed=existing,
            intended={"manager", *(existing - revoked)},
            higress=higress,
        )
        await self._remove_worker_descriptor(
            operation.operation_id,
            name,
            workers,
        )
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="revoke",
            name=name,
            descriptor=higress.descriptor(name),
            consumers=consumers,
            workers=workers,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def delete_mcp(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> MCPManagementReceipt:
        self._require_local_mcp()
        higress, _ = self._require_mcp_dependencies()
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CONFIGURE_MCP,
            target_key=f"mcp/{name}",
            request={"action": "delete", "name": name},
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return MCPManagementReceipt.model_validate(operation.result)
        manager = await self._agt.get_manager(self._manager_name)
        if manager is None:
            raise ConflictError(
                f"manager/{self._manager_name} is not readable",
            )
        await self._replace_manager_descriptors(
            operation.operation_id,
            tuple(
                server
                for server in manager.mcp_servers
                if server.name != name
            ),
        )
        workers = await self._agt.list_workers()
        affected = tuple(
            worker.name
            for worker in workers
            if any(
                server.name == name
                for server in _worker_mcp_servers(worker)
            )
        )
        await self._remove_worker_descriptor(
            operation.operation_id,
            name,
            affected,
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {"operation": "delete_mcp", "name": name},
        )
        try:
            await higress.delete_server(name)
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                exc,
            )
            raise
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="delete",
            name=name,
            workers=affected,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def publish_service(
        self,
        *,
        worker: str,
        ports: tuple[int, ...],
        context: MutationContext,
    ) -> ServicePublishingReceipt:
        requested = _validated_ports(ports)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.PUBLISH_SERVICE,
            target_key=f"worker/{worker}/expose",
            request={
                "action": "publish",
                "worker": worker,
                "ports": list(requested),
                "public": True,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return ServicePublishingReceipt.model_validate(operation.result)
        if self._runtime_mode == "aliyun":
            return await self._unsupported_service_receipt(
                operation.operation_id,
                action="publish",
                worker=worker,
                ports=requested,
            )
        current = await self._require_worker(worker)
        desired = tuple(
            sorted({*_desired_expose(current), *requested}),
        )
        observed = await self._replace_expose(
            operation.operation_id,
            worker,
            desired,
            current=current,
        )
        converged = await self._wait_for_service_state(
            operation.operation_id,
            worker,
            desired=desired,
            removed=(),
            first=observed,
        )
        route_map = _observed_routes(converged)
        routes = tuple(
            PublishedRoute(port=port, domain=route_map[port])
            for port in requested
        )
        receipt = ServicePublishingReceipt(
            operation_id=operation.operation_id,
            action="publish",
            worker=worker,
            ports=requested,
            routes=routes,
            domains=tuple(route.domain for route in routes),
            phase=converged.phase or "Unknown",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def unpublish_service(
        self,
        *,
        worker: str,
        ports: tuple[int, ...],
        context: MutationContext,
    ) -> ServicePublishingReceipt:
        requested = _validated_ports(ports)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.PUBLISH_SERVICE,
            target_key=f"worker/{worker}/expose",
            request={
                "action": "unpublish",
                "worker": worker,
                "ports": list(requested),
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return ServicePublishingReceipt.model_validate(operation.result)
        if self._runtime_mode == "aliyun":
            return await self._unsupported_service_receipt(
                operation.operation_id,
                action="unpublish",
                worker=worker,
                ports=requested,
            )
        current = await self._require_worker(worker)
        removed = set(requested)
        desired = tuple(
            port
            for port in _desired_expose(current)
            if port not in removed
        )
        observed = await self._replace_expose(
            operation.operation_id,
            worker,
            desired,
            current=current,
        )
        converged = await self._wait_for_service_state(
            operation.operation_id,
            worker,
            desired=desired,
            removed=requested,
            first=observed,
        )
        receipt = ServicePublishingReceipt(
            operation_id=operation.operation_id,
            action="unpublish",
            worker=worker,
            ports=requested,
            phase=converged.phase or "Unknown",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> (
        ModelSwitchReceipt
        | ManagerIdentityReceipt
        | MCPManagementReceipt
        | ServicePublishingReceipt
    ):
        """Converge one durable integration intent from external facts."""
        if operation.status is OperationStatus.SUCCEEDED:
            if operation.kind is OperationKind.SWITCH_MODEL:
                return ModelSwitchReceipt.model_validate(operation.result)
            if operation.kind is OperationKind.UPDATE_MANAGER_IDENTITY:
                return ManagerIdentityReceipt.model_validate(
                    operation.result,
                )
            if operation.kind is OperationKind.CONFIGURE_MCP:
                return MCPManagementReceipt.model_validate(operation.result)
            if operation.kind is OperationKind.PUBLISH_SERVICE:
                return ServicePublishingReceipt.model_validate(
                    operation.result,
                )
        if operation.status in {
            OperationStatus.FAILED,
            OperationStatus.NEEDS_ATTENTION,
        }:
            raise RecoveryError(
                f"cannot resume terminal integration operation "
                f"{operation.operation_id}",
            )
        if operation.status is OperationStatus.RETRY_WAIT:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.STORAGE,
                "integration_retry_resumed",
            )
        if operation.kind is OperationKind.SWITCH_MODEL:
            return await self._resume_model_switch(operation)
        if operation.kind is OperationKind.UPDATE_MANAGER_IDENTITY:
            return await self._resume_manager_identity(operation)
        if operation.kind is OperationKind.CONFIGURE_MCP:
            return await self._resume_mcp(operation)
        if operation.kind is OperationKind.PUBLISH_SERVICE:
            return await self._resume_service_publishing(operation)
        raise RecoveryError(
            f"IntegrationService cannot recover {operation.kind.value}",
        )

    async def _resume_manager_identity(
        self,
        operation: OperationRecord,
    ) -> ManagerIdentityReceipt:
        request = _recovery_identity_request(operation.request)
        identity = _recovery_string(operation.request, "identity")
        if identity != _render_manager_identity(request):
            raise RecoveryError(
                "Manager identity recovery payload is inconsistent",
            )
        baseline_revision = _recovery_revision(operation.request)
        manager = await self._agt.get_manager(self._manager_name)
        if manager is None:
            raise RecoveryError(
                f"manager/{self._manager_name} is not readable",
            )
        if manager.identity != identity:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "update_manager_identity",
                    "manager": self._manager_name,
                },
            )
            try:
                manager = await self._agt.update_manager_identity(
                    self._manager_name,
                    identity,
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    exc,
                )
                raise
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "manager": manager.name,
                    "phase": manager.phase,
                },
            )
        runtime_revision = await self._wait_for_new_runtime_revision(
            operation.operation_id,
            baseline_revision=baseline_revision,
        )
        receipt = _identity_receipt(
            operation_id=operation.operation_id,
            manager=self._manager_name,
            request=request,
            phase=manager.phase,
            runtime_revision=runtime_revision,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_model_switch(
        self,
        operation: OperationRecord,
    ) -> ModelSwitchReceipt:
        target = _recovery_string(operation.request, "target")
        name = _recovery_string(operation.request, "name")
        model = _recovery_string(operation.request, "model")
        raw_capabilities = operation.request.get("capabilities")
        if not isinstance(raw_capabilities, dict):
            raise RecoveryError(
                "model recovery request has no typed capabilities",
            )
        try:
            capabilities = ModelCapabilities.model_validate(
                raw_capabilities,
            )
        except Exception as exc:
            raise RecoveryError(
                "model recovery capabilities are invalid",
            ) from exc
        if capabilities.model != model:
            raise RecoveryError(
                "model recovery capabilities do not match the target",
            )
        if target == "manager":
            return await self._resume_manager_model(
                operation,
                name=name,
                capabilities=capabilities,
            )
        if target == "worker":
            return await self._resume_worker_model(
                operation,
                name=name,
                capabilities=capabilities,
            )
        raise RecoveryError("model recovery target is invalid")

    async def _resume_manager_model(
        self,
        operation: OperationRecord,
        *,
        name: str,
        capabilities: ModelCapabilities,
    ) -> ModelSwitchReceipt:
        if name != self._manager_name:
            raise RecoveryError(
                "model recovery targets a different Manager",
            )
        manager = await self._agt.get_manager(name)
        if manager is None:
            raise RecoveryError(f"manager/{name} is not readable")
        if manager.model != capabilities.model:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "update_manager_model",
                    "manager": name,
                    "model": capabilities.model,
                },
            )
            try:
                manager = await self._agt.update_manager_model(
                    name,
                    capabilities.model,
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    exc,
                )
                raise
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "manager": manager.name,
                    "phase": manager.phase,
                    "model": manager.model,
                },
            )
        runtime_revision = await self._wait_for_runtime_model(
            operation.operation_id,
            capabilities.model,
        )
        receipt = _model_receipt(
            operation_id=operation.operation_id,
            target=f"manager/{name}",
            capabilities=capabilities,
            phase=manager.phase,
            runtime_revision=runtime_revision,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_worker_model(
        self,
        operation: OperationRecord,
        *,
        name: str,
        capabilities: ModelCapabilities,
    ) -> ModelSwitchReceipt:
        worker = await self._require_worker(name)
        if worker.model != capabilities.model:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "operation": "update_worker_model",
                    "worker": name,
                    "model": capabilities.model,
                },
            )
            try:
                worker = await self._agt.update_worker(
                    WorkerUpdateRequest(
                        name=name,
                        model=capabilities.model,
                    ),
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.CONTROLLER,
                    exc,
                )
                raise
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.CONTROLLER,
                {
                    "worker": name,
                    "model": capabilities.model,
                },
            )
        worker = await self._wait_for_worker_model(
            operation.operation_id,
            name,
            capabilities.model,
            first=worker,
        )
        receipt = _model_receipt(
            operation_id=operation.operation_id,
            target=f"worker/{name}",
            capabilities=capabilities,
            phase=worker.phase or "Unknown",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_mcp(
        self,
        operation: OperationRecord,
    ) -> MCPManagementReceipt:
        self._require_local_mcp()
        action = _recovery_string(operation.request, "action")
        if action == "configure":
            return await self._resume_mcp_configuration(operation)
        if action == "grant":
            return await self._resume_mcp_grant(operation)
        if action == "revoke":
            return await self._resume_mcp_revoke(operation)
        if action == "delete":
            return await self._resume_mcp_delete(operation)
        raise RecoveryError("MCP recovery action is invalid")

    async def _resume_mcp_configuration(
        self,
        operation: OperationRecord,
    ) -> MCPManagementReceipt:
        higress, verifier = self._require_mcp_dependencies(
            require_verifier=True,
        )
        assert verifier is not None
        name = _recovery_string(operation.request, "name")
        workers = _recovery_workers(operation.request)
        verification_tool = _recovery_string(
            operation.request,
            "verification_tool",
        )
        arguments = _recovery_verification_arguments(operation.request)
        available = {
            server.name
            for server in await higress.list_mcp_servers()
        }
        if name not in available:
            raise RecoveryError(
                f"MCP server {name!r} is absent from Higress; its "
                "credential was deliberately not persisted, so the "
                "configuration must be submitted again",
            )
        descriptor = higress.descriptor(name)
        existing = await higress.get_consumers(name)
        consumers = await self._converge_consumers(
            operation.operation_id,
            name,
            observed=existing,
            intended={
                *existing,
                "manager",
                *(_worker_consumer(worker) for worker in workers),
            },
            higress=higress,
        )
        await self._install_descriptor(
            operation.operation_id,
            descriptor,
            workers=workers,
        )
        runtime_revision = await self._runtime_revision_for_descriptor(
            operation.operation_id,
            descriptor,
        )
        await self._wait_for_verification_tool(
            operation.operation_id,
            name,
            verification_tool,
            revision=runtime_revision,
            verifier=verifier,
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.PROCESS,
            {
                "operation": "verify_mcp_tool",
                "name": name,
                "tool": verification_tool,
                "argument_names": sorted(arguments),
            },
        )
        try:
            await verifier.call_server_tool(
                name,
                verification_tool,
                cast(dict[str, object], arguments),
                revision=runtime_revision,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.PROCESS,
                exc,
            )
            raise MCPVerificationError(
                "native AgentScope MCP recovery verification failed "
                f"({type(exc).__name__})",
            ) from None
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.PROCESS,
            {
                "operation": "verify_mcp_tool",
                "name": name,
                "tool": verification_tool,
            },
        )
        await self._notify_mcp_workers(
            operation.operation_id,
            name,
            workers,
        )
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="configure",
            name=name,
            descriptor=descriptor,
            consumers=consumers,
            workers=workers,
            verified=True,
            verification_tool=verification_tool,
            runtime_revision=runtime_revision,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX if workers else ExternalEffect.PROCESS,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_mcp_grant(
        self,
        operation: OperationRecord,
    ) -> MCPManagementReceipt:
        higress, _ = self._require_mcp_dependencies()
        name = _recovery_string(operation.request, "name")
        workers = _recovery_workers(operation.request)
        await self._require_mcp_server(higress, name)
        existing = await higress.get_consumers(name)
        consumers = await self._converge_consumers(
            operation.operation_id,
            name,
            observed=existing,
            intended={
                *existing,
                "manager",
                *(_worker_consumer(worker) for worker in workers),
            },
            higress=higress,
        )
        descriptor = higress.descriptor(name)
        await self._install_descriptor(
            operation.operation_id,
            descriptor,
            workers=workers,
        )
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="grant",
            name=name,
            descriptor=descriptor,
            consumers=consumers,
            workers=workers,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_mcp_revoke(
        self,
        operation: OperationRecord,
    ) -> MCPManagementReceipt:
        higress, _ = self._require_mcp_dependencies()
        name = _recovery_string(operation.request, "name")
        workers = _recovery_workers(operation.request)
        await self._require_mcp_server(higress, name)
        existing = await higress.get_consumers(name)
        revoked = {_worker_consumer(worker) for worker in workers}
        consumers = await self._converge_consumers(
            operation.operation_id,
            name,
            observed=existing,
            intended={"manager", *(existing - revoked)},
            higress=higress,
        )
        await self._remove_worker_descriptor(
            operation.operation_id,
            name,
            workers,
        )
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="revoke",
            name=name,
            descriptor=higress.descriptor(name),
            consumers=consumers,
            workers=workers,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_mcp_delete(
        self,
        operation: OperationRecord,
    ) -> MCPManagementReceipt:
        higress, _ = self._require_mcp_dependencies()
        name = _recovery_string(operation.request, "name")
        manager = await self._agt.get_manager(self._manager_name)
        if manager is None:
            raise RecoveryError(
                f"manager/{self._manager_name} is not readable",
            )
        desired_manager = tuple(
            server
            for server in manager.mcp_servers
            if server.name != name
        )
        if desired_manager != manager.mcp_servers:
            await self._replace_manager_descriptors(
                operation.operation_id,
                desired_manager,
            )
        workers = await self._agt.list_workers()
        affected = tuple(
            worker.name
            for worker in workers
            if any(
                server.name == name
                for server in _worker_mcp_servers(worker)
            )
        )
        await self._remove_worker_descriptor(
            operation.operation_id,
            name,
            affected,
        )
        available = {
            server.name
            for server in await higress.list_mcp_servers()
        }
        if name in available:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                {"operation": "delete_mcp", "name": name},
            )
            try:
                await higress.delete_server(name)
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.HIGRESS,
                    exc,
                )
                raise
        receipt = MCPManagementReceipt(
            operation_id=operation.operation_id,
            action="delete",
            name=name,
            workers=affected,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _resume_service_publishing(
        self,
        operation: OperationRecord,
    ) -> ServicePublishingReceipt:
        action = _recovery_string(operation.request, "action")
        if action not in {"publish", "unpublish"}:
            raise RecoveryError("service recovery action is invalid")
        typed_action = cast(ServicePublishingAction, action)
        worker_name = _recovery_string(operation.request, "worker")
        ports = _recovery_ports(operation.request)
        if self._runtime_mode == "aliyun":
            return await self._unsupported_service_receipt(
                operation.operation_id,
                action=typed_action,
                worker=worker_name,
                ports=ports,
            )
        current = await self._require_worker(worker_name)
        if action == "publish":
            desired = tuple(
                sorted({*_desired_expose(current), *ports}),
            )
            removed: tuple[int, ...] = ()
        else:
            removed = ports
            desired = tuple(
                port
                for port in _desired_expose(current)
                if port not in set(removed)
            )
        observed = await self._replace_expose(
            operation.operation_id,
            worker_name,
            desired,
            current=current,
        )
        converged = await self._wait_for_service_state(
            operation.operation_id,
            worker_name,
            desired=desired,
            removed=removed,
            first=observed,
        )
        if action == "publish":
            route_map = _observed_routes(converged)
            routes = tuple(
                PublishedRoute(port=port, domain=route_map[port])
                for port in ports
            )
        else:
            routes = ()
        receipt = ServicePublishingReceipt(
            operation_id=operation.operation_id,
            action=typed_action,
            worker=worker_name,
            ports=ports,
            routes=routes,
            domains=tuple(route.domain for route in routes),
            phase=converged.phase or "Unknown",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _unsupported_service_receipt(
        self,
        operation_id: str,
        *,
        action: ServicePublishingAction,
        worker: str,
        ports: tuple[int, ...],
    ) -> ServicePublishingReceipt:
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "check_service_exposure_support",
                "runtime": "aliyun",
            },
        )
        receipt = ServicePublishingReceipt(
            operation_id=operation_id,
            action=action,
            worker=worker,
            ports=ports,
            supported=False,
            phase="Unsupported",
            message=(
                "the configured cloud gateway provider does not manage "
                "Worker exposed ports"
            ),
        )
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.CONTROLLER,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _require_worker(self, name: str) -> WorkerResource:
        worker = await self._agt.get_worker(name)
        if worker is None:
            raise ConflictError(f"worker/{name} is not readable")
        return worker

    async def _replace_expose(
        self,
        operation_id: str,
        worker: str,
        ports: tuple[int, ...],
        *,
        current: WorkerResource,
    ) -> WorkerResource:
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "replace_worker_expose",
                "worker": worker,
                "ports": list(ports),
                "public": True,
            },
        )
        if _desired_expose(current) == ports:
            observed = current
        else:
            try:
                observed = await self._agt.update_worker_expose(
                    worker,
                    ports,
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation_id,
                    ExternalEffect.CONTROLLER,
                    exc,
                )
                raise
        await self._supervisor.effect_acknowledged(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "replace_worker_expose",
                "worker": worker,
                "ports": list(ports),
                "phase": observed.phase,
            },
        )
        return observed

    async def _wait_for_service_state(
        self,
        operation_id: str,
        worker: str,
        *,
        desired: tuple[int, ...],
        removed: tuple[int, ...],
        first: WorkerResource,
    ) -> WorkerResource:
        observed = first
        for attempt in range(self._poll_attempts):
            phase = (observed.phase or "").casefold()
            if phase in {"failed", "error"}:
                await self._supervisor.effect_failed(
                    operation_id,
                    ExternalEffect.CONTROLLER,
                    "worker entered a failed phase",
                )
                raise ConflictError(
                    f"worker/{worker} entered {observed.phase}",
                )
            route_map = _observed_routes(observed)
            if (
                _desired_expose(observed) == desired
                and all(
                    port in route_map and route_map[port]
                    for port in desired
                )
                and all(port not in route_map for port in removed)
            ):
                return observed
            if attempt + 1 < self._poll_attempts:
                await self._wait()
                observed = await self._require_worker(worker)
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.CONTROLLER,
            "worker_exposed_ports_not_observed",
        )
        raise AmbiguousEffectError(
            f"worker/{worker} service exposure did not converge",
        )

    def _require_local_mcp(self) -> None:
        if self._runtime_mode == "aliyun":
            raise CloudMCPManagementUnsupported(
                "local Higress MCP administration is unavailable when "
                "AGENTTEAMS_RUNTIME=aliyun; use the cloud AI Gateway console",
            )

    def _require_mcp_dependencies(
        self,
        *,
        require_verifier: bool = False,
    ) -> tuple[HigressMCPPort, MCPVerificationPort | None]:
        if self._higress is None:
            raise MCPIntegrationUnavailable(
                "Higress MCP administration is not configured",
            )
        if require_verifier and self._mcp_verifier is None:
            raise MCPIntegrationUnavailable(
                "AgentScope MCP verification is not configured",
            )
        return self._higress, self._mcp_verifier

    @staticmethod
    async def _require_mcp_server(
        higress: HigressMCPPort,
        name: str,
    ) -> None:
        if not any(
            server.name == name
            for server in await higress.list_mcp_servers()
        ):
            raise RecoveryError(
                f"MCP server {name!r} is absent from Higress",
            )

    async def _replace_consumers(
        self,
        operation_id: str,
        name: str,
        consumers: set[str],
        *,
        higress: HigressMCPPort,
    ) -> frozenset[str]:
        complete = {"manager", *consumers}
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.HIGRESS,
            {
                "operation": "replace_mcp_consumers",
                "name": name,
                "consumers": sorted(complete),
            },
        )
        try:
            replaced = await higress.replace_consumers(name, complete)
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.HIGRESS,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation_id,
            ExternalEffect.HIGRESS,
            {
                "operation": "replace_mcp_consumers",
                "name": name,
                "consumers": sorted(replaced),
            },
        )
        return replaced

    async def _converge_consumers(
        self,
        operation_id: str,
        name: str,
        *,
        observed: frozenset[str] | set[str],
        intended: set[str],
        higress: HigressMCPPort,
    ) -> frozenset[str]:
        complete = frozenset({"manager", *intended})
        if frozenset(observed) == complete:
            return complete
        return await self._replace_consumers(
            operation_id,
            name,
            set(complete),
            higress=higress,
        )

    async def _install_descriptor(
        self,
        operation_id: str,
        descriptor: MCPServerDocument,
        *,
        workers: tuple[str, ...],
    ) -> None:
        manager = await self._agt.get_manager(self._manager_name)
        if manager is None:
            raise ConflictError(
                f"manager/{self._manager_name} is not readable",
            )
        manager_servers = _upsert_descriptor(
            manager.mcp_servers,
            descriptor,
        )
        if manager_servers != manager.mcp_servers:
            await self._replace_manager_descriptors(
                operation_id,
                manager_servers,
            )
        for worker_name in workers:
            worker = await self._agt.get_worker(worker_name)
            if worker is None:
                raise ConflictError(
                    f"worker/{worker_name} is not readable",
                )
            existing = _worker_mcp_servers(worker)
            desired = _upsert_descriptor(
                existing,
                descriptor,
            )
            if desired != existing:
                await self._replace_worker_descriptors(
                    operation_id,
                    worker_name,
                    desired,
                )

    async def _replace_manager_descriptors(
        self,
        operation_id: str,
        servers: tuple[MCPServerDocument, ...],
    ) -> None:
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "replace_manager_mcp_descriptors",
                "manager": self._manager_name,
                "servers": [
                    server.model_dump(mode="json")
                    for server in servers
                ],
            },
        )
        try:
            await self._agt.replace_manager_mcp_servers(
                self._manager_name,
                servers,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.CONTROLLER,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "replace_manager_mcp_descriptors",
                "manager": self._manager_name,
                "server_names": [
                    server.name
                    for server in servers
                ],
            },
        )

    async def _replace_worker_descriptors(
        self,
        operation_id: str,
        worker: str,
        servers: tuple[MCPServerDocument, ...],
    ) -> None:
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "replace_worker_mcp_descriptors",
                "worker": worker,
                "servers": [
                    server.model_dump(mode="json")
                    for server in servers
                ],
            },
        )
        try:
            await self._agt.replace_worker_mcp_servers(worker, servers)
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.CONTROLLER,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation_id,
            ExternalEffect.CONTROLLER,
            {
                "operation": "replace_worker_mcp_descriptors",
                "worker": worker,
                "server_names": [
                    server.name
                    for server in servers
                ],
            },
        )

    async def _remove_worker_descriptor(
        self,
        operation_id: str,
        name: str,
        workers: tuple[str, ...],
    ) -> None:
        for worker_name in workers:
            worker = await self._agt.get_worker(worker_name)
            if worker is None:
                raise ConflictError(
                    f"worker/{worker_name} is not readable",
                )
            existing = _worker_mcp_servers(worker)
            desired = tuple(
                server
                for server in existing
                if server.name != name
            )
            if desired != existing:
                await self._replace_worker_descriptors(
                    operation_id,
                    worker_name,
                    desired,
                )

    async def _runtime_revision_for_descriptor(
        self,
        operation_id: str,
        descriptor: MCPServerDocument,
    ) -> int:
        generation = getattr(self._registry, "current", None)
        document = getattr(generation, "document", None)
        if (
            document is not None
            and descriptor in document.mcp_servers
        ):
            return self._registry.revision
        return await self._wait_for_runtime_descriptor(
            operation_id,
            descriptor,
            baseline_revision=max(-1, self._registry.revision - 1),
        )

    async def _wait_for_runtime_descriptor(
        self,
        operation_id: str,
        descriptor: MCPServerDocument,
        *,
        baseline_revision: int,
    ) -> int:
        for _ in range(self._poll_attempts):
            await self._watcher.poll_once()
            generation = getattr(self._registry, "current", None)
            document = getattr(generation, "document", None)
            if (
                self._registry.revision > baseline_revision
                and document is not None
                and descriptor in document.mcp_servers
            ):
                return self._registry.revision
            await self._wait()
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.STORAGE,
            "runtime_mcp_descriptor_not_observed",
        )
        raise AmbiguousEffectError(
            "Controller MCP descriptor is not visible in a higher runtime "
            "document revision",
        )

    async def _wait_for_runtime_model(
        self,
        operation_id: str,
        model: str,
    ) -> int:
        for _ in range(self._poll_attempts):
            generation = getattr(self._registry, "current", None)
            document = getattr(generation, "document", None)
            if document is not None and document.model == model:
                return self._registry.revision
            await self._watcher.poll_once()
            generation = getattr(self._registry, "current", None)
            document = getattr(generation, "document", None)
            if document is not None and document.model == model:
                return self._registry.revision
            await self._wait()
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.STORAGE,
            "runtime_model_not_observed",
        )
        raise AmbiguousEffectError(
            "Controller model update is not visible in the runtime "
            "document",
        )

    async def _wait_for_new_runtime_revision(
        self,
        operation_id: str,
        *,
        baseline_revision: int,
    ) -> int:
        for _ in range(self._poll_attempts):
            if self._registry.revision > baseline_revision:
                return self._registry.revision
            await self._watcher.poll_once()
            if self._registry.revision > baseline_revision:
                return self._registry.revision
            await self._wait()
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.STORAGE,
            "runtime_identity_revision_not_observed",
        )
        raise AmbiguousEffectError(
            "Controller identity update is not visible in a higher runtime "
            "document revision",
        )

    async def _wait_for_worker_model(
        self,
        operation_id: str,
        worker: str,
        model: str,
        *,
        first: WorkerResource,
    ) -> WorkerResource:
        observed = first
        for attempt in range(self._poll_attempts):
            phase = (observed.phase or "").casefold()
            if phase in {"failed", "error"}:
                await self._supervisor.effect_failed(
                    operation_id,
                    ExternalEffect.CONTROLLER,
                    "worker entered a failed phase",
                )
                raise ConflictError(
                    f"worker/{worker} entered {observed.phase}",
                )
            if observed.model == model and phase:
                return observed
            if attempt + 1 < self._poll_attempts:
                await self._wait()
                observed = await self._require_worker(worker)
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.CONTROLLER,
            "worker_model_not_observed",
        )
        raise AmbiguousEffectError(
            f"worker/{worker} model did not converge",
        )

    async def _wait_for_verification_tool(
        self,
        operation_id: str,
        server_name: str,
        tool_name: str,
        *,
        revision: int,
        verifier: MCPVerificationPort,
    ) -> None:
        last_error = "tool_not_discovered"
        try:
            async with asyncio.timeout(self._mcp_propagation_timeout):
                for _ in range(self._poll_attempts):
                    try:
                        tools = await verifier.list_server_tools(
                            server_name,
                            revision=revision,
                        )
                    except Exception as exc:
                        last_error = type(exc).__name__
                    else:
                        matching = next(
                            (
                                tool
                                for tool in tools
                                if tool.name == tool_name
                            ),
                            None,
                        )
                        if matching is not None:
                            if not matching.is_read_only:
                                await self._supervisor.effect_failed(
                                    operation_id,
                                    ExternalEffect.PROCESS,
                                    "verification_tool_is_not_read_only",
                                )
                                raise MCPVerificationError(
                                    "MCP verification tool must be "
                                    "read-only",
                                )
                            return
                        last_error = "tool_not_discovered"
                    await self._wait()
        except TimeoutError:
            last_error = "mcp_propagation_timeout"
        await self._supervisor.effect_ambiguous(
            operation_id,
            ExternalEffect.PROCESS,
            last_error,
        )
        raise MCPVerificationError(
            "MCP tools did not become available within the bounded "
            "verification window",
        )

    async def _notify_mcp_workers(
        self,
        operation_id: str,
        name: str,
        workers: tuple[str, ...],
    ) -> None:
        if not workers:
            return
        if self._worker_notifications is None:
            raise MCPIntegrationUnavailable(
                "Worker MCP notifications are not configured",
            )
        for worker in workers:
            await self._supervisor.before_effect(
                operation_id,
                ExternalEffect.MATRIX,
                {
                    "operation": "notify_worker_mcp_ready",
                    "worker": worker,
                    "name": name,
                },
            )
            try:
                await self._worker_notifications.notify_worker(
                    worker,
                    f"MCP server {name} is verified and ready.",
                    source_operation_id=_notification_source_id(
                        operation_id,
                        worker,
                    ),
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation_id,
                    ExternalEffect.MATRIX,
                    exc,
                )
                raise
            await self._supervisor.effect_acknowledged(
                operation_id,
                ExternalEffect.MATRIX,
                {
                    "operation": "notify_worker_mcp_ready",
                    "worker": worker,
                    "name": name,
                },
            )

    async def _wait(self) -> None:
        value = self._sleep(self._poll_interval)
        if inspect.isawaitable(value):
            await value

    async def _record_external_failure(
        self,
        operation_id: str,
        effect: ExternalEffect,
        exc: Exception,
    ) -> None:
        if isinstance(
            exc,
            (
                TimeoutError,
                ConnectionError,
                BrokenPipeError,
                ProcessTimeout,
                HigressTransportError,
            ),
        ):
            await self._supervisor.effect_ambiguous(
                operation_id,
                effect,
                type(exc).__name__,
            )
        else:
            await self._supervisor.effect_failed(
                operation_id,
                effect,
                type(exc).__name__,
            )


def _model_receipt(
    *,
    operation_id: str,
    target: str,
    capabilities: ModelCapabilities,
    phase: str,
    runtime_revision: int | None = None,
) -> ModelSwitchReceipt:
    return ModelSwitchReceipt(
        operation_id=operation_id,
        target=target,
        model=capabilities.model,
        context_window=capabilities.context_window,
        max_tokens=capabilities.max_tokens,
        reasoning=capabilities.reasoning,
        input_modalities=capabilities.input_modalities,
        phase=phase,
        runtime_revision=runtime_revision,
    )


def _render_manager_identity(request: ManagerIdentityRequest) -> str:
    lines = [
        f"- Name: {request.name}",
        f"- Default language: {request.default_language}",
        f"- Communication style: {request.communication_style}",
        "- Behavior guidelines:",
    ]
    if request.behavior_guidelines:
        lines.extend(
            f"  - {guideline}"
            for guideline in request.behavior_guidelines
        )
    else:
        lines.append("  - Follow the Manager operating contract.")
    return "\n".join(lines)


def _identity_receipt(
    *,
    operation_id: str,
    manager: str,
    request: ManagerIdentityRequest,
    phase: str,
    runtime_revision: int,
) -> ManagerIdentityReceipt:
    return ManagerIdentityReceipt(
        operation_id=operation_id,
        manager=manager,
        name=request.name,
        communication_style=request.communication_style,
        behavior_guidelines=request.behavior_guidelines,
        default_language=request.default_language,
        phase=phase,
        runtime_revision=runtime_revision,
    )


def _safe_configuration_request(
    request: MCPConfiguration,
    *,
    baseline_revision: int,
) -> dict[str, object]:
    server = request.server
    common: dict[str, object] = {
        "action": "configure",
        "name": server.name,
        "kind": "rest" if isinstance(server, RestMCPRequest) else "proxy",
        "workers": list(request.workers),
        "verification_tool": request.verification_tool,
        "verification_arguments": TypeAdapter(
            dict[str, JsonValue],
        ).dump_python(
            request.verification_arguments,
            mode="json",
        ),
        "verification_argument_names": sorted(
            request.verification_arguments,
        ),
        "baseline_revision": baseline_revision,
    }
    if isinstance(server, RestMCPRequest):
        common["service"] = server.service.model_dump(mode="json")
    else:
        parsed_url = urlsplit(server.backend_url)
        backend_origin = (
            f"{parsed_url.scheme}://{parsed_url.hostname}"
            + (f":{parsed_url.port}" if parsed_url.port is not None else "")
        )
        common.update(
            {
                "backend_origin": backend_origin,
                "transport": server.transport,
                "header_names": [
                    header.name
                    for header in server.headers
                ],
            },
        )
    return common


def _validate_safe_verification_arguments(
    value: dict[str, JsonValue],
) -> None:
    forbidden = (
        "apikey",
        "authorization",
        "credential",
        "password",
        "secret",
        "token",
    )

    def inspect_value(item: JsonValue) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                normalized = "".join(
                    character
                    for character in key.casefold()
                    if character.isalnum()
                )
                if any(term in normalized for term in forbidden):
                    raise ValueError(
                        "verification arguments must not contain "
                        "credential-like fields",
                    )
                inspect_value(child)
        elif isinstance(item, list):
            for child in item:
                inspect_value(child)

    inspect_value(value)


def _notification_source_id(
    operation_id: str,
    worker: str,
) -> str:
    material = f"{operation_id}\0mcp-ready\0{worker}".encode()
    return hashlib.sha256(material).hexdigest()[:32]


def _recovery_string(
    request: dict[str, object],
    key: str,
) -> str:
    value = request.get(key)
    if not isinstance(value, str) or not value:
        raise RecoveryError(
            f"integration recovery request has no valid {key}",
        )
    return value


def _recovery_revision(request: dict[str, object]) -> int:
    value = request.get("baseline_revision")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RecoveryError(
            "integration recovery request has no valid baseline_revision",
        )
    return value


def _recovery_identity_request(
    request: dict[str, object],
) -> ManagerIdentityRequest:
    try:
        return ManagerIdentityRequest.model_validate(
            {
                "name": request.get("name"),
                "communication_style": request.get(
                    "communication_style",
                ),
                "behavior_guidelines": request.get(
                    "behavior_guidelines",
                ),
                "default_language": request.get("default_language"),
            },
        )
    except Exception as exc:
        raise RecoveryError(
            "Manager identity recovery request is invalid",
        ) from exc


def _recovery_workers(
    request: dict[str, object],
) -> tuple[str, ...]:
    raw = request.get("workers", [])
    if (
        not isinstance(raw, list)
        or any(not isinstance(worker, str) for worker in raw)
    ):
        raise RecoveryError(
            "MCP recovery request has invalid Worker names",
        )
    workers = tuple(raw)
    try:
        _validate_worker_names(workers)
    except ValueError as exc:
        raise RecoveryError(
            "MCP recovery request has invalid Worker names",
        ) from exc
    return workers


def _recovery_verification_arguments(
    request: dict[str, object],
) -> dict[str, JsonValue]:
    raw = request.get("verification_arguments")
    try:
        arguments = TypeAdapter(
            dict[str, JsonValue],
        ).validate_python(raw)
        _validate_safe_verification_arguments(arguments)
    except (TypeError, ValueError) as exc:
        raise RecoveryError(
            "MCP recovery request has invalid verification arguments",
        ) from exc
    return arguments


def _recovery_ports(
    request: dict[str, object],
) -> tuple[int, ...]:
    raw = request.get("ports")
    if not isinstance(raw, list):
        raise RecoveryError(
            "service recovery request has invalid ports",
        )
    try:
        return _validated_ports(tuple(raw))
    except (TypeError, ValueError) as exc:
        raise RecoveryError(
            "service recovery request has invalid ports",
        ) from exc


def _worker_consumer(worker: str) -> str:
    _validate_worker_names((worker,))
    return f"worker-{worker}"


def _validate_worker_names(workers: tuple[str, ...]) -> None:
    if len(workers) != len(set(workers)):
        raise ValueError("MCP Worker names must be unique")
    for worker in workers:
        if (
            not worker
            or not worker[0].isalnum()
            or not all(
                character.islower()
                or character.isdigit()
                or character == "-"
                for character in worker
            )
        ):
            raise ValueError("invalid MCP Worker name")


def _upsert_descriptor(
    servers: tuple[MCPServerDocument, ...],
    descriptor: MCPServerDocument,
) -> tuple[MCPServerDocument, ...]:
    by_name = {
        server.name: server
        for server in servers
    }
    by_name[descriptor.name] = descriptor
    return tuple(
        by_name[name]
        for name in sorted(by_name)
    )


def _worker_mcp_servers(
    worker: WorkerResource,
) -> tuple[MCPServerDocument, ...]:
    raw = worker.spec.get("mcpServers", [])
    if not isinstance(raw, list):
        raise ConflictError(
            f"worker/{worker.name} has invalid MCP descriptor state",
        )
    try:
        return tuple(
            MCPServerDocument.model_validate(item)
            for item in raw
        )
    except Exception:
        raise ConflictError(
            f"worker/{worker.name} has invalid MCP descriptor state",
        ) from None


def _validated_ports(ports: tuple[int, ...]) -> tuple[int, ...]:
    if not ports:
        raise ValueError("at least one service port is required")
    if len(ports) != len(set(ports)):
        raise ValueError("service ports must be unique")
    if any(
        isinstance(port, bool)
        or not isinstance(port, int)
        or port < 1
        or port > 65535
        for port in ports
    ):
        raise ValueError("service ports must be integers from 1 to 65535")
    return tuple(sorted(ports))


def _desired_expose(worker: WorkerResource) -> tuple[int, ...]:
    raw = worker.spec.get("expose", [])
    if not isinstance(raw, list):
        raise ConflictError(
            f"worker/{worker.name} has invalid expose desired state",
        )
    try:
        ports = tuple(int(port) for port in raw)
    except (TypeError, ValueError):
        raise ConflictError(
            f"worker/{worker.name} has invalid expose desired state",
        ) from None
    if any(port < 1 or port > 65535 for port in ports):
        raise ConflictError(
            f"worker/{worker.name} has invalid expose desired state",
        )
    return tuple(sorted(set(ports)))


def _observed_routes(worker: WorkerResource) -> dict[int, str]:
    raw = worker.status.get("exposedPorts", [])
    if not isinstance(raw, list):
        raise ConflictError(
            f"worker/{worker.name} has invalid exposed port status",
        )
    routes: dict[int, str] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise ConflictError(
                f"worker/{worker.name} has invalid exposed port status",
            )
        port = item.get("port")
        domain = item.get("domain")
        if (
            isinstance(port, bool)
            or not isinstance(port, int)
            or port < 1
            or port > 65535
            or not isinstance(domain, str)
            or not domain
            or port in routes
        ):
            raise ConflictError(
                f"worker/{worker.name} has invalid exposed port status",
            )
        routes[port] = domain
    return routes
