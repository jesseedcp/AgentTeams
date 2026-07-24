"""Cross-system model, MCP, and service integration workflows."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.clients.agt import (
    AgtClient,
    WorkerUpdateRequest,
)
from agentteams_manager.clients.model_gateway import (
    ModelCapabilities,
    ModelGatewayClient,
    ModelSpec,
)
from agentteams_manager.clients.process import ProcessTimeout
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
)
from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
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


class RuntimeWatcherPort(Protocol):
    async def poll_once(self) -> object | None: ...


Sleep = Callable[[float], None | Awaitable[None]]


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
    ) -> None:
        if not manager_name:
            raise ValueError("manager_name must not be empty")
        if poll_attempts < 1 or poll_interval < 0:
            raise ValueError("invalid integration polling bounds")
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
