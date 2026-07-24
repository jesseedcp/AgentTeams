from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.clients.agt import ManagerResource
from agentteams_manager.clients.model_gateway import (
    ModelCapabilities,
    ModelNotReachable,
)
from agentteams_manager.domain.models import WorkerResource
from agentteams_manager.workflows.integrations import (
    IntegrationService,
    ModelSwitchRequest,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 23, 12, tzinfo=UTC)


class Gateway:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def preflight(self, request):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return ModelCapabilities(
            model=request.model,
            context_window=150_000,
            max_tokens=128_000,
            reasoning=True,
        )


class Agt:
    def __init__(self) -> None:
        self.manager_updates = 0
        self.worker_updates = 0
        self.worker_reads = 0

    async def update_manager_model(self, name: str, model: str):
        self.manager_updates += 1
        return ManagerResource(
            name=name,
            phase="Running",
            model=model,
            runtime="qwenpaw",
        )

    async def update_worker(self, request):
        self.worker_updates += 1
        return WorkerResource(
            name=request.name,
            runtime="qwenpaw",
            phase="Updating",
            model="old",
        )

    async def get_worker(self, name: str):
        self.worker_reads += 1
        model = "old" if self.worker_reads == 1 else "new"
        return WorkerResource(
            name=name,
            runtime="qwenpaw",
            phase="Running",
            model=model,
        )


class Registry:
    revision = 1


class Watcher:
    async def poll_once(self):
        return None


def _context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$model",
        tool_call_id="switch",
    )


@pytest.mark.asyncio
async def test_unreachable_model_never_updates_controller() -> None:
    agt = Agt()
    service = IntegrationService(
        agt=agt,
        gateway=Gateway(ModelNotReachable("unreachable")),
        supervisor=TaskSupervisor(Clock()),
        clock=Clock(),
        manager_name="manager",
        registry=Registry(),
        watcher=Watcher(),
        sleep=lambda _: None,
    )

    with pytest.raises(ModelNotReachable):
        await service.switch_manager_model(
            ModelSwitchRequest(model="missing"),
            context=_context(),
        )

    assert agt.manager_updates == 0


@pytest.mark.asyncio
async def test_worker_switch_waits_for_observed_model() -> None:
    agt = Agt()
    service = IntegrationService(
        agt=agt,
        gateway=Gateway(),
        supervisor=TaskSupervisor(Clock()),
        clock=Clock(),
        manager_name="manager",
        registry=Registry(),
        watcher=Watcher(),
        sleep=lambda _: None,
    )

    receipt = await service.switch_worker_model(
        worker="alice",
        request=ModelSwitchRequest(model="new"),
        context=_context(),
    )

    assert receipt.model == "new"
    assert receipt.target == "worker/alice"
    assert agt.worker_updates == 1
    assert agt.worker_reads == 2


@pytest.mark.asyncio
async def test_manager_model_recovery_uses_persisted_preflight() -> None:
    class RecoveringAgt:
        def __init__(self) -> None:
            self.manager = ManagerResource(
                name="manager",
                phase="Running",
                model="old",
                runtime="agentscope",
            )
            self.updates = 0

        async def get_manager(self, name: str):
            assert name == "manager"
            return self.manager

        async def update_manager_model(self, name: str, model: str):
            self.updates += 1
            self.manager = self.manager.model_copy(
                update={"model": model},
            )
            raise TimeoutError("lost Controller acknowledgement")

    class RecoveringRegistry:
        def __init__(self) -> None:
            self.revision = 1
            self.current = SimpleNamespace(
                document=SimpleNamespace(model="old"),
            )

    class RecoveringWatcher:
        def __init__(self, registry, agt) -> None:
            self.registry = registry
            self.agt = agt

        async def poll_once(self):
            self.registry.revision = 2
            self.registry.current = SimpleNamespace(
                document=SimpleNamespace(model=self.agt.manager.model),
            )

    agt = RecoveringAgt()
    gateway = Gateway()
    registry = RecoveringRegistry()
    supervisor = TaskSupervisor(Clock())
    service = IntegrationService(
        agt=agt,
        gateway=gateway,
        supervisor=supervisor,
        clock=Clock(),
        manager_name="manager",
        registry=registry,
        watcher=RecoveringWatcher(registry, agt),
        sleep=lambda _: None,
    )

    with pytest.raises(TimeoutError):
        await service.switch_manager_model(
            ModelSwitchRequest(model="new"),
            context=_context(),
        )
    operation = next(iter(supervisor.operations.values()))

    receipt = await service.resume_operation(operation)

    assert receipt.model == "new"
    assert receipt.runtime_revision == 2
    assert gateway.calls == 1
    assert agt.updates == 1
