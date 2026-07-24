from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import WorkerResource
from agentteams_manager.workflows.integrations import (
    IntegrationService,
    ServicePublishingReceipt,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 24, 10, tzinfo=UTC)


class Gateway:
    async def preflight(self, request):
        raise AssertionError(f"unexpected model preflight: {request}")


def _worker(
    *,
    expose: tuple[int, ...],
    observed: dict[int, str],
    phase: str = "Running",
) -> WorkerResource:
    return WorkerResource(
        name="alice",
        runtime="qwenpaw",
        phase=phase,
        spec={"expose": list(expose), "mcpServers": []},
        status={
            "exposedPorts": [
                {"port": port, "domain": domain}
                for port, domain in observed.items()
            ],
        },
    )


class Agt:
    def __init__(
        self,
        *,
        initial: WorkerResource,
        converged: WorkerResource,
    ) -> None:
        self.current = initial
        self.converged = converged
        self.updates: list[tuple[int, ...]] = []
        self.reads = 0

    async def get_worker(self, name: str):
        assert name == "alice"
        self.reads += 1
        if self.updates:
            self.current = self.converged
        return self.current

    async def update_worker_expose(
        self,
        name: str,
        ports: tuple[int, ...],
    ):
        assert name == "alice"
        self.updates.append(ports)
        self.current = self.current.model_copy(
            update={
                "phase": "Updating",
                "spec": {
                    **self.current.spec,
                    "expose": list(ports),
                },
            },
        )
        return self.current


class Registry:
    revision = 1


class Watcher:
    async def poll_once(self):
        return None


def _context(suffix: str) -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id=f"${suffix}",
        tool_call_id=f"{suffix}-service",
    )


def _service(
    agt: Agt,
    *,
    runtime_mode: str = "local",
) -> IntegrationService:
    return IntegrationService(
        agt=agt,
        gateway=Gateway(),
        supervisor=TaskSupervisor(Clock()),
        clock=Clock(),
        manager_name="manager",
        registry=Registry(),
        watcher=Watcher(),
        sleep=lambda _: None,
        runtime_mode=runtime_mode,
    )


@pytest.mark.asyncio
async def test_publish_returns_only_controller_observed_domain() -> None:
    agt = Agt(
        initial=_worker(
            expose=(9000,),
            observed={9000: "existing.example"},
        ),
        converged=_worker(
            expose=(8080, 9000),
            observed={
                8080: "controller-reported.example",
                9000: "existing.example",
            },
        ),
    )

    receipt = await _service(agt).publish_service(
        worker="alice",
        ports=(8080,),
        context=_context("publish"),
    )

    assert isinstance(receipt, ServicePublishingReceipt)
    assert receipt.supported is True
    assert receipt.public is True
    assert receipt.domains == ("controller-reported.example",)
    assert receipt.routes[0].port == 8080
    assert receipt.routes[0].domain == "controller-reported.example"
    assert agt.updates == [(8080, 9000)]


@pytest.mark.asyncio
async def test_unpublish_uses_complete_remainder_and_observes_removal() -> None:
    agt = Agt(
        initial=_worker(
            expose=(8080, 9000),
            observed={8080: "remove.example", 9000: "keep.example"},
        ),
        converged=_worker(
            expose=(9000,),
            observed={9000: "keep.example"},
        ),
    )

    receipt = await _service(agt).unpublish_service(
        worker="alice",
        ports=(8080,),
        context=_context("unpublish"),
    )

    assert receipt.action == "unpublish"
    assert receipt.ports == (8080,)
    assert receipt.domains == ()
    assert agt.updates == [(9000,)]


@pytest.mark.asyncio
async def test_clear_last_service_sends_explicit_empty_desired_set() -> None:
    agt = Agt(
        initial=_worker(
            expose=(8080,),
            observed={8080: "remove.example"},
        ),
        converged=_worker(expose=(), observed={}),
    )

    await _service(agt).unpublish_service(
        worker="alice",
        ports=(8080,),
        context=_context("clear"),
    )

    assert agt.updates == [()]


@pytest.mark.asyncio
async def test_cloud_service_publishing_returns_typed_unsupported_result() -> None:
    agt = Agt(
        initial=_worker(expose=(), observed={}),
        converged=_worker(expose=(), observed={}),
    )

    receipt = await _service(
        agt,
        runtime_mode="aliyun",
    ).publish_service(
        worker="alice",
        ports=(8080,),
        context=_context("cloud"),
    )

    assert receipt.supported is False
    assert receipt.domains == ()
    assert agt.updates == []


@pytest.mark.parametrize("ports", ((0,), (65536,), (8080, 8080)))
@pytest.mark.asyncio
async def test_publish_validates_ports_before_controller_mutation(
    ports: tuple[int, ...],
) -> None:
    agt = Agt(
        initial=_worker(expose=(), observed={}),
        converged=_worker(expose=(), observed={}),
    )

    with pytest.raises(ValueError):
        await _service(agt).publish_service(
            worker="alice",
            ports=ports,
            context=_context("invalid"),
        )

    assert agt.updates == []
