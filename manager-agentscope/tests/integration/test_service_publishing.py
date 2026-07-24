from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.clients.agt import AgtClient
from agentteams_manager.workflows.integrations import IntegrationService
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.fake_agt import FakeProcess
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 24, 11, tzinfo=UTC)


class Gateway:
    async def preflight(self, request):
        raise AssertionError(f"unexpected model preflight: {request}")


class Registry:
    revision = 1


class Watcher:
    async def poll_once(self):
        return None


def _worker(
    *,
    expose: tuple[int, ...],
    observed: dict[int, str],
) -> dict[str, object]:
    return {
        "name": "alice",
        "phase": "Running",
        "model": "qwen",
        "runtime": "qwenpaw",
        "expose": [{"port": port} for port in expose],
        "exposedPorts": [
            {"port": port, "domain": domain}
            for port, domain in observed.items()
        ],
    }


@pytest.mark.asyncio
async def test_publish_crosses_agt_boundary_and_waits_for_status_domain() -> None:
    process = FakeProcess()
    process.queue_json(_worker(expose=(), observed={}))
    process.queue_error("", returncode=0)
    process.queue_json(_worker(expose=(8080,), observed={}))
    process.queue_json(
        _worker(
            expose=(8080,),
            observed={8080: "route-issued-by-controller.example"},
        ),
    )
    service = IntegrationService(
        agt=AgtClient(process),
        gateway=Gateway(),
        supervisor=TaskSupervisor(Clock()),
        clock=Clock(),
        manager_name="manager",
        registry=Registry(),
        watcher=Watcher(),
        sleep=lambda _: None,
    )

    receipt = await service.publish_service(
        worker="alice",
        ports=(8080,),
        context=MutationContext(
            room_id="!admin:example",
            event_id="$publish",
            tool_call_id="publish-service",
        ),
    )

    assert receipt.domains == ("route-issued-by-controller.example",)
    assert process.calls[1][0] == (
        "agt",
        "update",
        "worker",
        "--name",
        "alice",
        "--expose",
        "8080",
    )
