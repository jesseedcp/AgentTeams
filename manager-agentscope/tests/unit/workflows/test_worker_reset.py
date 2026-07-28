from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import WorkerResource
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceService,
)
from tests.fixtures.task_workflow import FixedClock, TaskSupervisor


class Controller:
    def __init__(self) -> None:
        self.worker = WorkerResource(
            name="alice",
            runtime="qwenpaw",
            model="qwen3.6-plus",
            phase="Running",
            room_id="!alice:local",
            matrix_user_id="@alice:local",
            skills=("python",),
            spec={
                "image": "worker:qwenpaw",
                "identity": "Alice",
                "soul": "Be precise.",
                "package": "nacos://workers/alice@v1",
                "expose": [8080],
                "console": {"enabled": True, "port": 9090},
            },
            status={"containerState": "running"},
        )
        self.deleted = 0
        self.created = []

    async def get_worker(self, name):
        return self.worker if self.worker and self.worker.name == name else None

    async def delete_worker(self, name):
        assert self.worker is not None and self.worker.name == name
        self.deleted += 1
        self.worker = None

    async def create_worker(self, request):
        self.created.append(request)
        self.worker = WorkerResource(
            name=request.name,
            runtime=request.runtime,
            model=request.model,
            phase="Running",
            room_id="!alice:local",
            matrix_user_id="@alice:local",
            skills=request.skills,
            spec={
                "image": request.image or "",
                "identity": request.identity or "",
                "soul": request.soul or "",
                "package": request.package_uri or "",
                "expose": list(request.expose),
                "console": {
                    "enabled": request.console_enabled,
                    "port": request.console_port,
                },
            },
            status={"containerState": "running"},
        )
        return self.worker


class Topology:
    def __init__(self) -> None:
        self.refreshed = 0

    async def refresh(self):
        self.refreshed += 1


class Matrix:
    async def send_text(self, *args, **kwargs):
        del args, kwargs
        return "$event"


@pytest.mark.asyncio
async def test_reset_worker_recreates_exact_desired_configuration_once() -> None:
    controller = Controller()
    topology = Topology()
    supervisor = TaskSupervisor(FixedClock())
    service = ResourceService(
        controller=controller,
        supervisor=supervisor,
        topology=topology,
        matrix=Matrix(),
        worker_poll_delays=(0,),
    )
    context = MutationContext(
        room_id="!admin:local",
        event_id="$reset",
        tool_call_id="reset-worker",
    )

    first = await service.reset_worker("alice", context=context)
    second = await service.reset_worker("alice", context=context)

    assert first == second
    assert controller.deleted == 1
    assert len(controller.created) == 1
    request = controller.created[0]
    assert request.runtime == "qwenpaw"
    assert request.model == "qwen3.6-plus"
    assert request.image == "worker:qwenpaw"
    assert request.skills == ("python",)
    assert request.package_uri == "nacos://workers/alice@v1"
    assert request.expose == (8080,)
    assert request.console_enabled is True
    assert request.console_port == 9090
    assert topology.refreshed == 1
