from __future__ import annotations

from types import SimpleNamespace

import pytest

from agentteams_manager.clients.agt import (
    AgtCommandError,
    WorkerCreateRequest,
)
from agentteams_manager.clients.nacos import NacosWorker
from agentteams_manager.domain.models import (
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceService,
    WorkerImportError,
)


class Nacos:
    def __init__(self) -> None:
        self.candidate = NacosWorker(
            name="remote-coder",
            display_name="Remote Coder",
            description="Writes production code",
            runtime="hermes",
            package_uri=(
                "nacos://registry.example:8848/public/remote-coder/1.4.0"
            ),
            version="1.4.0",
            digest="sha256:" + ("a" * 64),
        )
        self.searches: list[str] = []
        self.inspected: list[str] = []
        self.verified: list[NacosWorker] = []

    async def search_workers(
        self,
        query: str,
    ) -> tuple[NacosWorker, ...]:
        self.searches.append(query)
        return (self.candidate,)

    async def verify_worker(self, candidate: NacosWorker) -> None:
        self.verified.append(candidate)

    async def inspect_worker_uri(self, package_uri: str) -> NacosWorker:
        self.inspected.append(package_uri)
        return self.candidate


class Controller:
    def __init__(self) -> None:
        self.workers: dict[str, WorkerResource] = {}
        self.apply_calls: list[tuple[str, str, str, str]] = []
        self.create_worker_calls = 0
        self.failure: Exception | None = None

    async def get_worker(self, name: str) -> WorkerResource | None:
        return self.workers.get(name)

    async def apply_worker_package(
        self,
        *,
        name: str,
        package_uri: str,
        expected_digest: str,
        runtime: str,
    ) -> WorkerResource:
        self.apply_calls.append(
            (name, package_uri, expected_digest, runtime),
        )
        if self.failure is not None:
            raise self.failure
        worker = WorkerResource(
            name=name,
            runtime=runtime,
            model="qwen3.6-plus",
            phase="Running",
            room_id=f"!{name}:example",
        )
        self.workers[name] = worker
        return worker

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource:
        del request
        self.create_worker_calls += 1
        raise AssertionError("generic Worker creation is forbidden")


class Supervisor:
    def __init__(self) -> None:
        self.statuses: dict[str, OperationStatus] = {}
        self.events: list[tuple[str, object]] = []

    async def begin(self, **kwargs: object) -> object:
        operation_id = str(kwargs["operation_id"])
        status = self.statuses.setdefault(
            operation_id,
            OperationStatus.PLANNED,
        )
        return SimpleNamespace(
            operation_id=operation_id,
            kind=kwargs["kind"],
            target_key=kwargs["target_key"],
            request=kwargs["request"],
            status=status,
        )

    async def before_effect(self, *args: object) -> object:
        self.events.append(("before", args))
        self.statuses[str(args[0])] = OperationStatus.DISPATCHED
        return SimpleNamespace()

    async def effect_acknowledged(self, *args: object) -> object:
        self.events.append(("acknowledged", args))
        self.statuses[str(args[0])] = OperationStatus.RUNNING
        return SimpleNamespace()

    async def effect_succeeded(self, *args: object) -> object:
        self.events.append(("succeeded", args))
        self.statuses[str(args[0])] = OperationStatus.SUCCEEDED
        return SimpleNamespace()

    async def effect_ambiguous(self, *args: object) -> object:
        self.events.append(("ambiguous", args))
        self.statuses[str(args[0])] = OperationStatus.RECONCILING
        return SimpleNamespace()

    async def effect_failed(self, *args: object) -> object:
        self.events.append(("failed", args))
        self.statuses[str(args[0])] = OperationStatus.FAILED
        return SimpleNamespace()


class Matrix:
    def __init__(self) -> None:
        self.sent = 0

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        del room_id, text, txn_id, thread_id, mentions
        self.sent += 1
        return "$greeting"


class Topology:
    async def refresh(self) -> object:
        return object()


def _context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id="find-worker",
    )


def _service(
    controller: Controller,
    nacos: Nacos,
) -> ResourceService:
    return ResourceService(
        controller=controller,
        supervisor=Supervisor(),
        topology=Topology(),
        matrix=Matrix(),
        nacos=nacos,
        confirmation_key=b"k" * 32,
        worker_poll_delays=(0,),
    )


@pytest.mark.asyncio
async def test_find_worker_requires_confirmation_before_import() -> None:
    controller = Controller()
    nacos = Nacos()
    service = _service(controller, nacos)

    result = await service.find_worker("coder")

    assert result.requires_confirmation
    assert result.candidates == (nacos.candidate,)
    assert controller.apply_calls == []
    assert controller.create_worker_calls == 0


@pytest.mark.asyncio
async def test_confirmed_candidate_is_bound_to_name_uri_and_digest() -> None:
    controller = Controller()
    nacos = Nacos()
    service = _service(controller, nacos)
    discovery = await service.find_worker("coder")
    confirmation = service.confirm_import(
        discovery,
        candidate_name="remote-coder",
        worker_name="alice",
    )

    worker = await service.import_worker(
        confirmation,
        context=_context(),
    )

    assert worker.name == "alice"
    assert controller.apply_calls == [
        (
            "alice",
            nacos.candidate.package_uri,
            nacos.candidate.digest,
            "hermes",
        ),
    ]
    assert nacos.verified == [nacos.candidate]
    assert controller.create_worker_calls == 0

    changed = confirmation.model_copy(
        update={"worker_name": "mallory"},
    )
    with pytest.raises(WorkerImportError, match="confirmation"):
        await service.import_worker(changed, context=_context())
    assert len(controller.apply_calls) == 1


@pytest.mark.asyncio
async def test_direct_uri_is_inspected_bound_and_verified_before_apply() -> None:
    controller = Controller()
    nacos = Nacos()
    service = _service(controller, nacos)

    confirmation = await service.confirm_direct_import(
        package_uri=nacos.candidate.package_uri,
        worker_name="alice",
    )
    worker = await service.import_worker(
        confirmation,
        context=_context(),
    )

    assert worker.name == "alice"
    assert nacos.searches == []
    assert nacos.inspected == [nacos.candidate.package_uri]
    assert nacos.verified == [nacos.candidate]
    assert controller.apply_calls == [
        (
            "alice",
            nacos.candidate.package_uri,
            nacos.candidate.digest,
            "hermes",
        ),
    ]


@pytest.mark.asyncio
async def test_failed_import_does_not_create_generic_worker() -> None:
    controller = Controller()
    controller.failure = AgtCommandError(
        "agt command failed (1): package signature invalid",
    )
    nacos = Nacos()
    service = _service(controller, nacos)
    discovery = await service.find_worker("coder")
    confirmation = service.confirm_import(
        discovery,
        candidate_name="remote-coder",
        worker_name="alice",
    )

    with pytest.raises(
        WorkerImportError,
        match="package signature invalid",
    ):
        await service.import_worker(
            confirmation,
            context=_context(),
        )

    assert controller.create_worker_calls == 0
    assert len(controller.apply_calls) == 1
