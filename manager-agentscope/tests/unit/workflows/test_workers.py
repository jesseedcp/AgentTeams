from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.clients.agt import (
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceHeartbeat,
    ResourceService,
)


class Supervisor:
    def __init__(self) -> None:
        self.statuses: dict[str, OperationStatus] = {}
        self.records: dict[str, SimpleNamespace] = {}
        self.last_operation_id = ""
        self.calls: list[tuple[str, object]] = []

    @property
    def status(self) -> OperationStatus:
        return self.statuses[self.last_operation_id]

    async def begin(self, **kwargs: object) -> object:
        self.calls.append(("begin", kwargs))
        operation_id = str(kwargs["operation_id"])
        self.last_operation_id = operation_id
        status = self.statuses.setdefault(
            operation_id,
            OperationStatus.PLANNED,
        )
        record = self.records.get(operation_id)
        if record is None:
            record = SimpleNamespace(
                operation_id=operation_id,
                kind=kwargs["kind"],
                target_key=kwargs["target_key"],
                request=kwargs["request"],
                status=status,
            )
            self.records[operation_id] = record
        record.status = status
        return record

    async def get(self, operation_id: str) -> object | None:
        record = self.records.get(operation_id)
        if record is not None:
            record.status = self.statuses[operation_id]
        return record

    async def before_effect(self, *args: object) -> object:
        self.calls.append(("before", args))
        self.statuses[str(args[0])] = OperationStatus.DISPATCHED
        return SimpleNamespace(sequence=len(self.calls))

    async def effect_acknowledged(self, *args: object) -> object:
        self.calls.append(("acknowledged", args))
        self.statuses[str(args[0])] = OperationStatus.RUNNING
        return SimpleNamespace(status=OperationStatus.RUNNING)

    async def effect_succeeded(self, *args: object) -> object:
        self.calls.append(("succeeded", args))
        self.statuses[str(args[0])] = OperationStatus.SUCCEEDED
        return SimpleNamespace(status=OperationStatus.SUCCEEDED)

    async def effect_ambiguous(self, *args: object) -> object:
        self.calls.append(("ambiguous", args))
        self.statuses[str(args[0])] = OperationStatus.RECONCILING
        return SimpleNamespace(status=OperationStatus.RECONCILING)

    async def effect_failed(self, *args: object) -> object:
        self.calls.append(("failed", args))
        self.statuses[str(args[0])] = OperationStatus.FAILED
        return SimpleNamespace(status=OperationStatus.FAILED)


class Controller:
    def __init__(self) -> None:
        self.workers: dict[str, WorkerResource] = {}
        self.get_sequences: dict[
            str,
            list[WorkerResource | None],
        ] = {}
        self.create_calls: list[WorkerCreateRequest] = []
        self.update_calls: list[WorkerUpdateRequest] = []
        self.lifecycle_calls: list[tuple[str, str]] = []
        self.delete_calls: list[str] = []

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource | None:
        self.create_calls.append(request)
        return WorkerResource(
            name=request.name,
            runtime=request.runtime,
            model=request.model,
            phase="Pending",
        )

    async def get_worker(self, name: str) -> WorkerResource | None:
        sequence = self.get_sequences.get(name)
        if sequence:
            return sequence.pop(0)
        return self.workers.get(name)

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        return tuple(self.workers.values())

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
    ) -> WorkerResource:
        self.update_calls.append(request)
        current = self.workers[request.name]
        spec = dict(current.spec)
        if request.console_enabled is not None:
            spec["console"] = {
                "enabled": request.console_enabled,
                "port": request.console_port or 8088,
            }
        elif request.console_port is not None:
            spec["console"] = {
                "enabled": True,
                "port": request.console_port,
            }
        changed = current.model_copy(
            update={
                "model": request.model or current.model,
                "runtime": request.runtime or current.runtime,
                "spec": spec,
            },
        )
        self.workers[request.name] = changed
        return changed

    async def sleep_worker(self, name: str) -> WorkerResource:
        self.lifecycle_calls.append(("sleep", name))
        changed = self.workers[name].model_copy(
            update={"status": {"containerState": "stopped"}},
        )
        self.workers[name] = changed
        return changed

    async def wake_worker(self, name: str) -> WorkerResource:
        self.lifecycle_calls.append(("wake", name))
        changed = self.workers[name].model_copy(
            update={"status": {"containerState": "running"}},
        )
        self.workers[name] = changed
        return changed

    async def delete_worker(self, name: str) -> None:
        self.delete_calls.append(name)
        self.workers.pop(name, None)


class Matrix:
    def __init__(self) -> None:
        self.sent: list[SimpleNamespace] = []

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        self.sent.append(
            SimpleNamespace(
                room_id=room_id,
                text=text,
                txn_id=txn_id,
                thread_id=thread_id,
                mentions=mentions,
            ),
        )
        return "$greeting"


class Topology:
    def __init__(self) -> None:
        self.refreshes = 0

    async def refresh(self) -> object:
        self.refreshes += 1
        return object()


def context(call: str = "call-1") -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id=call,
    )


def service(
    controller: Controller,
) -> tuple[ResourceService, Supervisor, Matrix, Topology]:
    supervisor = Supervisor()
    matrix = Matrix()
    topology = Topology()
    return (
        ResourceService(
            controller=controller,
            supervisor=supervisor,
            topology=topology,
            matrix=matrix,
            sleeper=_no_sleep,
            worker_poll_delays=(0, 0, 0),
        ),
        supervisor,
        matrix,
        topology,
    )


async def _no_sleep(delay: float) -> None:
    del delay


@pytest.mark.asyncio
async def test_create_worker_waits_for_room_then_greets() -> None:
    controller = Controller()
    controller.get_sequences["alice"] = [
        None,
        WorkerResource(
            name="alice",
            runtime="copaw",
            model="qwen3.6-plus",
            phase="Pending",
        ),
        WorkerResource(
            name="alice",
            runtime="copaw",
            model="qwen3.6-plus",
            phase="Running",
            room_id="!alice:example",
        ),
    ]
    workflow, supervisor, matrix, topology = service(controller)

    worker = await workflow.create_worker(
        WorkerCreateRequest(
            name="alice",
            runtime="copaw",
            model="qwen3.6-plus",
        ),
        context=context(),
    )

    assert worker.room_id == "!alice:example"
    assert len(controller.create_calls) == 1
    assert topology.refreshes == 1
    assert matrix.sent[-1].room_id == "!alice:example"
    assert matrix.sent[-1].txn_id.startswith("agentteams:")
    assert supervisor.status is OperationStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_all_four_worker_runtimes_are_accepted() -> None:
    accepted = {
        "openclaw",
        "copaw",
        "hermes",
        "qwenpaw",
    }
    for index, runtime in enumerate(sorted(accepted)):
        controller = Controller()
        name = f"worker-{runtime}"
        controller.get_sequences[name] = [
            None,
                WorkerResource(
                    name=name,
                    runtime=runtime,
                    model="qwen3.6-plus",
                    phase="Running",
                room_id=f"!{runtime}:example",
            ),
        ]
        workflow, _, _, _ = service(controller)

        worker = await workflow.create_worker(
            WorkerCreateRequest(
                name=name,
                runtime=runtime,
                model="qwen3.6-plus",
            ),
            context=context(f"call-{index}"),
        )

        assert worker.runtime == runtime


@pytest.mark.asyncio
async def test_update_sleep_wake_and_delete_prove_controller_state() -> None:
    controller = Controller()
    controller.workers["alice"] = WorkerResource(
        name="alice",
        runtime="copaw",
        model="old-model",
        phase="Running",
        room_id="!alice:example",
        status={"containerState": "running"},
    )
    workflow, _, _, topology = service(controller)

    updated = await workflow.update_worker(
        WorkerUpdateRequest(name="alice", model="new-model"),
        context=context("update"),
    )
    sleeping = await workflow.sleep_worker(
        "alice",
        context=context("sleep"),
    )
    awake = await workflow.wake_worker(
        "alice",
        context=context("wake"),
    )
    await workflow.delete_worker(
        "alice",
        context=context("delete"),
    )

    assert updated.model == "new-model"
    assert sleeping.status["containerState"] == "stopped"
    assert awake.status["containerState"] == "running"
    assert controller.lifecycle_calls == [
        ("sleep", "alice"),
        ("wake", "alice"),
    ]
    assert controller.delete_calls == ["alice"]
    assert await controller.get_worker("alice") is None
    assert topology.refreshes == 4


@pytest.mark.asyncio
async def test_delete_worker_retry_succeeds_after_worker_disappears() -> None:
    controller = Controller()
    controller.workers["alice"] = WorkerResource(
        name="alice",
        runtime="qwenpaw",
        model="qwen3.6-plus",
        phase="Running",
        room_id="!alice:example",
    )
    workflow, _, _, topology = service(controller)
    mutation = context("delete-retry")

    await workflow.delete_worker("alice", context=mutation)
    await workflow.delete_worker("alice", context=mutation)

    assert controller.delete_calls == ["alice"]
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_update_worker_console_proves_controller_state() -> None:
    controller = Controller()
    controller.workers["alice"] = WorkerResource(
        name="alice",
        runtime="copaw",
        model="qwen3.6-plus",
        phase="Running",
        room_id="!alice:example",
        spec={"console": {"enabled": False, "port": 8088}},
    )
    workflow, _, _, _ = service(controller)

    updated = await workflow.update_worker(
        WorkerUpdateRequest(
            name="alice",
            console_enabled=True,
            console_port=9090,
        ),
        context=context("console"),
    )

    assert updated.spec["console"] == {"enabled": True, "port": 9090}
    assert controller.update_calls == [
        WorkerUpdateRequest(
            name="alice",
            console_enabled=True,
            console_port=9090,
        ),
    ]


@pytest.mark.asyncio
async def test_resource_heartbeat_resumes_without_repeating_create() -> None:
    controller = Controller()
    controller.workers["alice"] = WorkerResource(
        name="alice",
        runtime="hermes",
        model="qwen3.6-plus",
        phase="Running",
        room_id="!alice:example",
    )
    workflow, _, matrix, _ = service(controller)
    now = datetime.now(UTC)
    operation = OperationRecord(
        operation_id="c" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        status=OperationStatus.RECONCILING,
        request=WorkerCreateRequest(
            name="alice",
            runtime="hermes",
            model="qwen3.6-plus",
        ).model_dump(mode="json"),
        created_at=now,
        updated_at=now,
    )

    class Operations:
        async def list_recoverable(
            self,
        ) -> tuple[OperationRecord, ...]:
            return (operation,)

    report = await ResourceHeartbeat(
        operations=Operations(),
        resources=workflow,
    ).reconcile_pending_workers()

    assert report.reconciled == ("c" * 32,)
    assert not report.pending
    assert controller.create_calls == []
    assert matrix.sent[0].txn_id == f"agentteams:{'c' * 32}:0"
