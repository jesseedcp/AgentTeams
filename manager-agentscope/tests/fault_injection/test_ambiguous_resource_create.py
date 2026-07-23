from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.clients.agt import WorkerCreateRequest
from agentteams_manager.clients.process import ProcessTimeout
from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationRecord,
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ReconcileDisposition,
    ResourceReconciler,
    ResourceService,
)


class LostCreateResultController:
    def __init__(self) -> None:
        self.create_calls = 1
        self.get_calls = 0

    async def get_worker(self, name: str) -> WorkerResource:
        self.get_calls += 1
        return WorkerResource(
            name=name,
            runtime="openhuman",
            phase="Running",
            room_id="!alice:example",
        )

    async def get_team(self, name: str) -> None:
        del name
        return None

    async def get_human(self, name: str) -> None:
        del name
        return None


@pytest.mark.asyncio
async def test_lost_create_result_queries_before_any_retry() -> None:
    now = datetime.now(UTC)
    operation = OperationRecord(
        operation_id="b" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        status=OperationStatus.RECONCILING,
        request={"name": "alice"},
        result={"ambiguous_reason": "process result lost"},
        created_at=now,
        updated_at=now,
    )
    controller = LostCreateResultController()

    result = await ResourceReconciler(controller).reconcile(operation)

    assert result.disposition is ReconcileDisposition.SUCCEEDED
    assert controller.create_calls == 1
    assert controller.get_calls == 1


class AcceptedThenTimedOutController:
    def __init__(self) -> None:
        self.create_calls = 0
        self.get_calls = 0

    async def get_worker(self, name: str) -> WorkerResource | None:
        self.get_calls += 1
        if self.get_calls == 1:
            return None
        return WorkerResource(
            name=name,
            runtime="qwenpaw",
            model="qwen3.6-plus",
            phase="Running",
            room_id="!alice:example",
        )

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource:
        del request
        self.create_calls += 1
        raise ProcessTimeout("agt timed out after Controller accept")


class Supervisor:
    def __init__(self) -> None:
        self.status = OperationStatus.PLANNED

    async def begin(self, **kwargs: object) -> object:
        return SimpleNamespace(
            operation_id=kwargs["operation_id"],
            kind=kwargs["kind"],
            target_key=kwargs["target_key"],
            request=kwargs["request"],
            status=self.status,
        )

    async def before_effect(self, *args: object) -> object:
        del args
        self.status = OperationStatus.DISPATCHED
        return SimpleNamespace(sequence=1)

    async def effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> object:
        del operation_id, effect, reason
        self.status = OperationStatus.RECONCILING
        return SimpleNamespace(status=self.status)

    async def effect_acknowledged(self, *args: object) -> object:
        del args
        self.status = OperationStatus.RUNNING
        return SimpleNamespace(status=self.status)

    async def effect_succeeded(self, *args: object) -> object:
        del args
        self.status = OperationStatus.SUCCEEDED
        return SimpleNamespace(status=self.status)

    async def effect_failed(self, *args: object) -> object:
        del args
        self.status = OperationStatus.FAILED
        return SimpleNamespace(status=self.status)


class Matrix:
    def __init__(self) -> None:
        self.transactions: list[str] = []

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        del room_id, text, thread_id, mentions
        self.transactions.append(txn_id)
        return "$greeting"


class FlakyMatrix(Matrix):
    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        self.transactions.append(txn_id)
        del room_id, text, thread_id, mentions
        if len(self.transactions) == 1:
            raise TimeoutError("Matrix accepted but response was lost")
        return "$same-greeting"


class Topology:
    async def refresh(self) -> object:
        return object()


@pytest.mark.asyncio
async def test_timed_out_create_queries_before_retrying() -> None:
    controller = AcceptedThenTimedOutController()
    supervisor = Supervisor()
    matrix = Matrix()
    context = MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id="create-alice",
    )
    service = ResourceService(
        controller=controller,
        supervisor=supervisor,
        topology=Topology(),
        matrix=matrix,
        worker_poll_delays=(0,),
    )

    worker = await service.create_worker(
        WorkerCreateRequest(
            name="alice",
            runtime="qwenpaw",
            model="qwen3.6-plus",
        ),
        context=context,
    )

    assert worker.room_id == "!alice:example"
    assert controller.create_calls == 1
    assert controller.get_calls == 2
    assert supervisor.status is OperationStatus.SUCCEEDED
    assert matrix.transactions == [
        f"agentteams:{context.operation_id}:0",
    ]


@pytest.mark.asyncio
async def test_greeting_retry_reuses_the_same_matrix_transaction() -> None:
    controller = AcceptedThenTimedOutController()
    supervisor = Supervisor()
    matrix = FlakyMatrix()
    context = MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id="create-with-lost-greeting",
    )
    service = ResourceService(
        controller=controller,
        supervisor=supervisor,
        topology=Topology(),
        matrix=matrix,
        worker_poll_delays=(0,),
    )
    request = WorkerCreateRequest(
        name="alice",
        runtime="qwenpaw",
        model="qwen3.6-plus",
    )

    with pytest.raises(TimeoutError):
        await service.create_worker(request, context=context)
    worker = await service.create_worker(request, context=context)

    expected = f"agentteams:{context.operation_id}:0"
    assert worker.room_id == "!alice:example"
    assert controller.create_calls == 1
    assert matrix.transactions == [expected, expected]
    assert supervisor.status is OperationStatus.SUCCEEDED
