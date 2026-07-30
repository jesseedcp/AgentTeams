from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.agt import WorkerCreateRequest
from agentteams_manager.domain.models import (
    OperationKind,
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.journal import S3Journal
from agentteams_manager.state.operations import OperationRepository
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceService,
)
from agentteams_manager.workflows.supervisor import OperationSupervisor


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        if_none_match: bool,
    ) -> str:
        del content_type
        if if_none_match and key in self.objects:
            raise FileExistsError(key)
        self.objects[key] = data
        return "etag"

    async def get(self, key: str) -> bytes:
        return self.objects[key]

    async def list(self, prefix: str) -> tuple[str, ...]:
        return tuple(key for key in self.objects if key.startswith(prefix))


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 23, tzinfo=UTC)


class Controller:
    def __init__(self) -> None:
        self.created = 0
        self.gets = 0

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource:
        self.created += 1
        return WorkerResource(
            name=request.name,
            runtime=request.runtime,
            phase="Pending",
        )

    async def get_worker(self, name: str) -> WorkerResource | None:
        self.gets += 1
        if self.gets == 1:
            return None
        return WorkerResource(
            name=name,
            runtime="qwenpaw",
            model="qwen3.6-plus",
            phase="Running",
            room_id="!alice:example",
        )


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
        return "$event"


class Topology:
    async def refresh(self) -> object:
        return object()


@pytest.mark.asyncio
async def test_worker_create_is_journaled_through_greeting(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    operations = OperationRepository(database)
    journal_store = MemoryStore()
    supervisor = OperationSupervisor(
        operations=operations,
        journal=S3Journal(journal_store, prefix="agentteams"),
        clock=Clock(),
        reconcilers={},
    )
    controller = Controller()
    matrix = Matrix()
    service = ResourceService(
        controller=controller,
        supervisor=supervisor,
        topology=Topology(),
        matrix=matrix,
        worker_poll_delays=(0,),
        admin_room_id="!admin:example",
    )
    context = MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id="call-worker",
    )

    worker = await service.create_worker(
        WorkerCreateRequest(
            name="alice",
            runtime="qwenpaw",
            model="qwen3.6-plus",
        ),
        context=context,
    )
    accepted_operation = await operations.get(context.operation_id)

    assert worker.phase == "Pending"
    assert accepted_operation is not None
    assert accepted_operation.status is OperationStatus.RUNNING

    await asyncio.wait_for(
        service.wait_for_background_worker_creates(),
        timeout=1,
    )
    operation = await operations.get(context.operation_id)
    events = await operations.events_for(context.operation_id)

    assert operation is not None
    assert operation.kind is OperationKind.CREATE_WORKER
    assert operation.status is OperationStatus.SUCCEEDED
    assert [event.event_type for event in events] == [
        "operation_started",
        "effect_planned",
        "effect_acknowledged",
        "effect_acknowledged",
        "effect_planned",
        "effect_succeeded",
    ]
    assert len(journal_store.objects) == 6
    assert matrix.transactions == [
        f"agentteams:{context.operation_id}:0",
        f"agentteams:{context.operation_id}:1",
    ]
