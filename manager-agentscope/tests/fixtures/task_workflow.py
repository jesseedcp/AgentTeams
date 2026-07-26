from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationRecord,
    OperationStatus,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.state.tasks import TaskRepository


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 7, 23, 12, tzinfo=UTC)


class TaskController:
    def __init__(self) -> None:
        self.workers = {
            "alice": WorkerResource(
                name="alice",
                runtime="qwenpaw",
                phase="Running",
                room_id="!alice:example",
                matrix_user_id="@worker-alice:example",
            ),
            "bob": WorkerResource(
                name="bob",
                runtime="qwenpaw",
                phase="Running",
                room_id="!bob:example",
                matrix_user_id="@worker-bob:example",
            ),
            "charlie": WorkerResource(
                name="charlie",
                runtime="qwenpaw",
                phase="Running",
                room_id="!charlie:example",
                matrix_user_id="@worker-charlie:example",
            ),
        }
        self.teams: dict[str, TeamResource] = {}

    async def get_worker(self, name: str) -> WorkerResource | None:
        return self.workers.get(name)

    async def get_team(self, name: str) -> TeamResource | None:
        return self.teams.get(name)


class TaskMatrix:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.attempts: list[SimpleNamespace] = []
        self.visible: dict[str, str] = {}
        self.timeout_once = False

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        self.order.append("matrix.assignment")
        self.attempts.append(
            SimpleNamespace(
                room_id=room_id,
                text=text,
                txn_id=txn_id,
                thread_id=thread_id,
                mentions=mentions,
            ),
        )
        event_id = self.visible.setdefault(
            txn_id,
            f"$assignment-{len(self.visible) + 1}",
        )
        if self.timeout_once:
            self.timeout_once = False
            raise TimeoutError("Matrix accepted the transaction")
        return event_id


class TaskStorage:
    def __init__(self, client: Any, order: list[str]) -> None:
        self.client = client
        self.order = order

    async def put_json_if_version(self, key: str, value: Any, **kwargs: Any):
        status = value.get("status") if isinstance(value, dict) else None
        self.order.append(
            "minio.meta"
            if key.endswith("/meta.json")
            and status in {"prepared", "planning"}
            else f"minio.{status}",
        )
        return await self.client.put_json_if_version(key, value, **kwargs)

    async def put_bytes_if_version(
        self,
        key: str,
        value: bytes,
        **kwargs: Any,
    ):
        self.order.append("minio.spec")
        return await self.client.put_bytes_if_version(key, value, **kwargs)

    async def head(self, key: str):
        return await self.client.head(key)

    async def get_bytes(self, key: str) -> bytes:
        return await self.client.get_bytes(key)

    async def get_json(self, key: str) -> Any:
        return await self.client.get_json(key)

    async def list_prefix(self, prefix: str):
        return await self.client.list_prefix(prefix)

    async def mirror_down(self, prefix: str, destination: Path):
        return await self.client.mirror_down(prefix, destination)


class OrderedTaskRepository:
    def __init__(
        self,
        repository: TaskRepository,
        order: list[str],
    ) -> None:
        self.repository = repository
        self.order = order

    async def create(self, task: Any):
        self.order.append("sqlite.prepare")
        return await self.repository.create(task)

    async def get(self, task_id: str):
        return await self.repository.get(task_id)

    async def transition(self, task_id: str, **kwargs: Any):
        return await self.repository.transition(task_id, **kwargs)


class TaskSupervisor:
    def __init__(self, clock: FixedClock) -> None:
        self.clock = clock
        self.operations: dict[str, OperationRecord] = {}
        self.events: list[tuple[str, ExternalEffect, dict[str, object]]] = []

    async def begin(self, **kwargs: Any) -> OperationRecord:
        operation_id = str(kwargs["operation_id"])
        existing = self.operations.get(operation_id)
        if existing is not None:
            return existing
        operation = OperationRecord.new(
            operation_id=operation_id,
            kind=kwargs["kind"],
            target_key=str(kwargs["target_key"]),
            request=dict(kwargs["request"]),
        ).model_copy(
            update={
                "created_at": self.clock.now(),
                "updated_at": self.clock.now(),
            },
        )
        self.operations[operation_id] = operation
        return operation

    async def before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> object:
        self.events.append(("before", effect, request))
        self._status(operation_id, OperationStatus.DISPATCHED)
        return SimpleNamespace(sequence=len(self.events))

    async def effect_acknowledged(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord:
        self.events.append(("acknowledged", effect, receipt))
        return self._status(operation_id, OperationStatus.RUNNING)

    async def effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord:
        self.events.append(("succeeded", effect, receipt))
        return self._status(
            operation_id,
            OperationStatus.SUCCEEDED,
            result=receipt,
        )

    async def effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord:
        self.events.append(("ambiguous", effect, {"reason": reason}))
        return self._status(operation_id, OperationStatus.RECONCILING)

    async def effect_failed(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord:
        self.events.append(("failed", effect, {"reason": reason}))
        return self._status(operation_id, OperationStatus.FAILED)

    def _status(
        self,
        operation_id: str,
        status: OperationStatus,
        *,
        result: dict[str, object] | None = None,
    ) -> OperationRecord:
        operation = self.operations[operation_id].model_copy(
            update={
                "status": status,
                "result": result or self.operations[operation_id].result,
                "updated_at": self.clock.now(),
            },
        )
        self.operations[operation_id] = operation
        return operation
