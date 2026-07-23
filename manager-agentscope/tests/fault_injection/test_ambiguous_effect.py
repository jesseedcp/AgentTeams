from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.journal import S3Journal
from agentteams_manager.state.operations import OperationRepository
from agentteams_manager.workflows.supervisor import OperationSupervisor


class MemoryObjectStore:
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


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 7, 23, tzinfo=UTC)


@pytest.mark.asyncio
async def test_recovery_handler_owns_ambiguous_operation_once(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    operations = OperationRepository(database)
    reconciled: list[str] = []

    async def reconcile(operation) -> None:
        reconciled.append(operation.operation_id)

    supervisor = OperationSupervisor(
        operations=operations,
        journal=S3Journal(MemoryObjectStore(), prefix="agentteams"),
        clock=FixedClock(),
        reconcilers={OperationKind.CREATE_WORKER: reconcile},
    )
    operation = await supervisor.begin(
        operation_id="1" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    await supervisor.before_effect(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        {"argv": ["agt", "create", "worker"]},
    )
    await supervisor.effect_ambiguous(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        "timeout",
    )

    report = await supervisor.recover_all()

    assert report.reconciled_operations == 1
    assert reconciled == [operation.operation_id]
    stored = await operations.get(operation.operation_id)
    assert stored is not None
    assert stored.status is OperationStatus.RECONCILING
