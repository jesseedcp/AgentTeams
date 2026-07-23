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


async def make_supervisor(
    tmp_path: Path,
) -> tuple[OperationSupervisor, OperationRepository, MemoryObjectStore]:
    database = Database(tmp_path / "manager.db")
    await database.open()
    operations = OperationRepository(database)
    store = MemoryObjectStore()
    supervisor = OperationSupervisor(
        operations=operations,
        journal=S3Journal(store, prefix="agentteams"),
        clock=FixedClock(),
        reconcilers={},
    )
    return supervisor, operations, store


@pytest.mark.asyncio
async def test_timeout_moves_operation_to_reconciling(
    tmp_path: Path,
) -> None:
    supervisor, _, _ = await make_supervisor(tmp_path)
    operation = await supervisor.begin(
        operation_id="d" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    await supervisor.before_effect(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        {"argv": ["agt", "create", "worker", "--name", "alice"]},
    )

    changed = await supervisor.effect_ambiguous(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        "timeout waiting for agt",
    )

    assert changed.status is OperationStatus.RECONCILING
    assert changed.retry_count == 0


@pytest.mark.asyncio
async def test_repeated_begin_returns_existing_operation(
    tmp_path: Path,
) -> None:
    supervisor, _, _ = await make_supervisor(tmp_path)
    first = await supervisor.begin(
        operation_id="e" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    second = await supervisor.begin(
        operation_id="e" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )

    assert first.operation_id == second.operation_id
    assert first.created_at == second.created_at


@pytest.mark.asyncio
async def test_before_effect_redacts_nested_secrets(
    tmp_path: Path,
) -> None:
    supervisor, operations, store = await make_supervisor(tmp_path)
    operation = await supervisor.begin(
        operation_id="f" * 32,
        kind=OperationKind.CONFIGURE_MCP,
        target_key="mcp/github",
        request={"name": "github"},
    )

    event = await supervisor.before_effect(
        operation.operation_id,
        ExternalEffect.HIGRESS,
        {
            "headers": {"Authorization": "Bearer secret"},
            "api_key": "secret",
        },
    )

    encoded = next(iter(store.objects.values())).decode("utf-8")
    assert "Bearer secret" not in encoded
    assert '"api_key":"secret"' not in encoded
    assert event.payload["request"]["api_key"] == "[REDACTED]"
    assert len(await operations.events_for(operation.operation_id)) == 1


@pytest.mark.asyncio
async def test_acknowledged_effect_keeps_multistep_operation_running(
    tmp_path: Path,
) -> None:
    supervisor, operations, _ = await make_supervisor(tmp_path)
    operation = await supervisor.begin(
        operation_id="a" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    await supervisor.before_effect(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        {"operation": "create_worker"},
    )

    changed = await supervisor.effect_acknowledged(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        {"name": "alice", "accepted": True},
    )

    assert changed.status is OperationStatus.RUNNING
    assert [
        event.event_type
        for event in await operations.events_for(operation.operation_id)
    ] == ["effect_planned", "effect_acknowledged"]


@pytest.mark.asyncio
async def test_definite_effect_failure_is_terminal(
    tmp_path: Path,
) -> None:
    supervisor, operations, _ = await make_supervisor(tmp_path)
    operation = await supervisor.begin(
        operation_id="b" * 32,
        kind=OperationKind.UPDATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice", "model": "new"},
    )
    await supervisor.before_effect(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        {"operation": "update_worker"},
    )

    changed = await supervisor.effect_failed(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        "validation rejected",
    )

    assert changed.status is OperationStatus.FAILED
    assert changed.result["reason"] == "validation rejected"
    events = await operations.events_for(operation.operation_id)
    assert events[-1].event_type == "effect_failed"
