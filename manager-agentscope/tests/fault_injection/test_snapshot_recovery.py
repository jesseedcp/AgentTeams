from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.models import (
    ExternalEffect,
    JournalEvent,
    OperationKind,
    OperationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.journal import S3Journal
from agentteams_manager.state.operations import OperationRepository
from agentteams_manager.state.recovery import RecoveryCoordinator
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
        return f"etag-{len(data)}"

    async def get(self, key: str) -> bytes:
        return self.objects[key]

    async def list(self, prefix: str) -> tuple[str, ...]:
        return tuple(sorted(key for key in self.objects if key.startswith(prefix)))


@pytest.mark.asyncio
async def test_restore_replays_events_after_snapshot(tmp_path: Path) -> None:
    store = MemoryObjectStore()
    journal = S3Journal(store, prefix="agentteams")
    source = Database(tmp_path / "source.db")
    await source.open()
    await source.write(
        lambda connection: connection.execute(
            "INSERT INTO key_values(key, value, updated_at) "
            "VALUES ('seed', 'present', '2026-07-23T00:00:00Z')",
        ),
    )
    snapshot_path = tmp_path / "snapshot.db"
    await source.backup_to(snapshot_path)
    await journal.upload_snapshot(snapshot_path, sequence=4)
    await journal.append(
        JournalEvent(
            operation_id="d" * 32,
            sequence=5,
            event_type="effect_planned",
            payload={"effect": "matrix"},
            created_at=datetime.now(UTC),
        ),
    )

    replayed: list[JournalEvent] = []
    target = Database(tmp_path / "target.db")
    await target.open()
    coordinator = RecoveryCoordinator(
        database=target,
        journal=journal,
        replay_event=replayed.append,
        temp_directory=tmp_path / "restore",
    )

    report = await coordinator.restore()

    assert report.snapshot_sequence == 4
    assert report.replayed_events == 1
    assert [event.sequence for event in replayed] == [5]
    seed = await target.read(
        lambda connection: connection.execute(
            "SELECT value FROM key_values WHERE key='seed'",
        ).fetchone()[0],
    )
    assert seed == "present"


@pytest.mark.asyncio
async def test_corrupt_snapshot_stops_before_replay(tmp_path: Path) -> None:
    store = MemoryObjectStore()
    journal = S3Journal(store, prefix="agentteams")
    snapshot_path = tmp_path / "snapshot.db"
    snapshot_path.write_bytes(b"valid-before-corruption")
    await journal.upload_snapshot(snapshot_path, sequence=2)
    store.objects["agentteams/manager/snapshots/00000000000000000002.db"] = (
        b"corrupt"
    )
    replayed: list[JournalEvent] = []
    target = Database(tmp_path / "target.db")
    await target.open()
    coordinator = RecoveryCoordinator(
        database=target,
        journal=journal,
        replay_event=replayed.append,
        temp_directory=tmp_path / "restore",
    )

    with pytest.raises(ValueError, match="checksum"):
        await coordinator.restore()

    assert replayed == []


@pytest.mark.asyncio
async def test_journal_replay_rebuilds_operation_created_after_snapshot(
    tmp_path: Path,
) -> None:
    store = MemoryObjectStore()
    journal = S3Journal(store, prefix="agentteams")
    source_database = Database(tmp_path / "source.db")
    await source_database.open()
    source_operations = OperationRepository(source_database)
    supervisor = OperationSupervisor(
        operations=source_operations,
        journal=journal,
        clock=type(
            "Clock",
            (),
            {"now": lambda self: datetime(2026, 7, 23, tzinfo=UTC)},
        )(),
        reconcilers={},
    )
    operation_id = "a" * 32
    await supervisor.begin(
        operation_id=operation_id,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice", "runtime": "qwenpaw"},
    )
    await supervisor.before_effect(
        operation_id,
        ExternalEffect.CONTROLLER,
        {"operation": "create_worker", "name": "alice"},
    )
    await supervisor.effect_ambiguous(
        operation_id,
        ExternalEffect.CONTROLLER,
        "controller_timeout",
    )

    target_database = Database(tmp_path / "target.db")
    await target_database.open()
    target_operations = OperationRepository(target_database)
    coordinator = RecoveryCoordinator(
        database=target_database,
        journal=journal,
        replay_event=target_operations.replay_event,
        temp_directory=tmp_path / "restore",
    )

    report = await coordinator.restore()
    restored = await target_operations.get(operation_id)

    assert report.replayed_events == 3
    assert restored is not None
    assert restored.kind is OperationKind.CREATE_WORKER
    assert restored.request == {"name": "alice", "runtime": "qwenpaw"}
    assert restored.status is OperationStatus.RECONCILING
    assert await target_operations.current_sequence() == 3
    assert await target_operations.current_applied_sequence() == 3


@pytest.mark.asyncio
async def test_replay_applies_event_captured_before_its_state_transition(
    tmp_path: Path,
) -> None:
    store = MemoryObjectStore()
    journal = S3Journal(store, prefix="agentteams")
    source_database = Database(tmp_path / "source-race.db")
    await source_database.open()
    source_operations = OperationRepository(source_database)
    supervisor = OperationSupervisor(
        operations=source_operations,
        journal=journal,
        clock=type(
            "Clock",
            (),
            {"now": lambda self: datetime(2026, 7, 23, tzinfo=UTC)},
        )(),
        reconcilers={},
    )
    operation_id = "b" * 32
    await supervisor.begin(
        operation_id=operation_id,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/bob",
        request={"name": "bob"},
    )
    sequence = await source_operations.next_sequence(operation_id)
    pending_event = JournalEvent(
        operation_id=operation_id,
        sequence=sequence,
        event_type="effect_planned",
        payload={
            "effect": ExternalEffect.CONTROLLER.value,
            "request": {"operation": "create_worker"},
        },
        created_at=datetime(2026, 7, 23, tzinfo=UTC),
    )
    await journal.append(pending_event)
    await source_operations.append_event(pending_event)
    snapshot_path = tmp_path / "race-snapshot.db"
    await source_database.backup_to(snapshot_path)
    applied = await source_operations.current_applied_sequence()
    await journal.upload_snapshot(snapshot_path, sequence=applied)

    target_database = Database(tmp_path / "race-target.db")
    await target_database.open()
    target_operations = OperationRepository(target_database)
    coordinator = RecoveryCoordinator(
        database=target_database,
        journal=journal,
        replay_event=target_operations.replay_event,
        temp_directory=tmp_path / "race-restore",
    )

    report = await coordinator.restore()
    restored = await target_operations.get(operation_id)

    assert applied == 1
    assert report.snapshot_sequence == 1
    assert report.replayed_events == 1
    assert restored is not None
    assert restored.status is OperationStatus.DISPATCHED
    assert await target_operations.current_applied_sequence() == 2
