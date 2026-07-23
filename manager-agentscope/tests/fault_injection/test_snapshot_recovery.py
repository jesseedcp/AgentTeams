from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.models import JournalEvent
from agentteams_manager.state.database import Database
from agentteams_manager.state.journal import S3Journal
from agentteams_manager.state.recovery import RecoveryCoordinator


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
