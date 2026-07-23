from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.models import JournalEvent
from agentteams_manager.state.journal import S3Journal


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
async def test_journal_never_overwrites_an_existing_sequence() -> None:
    journal = S3Journal(MemoryObjectStore(), prefix="agentteams")
    event = JournalEvent.example(
        operation_id="c" * 32,
        sequence=1,
        event_type="effect_planned",
    )

    await journal.append(event)

    with pytest.raises(FileExistsError):
        await journal.append(event)


@pytest.mark.asyncio
async def test_snapshot_metadata_is_published_last(tmp_path: Path) -> None:
    store = MemoryObjectStore()
    journal = S3Journal(store, prefix="agentteams")
    database_path = tmp_path / "manager.db"
    database_path.write_bytes(b"sqlite-bytes")

    metadata = await journal.upload_snapshot(database_path, sequence=9)

    assert metadata.sequence == 9
    assert metadata.size == len(b"sqlite-bytes")
    assert tuple(store.objects)[-1] == "agentteams/manager/snapshots/latest.json"

