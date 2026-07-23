"""Immutable MinIO/S3 recovery journal and verified snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.models import JournalEvent


class JournalObjectStore(Protocol):
    async def put(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        if_none_match: bool,
    ) -> str: ...

    async def get(self, key: str) -> bytes: ...

    async def list(self, prefix: str) -> tuple[str, ...]: ...


class SnapshotMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(ge=0)
    key: str
    sha256: str = Field(min_length=64, max_length=64)
    size: int = Field(ge=0)
    created_at: datetime


def event_key(prefix: str, event: JournalEvent) -> str:
    return (
        f"{prefix}/manager/journal/{event.operation_id}/"
        f"{event.sequence:020d}.json"
    )


class S3Journal:
    """Journal logic independent of the concrete aioboto3 adapter."""

    def __init__(self, store: JournalObjectStore, *, prefix: str) -> None:
        self._store = store
        self._prefix = prefix.strip("/")

    @property
    def journal_prefix(self) -> str:
        return f"{self._prefix}/manager/journal/"

    @property
    def snapshot_prefix(self) -> str:
        return f"{self._prefix}/manager/snapshots"

    async def append(self, event: JournalEvent) -> str:
        """Write one event exactly once."""
        return await self._store.put(
            event_key(self._prefix, event),
            event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            if_none_match=True,
        )

    async def list_after(self, sequence: int) -> tuple[JournalEvent, ...]:
        """Return globally sequenced journal events after a snapshot."""
        keys = await self._store.list(self.journal_prefix)
        events: list[JournalEvent] = []
        for key in keys:
            if not key.endswith(".json"):
                continue
            event = JournalEvent.model_validate_json(
                await self._store.get(key),
            )
            if event.sequence > sequence:
                events.append(event)
        events.sort(key=lambda item: (item.sequence, item.operation_id))
        return tuple(events)

    async def upload_snapshot(
        self,
        path: Path,
        *,
        sequence: int,
    ) -> SnapshotMetadata:
        """Publish immutable bytes and metadata before the latest pointer."""
        data = path.read_bytes()
        digest = sha256(data).hexdigest()
        stem = f"{self.snapshot_prefix}/{sequence:020d}"
        database_key = f"{stem}.db"
        metadata_key = f"{stem}.json"
        metadata = SnapshotMetadata(
            sequence=sequence,
            key=database_key,
            sha256=digest,
            size=len(data),
            created_at=datetime.now(UTC),
        )
        encoded = metadata.model_dump_json().encode("utf-8")

        await self._store.put(
            database_key,
            data,
            content_type="application/vnd.sqlite3",
            if_none_match=True,
        )
        await self._store.put(
            metadata_key,
            encoded,
            content_type="application/json",
            if_none_match=True,
        )
        await self._store.put(
            f"{self.snapshot_prefix}/latest.json",
            encoded,
            content_type="application/json",
            if_none_match=False,
        )
        return metadata

    async def download_latest_snapshot(
        self,
    ) -> tuple[SnapshotMetadata, bytes] | None:
        try:
            encoded = await self._store.get(
                f"{self.snapshot_prefix}/latest.json",
            )
        except KeyError:
            return None
        metadata = SnapshotMetadata.model_validate_json(encoded)
        data = await self._store.get(metadata.key)
        digest = sha256(data).hexdigest()
        if len(data) != metadata.size or digest != metadata.sha256:
            raise ValueError(
                "snapshot checksum/size mismatch; refusing recovery",
            )
        return metadata, data

