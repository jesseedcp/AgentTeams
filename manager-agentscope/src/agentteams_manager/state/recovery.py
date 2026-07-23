"""Restore SQLite and replay remote intent before normal operation."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from pathlib import Path

from agentteams_manager.domain.models import JournalEvent, RecoveryReport

from .database import Database
from .journal import S3Journal

ReplayEvent = Callable[[JournalEvent], Awaitable[None] | None]


class RecoveryCoordinator:
    def __init__(
        self,
        *,
        database: Database,
        journal: S3Journal,
        replay_event: ReplayEvent,
        temp_directory: Path,
    ) -> None:
        self._database = database
        self._journal = journal
        self._replay_event = replay_event
        self._temp_directory = temp_directory

    async def restore(self) -> RecoveryReport:
        """Restore the latest valid snapshot, then replay later intents."""
        snapshot = await self._journal.download_latest_snapshot()
        snapshot_sequence = 0
        if snapshot is not None:
            metadata, data = snapshot
            self._temp_directory.mkdir(parents=True, exist_ok=True)
            path = self._temp_directory / "manager-restore.db"
            path.write_bytes(data)
            await self._database.replace_from(path)
            snapshot_sequence = metadata.sequence

        events = await self._journal.list_after(snapshot_sequence)
        for event in events:
            result = self._replay_event(event)
            if inspect.isawaitable(result):
                await result

        return RecoveryReport(
            snapshot_sequence=snapshot_sequence,
            replayed_events=len(events),
        )
