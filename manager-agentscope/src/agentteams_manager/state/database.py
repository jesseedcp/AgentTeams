"""Small asynchronous boundary around standard-library SQLite."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from .schema import (
    SCHEMA_SQL,
    SCHEMA_VERSION,
    SESSION_SETTINGS_MIGRATION_COLUMNS,
)

T = TypeVar("T")


class Database:
    """Open one SQLite connection per worker-thread transaction."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    async def open(self) -> None:
        """Create the database and apply the initial idempotent schema."""

        def run() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as connection:
                current = connection.execute(
                    "PRAGMA user_version",
                ).fetchone()[0]
                if current > SCHEMA_VERSION:
                    raise RuntimeError(
                        "database schema is newer than this Manager "
                        f"({current} > {SCHEMA_VERSION})",
                    )
                connection.executescript(SCHEMA_SQL)
                columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(session_settings)",
                    )
                }
                for name, declaration in (
                    SESSION_SETTINGS_MIGRATION_COLUMNS.items()
                ):
                    if name not in columns:
                        connection.execute(
                            "ALTER TABLE session_settings "
                            f"ADD COLUMN {name} {declaration}",
                        )
                connection.execute(
                    f"PRAGMA user_version={SCHEMA_VERSION}",
                )

        await asyncio.to_thread(run)

    async def read(
        self,
        callback: Callable[[sqlite3.Connection], T],
    ) -> T:
        """Run a read callback on a dedicated thread and connection."""

        def run() -> T:
            with self._connect() as connection:
                return callback(connection)

        return await asyncio.to_thread(run)

    async def write(
        self,
        callback: Callable[[sqlite3.Connection], T],
    ) -> T:
        """Run one callback as one committed transaction."""

        def run() -> T:
            with self._connect() as connection:
                return callback(connection)

        return await asyncio.to_thread(run)

    async def backup_to(self, destination: Path) -> None:
        """Create a transactionally consistent SQLite backup."""

        def run() -> None:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as source:
                source.execute("PRAGMA wal_checkpoint(PASSIVE)")
                with sqlite3.connect(destination) as target:
                    source.backup(target)

        await asyncio.to_thread(run)

    async def replace_from(self, source: Path) -> None:
        """Replace local contents with a verified SQLite database."""

        def run() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(source) as source_connection:
                with self._connect() as target:
                    source_connection.backup(target)

        await asyncio.to_thread(run)

    async def close(self) -> None:
        """Connections are transaction-scoped, so shutdown needs no action."""
