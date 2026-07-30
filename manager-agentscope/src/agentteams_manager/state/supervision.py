"""Durable progress-observation state for proactive heartbeat checks."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from .database import Database


class SupervisionStateRepository:
    """Count consecutive heartbeat cycles without observable progress."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def record_ping(
        self,
        *,
        subject_key: str,
        observed_token: str,
        pinged_at: datetime,
    ) -> int:
        if pinged_at.tzinfo is None or pinged_at.utcoffset() is None:
            raise ValueError("pinged_at must be timezone-aware")

        def write(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                """
                SELECT observed_token, missed_cycles
                  FROM supervision_checks
                 WHERE subject_key=?
                """,
                (subject_key,),
            ).fetchone()
            missed_cycles = (
                int(row["missed_cycles"]) + 1
                if row is not None
                and row["observed_token"] == observed_token
                else 0
            )
            now = datetime.now(UTC).isoformat()
            connection.execute(
                """
                INSERT INTO supervision_checks(
                    subject_key, observed_token, missed_cycles,
                    last_ping_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(subject_key) DO UPDATE SET
                    observed_token=excluded.observed_token,
                    missed_cycles=excluded.missed_cycles,
                    last_ping_at=excluded.last_ping_at,
                    updated_at=excluded.updated_at
                """,
                (
                    subject_key,
                    observed_token,
                    missed_cycles,
                    pinged_at.astimezone(UTC).isoformat(),
                    now,
                ),
            )
            return missed_cycles

        return await self._database.write(write)
