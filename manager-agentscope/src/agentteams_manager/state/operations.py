"""Durable operation and idempotency repositories."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from agentteams_manager.domain.errors import InvalidTransitionError
from agentteams_manager.domain.models import (
    JournalEvent,
    OperationRecord,
    OperationStatus,
)

from .database import Database


def _json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _operation_from_row(row: sqlite3.Row) -> OperationRecord:
    return OperationRecord(
        operation_id=row["operation_id"],
        kind=row["kind"],
        target_key=row["target_key"],
        status=row["status"],
        request=json.loads(row["request_json"]),
        result=json.loads(row["result_json"]),
        retry_count=row["retry_count"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class OperationRepository:
    """Compare-and-swap state changes for recoverable operations."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(self, record: OperationRecord) -> OperationRecord:
        def write(connection: sqlite3.Connection) -> OperationRecord:
            connection.execute(
                """
                INSERT INTO operations(
                    operation_id, kind, target_key, status, request_json,
                    result_json, retry_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.operation_id,
                    record.kind.value,
                    record.target_key,
                    record.status.value,
                    _json(record.request),
                    _json(record.result),
                    record.retry_count,
                    record.created_at.isoformat(),
                    record.updated_at.isoformat(),
                ),
            )
            return record

        return await self._database.write(write)

    async def get(self, operation_id: str) -> OperationRecord | None:
        def read(connection: sqlite3.Connection) -> OperationRecord | None:
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            return _operation_from_row(row) if row else None

        return await self._database.read(read)

    async def transition(
        self,
        operation_id: str,
        *,
        expected: set[OperationStatus],
        target: OperationStatus,
        result: dict[str, object] | None = None,
        increment_retry: bool = False,
    ) -> OperationRecord | None:
        """Atomically move an operation when its current status matches."""
        if not expected:
            raise ValueError("expected statuses must not be empty")

        def write(
            connection: sqlite3.Connection,
        ) -> OperationRecord | None:
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            current = _operation_from_row(row)
            if current.status not in expected:
                return None
            if not current.can_transition_to(target):
                raise InvalidTransitionError(
                    f"{current.status} cannot transition to {target}",
                )

            updated_at = datetime.now(UTC).isoformat()
            result_json = _json(result if result is not None else current.result)
            retry_delta = 1 if increment_retry else 0
            placeholders = ",".join("?" for _ in expected)
            cursor = connection.execute(
                f"""
                UPDATE operations
                   SET status=?, result_json=?,
                       retry_count=retry_count+?, updated_at=?
                 WHERE operation_id=?
                   AND status IN ({placeholders})
                """,
                (
                    target.value,
                    result_json,
                    retry_delta,
                    updated_at,
                    operation_id,
                    *(status.value for status in sorted(expected)),
                ),
            )
            if cursor.rowcount == 0:
                return None
            changed = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            return _operation_from_row(changed)

        return await self._database.write(write)

    async def list_recoverable(self) -> tuple[OperationRecord, ...]:
        recoverable = (
            OperationStatus.PREPARED,
            OperationStatus.DISPATCHED,
            OperationStatus.RUNNING,
            OperationStatus.RETRY_WAIT,
            OperationStatus.RECONCILING,
        )

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[OperationRecord, ...]:
            placeholders = ",".join("?" for _ in recoverable)
            rows = connection.execute(
                f"""
                SELECT * FROM operations
                 WHERE status IN ({placeholders})
                 ORDER BY updated_at, operation_id
                """,
                tuple(status.value for status in recoverable),
            ).fetchall()
            return tuple(_operation_from_row(row) for row in rows)

        return await self._database.read(read)

    async def claim_matrix_event(self, room_id: str, event_id: str) -> bool:
        def write(connection: sqlite3.Connection) -> bool:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO processed_matrix_events(
                    room_id, event_id, processed_at
                ) VALUES (?, ?, ?)
                """,
                (room_id, event_id, datetime.now(UTC).isoformat()),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)

    async def next_sequence(self, operation_id: str) -> int:
        def read(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                  FROM operation_events
                 WHERE operation_id=?
                """,
                (operation_id,),
            ).fetchone()
            return int(row[0])

        return await self._database.read(read)

    async def append_event(self, event: JournalEvent) -> None:
        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO operation_events(
                    operation_id, sequence, event_type,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.operation_id,
                    event.sequence,
                    event.event_type,
                    _json(event.payload),
                    event.created_at.isoformat(),
                ),
            )

        await self._database.write(write)

    async def events_for(
        self,
        operation_id: str,
    ) -> tuple[JournalEvent, ...]:
        def read(connection: sqlite3.Connection) -> tuple[JournalEvent, ...]:
            rows = connection.execute(
                """
                SELECT * FROM operation_events
                 WHERE operation_id=?
                 ORDER BY sequence
                """,
                (operation_id,),
            ).fetchall()
            return tuple(
                JournalEvent(
                    operation_id=row["operation_id"],
                    sequence=row["sequence"],
                    event_type=row["event_type"],
                    payload=json.loads(row["payload_json"]),
                    created_at=row["created_at"],
                )
                for row in rows
            )

        return await self._database.read(read)
