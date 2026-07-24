"""Durable operation and idempotency repositories."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from agentteams_manager.domain.errors import (
    ConflictError,
    InvalidTransitionError,
    RecoveryError,
)
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

    async def get_value(self, key: str) -> str | None:
        """Read a durable process cursor or transport value."""
        def read(connection: sqlite3.Connection) -> str | None:
            row = connection.execute(
                "SELECT value FROM key_values WHERE key=?",
                (key,),
            ).fetchone()
            return str(row["value"]) if row is not None else None

        return await self._database.read(read)

    async def set_value(self, key: str, value: str) -> None:
        """Atomically create or replace a durable process value."""
        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO key_values(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, value, datetime.now(UTC).isoformat()),
            )

        await self._database.write(write)

    async def next_sequence(self, operation_id: str) -> int:
        del operation_id

        def write(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                """
                INSERT INTO key_values(key, value, updated_at)
                VALUES ('journal_sequence', '1', ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=CAST(key_values.value AS INTEGER) + 1,
                    updated_at=excluded.updated_at
                RETURNING CAST(value AS INTEGER)
                """,
                (datetime.now(UTC).isoformat(),),
            ).fetchone()
            return int(row[0])

        return await self._database.write(write)

    async def current_sequence(self) -> int:
        def read(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                "SELECT value FROM key_values WHERE key='journal_sequence'",
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)

    async def current_applied_sequence(self) -> int:
        """Return the highest journal event fully reflected in local state."""
        def read(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                """
                SELECT value FROM key_values
                 WHERE key='journal_applied_sequence'
                """,
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)

    async def mark_event_applied(self, sequence: int) -> None:
        """Advance the snapshot-safe watermark after a state transition."""
        if sequence < 1:
            raise ValueError("applied journal sequence must be positive")

        def write(connection: sqlite3.Connection) -> None:
            row = connection.execute(
                "SELECT 1 FROM operation_events WHERE sequence=?",
                (sequence,),
            ).fetchone()
            if row is None:
                raise RecoveryError(
                    f"cannot apply missing journal event {sequence}",
                )
            _advance_applied_sequence(
                connection,
                sequence=sequence,
                updated_at=datetime.now(UTC).isoformat(),
            )

        await self._database.write(write)

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

    async def replay_event(self, event: JournalEvent) -> None:
        """Materialize one immutable remote event into restored SQLite state."""
        started = _started_operation(event)

        def write(connection: sqlite3.Connection) -> None:
            duplicate = connection.execute(
                "SELECT * FROM operation_events WHERE sequence=?",
                (event.sequence,),
            ).fetchone()
            if duplicate is not None:
                persisted = JournalEvent(
                    operation_id=duplicate["operation_id"],
                    sequence=duplicate["sequence"],
                    event_type=duplicate["event_type"],
                    payload=json.loads(duplicate["payload_json"]),
                    created_at=duplicate["created_at"],
                )
                if persisted != event:
                    raise RecoveryError(
                        "journal sequence collision at "
                        f"{event.sequence}",
                    )
                _apply_replayed_outcome(connection, event)
                _advance_journal_sequence(connection, event)
                _advance_applied_sequence(
                    connection,
                    sequence=event.sequence,
                    updated_at=event.created_at.isoformat(),
                )
                return

            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (event.operation_id,),
            ).fetchone()
            if started is not None:
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO operations(
                            operation_id, kind, target_key, status,
                            request_json, result_json, retry_count,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            started.operation_id,
                            started.kind.value,
                            started.target_key,
                            started.status.value,
                            _json(started.request),
                            _json(started.result),
                            started.retry_count,
                            started.created_at.isoformat(),
                            started.updated_at.isoformat(),
                        ),
                    )
                else:
                    existing = _operation_from_row(row)
                    if (
                        existing.kind is not started.kind
                        or existing.target_key != started.target_key
                        or existing.request != started.request
                    ):
                        raise ConflictError(
                            "restored operation identity conflicts with "
                            f"journal event {event.sequence}",
                        )
            elif row is None:
                raise RecoveryError(
                    f"journal event {event.sequence} for "
                    f"{event.operation_id} has no operation_started intent",
                )

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
            _apply_replayed_outcome(connection, event)
            _advance_journal_sequence(connection, event)
            _advance_applied_sequence(
                connection,
                sequence=event.sequence,
                updated_at=event.created_at.isoformat(),
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


def _started_operation(event: JournalEvent) -> OperationRecord | None:
    if event.event_type != "operation_started":
        return None
    raw = event.payload.get("operation")
    if not isinstance(raw, dict):
        raise RecoveryError(
            f"operation_started event {event.sequence} has no operation",
        )
    operation = OperationRecord.model_validate(raw)
    if operation.operation_id != event.operation_id:
        raise RecoveryError(
            f"operation_started event {event.sequence} ID mismatch",
        )
    if operation.status is not OperationStatus.PLANNED:
        raise RecoveryError(
            f"operation_started event {event.sequence} is not planned",
        )
    return operation


def _advance_journal_sequence(
    connection: sqlite3.Connection,
    event: JournalEvent,
) -> None:
    connection.execute(
        """
        INSERT INTO key_values(key, value, updated_at)
        VALUES ('journal_sequence', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=CAST(
                MAX(
                    CAST(key_values.value AS INTEGER),
                    CAST(excluded.value AS INTEGER)
                )
                AS TEXT
            ),
            updated_at=excluded.updated_at
        """,
        (
            str(event.sequence),
            event.created_at.isoformat(),
        ),
    )


def _advance_applied_sequence(
    connection: sqlite3.Connection,
    *,
    sequence: int,
    updated_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO key_values(key, value, updated_at)
        VALUES ('journal_applied_sequence', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=CAST(
                MAX(
                    CAST(key_values.value AS INTEGER),
                    CAST(excluded.value AS INTEGER)
                )
                AS TEXT
            ),
            updated_at=excluded.updated_at
        """,
        (str(sequence), updated_at),
    )


def _apply_replayed_outcome(
    connection: sqlite3.Connection,
    event: JournalEvent,
) -> None:
    if event.event_type == "operation_started":
        return
    row = connection.execute(
        "SELECT * FROM operations WHERE operation_id=?",
        (event.operation_id,),
    ).fetchone()
    if row is None:
        raise RecoveryError(event.operation_id)
    operation = _operation_from_row(row)
    status = operation.status
    result = operation.result

    if event.event_type == "effect_planned":
        if status in {OperationStatus.PLANNED, OperationStatus.PREPARED}:
            status = OperationStatus.DISPATCHED
    elif event.event_type == "effect_acknowledged":
        if status not in {OperationStatus.SUCCEEDED, OperationStatus.FAILED}:
            status = OperationStatus.RUNNING
            result = _receipt(event)
    elif event.event_type == "effect_succeeded":
        if status is OperationStatus.FAILED:
            raise RecoveryError(
                f"operation {event.operation_id} has conflicting terminal "
                "journal outcomes",
            )
        status = OperationStatus.SUCCEEDED
        result = _receipt(event)
    elif event.event_type == "effect_failed":
        if status is OperationStatus.SUCCEEDED:
            raise RecoveryError(
                f"operation {event.operation_id} has conflicting terminal "
                "journal outcomes",
            )
        status = OperationStatus.FAILED
        result = {
            "effect": event.payload.get("effect"),
            "reason": event.payload.get("reason", ""),
        }
    elif event.event_type == "effect_ambiguous":
        if status not in {OperationStatus.SUCCEEDED, OperationStatus.FAILED}:
            status = OperationStatus.RECONCILING
            result = {
                "effect": event.payload.get("effect"),
                "ambiguous_reason": event.payload.get("reason", ""),
            }
    else:
        return

    connection.execute(
        """
        UPDATE operations
           SET status=?, result_json=?, updated_at=?
         WHERE operation_id=?
        """,
        (
            status.value,
            _json(result),
            event.created_at.isoformat(),
            event.operation_id,
        ),
    )


def _receipt(event: JournalEvent) -> dict[str, object]:
    receipt = event.payload.get("receipt", {})
    if not isinstance(receipt, dict):
        raise RecoveryError(
            f"journal event {event.sequence} has an invalid receipt",
        )
    return receipt
