"""Exactly-once notification materialization."""

from __future__ import annotations

import sqlite3
from datetime import datetime

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import NotificationRecord

from .database import Database


def _notification_from_row(row: sqlite3.Row) -> NotificationRecord:
    return NotificationRecord(
        notification_id=row["notification_id"],
        source_operation_id=row["source_operation_id"],
        recipient=row["recipient"],
        room_id=row["room_id"],
        text=row["text"],
        txn_id=row["txn_id"],
        status=row["status"],
        event_id=row["event_id"],
        created_at=row["created_at"],
        sent_at=row["sent_at"],
    )


class NotificationRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def get(
        self,
        notification_id: str,
    ) -> NotificationRecord | None:
        def read(
            connection: sqlite3.Connection,
        ) -> NotificationRecord | None:
            row = connection.execute(
                "SELECT * FROM notifications WHERE notification_id=?",
                (notification_id,),
            ).fetchone()
            return _notification_from_row(row) if row else None

        return await self._database.read(read)

    async def get_by_source(
        self,
        source_operation_id: str,
    ) -> NotificationRecord | None:
        def read(
            connection: sqlite3.Connection,
        ) -> NotificationRecord | None:
            row = connection.execute(
                """
                SELECT * FROM notifications
                 WHERE source_operation_id=?
                """,
                (source_operation_id,),
            ).fetchone()
            return _notification_from_row(row) if row else None

        return await self._database.read(read)

    async def prepare(
        self,
        record: NotificationRecord,
    ) -> NotificationRecord:
        def write(connection: sqlite3.Connection) -> NotificationRecord:
            connection.execute(
                """
                INSERT OR IGNORE INTO notifications(
                    notification_id, source_operation_id, recipient,
                    room_id, text, txn_id, status, event_id,
                    created_at, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.notification_id,
                    record.source_operation_id,
                    record.recipient,
                    record.room_id,
                    record.text,
                    record.txn_id,
                    record.status,
                    record.event_id,
                    record.created_at.isoformat(),
                    (
                        record.sent_at.isoformat()
                        if record.sent_at is not None
                        else None
                    ),
                ),
            )
            row = connection.execute(
                """
                SELECT * FROM notifications
                 WHERE source_operation_id=?
                """,
                (record.source_operation_id,),
            ).fetchone()
            existing = _notification_from_row(row)
            identity = (
                existing.notification_id,
                existing.recipient,
                existing.room_id,
                existing.text,
                existing.txn_id,
            )
            requested = (
                record.notification_id,
                record.recipient,
                record.room_id,
                record.text,
                record.txn_id,
            )
            if identity != requested:
                raise ConflictError(
                    "notification source was reused with different content",
                )
            return existing

        return await self._database.write(write)

    async def mark_sent(
        self,
        notification_id: str,
        *,
        event_id: str,
        sent_at: datetime,
    ) -> NotificationRecord:
        def write(connection: sqlite3.Connection) -> NotificationRecord:
            connection.execute(
                """
                UPDATE notifications
                   SET status='sent',
                       event_id=COALESCE(event_id, ?),
                       sent_at=COALESCE(sent_at, ?)
                 WHERE notification_id=?
                """,
                (event_id, sent_at.isoformat(), notification_id),
            )
            row = connection.execute(
                "SELECT * FROM notifications WHERE notification_id=?",
                (notification_id,),
            ).fetchone()
            if row is None:
                raise KeyError(notification_id)
            record = _notification_from_row(row)
            if record.event_id != event_id:
                raise ConflictError(
                    "notification already has a different Matrix event",
                )
            return record

        return await self._database.write(write)
