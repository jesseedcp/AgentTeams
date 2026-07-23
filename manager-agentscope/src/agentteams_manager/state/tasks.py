"""Durable finite and recurring task records."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from agentteams_manager.domain.models import TaskRecord

from .database import Database


def _task_from_row(row: sqlite3.Row) -> TaskRecord:
    return TaskRecord(
        task_id=row["task_id"],
        task_type=row["task_type"],
        status=row["status"],
        title=row["title"],
        assigned_to=row["assigned_to"],
        room_id=row["room_id"],
        project_id=row["project_id"],
        delegated_to_team=row["delegated_to_team"],
        schedule=row["schedule"],
        timezone=row["timezone"],
        last_executed_at=row["last_executed_at"],
        next_scheduled_at=row["next_scheduled_at"],
        metadata=json.loads(row["metadata_json"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class TaskRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(self, task: TaskRecord) -> TaskRecord:
        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO tasks(
                    task_id, task_type, status, title, assigned_to,
                    room_id, project_id, delegated_to_team, schedule,
                    timezone, last_executed_at, next_scheduled_at,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    task.task_type,
                    task.status,
                    task.title,
                    task.assigned_to,
                    task.room_id,
                    task.project_id,
                    task.delegated_to_team,
                    task.schedule,
                    task.timezone,
                    (
                        task.last_executed_at.isoformat()
                        if task.last_executed_at
                        else None
                    ),
                    (
                        task.next_scheduled_at.isoformat()
                        if task.next_scheduled_at
                        else None
                    ),
                    json.dumps(
                        task.metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    task.created_at.isoformat(),
                    task.updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return task

    async def get(self, task_id: str) -> TaskRecord | None:
        def read(connection: sqlite3.Connection) -> TaskRecord | None:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(row) if row else None

        return await self._database.read(read)

    async def transition(
        self,
        task_id: str,
        *,
        expected: set[str],
        target: str,
        last_executed_at: datetime | None = None,
        next_scheduled_at: datetime | None = None,
        metadata: dict[str, object] | None = None,
    ) -> TaskRecord | None:
        if not expected:
            raise ValueError("expected statuses must not be empty")

        def write(connection: sqlite3.Connection) -> TaskRecord | None:
            placeholders = ",".join("?" for _ in expected)
            cursor = connection.execute(
                f"""
                UPDATE tasks
                   SET status=?,
                       last_executed_at=COALESCE(?, last_executed_at),
                       next_scheduled_at=COALESCE(?, next_scheduled_at),
                       metadata_json=COALESCE(?, metadata_json),
                       updated_at=?
                 WHERE task_id=?
                   AND status IN ({placeholders})
                """,
                (
                    target,
                    (
                        last_executed_at.isoformat()
                        if last_executed_at
                        else None
                    ),
                    (
                        next_scheduled_at.isoformat()
                        if next_scheduled_at
                        else None
                    ),
                    (
                        json.dumps(
                            metadata,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                        if metadata is not None
                        else None
                    ),
                    datetime.now(UTC).isoformat(),
                    task_id,
                    *sorted(expected),
                ),
            )
            if cursor.rowcount == 0:
                return None
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(row)

        return await self._database.write(write)

    async def due_schedules(self, now: datetime) -> tuple[TaskRecord, ...]:
        def read(connection: sqlite3.Connection) -> tuple[TaskRecord, ...]:
            rows = connection.execute(
                """
                SELECT * FROM tasks
                 WHERE task_type IN ('infinite', 'recurring')
                   AND status='active'
                   AND next_scheduled_at IS NOT NULL
                   AND next_scheduled_at <= ?
                   AND (
                       last_executed_at IS NULL
                       OR last_executed_at < next_scheduled_at
                   )
                 ORDER BY next_scheduled_at, task_id
                """,
                (now.isoformat(),),
            ).fetchall()
            return tuple(_task_from_row(row) for row in rows)

        return await self._database.read(read)

    async def list_by_project(
        self,
        project_id: str,
    ) -> tuple[TaskRecord, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[TaskRecord, ...]:
            rows = connection.execute(
                """
                SELECT * FROM tasks
                 WHERE project_id=?
                 ORDER BY created_at, task_id
                """,
                (project_id,),
            ).fetchall()
            return tuple(_task_from_row(row) for row in rows)

        return await self._database.read(read)
