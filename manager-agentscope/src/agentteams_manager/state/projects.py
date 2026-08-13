"""Durable project records independent from Matrix and MinIO.

保存 Project 生命周期、计划版本和参与者的本地事务记录。

Project 的对话房间属于 Matrix，artifact 属于 MinIO，资源期望状态可能来自 Controller；
本表保存 Manager 协调这些系统时所需的 project ID、planning/active 等状态和已确认计划
版本。它不是独占真相，workflow 会将本地记录与各权威系统对账后才报告成功。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from agentteams_manager.domain.models import ProjectRecord

from .database import Database


def _project_from_row(row: sqlite3.Row) -> ProjectRecord:
    return ProjectRecord(
        project_id=row["project_id"],
        name=row["name"],
        room_id=row["room_id"],
        status=row["status"],
        metadata=json.loads(row["metadata_json"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class ProjectRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(self, project: ProjectRecord) -> ProjectRecord:
        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO projects(
                    project_id, name, room_id, status, metadata_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.project_id,
                    project.name,
                    project.room_id,
                    project.status,
                    json.dumps(
                        project.metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    project.created_at.isoformat(),
                    project.updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return project

    async def get(self, project_id: str) -> ProjectRecord | None:
        def read(connection: sqlite3.Connection) -> ProjectRecord | None:
            row = connection.execute(
                "SELECT * FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            return _project_from_row(row) if row else None

        return await self._database.read(read)

    async def list_all(self) -> tuple[ProjectRecord, ...]:
        """Return every durable project in stable creation order."""

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectRecord, ...]:
            rows = connection.execute(
                """
                SELECT * FROM projects
                 ORDER BY created_at, project_id
                """,
            ).fetchall()
            return tuple(_project_from_row(row) for row in rows)

        return await self._database.read(read)

    async def update(
        self,
        project_id: str,
        *,
        expected: set[str],
        status: str,
        room_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ProjectRecord | None:
        if not expected:
            raise ValueError("expected project statuses must not be empty")

        def write(connection: sqlite3.Connection) -> ProjectRecord | None:
            placeholders = ",".join("?" for _ in expected)
            cursor = connection.execute(
                f"""
                UPDATE projects
                   SET status=?,
                       room_id=COALESCE(?, room_id),
                       metadata_json=COALESCE(?, metadata_json),
                       updated_at=?
                 WHERE project_id=?
                   AND status IN ({placeholders})
                """,
                (
                    status,
                    room_id,
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
                    project_id,
                    *sorted(expected),
                ),
            )
            if cursor.rowcount == 0:
                return None
            row = connection.execute(
                "SELECT * FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            return _project_from_row(row)

        return await self._database.write(write)

    async def list_active(self) -> tuple[ProjectRecord, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectRecord, ...]:
            rows = connection.execute(
                """
                SELECT * FROM projects
                 WHERE status IN ('planning', 'active')
                 ORDER BY created_at, project_id
                """,
            ).fetchall()
            return tuple(_project_from_row(row) for row in rows)

        return await self._database.read(read)
