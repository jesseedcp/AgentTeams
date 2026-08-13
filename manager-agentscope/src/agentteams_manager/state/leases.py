"""SQLite materialization of authoritative remote processing leases.

把远端 processing lease 的当前状态物化到 SQLite。

Git 或 Coding CLI 会改动共享 workspace，同一时刻只能有一个持有者。远端对象存储中的
lease 是权威事实，本地表保存便于恢复和查询的镜像；租约含 owner、期限和 generation，
过期回收也必须比较这些字段，不能仅凭本地时间删除另一个进程刚续租的 lease。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from agentteams_manager.domain.models import ProcessingLeaseRecord

from .database import Database


def _lease_from_row(row: sqlite3.Row) -> ProcessingLeaseRecord:
    return ProcessingLeaseRecord(
        task_id=row["task_id"],
        lease_id=row["lease_id"],
        processor=row["processor"],
        operation=row["operation"],
        started_at=row["started_at"],
        expires_at=row["expires_at"],
        remote_etag=row["remote_etag"],
        updated_at=row["updated_at"],
    )


class LeaseRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def get(self, task_id: str) -> ProcessingLeaseRecord | None:
        def read(
            connection: sqlite3.Connection,
        ) -> ProcessingLeaseRecord | None:
            row = connection.execute(
                "SELECT * FROM processing_leases WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _lease_from_row(row) if row else None

        return await self._database.read(read)

    async def upsert(
        self,
        lease: ProcessingLeaseRecord,
    ) -> ProcessingLeaseRecord:
        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO processing_leases(
                    task_id, lease_id, processor, operation, started_at,
                    expires_at, remote_etag, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    lease_id=excluded.lease_id,
                    processor=excluded.processor,
                    operation=excluded.operation,
                    started_at=excluded.started_at,
                    expires_at=excluded.expires_at,
                    remote_etag=excluded.remote_etag,
                    updated_at=excluded.updated_at
                """,
                (
                    lease.task_id,
                    lease.lease_id,
                    lease.processor,
                    lease.operation,
                    lease.started_at.isoformat(),
                    lease.expires_at.isoformat(),
                    lease.remote_etag,
                    lease.updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return lease

    async def delete(self, task_id: str, lease_id: str) -> bool:
        def write(connection: sqlite3.Connection) -> bool:
            cursor = connection.execute(
                """
                DELETE FROM processing_leases
                 WHERE task_id=? AND lease_id=?
                """,
                (task_id, lease_id),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)

    async def expired(
        self,
        now: datetime,
    ) -> tuple[ProcessingLeaseRecord, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProcessingLeaseRecord, ...]:
            rows = connection.execute(
                """
                SELECT * FROM processing_leases
                 WHERE expires_at <= ?
                 ORDER BY expires_at, task_id
                """,
                (now.isoformat(),),
            ).fetchall()
            return tuple(_lease_from_row(row) for row in rows)

        return await self._database.read(read)
