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
    # 逻辑说明：把本地 lease 镜像行还原为领域记录；保留远端 ETag 与期限，后续释放和恢复才能防止误删已续租的新 generation。
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
        # 逻辑说明：保存本地处理租约镜像所用数据库；构造时不领取远端 lease，真实读取与写入由各异步方法在事务中完成。
        self._database = database

    async def get(self, task_id: str) -> ProcessingLeaseRecord | None:
        # 逻辑说明：在只读事务按 task ID 返回当前本地租约镜像；不存在返回 None，本地记录不替代远端权威判断。
        def read(
            connection: sqlite3.Connection,
        ) -> ProcessingLeaseRecord | None:
            # 逻辑说明：执行参数化单行查询并转换结果，不修改租约状态。
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
        # 逻辑说明：在一个事务中按 task ID 插入或覆盖远端租约镜像，保存 owner、到期时间和 ETag；成功后返回输入记录，失败则回滚并交由调用方重试。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：用数据库 upsert 原子替换整组租约字段，避免读后写竞争留下混合 generation。
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
        # 逻辑说明：只在 task ID 与 lease ID 同时匹配时删除本地镜像，返回是否真正删除；旧持有者因此不能清掉后来续租的新租约。
        def write(connection: sqlite3.Connection) -> bool:
            # 逻辑说明：条件删除并以影响行数报告结果，整个检查和删除由一条 SQL 原子完成。
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
        # 逻辑说明：读取本地已到期租约并按期限稳定排序，供恢复流程逐个与远端权威状态对账；这里只筛选，不直接释放租约。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProcessingLeaseRecord, ...]:
            # 逻辑说明：在同一快照查询截止时间之前的行并批量转换为领域记录。
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
