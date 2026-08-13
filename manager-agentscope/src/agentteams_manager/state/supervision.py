"""Durable progress-observation state for proactive heartbeat checks.

保存 heartbeat 观察任务进展时使用的游标和告警节流状态。

Supervisor 需要知道上次见到的进度、连续无变化次数和最近告警时间，重启后才能继续
判断“真的卡住”而非把所有任务都当成新任务。该表只保存观察结果，不直接改变 Task
状态；任务完成、阻塞或返修仍须经过对应 workflow 的证据检查。
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from .database import Database


class SupervisionStateRepository:
    """Count consecutive heartbeat cycles without observable progress."""

    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存监督心跳计数器使用的数据库；构造过程不记录 ping，连续无进展次数只由 record_ping 的原子事务推进。
        self._database = database

    async def record_ping(
        self,
        *,
        subject_key: str,
        observed_token: str,
        pinged_at: datetime,
    ) -> int:
        # 逻辑说明：先拒绝无时区时间，再在单一事务比较本次与上次进度 token；相同则累计无进展次数，变化则清零，并返回新的计数供告警节流。
        if pinged_at.tzinfo is None or pinged_at.utcoffset() is None:
            raise ValueError("pinged_at must be timezone-aware")

        def write(connection: sqlite3.Connection) -> int:
            # 逻辑说明：读取旧观察值、计算 missed_cycles 并原子 upsert 新游标；事务失败不留下已递增但未保存的半状态。
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
