"""Restore SQLite and replay remote intent before normal operation.

在接收新 Matrix 消息前恢复 SQLite，并重放远端操作意图。

若 PVC 中已有有效数据库，优先从其已应用 sequence 继续；本地状态丢失时则下载最新
MinIO 快照，再按顺序重放后续 journal。重放只恢复确定性状态和待对账操作，不把超时
自动判成失败。application 必须等这里完成后才启动 Matrix，否则旧操作与新指令可能
交叉执行。
"""

from __future__ import annotations

import inspect
import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path

from agentteams_manager.domain.models import JournalEvent, RecoveryReport

from .database import Database
from .journal import S3Journal

ReplayEvent = Callable[[JournalEvent], Awaitable[None] | None]


class RecoveryCoordinator:
    """选择可信恢复起点，并按 sequence 重放其后的不可变事件。"""
    def __init__(
        self,
        *,
        database: Database,
        journal: S3Journal,
        replay_event: ReplayEvent,
        temp_directory: Path,
        prefer_local_database: bool = False,
    ) -> None:
        self._database = database
        self._journal = journal
        self._replay_event = replay_event
        self._temp_directory = temp_directory
        self._preserve_local_database = (
            prefer_local_database
            and database.path.is_file()
            and database.path.stat().st_size > 0
        )

    async def restore(self) -> RecoveryReport:
        """Keep a valid PVC database, or restore MinIO after local loss."""
        snapshot_sequence = 0
        if self._preserve_local_database:
            snapshot_sequence = await self._local_applied_sequence()
        else:
            snapshot = await self._journal.download_latest_snapshot()
            if snapshot is not None:
                metadata, data = snapshot
                self._temp_directory.mkdir(parents=True, exist_ok=True)
                path = self._temp_directory / "manager-restore.db"
                path.write_bytes(data)
                await self._database.replace_from(path)
                snapshot_sequence = metadata.sequence

        events = await self._journal.list_after(snapshot_sequence)
        for event in events:
            result = self._replay_event(event)
            if inspect.isawaitable(result):
                await result

        return RecoveryReport(
            snapshot_sequence=snapshot_sequence,
            replayed_events=len(events),
        )

    async def _local_applied_sequence(self) -> int:
        def read(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                """
                SELECT value FROM key_values
                 WHERE key='journal_applied_sequence'
                """,
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)
