"""Small asynchronous boundary around standard-library SQLite.

用标准库 SQLite 为异步 Manager 提供小型事务边界。

SQLite 调用会阻塞线程，所以每次读写通过 ``asyncio.to_thread`` 放到工作线程，并使用
独立连接；一个 callback 对应一个提交或回滚的事务。WAL 允许读写更好地并存，但这里
仍遵守单 Manager writer 架构。启动时幂等建表和迁移；备份使用 SQLite backup API，
不能直接复制仍有 WAL 的数据库文件。
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from .schema import (
    MATRIX_EVENT_MIGRATION_COLUMNS,
    PROJECT_DECISION_MIGRATION_COLUMNS,
    SCHEMA_SQL,
    SCHEMA_VERSION,
    SESSION_SETTINGS_MIGRATION_COLUMNS,
)

T = TypeVar("T")


class Database:
    """为每个工作线程事务打开独立 SQLite 连接。

    ``read``/``write`` 的 callback 在后台线程执行，不能在其中 ``await``；离开连接的
    context manager 时，成功路径提交，异常路径回滚。连接不跨线程共享，避免标准库
    sqlite3 的线程限制和长生命周期连接状态泄漏。
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    async def open(self) -> None:
        """Create the database and apply the initial idempotent schema."""

        def run() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as connection:
                current = connection.execute(
                    "PRAGMA user_version",
                ).fetchone()[0]
                if current > SCHEMA_VERSION:
                    raise RuntimeError(
                        "database schema is newer than this Manager "
                        f"({current} > {SCHEMA_VERSION})",
                    )
                connection.executescript(SCHEMA_SQL)
                columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(session_settings)",
                    )
                }
                for name, declaration in (
                    SESSION_SETTINGS_MIGRATION_COLUMNS.items()
                ):
                    if name not in columns:
                        connection.execute(
                            "ALTER TABLE session_settings "
                            f"ADD COLUMN {name} {declaration}",
                        )
                matrix_event_columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(processed_matrix_events)",
                    )
                }
                for name, declaration in (
                    MATRIX_EVENT_MIGRATION_COLUMNS.items()
                ):
                    if name not in matrix_event_columns:
                        connection.execute(
                            "ALTER TABLE processed_matrix_events "
                            f"ADD COLUMN {name} {declaration}",
                        )
                project_decision_columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(project_decisions)",
                    )
                }
                for name, declaration in (
                    PROJECT_DECISION_MIGRATION_COLUMNS.items()
                ):
                    if name not in project_decision_columns:
                        connection.execute(
                            "ALTER TABLE project_decisions "
                            f"ADD COLUMN {name} {declaration}",
                        )
                # CREATE INDEX in SCHEMA_SQL runs before ALTER TABLE on an
                # upgraded database, so create the recovery index only after
                # the migration columns exist.
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                      processed_matrix_events_recovery_idx
                    ON processed_matrix_events(
                      status, next_attempt_at, updated_at
                    )
                    """,
                )
                connection.execute(
                    f"PRAGMA user_version={SCHEMA_VERSION}",
                )

        await asyncio.to_thread(run)

    async def read(
        self,
        callback: Callable[[sqlite3.Connection], T],
    ) -> T:
        """Run a read callback on a dedicated thread and connection."""

        def run() -> T:
            with self._connect() as connection:
                return callback(connection)

        return await asyncio.to_thread(run)

    async def write(
        self,
        callback: Callable[[sqlite3.Connection], T],
    ) -> T:
        """把一个同步 callback 作为完整事务执行。

        callback 中所有 SQL 要么一起提交，要么在异常时一起回滚。调用方不应在 callback
        内执行网络 I/O，否则会长时间占用写锁；跨系统效果由 workflow 在事务外按
        “先记录意图、再执行效果、最后记录回执”的顺序协调。
        """

        def run() -> T:
            with self._connect() as connection:
                return callback(connection)

        return await asyncio.to_thread(run)

    async def backup_to(self, destination: Path) -> None:
        """Create a transactionally consistent SQLite backup."""

        def run() -> None:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as source:
                source.execute("PRAGMA wal_checkpoint(PASSIVE)")
                with sqlite3.connect(destination) as target:
                    source.backup(target)

        await asyncio.to_thread(run)

    async def replace_from(self, source: Path) -> None:
        """Replace local contents with a verified SQLite database."""

        def run() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(source) as source_connection:
                with self._connect() as target:
                    source_connection.backup(target)

        await asyncio.to_thread(run)

    async def close(self) -> None:
        """Connections are transaction-scoped, so shutdown needs no action."""
