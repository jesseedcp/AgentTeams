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
        # 逻辑说明：只保存 SQLite 文件路径；连接、WAL/外键设置和事务边界延迟到 initialize/read/write，便于启动前安全组装依赖。
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        # 逻辑说明：为当前事务创建独立连接，并统一启用行对象、外键、WAL 与忙等待；连接失败直接抛出，让调用层决定是否重试。
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    async def open(self) -> None:
        """Create the database and apply the initial idempotent schema."""
        # 逻辑说明：在线程中幂等创建目录、表和缺失迁移列，拒绝比程序更新的数据库版本；成功写入 schema 版本，任一步失败都由 SQLite 回滚并向上抛出。

        def run() -> None:
            # 逻辑说明：这个同步闭包独占一次 SQLite 连接完成版本检查和全部 DDL；它由 open 放入工作线程，避免阻塞异步事件循环。
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
        # 逻辑说明：把调用方的同步查询交给工作线程和独立连接，等待并返回 callback 的结果；查询异常原样传播，不在仓储层吞掉或重试。

        def run() -> T:
            # 逻辑说明：在连接上下文中执行一次同步查询并把结果带回事件循环；上下文负责释放连接。
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
        # 逻辑说明：把同步写 callback 放到工作线程的一次连接事务中并返回结果；正常退出提交，异常退出回滚且错误向上传播，重试由更上层按幂等规则决定。

        def run() -> T:
            # 逻辑说明：连接上下文界定完整事务边界，确保 callback 中的多条 SQL 要么全部提交、要么全部撤销。
            with self._connect() as connection:
                return callback(connection)

        return await asyncio.to_thread(run)

    async def backup_to(self, destination: Path) -> None:
        """Create a transactionally consistent SQLite backup."""
        # 逻辑说明：在工作线程创建目标目录，先做被动 WAL 检查点，再用 SQLite backup API 生成一致快照；I/O 或数据库错误直接返回调用方处理。

        def run() -> None:
            # 逻辑说明：同步完成检查点和数据库级复制，避免直接复制带 WAL 的文件得到不一致备份。
            destination.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as source:
                source.execute("PRAGMA wal_checkpoint(PASSIVE)")
                with sqlite3.connect(destination) as target:
                    source.backup(target)

        await asyncio.to_thread(run)

    async def replace_from(self, source: Path) -> None:
        """Replace local contents with a verified SQLite database."""
        # 逻辑说明：在工作线程打开来源库并通过 backup API 覆盖本地数据库；复制保持 SQLite 页面一致性，失败时异常向上抛出而不会伪报恢复成功。

        def run() -> None:
            # 逻辑说明：同步打开来源与目标连接并执行数据库级复制，连接上下文负责提交或在异常时回滚。
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(source) as source_connection:
                with self._connect() as target:
                    source_connection.backup(target)

        await asyncio.to_thread(run)

    async def close(self) -> None:
        """Connections are transaction-scoped, so shutdown needs no action."""
        # 逻辑说明：连接都由每次 read/write 的上下文及时关闭，因此这里只提供统一生命周期接口，不产生额外状态或外部副作用。
