"""Exactly-once notification materialization.

物化通知的计划、发送中和已发送状态，实现效果上的 exactly-once。

Matrix 发送超时时，消息可能已被 homeserver 接收。仓库先保存稳定 notification ID 与
transaction ID，再记录回执；恢复时使用相同 transaction ID 查询或重发，Matrix 会
按幂等键去重。这里的 exactly-once 指可观察消息不重复，并不假设网络调用只发生一次。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import NotificationRecord

from .database import Database


def _notification_from_row(row: sqlite3.Row) -> NotificationRecord:
    # 逻辑说明：把 SQLite 行完整还原为不可变通知记录，供所有查询与状态迁移共享同一字段映射；缺字段会立即暴露 schema 不一致。
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
        # 逻辑说明：保存通知 outbox 的数据库边界；此时不投递或领取通知，避免应用装配阶段产生外部消息副作用。
        self._database = database

    async def get(
        self,
        notification_id: str,
    ) -> NotificationRecord | None:
        # 逻辑说明：在独立读事务按稳定 notification ID 查询并返回记录；不存在返回 None，数据库错误由上层处理。
        def read(
            connection: sqlite3.Connection,
        ) -> NotificationRecord | None:
            # 逻辑说明：执行单条参数化查询并把命中行转换为领域对象，不产生写入副作用。
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
        # 逻辑说明：按来源 operation ID 查找已物化通知，用于恢复时复用相同 transaction ID，避免同一操作产生两条消息。
        def read(
            connection: sqlite3.Connection,
        ) -> NotificationRecord | None:
            # 逻辑说明：在当前读连接执行来源唯一键查询，命中时返回统一的通知记录。
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
        # 逻辑说明：在一个写事务中按来源操作幂等插入，再读取实际记录并核对内容身份；重复同内容返回原记录，复用来源但内容不同则冲突回滚。
        def write(connection: sqlite3.Connection) -> NotificationRecord:
            # 逻辑说明：用 INSERT OR IGNORE 抢占稳定通知身份并在同一事务校验所有去重字段，避免并发重试改变收件人或正文。
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
        # 逻辑说明：在事务中把通知标记为 sent，并用 COALESCE 保留首次 Matrix 回执；不存在时报 KeyError，回执不同时报冲突，防止重试掩盖重复发送。
        def write(connection: sqlite3.Connection) -> NotificationRecord:
            # 逻辑说明：更新后立即回读并核对 event ID，使状态迁移与一致性验证在同一 SQLite 事务内完成。
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
