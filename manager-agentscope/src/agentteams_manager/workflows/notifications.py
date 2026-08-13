"""Exactly-once Matrix notifications and append-once daily memory.

发送可恢复的 Matrix 通知，并把重要结果 append-once 到每日记忆。

workflow 先在 SQLite 保存通知 intent 和稳定 Matrix transaction ID，再发送消息并记录
event ID。超时进入恢复查询，不能换 ID 直接再发。通知成功后才追加对应 daily memory，
来源键确保重放不会留下重复记录，从而让页面消息与本地记忆最终收敛。
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from agentteams_manager.clients.minio import ObjectVersionConflict
from agentteams_manager.domain.errors import ConflictError, RecoveryError
from agentteams_manager.domain.ids import matrix_transaction_id
from agentteams_manager.domain.models import (
    ExternalEffect,
    NotificationRecord,
    OperationKind,
    OperationRecord,
    OperationStatus,
    TaskRecord,
)
from agentteams_manager.domain.ports import ArtifactPort, Clock, MatrixPort
from agentteams_manager.workflows.tasks import TaskSupervisorPort


class NotificationRepositoryPort(Protocol):
    async def get_by_source(
        self,
        source_operation_id: str,
    ) -> NotificationRecord | None: ...

    async def prepare(
        self,
        record: NotificationRecord,
    ) -> NotificationRecord: ...

    async def mark_sent(
        self,
        notification_id: str,
        *,
        event_id: str,
        sent_at: datetime,
    ) -> NotificationRecord: ...


class NotificationRoomResolver(Protocol):
    async def notification_room(self, *, recipient: str) -> str: ...


class CuratedDailyMemory(Protocol):
    async def append_daily(
        self,
        *,
        room_id: str,
        content: str,
        source_event_id: str,
        now: datetime,
    ) -> object: ...


class NotificationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    notification_id: str
    source_operation_id: str
    room_id: str
    txn_id: str
    event_id: str


class MemoryReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str
    entry_key: str
    daily_key: str
    daily_etag: str


class DailyMemory:
    """Append a human-readable line exactly once per durable operation."""

    def __init__(self, *, storage: ArtifactPort, clock: Clock) -> None:
        # 逻辑说明：`__init__` 校验并保存 `storage`、`clock`，为幂等通知建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
        self._storage = storage
        self._clock = clock

    async def append_once(
        self,
        *,
        operation_id: str,
        entry: str,
        day: date | None = None,
    ) -> MemoryReceipt:
        # 逻辑说明：`append_once` 接收 `operation_id`、`entry`、`day`，追加 once，核心调用为 `join`、`split`、`ValueError`，返回 `MemoryReceipt`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        normalized = " ".join(entry.split())
        if not normalized:
            raise ValueError("memory entry must not be empty")
        memory_day = day or self._clock.now().astimezone(UTC).date()
        day_text = memory_day.isoformat()
        entry_key = (
            f"manager/memory/entries/{day_text}/{operation_id}.json"
        )
        entry_value = {
            "schema_version": 1,
            "operation_id": operation_id,
            "date": day_text,
            "entry": normalized,
            "created_at": self._clock.now().astimezone(UTC).isoformat(),
        }
        existing_entry = await self._storage.head(entry_key)
        if existing_entry is None:
            try:
                await self._storage.put_json_if_version(
                    entry_key,
                    entry_value,
                    expected_etag=None,
                )
            except ObjectVersionConflict:
                pass
        persisted_entry = await self._storage.get_json(entry_key)
        if (
            not isinstance(persisted_entry, dict)
            or persisted_entry.get("operation_id") != operation_id
            or persisted_entry.get("entry") != normalized
        ):
            raise ConflictError(
                f"memory entry {entry_key} has incompatible contents",
            )

        daily_key = f"manager/memory/{day_text}.md"
        marker = f"<!-- agentteams-operation:{operation_id} -->"
        block = f"{marker}\n- {normalized}\n"
        for _ in range(4):
            current = await self._storage.head(daily_key)
            if current is None:
                current_text = ""
                expected_etag = None
            else:
                current_text = (
                    await self._storage.get_bytes(daily_key)
                ).decode("utf-8")
                expected_etag = current.etag
            if marker in current_text:
                if current is None:
                    raise ConflictError("daily memory marker has no object")
                return MemoryReceipt(
                    operation_id=operation_id,
                    entry_key=entry_key,
                    daily_key=daily_key,
                    daily_etag=current.etag,
                )
            target = (
                current_text.rstrip() + "\n\n" + block
                if current_text.strip()
                else f"# {day_text}\n\n{block}"
            ).encode("utf-8")
            try:
                receipt = await self._storage.put_bytes_if_version(
                    daily_key,
                    target,
                    expected_etag=expected_etag,
                    content_type="text/markdown",
                )
                return MemoryReceipt(
                    operation_id=operation_id,
                    entry_key=entry_key,
                    daily_key=daily_key,
                    daily_etag=receipt.etag,
                )
            except ObjectVersionConflict:
                continue
        raise ConflictError("daily memory changed too often to append safely")


class NotificationService:
    """Resolve one Matrix room, then reuse one transaction until confirmed."""

    def __init__(
        self,
        *,
        notifications: NotificationRepositoryPort,
        resolver: NotificationRoomResolver,
        matrix: MatrixPort,
        supervisor: TaskSupervisorPort,
        memory: DailyMemory,
        clock: Clock,
        admin_user_id: str,
        curated_memory: CuratedDailyMemory | None = None,
    ) -> None:
        # 逻辑说明：`__init__` 校验并保存 `notifications`、`resolver`、`matrix`、`supervisor`、`memory`、`clock`、`admin_user_id`、`curated_memory`，为幂等通知建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
        self._notifications = notifications
        self._resolver = resolver
        self._matrix = matrix
        self._supervisor = supervisor
        self._memory = memory
        self._clock = clock
        self._admin_user_id = admin_user_id
        self._curated_memory = curated_memory

    async def resolve_room(self) -> str:
        # 逻辑说明：`resolve_room` 让 ChannelResolver 为管理员用户挑选仍可用的通知房间并返回 room_id；它只解析路由，不创建房间或发送正文。
        return await self._resolver.notification_room(
            recipient=self._admin_user_id,
        )

    async def send_confirmation_request(
        self,
        *,
        confirmation_id: str,
        text: str,
    ) -> NotificationReceipt:
        """Deliver one approval prompt idempotently to the admin channel."""
        # 逻辑说明：`send_confirmation_request` 接收 `confirmation_id`、`text`，发送 confirmation request，核心调用为 `send_once`，返回 `NotificationReceipt`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self.send_once(
            source_operation_id=f"confirmation:{confirmation_id}",
            text=text,
        )

    async def send_once(
        self,
        *,
        source_operation_id: str,
        text: str,
    ) -> NotificationReceipt:
        # 逻辑说明：`send_once` 接收 `source_operation_id`、`text`，发送 once，核心调用为 `_notification_id`、`_notification_source_key`、`get_by_source`，返回 `NotificationReceipt`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        notification_id = _notification_id(source_operation_id)
        source_key = _notification_source_key(source_operation_id)
        existing = await self._notifications.get_by_source(
            source_key,
        )
        room_id = existing.room_id if existing else await self.resolve_room()
        txn_id = matrix_transaction_id(notification_id, 0)
        operation = await self._supervisor.begin(
            operation_id=notification_id,
            kind=OperationKind.SEND_NOTIFICATION,
            target_key=f"matrix-notification/{room_id}",
            request={
                "source_operation_id": source_operation_id,
                "recipient": self._admin_user_id,
                "room_id": room_id,
                "text": text,
                "txn_id": txn_id,
            },
        )
        return await self._deliver(operation)

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> NotificationReceipt:
        """Resume one journal-restored Matrix notification intent."""
        # 逻辑说明：`resume_operation` 从持久化 operation/request 重建幂等通知上下文，通过 `ValueError`、`_deliver` 证明或补做下一阶段，最终返回 `NotificationReceipt`；字段缺失、状态冲突或效果不可证明时保持失败/歧义状态而不是重复执行。
        if operation.kind is not OperationKind.SEND_NOTIFICATION:
            raise ValueError("operation is not a Matrix notification")
        return await self._deliver(operation)

    async def _deliver(
        self,
        operation: OperationRecord,
    ) -> NotificationReceipt:
        # 逻辑说明：`_deliver` 接收 `operation`，投递 幂等通知，核心调用为 `_notification_intent`、`_notification_source_key`、`prepare`，返回 `NotificationReceipt`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        intent = _notification_intent(operation)
        notification_id = operation.operation_id
        source_operation_id = _notification_source_key(
            intent["source_operation_id"],
        )
        recipient = intent["recipient"]
        room_id = intent["room_id"]
        text = intent["text"]
        txn_id = intent["txn_id"]
        record = await self._notifications.prepare(
            NotificationRecord(
                notification_id=notification_id,
                source_operation_id=source_operation_id,
                recipient=recipient,
                room_id=room_id,
                text=text,
                txn_id=txn_id,
                status="prepared",
                created_at=operation.created_at,
            ),
        )
        if operation.status is OperationStatus.SUCCEEDED:
            event_id = operation.result.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise RecoveryError(
                    "succeeded notification has no Matrix event receipt",
                )
            record = await self._notifications.mark_sent(
                notification_id,
                event_id=event_id,
                sent_at=self._clock.now().astimezone(UTC),
            )
            await self._remember_delivery(record)
            return _notification_receipt(record)
        if record.status == "sent":
            if record.event_id is None:
                raise ConflictError("sent notification has no Matrix event")
            if operation.status is not OperationStatus.SUCCEEDED:
                if operation.status is OperationStatus.PLANNED:
                    await self._supervisor.before_effect(
                        notification_id,
                        ExternalEffect.MATRIX,
                        {"operation": "recover_sent_notification"},
                    )
                await self._supervisor.effect_succeeded(
                    notification_id,
                    ExternalEffect.MATRIX,
                    {
                        "event_id": record.event_id,
                        "room_id": room_id,
                        "txn_id": txn_id,
                    },
                )
            await self._remember_delivery(record)
            return _notification_receipt(record)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError("notification operation previously failed")
        await self._supervisor.before_effect(
            notification_id,
            ExternalEffect.MATRIX,
            {
                "operation": "send_notification",
                "room_id": room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                room_id,
                text,
                txn_id=txn_id,
                mentions=(recipient,),
            )
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                notification_id,
                ExternalEffect.MATRIX,
                type(exc).__name__,
            )
            raise
        record = await self._notifications.mark_sent(
            notification_id,
            event_id=event_id,
            sent_at=self._clock.now().astimezone(UTC),
        )
        await self._supervisor.effect_succeeded(
            notification_id,
            ExternalEffect.MATRIX,
            {
                "event_id": event_id,
                "room_id": room_id,
                "txn_id": txn_id,
            },
        )
        await self._remember_delivery(record)
        return _notification_receipt(record)

    async def _remember_delivery(
        self,
        record: NotificationRecord,
    ) -> None:
        # 逻辑说明：`_remember_delivery` 接收 `record`，写入记忆 delivery，核心调用为 `append_daily`、`astimezone`、`now`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        if self._curated_memory is None:
            return
        await self._curated_memory.append_daily(
            room_id=record.room_id,
            content=record.text,
            source_event_id=f"notification:{record.notification_id}",
            now=self._clock.now().astimezone(UTC),
        )

    async def send_completion(
        self,
        *,
        operation_id: str,
        task: TaskRecord,
        summary: str,
    ) -> NotificationReceipt:
        # 逻辑说明：`send_completion` 接收 `operation_id`、`task`、`summary`，发送 completion，核心调用为 `append_once`、`_typed_notification_source`、`send_once`，返回 `NotificationReceipt`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        text = (
            f"[Task Completed] {task.task_id}: {task.title} — "
            f"assigned to {task.assigned_to}. {summary}"
        )
        await self._memory.append_once(
            operation_id=operation_id,
            entry=text,
        )
        source_operation_id = await self._typed_notification_source(
            operation_id=operation_id,
            notification_kind="completion",
            text=text,
        )
        return await self.send_once(
            source_operation_id=source_operation_id,
            text=text,
        )

    async def already_sent(self, operation_id: str) -> bool:
        # 逻辑说明：`already_sent` 由 operation_id 生成稳定 source key 并查询通知仓库，返回是否已有投递记录；这是幂等性只读检查，不会再次发送 Matrix 消息。
        for source_operation_id in (
            f"failure:{operation_id}",
            operation_id,
        ):
            record = await self._notifications.get_by_source(
                _notification_source_key(source_operation_id),
            )
            if record is not None and record.status == "sent":
                return True
        return False

    async def send_terminal_failure(self, operation_id: str) -> None:
        # 逻辑说明：`send_terminal_failure` 接收 `operation_id`，发送 terminal failure，核心调用为 `_typed_notification_source`、`send_once`，返回 `None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        text = f"[Manager Operation Failed] {operation_id}"
        source_operation_id = await self._typed_notification_source(
            operation_id=operation_id,
            notification_kind="failure",
            text=text,
        )
        await self.send_once(
            source_operation_id=source_operation_id,
            text=text,
        )

    async def _typed_notification_source(
        self,
        *,
        operation_id: str,
        notification_kind: str,
        text: str,
    ) -> str:
        """Keep legacy intents reusable without conflating final outcomes."""

        # 逻辑说明：`_typed_notification_source` 接收 `operation_id`、`notification_kind`、`text`，处理 notification source，核心调用为 `get_by_source`、`_notification_source_key`，返回 `str`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        legacy = await self._notifications.get_by_source(
            _notification_source_key(operation_id),
        )
        if legacy is not None and legacy.text == text:
            return operation_id
        return f"{notification_kind}:{operation_id}"


def _notification_id(source_operation_id: str) -> str:
    # 逻辑说明：`_notification_id` 从稳定输入生成可重复的 id，返回 `str` 供幂等写入/发送使用；同一输入得到同一标识，不产生外部副作用。
    return hashlib.sha256(
        f"notification\0{source_operation_id}".encode("utf-8"),
    ).hexdigest()[:32]


def _notification_source_key(source_operation_id: str) -> str:
    # 逻辑说明：`_notification_source_key` 从稳定输入生成可重复的 source key，返回 `str` 供幂等写入/发送使用；同一输入得到同一标识，不产生外部副作用。
    if len(source_operation_id) == 32:
        return source_operation_id
    return hashlib.sha256(
        f"notification-source\0{source_operation_id}".encode("utf-8"),
    ).hexdigest()[:32]


def _notification_intent(
    operation: OperationRecord,
) -> dict[str, str]:
    # 逻辑说明：`_notification_intent` 从 operation.request 取出 source_operation_id、room_id、text 与 transaction_id，补算稳定 notification_id 并返回投递意图；缺字段时抛 RecoveryError，禁止猜测收件房间。
    required = (
        "source_operation_id",
        "recipient",
        "room_id",
        "text",
        "txn_id",
    )
    intent: dict[str, str] = {}
    for key in required:
        value = operation.request.get(key)
        if not isinstance(value, str) or not value:
            raise RecoveryError(
                f"notification operation has invalid {key}",
            )
        intent[key] = value
    expected_id = _notification_id(intent["source_operation_id"])
    if operation.operation_id != expected_id:
        raise RecoveryError("notification operation ID does not match source")
    if operation.target_key != (
        f"matrix-notification/{intent['room_id']}"
    ):
        raise RecoveryError("notification operation room does not match target")
    if intent["txn_id"] != matrix_transaction_id(operation.operation_id, 0):
        raise RecoveryError(
            "notification operation has an unstable transaction ID",
        )
    return intent


def _notification_receipt(
    record: NotificationRecord,
) -> NotificationReceipt:
    # 逻辑说明：`_notification_receipt` 从 `record` 构造 `NotificationReceipt`，统一调用方看到的幂等通知结果；它只转换数据，不执行远端 I/O。
    if record.event_id is None:
        raise ConflictError("notification has no Matrix event")
    return NotificationReceipt(
        notification_id=record.notification_id,
        source_operation_id=record.source_operation_id,
        room_id=record.room_id,
        txn_id=record.txn_id,
        event_id=record.event_id,
    )
