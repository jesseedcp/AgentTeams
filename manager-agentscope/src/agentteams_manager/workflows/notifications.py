"""Exactly-once Matrix notifications and append-once daily memory."""

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
        self._storage = storage
        self._clock = clock

    async def append_once(
        self,
        *,
        operation_id: str,
        entry: str,
        day: date | None = None,
    ) -> MemoryReceipt:
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
    ) -> None:
        self._notifications = notifications
        self._resolver = resolver
        self._matrix = matrix
        self._supervisor = supervisor
        self._memory = memory
        self._clock = clock
        self._admin_user_id = admin_user_id

    async def resolve_room(self) -> str:
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
        if operation.kind is not OperationKind.SEND_NOTIFICATION:
            raise ValueError("operation is not a Matrix notification")
        return await self._deliver(operation)

    async def _deliver(
        self,
        operation: OperationRecord,
    ) -> NotificationReceipt:
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
        return _notification_receipt(record)

    async def send_completion(
        self,
        *,
        operation_id: str,
        task: TaskRecord,
        summary: str,
    ) -> NotificationReceipt:
        text = (
            f"[Task Completed] {task.task_id}: {task.title} — "
            f"assigned to {task.assigned_to}. {summary}"
        )
        await self._memory.append_once(
            operation_id=operation_id,
            entry=text,
        )
        return await self.send_once(
            source_operation_id=operation_id,
            text=text,
        )

    async def already_sent(self, operation_id: str) -> bool:
        record = await self._notifications.get_by_source(
            _notification_source_key(operation_id),
        )
        return record is not None and record.status == "sent"

    async def send_terminal_failure(self, operation_id: str) -> None:
        await self.send_once(
            source_operation_id=operation_id,
            text=f"[Manager Operation Failed] {operation_id}",
        )


def _notification_id(source_operation_id: str) -> str:
    return hashlib.sha256(
        f"notification\0{source_operation_id}".encode("utf-8"),
    ).hexdigest()[:32]


def _notification_source_key(source_operation_id: str) -> str:
    if len(source_operation_id) == 32:
        return source_operation_id
    return hashlib.sha256(
        f"notification-source\0{source_operation_id}".encode("utf-8"),
    ).hexdigest()[:32]


def _notification_intent(
    operation: OperationRecord,
) -> dict[str, str]:
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
    if record.event_id is None:
        raise ConflictError("notification has no Matrix event")
    return NotificationReceipt(
        notification_id=record.notification_id,
        source_operation_id=record.source_operation_id,
        room_id=record.room_id,
        txn_id=record.txn_id,
        event_id=record.event_id,
    )
