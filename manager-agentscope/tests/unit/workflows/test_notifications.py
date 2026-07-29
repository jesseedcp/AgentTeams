from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.notifications import NotificationRepository
from agentteams_manager.workflows.matrix_resources import ChannelResolver
from agentteams_manager.workflows.notifications import (
    DailyMemory,
    NotificationService,
)
from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import FixedClock, TaskSupervisor


class Channels:
    async def primary_channel(self, user_id: str) -> str | None:
        del user_id
        return "!primary:example"

    async def trusted_channels(self, user_id: str) -> tuple[str, ...]:
        del user_id
        return ()


class Matrix:
    def __init__(self) -> None:
        self.visible: dict[str, str] = {}
        self.attempts: list[str] = []

    async def joined_rooms(self) -> tuple[str, ...]:
        return ("!primary:example", "!admin:example")

    async def members(self, room_id: str) -> tuple[str, ...]:
        del room_id
        return ("@admin:example", "@manager:example")

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        del room_id, text, thread_id, mentions
        self.attempts.append(txn_id)
        return self.visible.setdefault(txn_id, "$notification")


@pytest.mark.asyncio
async def test_send_once_resolves_primary_and_deduplicates(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    storage = MinioClient(FakeS3(), bucket="agentteams")
    matrix = Matrix()
    clock = FixedClock()
    service = NotificationService(
        notifications=NotificationRepository(database),
        resolver=ChannelResolver(
            channels=Channels(),
            matrix=matrix,
            manager_admin_room="!admin:example",
        ),
        matrix=matrix,
        supervisor=TaskSupervisor(clock),
        memory=DailyMemory(storage=storage, clock=clock),
        clock=clock,
        admin_user_id="@admin:example",
    )

    first = await service.send_once(
        source_operation_id="a" * 32,
        text="Task finished",
    )
    second = await service.send_once(
        source_operation_id="a" * 32,
        text="Task finished",
    )

    assert first == second
    assert first.room_id == "!primary:example"
    assert len(matrix.visible) == 1


@pytest.mark.asyncio
async def test_resume_notification_reuses_recorded_matrix_transaction(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    storage = MinioClient(FakeS3(), bucket="agentteams")
    matrix = Matrix()
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    source_operation_id = (
        "supervision:task_overdue:"
        "e00b6f2eb056f78c1a4be9a5"
    )
    notification_id = hashlib.sha256(
        f"notification\0{source_operation_id}".encode(),
    ).hexdigest()[:32]
    txn_id = f"agentteams:{notification_id}:0"
    operation = await supervisor.begin(
        operation_id=notification_id,
        kind=OperationKind.SEND_NOTIFICATION,
        target_key="matrix-notification/!primary:example",
        request={
            "source_operation_id": source_operation_id,
            "recipient": "@admin:example",
            "room_id": "!primary:example",
            "text": "Recovered notification",
            "txn_id": txn_id,
        },
    )
    await supervisor.before_effect(
        notification_id,
        ExternalEffect.MATRIX,
        {"operation": "send_notification"},
    )
    operation = await supervisor.effect_ambiguous(
        notification_id,
        ExternalEffect.MATRIX,
        "connection_lost",
    )
    service = NotificationService(
        notifications=NotificationRepository(database),
        resolver=ChannelResolver(
            channels=Channels(),
            matrix=matrix,
            manager_admin_room="!admin:example",
        ),
        matrix=matrix,
        supervisor=supervisor,
        memory=DailyMemory(storage=storage, clock=clock),
        clock=clock,
        admin_user_id="@admin:example",
    )

    receipt = await service.resume_operation(operation)

    assert receipt.event_id == "$notification"
    assert receipt.source_operation_id == hashlib.sha256(
        f"notification-source\0{source_operation_id}".encode(),
    ).hexdigest()[:32]
    assert matrix.attempts == [txn_id]
    assert (
        supervisor.operations[notification_id].status
        is OperationStatus.SUCCEEDED
    )
