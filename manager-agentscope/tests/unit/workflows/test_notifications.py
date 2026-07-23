from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
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
