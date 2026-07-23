from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.leases import LeaseRepository
from agentteams_manager.workflows.git_delegation import (
    ProcessingLeaseService,
)
from tests.fixtures.fake_s3 import FakeS3


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 23, 12, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


@pytest.mark.asyncio
async def test_expired_remote_lease_is_reclaimed_conditionally(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    clock = MutableClock()
    service = ProcessingLeaseService(
        leases=LeaseRepository(database),
        storage=MinioClient(FakeS3(), bucket="agentteams"),
        clock=clock,
    )
    first = await service.acquire(
        "task-20260723-120000-abc123",
        processor="manager",
        operation="git-delegation",
    )
    clock.value += timedelta(minutes=16)

    replacement = await service.acquire(
        first.task_id,
        processor="worker-alice",
        operation="file-sync",
    )

    assert replacement.lease_id != first.lease_id
    assert replacement.processor == "worker-alice"
