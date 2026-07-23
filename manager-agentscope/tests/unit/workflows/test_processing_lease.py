from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.state.database import Database
from agentteams_manager.state.leases import LeaseRepository
from agentteams_manager.workflows.git_delegation import (
    LeaseConflict,
    ProcessingLeaseService,
)
from tests.fixtures.fake_s3 import FakeS3


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 23, 12, tzinfo=UTC)


@pytest.mark.asyncio
async def test_live_remote_lease_blocks_second_processor(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    service = ProcessingLeaseService(
        leases=LeaseRepository(database),
        storage=MinioClient(FakeS3(), bucket="agentteams"),
        clock=Clock(),
    )
    first = await service.acquire(
        "task-20260723-120000-abc123",
        processor="manager",
        operation="git-delegation",
    )

    with pytest.raises(LeaseConflict):
        await service.acquire(
            first.task_id,
            processor="worker-alice",
            operation="file-sync",
        )

    await service.release(first)
