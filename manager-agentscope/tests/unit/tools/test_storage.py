from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.tools.storage import (
    TaskArtifactSet,
    TaskMetadata,
)
from tests.fixtures.fake_s3 import FakeS3


@pytest.mark.asyncio
async def test_prepared_task_writes_versioned_meta_before_spec() -> None:
    s3 = FakeS3()
    storage = MinioClient(s3, bucket="agentteams")
    metadata = TaskMetadata(
        task_id="task-20260723-120000-abc123",
        task_type="finite",
        status="prepared",
        title="Fix login",
        assigned_to="alice",
        room_id="!alice:example",
        created_at=datetime(2026, 7, 23, 12, tzinfo=UTC),
    )

    receipts = await TaskArtifactSet(
        storage=storage,
        metadata=metadata,
        specification="Acceptance: tests pass",
    ).write_prepared()

    prefix = f"shared/tasks/{metadata.task_id}"
    assert [receipt.key for receipt in receipts] == [
        f"{prefix}/meta.json",
        f"{prefix}/spec.md",
    ]
    assert s3.puts == [
        f"{prefix}/meta.json",
        f"{prefix}/spec.md",
    ]
    encoded = json.loads(s3.head(f"{prefix}/meta.json").data)
    assert encoded["schema_version"] == 1
    assert encoded["status"] == "prepared"


def test_recurring_task_metadata_requires_schedule_and_timezone() -> None:
    with pytest.raises(ValueError, match="schedule"):
        TaskMetadata(
            task_id="task-20260723-120000-abc123",
            task_type="infinite",
            status="prepared",
            title="Monitor releases",
            assigned_to="alice",
            room_id="!alice:example",
            created_at=datetime.now(UTC),
        )
