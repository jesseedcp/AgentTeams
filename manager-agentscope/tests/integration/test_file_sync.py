from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.models import TaskRecord
from agentteams_manager.state.database import Database
from agentteams_manager.state.leases import LeaseRepository
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.tools.storage import FileSyncService
from agentteams_manager.workflows.git_delegation import (
    ProcessingLeaseService,
)
from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import FixedClock


@pytest.mark.asyncio
async def test_worker_uploaded_result_is_pulled_before_read(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    task_id = "task-20260723-120000-abc123"
    now = datetime(2026, 7, 23, 12, tzinfo=UTC)
    tasks = TaskRepository(database)
    await tasks.create(
        TaskRecord(
            task_id=task_id,
            task_type="finite",
            status="assigned",
            title="Task",
            assigned_to="alice",
            room_id="!alice:example",
            created_at=now,
            updated_at=now,
        ),
    )
    storage = MinioClient(FakeS3(), bucket="agentteams")
    await storage.put_bytes(
        f"shared/tasks/{task_id}/result.md",
        b"fresh",
        content_type="text/markdown",
    )
    cache_root = tmp_path / "cache"
    stale = cache_root / "shared" / "tasks" / task_id / "result.md"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale", encoding="utf-8")
    clock = FixedClock()
    service = FileSyncService(
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=clock,
        ),
        tasks=tasks,
        cache_root=cache_root,
    )

    root = await service.pull_task(task_id)

    assert (root / "result.md").read_text(encoding="utf-8") == "fresh"


@pytest.mark.asyncio
async def test_worker_push_cannot_overwrite_manager_owned_files(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    task_id = "task-20260723-120000-abc123"
    now = datetime(2026, 7, 23, 12, tzinfo=UTC)
    tasks = TaskRepository(database)
    await tasks.create(
        TaskRecord(
            task_id=task_id,
            task_type="finite",
            status="assigned",
            title="Task",
            assigned_to="alice",
            room_id="!alice:example",
            created_at=now,
            updated_at=now,
        ),
    )
    storage = MinioClient(FakeS3(), bucket="agentteams")
    await storage.put_bytes(
        f"shared/tasks/{task_id}/spec.md",
        b"trusted",
        content_type="text/markdown",
    )
    cache_root = tmp_path / "cache"
    root = cache_root / "shared" / "tasks" / task_id
    root.mkdir(parents=True)
    (root / "spec.md").write_text("tampered", encoding="utf-8")
    (root / "result.md").write_text("done", encoding="utf-8")
    clock = FixedClock()
    service = FileSyncService(
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=clock,
        ),
        tasks=tasks,
        cache_root=cache_root,
    )

    receipt = await service.push_task(
        task_id,
        processor="worker-alice",
    )

    assert await storage.get_bytes(
        f"shared/tasks/{task_id}/spec.md",
    ) == b"trusted"
    assert await storage.get_bytes(
        f"shared/tasks/{task_id}/result.md",
    ) == b"done"
    assert receipt.files == 1
