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
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskMatrix, TaskSupervisor


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
    result = await service.read_task_file(
        task_id,
        "result.md",
    )

    assert (root / "result.md").read_text(encoding="utf-8") == "fresh"
    assert result.task_id == task_id
    assert result.path == "result.md"
    assert result.content == "fresh"
    assert result.bytes_read == 5

    with pytest.raises(ValueError, match="relative"):
        await service.read_task_file(task_id, "../result.md")


@pytest.mark.asyncio
async def test_team_task_sync_uses_team_remote_prefix_and_shared_local_path(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    task_id = "task-20260723-120000-team12"
    now = datetime(2026, 7, 23, 12, tzinfo=UTC)
    tasks = TaskRepository(database)
    await tasks.create(
        TaskRecord(
            task_id=task_id,
            task_type="finite",
            status="assigned",
            title="Team task",
            assigned_to="lead",
            room_id="!lead:example",
            metadata={"storage_team_name": "alpha"},
            created_at=now,
            updated_at=now,
        ),
    )
    storage = MinioClient(FakeS3(), bucket="agentteams")
    remote_prefix = f"teams/alpha/shared/tasks/{task_id}/"
    await storage.put_bytes(
        f"{remote_prefix}result.md",
        b"team result",
        content_type="text/markdown",
    )
    cache_root = tmp_path / "cache"
    service = FileSyncService(
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=FixedClock(),
        ),
        tasks=tasks,
        cache_root=cache_root,
    )

    local_root = await service.pull_task(task_id)
    assert local_root == cache_root / "shared" / "tasks" / task_id
    assert (local_root / "result.md").read_text(encoding="utf-8") == (
        "team result"
    )

    (local_root / "worker-note.md").write_text("published", encoding="utf-8")
    receipt = await service.push_task(task_id, processor="manager")

    assert receipt.prefix == remote_prefix
    assert await storage.get_bytes(
        f"{remote_prefix}worker-note.md",
    ) == b"published"


@pytest.mark.asyncio
async def test_task_file_read_rejects_symlinks_and_oversized_content(
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
    cache_root = tmp_path / "cache"
    root = cache_root / "shared" / "tasks" / task_id
    root.mkdir(parents=True)
    (root / "large.md").write_text("0123456789", encoding="utf-8")
    service = FileSyncService(
        storage=MinioClient(FakeS3(), bucket="agentteams"),
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=MinioClient(FakeS3(), bucket="leases"),
            clock=FixedClock(),
        ),
        tasks=tasks,
        cache_root=cache_root,
    )

    with pytest.raises(ValueError, match="maximum"):
        await service.read_task_file(
            task_id,
            "large.md",
            max_bytes=5,
        )

    target = root / "target.md"
    target.write_text("secret", encoding="utf-8")
    link = root / "link.md"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")
    with pytest.raises(ValueError, match="symlink"):
        await service.read_task_file(task_id, "link.md")


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


@pytest.mark.asyncio
async def test_sync_roots_cover_workspace_and_shared_knowledge(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    storage = MinioClient(FakeS3(), bucket="agentteams")
    service = FileSyncService(
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=FixedClock(),
        ),
        tasks=TaskRepository(database),
        cache_root=tmp_path / "cache",
    )
    workspace = service.root_path(
        "worker_workspace",
        worker_name="alice",
    )
    workspace.mkdir(parents=True)
    (workspace / "notes.md").write_text("worker notes", encoding="utf-8")
    knowledge = service.root_path("shared_knowledge")
    knowledge.mkdir(parents=True)
    (knowledge / "guide.md").write_text("shared guide", encoding="utf-8")

    worker_receipt = await service.push_root(
        "worker_workspace",
        worker_name="alice",
        processor="manager",
    )
    knowledge_receipt = await service.push_root(
        "shared_knowledge",
        processor="manager",
    )

    assert worker_receipt.prefix == "workers/alice/workspace/"
    assert knowledge_receipt.prefix == "shared/knowledge/"
    assert await storage.get_bytes(
        "workers/alice/workspace/notes.md",
    ) == b"worker notes"
    assert await storage.get_bytes(
        "shared/knowledge/guide.md",
    ) == b"shared guide"
    with pytest.raises(ValueError, match="invalid Worker name"):
        service.root_path(
            "worker_workspace",
            worker_name="../alice",
        )


@pytest.mark.asyncio
async def test_task_upload_and_worker_mention_share_one_operation(
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
            metadata={"matrix_user_id": "@alice:example"},
            created_at=now,
            updated_at=now,
        ),
    )
    storage = MinioClient(FakeS3(), bucket="agentteams")
    cache_root = tmp_path / "cache"
    root = cache_root / "shared" / "tasks" / task_id
    root.mkdir(parents=True)
    (root / "result.md").write_text("done", encoding="utf-8")
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix([])
    service = FileSyncService(
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=clock,
        ),
        tasks=tasks,
        cache_root=cache_root,
        supervisor=supervisor,
        matrix=matrix,
    )
    context = MutationContext(
        room_id="!admin:example",
        event_id="$sync",
        tool_call_id="sync-files",
    )

    first = await service.sync_task(
        task_id,
        direction="push",
        processor="manager",
        context=context,
    )
    second = await service.sync_task(
        task_id,
        direction="push",
        processor="manager",
        context=context,
    )

    assert first == second
    assert len(matrix.visible) == 1
    assert matrix.attempts[0].mentions == ("@alice:example",)
    assert supervisor.operations[context.operation_id].status.value == (
        "succeeded"
    )
