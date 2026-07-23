from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.errors import RecoveryError
from agentteams_manager.domain.models import (
    ExternalEffect,
    JournalEvent,
    OperationKind,
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.leases import LeaseRepository
from agentteams_manager.workflows.git_delegation import (
    GitDelegationService,
    ProcessingLeaseService,
)
from tests.fixtures.fake_s3 import FakeS3


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 23, 12, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


class Events:
    def __init__(self, events: tuple[JournalEvent, ...]) -> None:
        self.events = events

    async def events_for(self, operation_id: str):
        del operation_id
        return self.events


class NeverRun:
    async def run(self, *args, **kwargs):
        raise AssertionError("Git process must not be replayed")


def _git_operation() -> OperationRecord:
    return OperationRecord.new(
        operation_id="b" * 32,
        kind=OperationKind.GIT_DELEGATION,
        target_key="task/task-20260723-120000-abc123/git",
        request={
            "task_id": "task-20260723-120000-abc123",
            "workspace": ".",
            "operations": [
                {
                    "argv": ["git", "status"],
                    "risk": "low",
                },
            ],
            "context": None,
        },
    ).model_copy(update={"status": OperationStatus.RECONCILING})


@pytest.mark.asyncio
async def test_reclaim_requires_matching_expired_remote_identity(
    tmp_path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    clock = MutableClock()
    s3 = FakeS3()
    storage = MinioClient(s3, bucket="agentteams")
    repository = LeaseRepository(database)
    service = ProcessingLeaseService(
        leases=repository,
        storage=storage,
        clock=clock,
    )
    lease = await service.acquire(
        "task-20260723-120000-abc123",
        processor="manager",
        operation="git-delegation",
    )
    clock.value = lease.expires_at
    remote_key = f"shared/tasks/{lease.task_id}/.processing"
    receipt = await storage.head(remote_key)
    assert receipt is not None
    remote = await storage.get_json(remote_key)
    remote["lease_id"] = "f" * 32
    await storage.put_json_if_version(
        remote_key,
        remote,
        expected_etag=receipt.etag,
    )

    report = await service.reclaim_expired(clock.value)

    assert report.reclaimed == ()
    assert report.conflicted == (lease.task_id,)
    assert await repository.get(lease.task_id) is not None


@pytest.mark.asyncio
async def test_git_process_boundary_is_never_blindly_replayed(
    tmp_path,
) -> None:
    operation = _git_operation()
    event = JournalEvent(
        operation_id=operation.operation_id,
        sequence=1,
        event_type="effect_planned",
        payload={
            "effect": ExternalEffect.PROCESS.value,
            "request": {"operation": "execute_git"},
        },
        created_at=datetime(2026, 7, 23, 12, tzinfo=UTC),
    )
    service = GitDelegationService(
        storage=object(),
        leases=object(),
        git=NeverRun(),
        tasks=object(),
        matrix=object(),
        supervisor=object(),
        cache_root=tmp_path,
        events=Events((event,)),
    )

    with pytest.raises(RecoveryError, match="refusing blind replay"):
        await service.resume_operation(operation)
