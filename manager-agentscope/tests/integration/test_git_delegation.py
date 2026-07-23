from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.clients.git import GitClient, GitRequestParser
from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.clients.process import ProcessResult
from agentteams_manager.domain.models import TaskRecord
from agentteams_manager.state.database import Database
from agentteams_manager.state.leases import LeaseRepository
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.workflows.git_delegation import (
    GitDelegationService,
    ProcessingLeaseService,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import (
    FixedClock,
    TaskMatrix,
    TaskSupervisor,
)


class Process:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def run(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        timeout: float,
    ) -> ProcessResult:
        del cwd, timeout
        self.calls.append(argv)
        return ProcessResult(
            argv=argv,
            returncode=0,
            stdout=b"clean\n",
            stderr=b"",
        )


@pytest.mark.asyncio
async def test_git_delegation_pulls_executes_pushes_and_replies(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    tasks = TaskRepository(database)
    now = datetime(2026, 7, 23, 12, tzinfo=UTC)
    task_id = "task-20260723-120000-abc123"
    await tasks.create(
        TaskRecord(
            task_id=task_id,
            task_type="finite",
            status="assigned",
            title="Inspect repo",
            assigned_to="alice",
            room_id="!alice:example",
            created_at=now,
            updated_at=now,
        ),
    )
    s3 = FakeS3()
    storage = MinioClient(s3, bucket="agentteams")
    await storage.put_bytes(
        f"shared/tasks/{task_id}/workspace/README.md",
        b"hello",
        content_type="text/markdown",
    )
    cache_root = tmp_path / "cache"
    workspace = (
        cache_root / "shared" / "tasks" / task_id / "workspace"
    )
    request = GitRequestParser.parse(
        (
            f"{task_id} git-request:\n"
            f"workspace: {workspace}\n"
            "operations:\n"
            "  - git status --short"
        ),
    )
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix([])
    process = Process()
    service = GitDelegationService(
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=clock,
        ),
        git=GitClient(process),
        tasks=tasks,
        matrix=matrix,
        supervisor=supervisor,
        cache_root=cache_root,
    )

    receipt = await service.execute(
        request,
        context=MutationContext(
            room_id="!alice:example",
            event_id="$git-request",
            tool_call_id="git-delegation",
        ),
    )

    assert receipt.success is True
    assert process.calls[0][:5] == (
        "git",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "protocol.ext.allow=never",
    )
    assert matrix.attempts[-1].room_id == "!alice:example"
    assert "git-result:" in matrix.attempts[-1].text
    assert await storage.head(f"shared/tasks/{task_id}/.processing") is None
