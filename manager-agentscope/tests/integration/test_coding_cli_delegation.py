from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.permission import PermissionBehavior, PermissionContext
from agentteams_manager.clients.coding_cli import CodingCLIReceipt
from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.domain.models import (
    RoomKind,
    RoomPolicy,
    TaskRecord,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.leases import LeaseRepository
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.tools.coding_cli import CodingCLIToolkit
from agentteams_manager.workflows.coding_cli import (
    CodingCLIDelegationService,
)
from agentteams_manager.workflows.git_delegation import (
    ProcessingLeaseService,
)
from agentteams_manager.workflows.resources import MutationContext

from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import (
    FixedClock,
    TaskMatrix,
    TaskSupervisor,
)

TASK_ID = "task-20260728-120000-abc123"
ADMIN_ROOM = "!admin:example"
WORKER_ROOM = "!alice:example"


class EditingCLI:
    def status(self):
        return {
            "claude": {"configured": True, "available": True},
            "gemini": {"configured": False, "available": False},
            "qodercli": {"configured": False, "available": False},
        }

    async def run(
        self,
        provider: str,
        *,
        workspace: Path,
        prompt: str,
        timeout_seconds: float | None = None,
    ) -> CodingCLIReceipt:
        del prompt, timeout_seconds
        (workspace / "result.txt").write_text(
            "changed by bounded CLI\n",
            encoding="utf-8",
        )
        return CodingCLIReceipt(
            provider=provider,
            success=True,
            returncode=0,
            stdout="implemented and verified",
            stderr="",
        )


def _context() -> MutationContext:
    return MutationContext(
        room_id=ADMIN_ROOM,
        event_id="$coding",
        tool_call_id="coding-cli",
    )


def _policy(*, admin: bool = True) -> RoomPolicy:
    names = {"coding_cli_status", "delegate_coding_cli"}
    return RoomPolicy(
        room_id=ADMIN_ROOM,
        kind=RoomKind.ADMIN_DM if admin else RoomKind.WORKER_ROOM,
        revision=1,
        allowed_tools=frozenset(names if admin else ()),
        confirm_tools=frozenset(
            {"delegate_coding_cli"} if admin else (),
        ),
    )


@pytest.mark.asyncio
async def test_confirmed_tool_leases_executes_mirrors_and_notifies(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    tasks = TaskRepository(database)
    now = datetime(2026, 7, 28, 12, tzinfo=UTC)
    await tasks.create(
        TaskRecord(
            task_id=TASK_ID,
            task_type="finite",
            status="assigned",
            title="Implement feature",
            assigned_to="alice",
            room_id=WORKER_ROOM,
            created_at=now,
            updated_at=now,
        ),
    )
    storage = MinioClient(FakeS3(), bucket="agentteams")
    await storage.put_bytes(
        f"shared/tasks/{TASK_ID}/workspace/README.md",
        b"source\n",
        content_type="text/markdown",
    )
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix([])
    service = CodingCLIDelegationService(
        enabled=True,
        admin_room_id=ADMIN_ROOM,
        storage=storage,
        leases=ProcessingLeaseService(
            leases=LeaseRepository(database),
            storage=storage,
            clock=clock,
        ),
        cli=EditingCLI(),
        tasks=tasks,
        matrix=matrix,
        supervisor=supervisor,
        cache_root=tmp_path / "cache",
        renewal_interval=30,
    )
    toolkit = CodingCLIToolkit(
        policy=_policy(),
        service=service,
        context_provider=_context,
    )
    tools = {tool.name: tool for tool in toolkit.tools}

    decision = await tools["delegate_coding_cli"].check_permissions(
        {},
        PermissionContext(),
    )
    assert decision.behavior is PermissionBehavior.ASK
    chunk = await tools["delegate_coding_cli"].call(
        task_id=TASK_ID,
        provider="claude",
        workspace=".",
        prompt="Implement the requested feature and preserve public APIs.",
    )

    result = json.loads(chunk.content[0].text)
    assert result["success"] is True
    assert (
        await storage.get_bytes(
            f"shared/tasks/{TASK_ID}/workspace/result.txt",
        )
    ).decode().strip() == "changed by bounded CLI"
    assert await storage.head(f"shared/tasks/{TASK_ID}/.processing") is None
    operation = supervisor.operations[_context().operation_id]
    assert "prompt" not in operation.request
    assert await storage.get_bytes(
        f"shared/tasks/{TASK_ID}/coding-prompts/"
        f"{operation.operation_id}.txt",
    )
    assert await storage.get_bytes(
        f"shared/tasks/{TASK_ID}/coding-cli-logs/"
        f"{operation.operation_id}.json",
    )
    assert "coding-result:" in matrix.attempts[-1].text
    await database.close()


def test_non_admin_policy_gets_no_coding_cli_tools() -> None:
    toolkit = CodingCLIToolkit(
        policy=_policy(admin=False),
        service=object(),
        context_provider=_context,
    )
    assert toolkit.tools == ()
