from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from agentteams_manager.clients.coding_cli import CodingCLIReceipt
from agentteams_manager.domain.errors import RecoveryError
from agentteams_manager.domain.models import (
    ExternalEffect,
    JournalEvent,
    OperationKind,
    OperationStatus,
    TaskRecord,
)
from agentteams_manager.workflows.coding_cli import (
    CodingCLIConfirmationRequired,
    CodingCLIDelegationDisabled,
    CodingCLIDelegationRequest,
    CodingCLIDelegationService,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import (
    FixedClock,
    TaskMatrix,
    TaskSupervisor,
)

TASK_ID = "task-20260728-120000-abc123"
ADMIN_ROOM = "!admin:example"
WORKER_ROOM = "!alice:example"


class Tasks:
    def __init__(self) -> None:
        now = datetime(2026, 7, 28, 12, tzinfo=UTC)
        self.task = TaskRecord(
            task_id=TASK_ID,
            task_type="finite",
            status="assigned",
            title="Fix the build",
            assigned_to="alice",
            room_id=WORKER_ROOM,
            created_at=now,
            updated_at=now,
        )

    async def get(self, task_id: str):
        return self.task if task_id == TASK_ID else None


class Storage:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def mirror_down(self, prefix: str, destination: Path):
        self.calls.append(f"down:{prefix}")
        (destination / "workspace").mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(
            model_dump=lambda **_: {
                "prefix": prefix,
                "files": 0,
                "bytes_transferred": 0,
                "manifest_sha256": "down",
            },
        )

    async def mirror_up(self, source: Path, prefix: str):
        self.calls.append(f"up:{prefix}")
        assert source.is_dir()
        return SimpleNamespace(
            manifest_sha256=f"manifest-{len(self.calls)}",
            model_dump=lambda **_: {
                "prefix": prefix,
                "files": 2,
                "bytes_transferred": 20,
                "manifest_sha256": f"manifest-{len(self.calls)}",
            },
        )


class Leases:
    def __init__(self) -> None:
        self.acquired = 0
        self.released = 0
        self.renewed = 0

    async def acquire(self, task_id: str, **kwargs: str):
        del kwargs
        self.acquired += 1
        return SimpleNamespace(
            task_id=task_id,
            lease_id="a" * 32,
            expires_at=datetime(2026, 7, 28, 12, 15, tzinfo=UTC),
        )

    async def release(self, lease: object) -> None:
        del lease
        self.released += 1

    async def renew(self, lease: object):
        self.renewed += 1
        return lease


class CLI:
    def __init__(self, *, success: bool = True) -> None:
        self.success = success
        self.calls: list[tuple[str, Path, str]] = []

    def status(self):
        return {
            "claude": {"configured": True, "available": True},
        }

    async def run(
        self,
        provider: str,
        *,
        workspace: Path,
        prompt: str,
        timeout_seconds: float | None = None,
    ) -> CodingCLIReceipt:
        del timeout_seconds
        self.calls.append((provider, workspace, prompt))
        return CodingCLIReceipt(
            provider="claude",
            success=self.success,
            returncode=0 if self.success else 9,
            stdout="changed two files" if self.success else "",
            stderr="" if self.success else "authentication failed",
        )


class Events:
    def __init__(self, events: tuple[JournalEvent, ...]) -> None:
        self.events = events

    async def events_for(self, operation_id: str):
        del operation_id
        return self.events


def _request() -> CodingCLIDelegationRequest:
    return CodingCLIDelegationRequest(
        task_id=TASK_ID,
        provider="claude",
        workspace=".",
        prompt="Fix the failing tests without changing public APIs.",
    )


def _context(room_id: str = ADMIN_ROOM) -> MutationContext:
    return MutationContext(
        room_id=room_id,
        event_id="$coding-request",
        tool_call_id="delegate-coding-cli",
    )


def _service(
    tmp_path: Path,
    *,
    enabled: bool = True,
    cli: CLI | None = None,
    events: Events | None = None,
):
    clock = FixedClock()
    leases = Leases()
    storage = Storage()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix([])
    service = CodingCLIDelegationService(
        enabled=enabled,
        admin_room_id=ADMIN_ROOM,
        storage=storage,
        leases=leases,
        cli=cli or CLI(),
        tasks=Tasks(),
        matrix=matrix,
        supervisor=supervisor,
        cache_root=tmp_path,
        events=events,
        renewal_interval=30,
    )
    return service, leases, storage, supervisor, matrix


@pytest.mark.asyncio
async def test_feature_flag_admin_room_and_confirmation_are_mandatory(
    tmp_path: Path,
) -> None:
    disabled, leases, _, _, _ = _service(tmp_path, enabled=False)
    with pytest.raises(CodingCLIDelegationDisabled):
        await disabled.execute(_request(), context=_context(), confirmed=True)
    assert leases.acquired == 0

    enabled, leases, _, _, _ = _service(tmp_path)
    with pytest.raises(PermissionError):
        await enabled.execute(
            _request(),
            context=_context(WORKER_ROOM),
            confirmed=True,
        )
    with pytest.raises(CodingCLIConfirmationRequired):
        await enabled.execute(
            _request(),
            context=_context(),
            confirmed=False,
        )
    assert leases.acquired == 0


@pytest.mark.asyncio
async def test_success_is_journaled_mirrored_notified_and_releases_lease(
    tmp_path: Path,
) -> None:
    cli = CLI()
    service, leases, storage, supervisor, matrix = _service(
        tmp_path,
        cli=cli,
    )

    receipt = await service.execute(
        _request(),
        context=_context(),
        confirmed=True,
    )

    assert receipt.success is True
    assert leases.acquired == leases.released == 1
    assert [item.split(":", 1)[0] for item in storage.calls] == [
        "down",
        "up",
        "up",
    ]
    assert cli.calls[0][0] == "claude"
    assert cli.calls[0][2].startswith("Fix the failing")
    assert matrix.attempts[-1].room_id == WORKER_ROOM
    assert "coding-result:" in matrix.attempts[-1].text
    operation = supervisor.operations[_context().operation_id]
    assert operation.kind is OperationKind.CODING_CLI_DELEGATION
    assert operation.status is OperationStatus.SUCCEEDED
    assert "prompt" not in operation.request
    assert operation.request["prompt_sha256"]
    assert any(
        effect is ExternalEffect.PROCESS
        for _, effect, _ in supervisor.events
    )


@pytest.mark.asyncio
async def test_definite_cli_failure_is_reported_and_releases_lease(
    tmp_path: Path,
) -> None:
    service, leases, _, _, matrix = _service(
        tmp_path,
        cli=CLI(success=False),
    )

    receipt = await service.execute(
        _request(),
        context=_context(),
        confirmed=True,
    )

    assert receipt.success is False
    assert receipt.returncode == 9
    assert leases.released == 1
    assert "coding-failed:" in matrix.attempts[-1].text
    assert "authentication failed" in receipt.summary


@pytest.mark.asyncio
async def test_recovery_refuses_to_replay_after_process_was_planned(
    tmp_path: Path,
) -> None:
    operation_id = "f" * 32
    event = JournalEvent(
        operation_id=operation_id,
        sequence=1,
        event_type="effect_planned",
        payload={
            "effect": ExternalEffect.PROCESS.value,
            "request": {"operation": "execute_coding_cli"},
        },
        created_at=datetime(2026, 7, 28, 12, tzinfo=UTC),
    )
    service, _, _, _, _ = _service(
        tmp_path,
        events=Events((event,)),
    )
    operation = TaskSupervisor(FixedClock())
    record = await operation.begin(
        operation_id=operation_id,
        kind=OperationKind.CODING_CLI_DELEGATION,
        target_key=f"task/{TASK_ID}/coding-cli",
        request={
            "task_id": TASK_ID,
            "provider": "claude",
            "workspace": ".",
            "prompt_sha256": "a" * 64,
        },
    )

    with pytest.raises(RecoveryError, match="may have started"):
        await service.resume_operation(record)
