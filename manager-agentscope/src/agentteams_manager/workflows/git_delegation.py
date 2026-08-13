"""Expiring workspace leases and recoverable Git delegation.

使用可过期 processing lease 协调共享 workspace 的 Git 委托。

一个 Project 的多个 Agent 可能同时碰同一仓库。workflow 先在远端取得带 generation 的
lease，再记录 operation 意图并执行受限 Git；完成或确定失败后释放 lease。崩溃恢复
会核对 lease 所有者、到期时间和 operation event，不能随意抢占仍在工作的进程。
"""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from agentteams_manager.clients.git import (
    GitClient,
    GitReceipt,
    GitRequest,
)
from agentteams_manager.clients.minio import ObjectVersionConflict
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
    RecoveryError,
)
from agentteams_manager.domain.ids import matrix_transaction_id
from agentteams_manager.domain.models import (
    ExternalEffect,
    JournalEvent,
    OperationKind,
    OperationRecord,
    OperationStatus,
    ProcessingLeaseRecord,
    TaskRecord,
)
from agentteams_manager.domain.ports import ArtifactPort, Clock, MatrixPort
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskSupervisorPort


class LeaseError(RuntimeError):
    """Base failure for shared workspace lease operations."""


class LeaseConflict(LeaseError):
    """Another unexpired processor owns the task workspace."""


class ConfirmationRequired(RuntimeError):
    """A high-risk Git request has not passed AgentScope confirmation."""


class LeaseRepositoryPort(Protocol):
    async def get(self, task_id: str) -> ProcessingLeaseRecord | None: ...

    async def expired(
        self,
        now: datetime,
    ) -> tuple[ProcessingLeaseRecord, ...]: ...

    async def upsert(
        self,
        lease: ProcessingLeaseRecord,
    ) -> ProcessingLeaseRecord: ...

    async def delete(self, task_id: str, lease_id: str) -> bool: ...


class TaskReader(Protocol):
    async def get(self, task_id: str) -> TaskRecord | None: ...


class OperationEventReader(Protocol):
    async def events_for(
        self,
        operation_id: str,
    ) -> tuple[JournalEvent, ...]: ...


class ProcessingMarker(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1, le=1)
    task_id: str
    lease_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    processor: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    started_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def validate_times(self) -> Self:
        if (
            self.started_at.tzinfo is None
            or self.started_at.utcoffset() is None
            or self.expires_at.tzinfo is None
            or self.expires_at.utcoffset() is None
        ):
            raise ValueError("processing lease timestamps must be aware")
        if self.expires_at <= self.started_at:
            raise ValueError("processing lease expiry must follow its start")
        return self


class ProcessingLease(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str
    lease_id: str
    processor: str
    operation: str
    started_at: datetime
    expires_at: datetime
    etag: str


class LeaseReclaimReport(BaseModel):
    """Evidence from one conditional expired-lease cleanup pass."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reclaimed: tuple[str, ...] = ()
    live: tuple[str, ...] = ()
    conflicted: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()


class GitDelegationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    task_id: str
    success: bool
    commands: int = Field(ge=0)
    message_event_id: str
    mirror_manifest_sha256: str
    summary: str


class ProcessingLeaseService:
    """Keep SQLite synchronized with an authoritative conditional S3 marker."""

    def __init__(
        self,
        *,
        leases: LeaseRepositoryPort,
        storage: ArtifactPort,
        clock: Clock,
        duration: timedelta = timedelta(minutes=15),
    ) -> None:
        if duration <= timedelta(0):
            raise ValueError("lease duration must be positive")
        self._leases = leases
        self._storage = storage
        self._clock = clock
        self._duration = duration

    async def acquire(
        self,
        task_id: str,
        *,
        processor: str,
        operation: str,
    ) -> ProcessingLease:
        _validate_task_id(task_id)
        if not processor or not operation:
            raise ValueError("lease processor and operation are required")
        now = self._clock.now().astimezone(UTC)
        marker = ProcessingMarker(
            task_id=task_id,
            lease_id=uuid.uuid4().hex,
            processor=processor,
            operation=operation,
            started_at=now,
            expires_at=now + self._duration,
        )
        key = _lease_key(task_id)
        for attempt in range(2):
            current_receipt = await self._storage.head(key)
            expected_etag: str | None = None
            if current_receipt is not None:
                current = ProcessingMarker.model_validate(
                    await self._storage.get_json(key),
                )
                if current.task_id != task_id:
                    raise LeaseConflict(
                        "remote lease marker task identity is invalid",
                    )
                if current.expires_at > now:
                    raise LeaseConflict(
                        f"task {task_id} is leased by "
                        f"{current.processor} until "
                        f"{current.expires_at.isoformat()}",
                    )
                expected_etag = current_receipt.etag
            try:
                receipt = await self._storage.put_json_if_version(
                    key,
                    marker.model_dump(mode="json"),
                    expected_etag=expected_etag,
                )
                return await self._materialize(marker, receipt.etag)
            except ObjectVersionConflict:
                if attempt:
                    raise LeaseConflict(
                        f"task {task_id} lease changed concurrently",
                    ) from None
            except Exception:
                proven = await self._prove_marker(key, marker.lease_id)
                if proven is not None:
                    proven_marker, etag = proven
                    return await self._materialize(proven_marker, etag)
                raise
        raise LeaseConflict(f"task {task_id} lease could not be acquired")

    async def renew(self, lease: ProcessingLease) -> ProcessingLease:
        local = await self._leases.get(lease.task_id)
        if local is None or local.lease_id != lease.lease_id:
            raise LeaseConflict("local lease identity does not match")
        key = _lease_key(lease.task_id)
        remote_receipt = await self._storage.head(key)
        if remote_receipt is None:
            raise LeaseConflict("remote lease marker is missing")
        remote = ProcessingMarker.model_validate(
            await self._storage.get_json(key),
        )
        if remote.lease_id != lease.lease_id:
            raise LeaseConflict("remote lease identity does not match")
        now = self._clock.now().astimezone(UTC)
        renewed = remote.model_copy(
            update={"expires_at": now + self._duration},
        )
        try:
            receipt = await self._storage.put_json_if_version(
                key,
                renewed.model_dump(mode="json"),
                expected_etag=remote_receipt.etag,
            )
        except Exception:
            proven = await self._prove_marker(key, lease.lease_id)
            if proven is None:
                raise
            proven_marker, etag = proven
            if proven_marker.expires_at <= remote.expires_at:
                raise
            return await self._materialize(proven_marker, etag)
        return await self._materialize(renewed, receipt.etag)

    async def release(self, lease: ProcessingLease) -> None:
        local = await self._leases.get(lease.task_id)
        if local is not None and local.lease_id != lease.lease_id:
            raise LeaseConflict("local lease identity does not match")
        key = _lease_key(lease.task_id)
        remote_receipt = await self._storage.head(key)
        if remote_receipt is None:
            await self._leases.delete(lease.task_id, lease.lease_id)
            return
        remote = ProcessingMarker.model_validate(
            await self._storage.get_json(key),
        )
        if remote.lease_id != lease.lease_id:
            raise LeaseConflict("remote lease identity does not match")
        try:
            await self._storage.delete_if_version(
                key,
                expected_etag=remote_receipt.etag,
            )
        except ObjectVersionConflict as exc:
            raise LeaseConflict("lease changed before release") from exc
        except Exception:
            after = await self._storage.head(key)
            if after is not None:
                raise
        await self._leases.delete(lease.task_id, lease.lease_id)

    async def reclaim_expired(
        self,
        now: datetime | None = None,
    ) -> LeaseReclaimReport:
        """Delete only an expired remote marker matching local identity."""
        instant = (now or self._clock.now()).astimezone(UTC)
        expired = await self._leases.expired(instant)
        reclaimed: list[str] = []
        live: list[str] = []
        conflicted: list[str] = []
        pending: list[str] = []
        for local in expired:
            key = _lease_key(local.task_id)
            try:
                receipt = await self._storage.head(key)
                if receipt is None:
                    await self._leases.delete(
                        local.task_id,
                        local.lease_id,
                    )
                    reclaimed.append(local.task_id)
                    continue
                remote = ProcessingMarker.model_validate(
                    await self._storage.get_json(key),
                )
                if (
                    remote.task_id != local.task_id
                    or remote.lease_id != local.lease_id
                ):
                    conflicted.append(local.task_id)
                    continue
                if remote.expires_at > instant:
                    await self._materialize(remote, receipt.etag)
                    live.append(local.task_id)
                    continue
                await self._storage.delete_if_version(
                    key,
                    expected_etag=receipt.etag,
                )
                await self._leases.delete(
                    local.task_id,
                    local.lease_id,
                )
                reclaimed.append(local.task_id)
            except ObjectVersionConflict:
                conflicted.append(local.task_id)
            except Exception:
                pending.append(local.task_id)
        return LeaseReclaimReport(
            inspected=len(expired),
            reclaimed=tuple(reclaimed),
            live=tuple(live),
            conflicted=tuple(conflicted),
            pending=tuple(pending),
        )

    async def _prove_marker(
        self,
        key: str,
        lease_id: str,
    ) -> tuple[ProcessingMarker, str] | None:
        receipt = await self._storage.head(key)
        if receipt is None:
            return None
        marker = ProcessingMarker.model_validate(
            await self._storage.get_json(key),
        )
        if marker.lease_id != lease_id:
            return None
        return marker, receipt.etag

    async def _materialize(
        self,
        marker: ProcessingMarker,
        etag: str,
    ) -> ProcessingLease:
        now = self._clock.now().astimezone(UTC)
        await self._leases.upsert(
            ProcessingLeaseRecord(
                task_id=marker.task_id,
                lease_id=marker.lease_id,
                processor=marker.processor,
                operation=marker.operation,
                started_at=marker.started_at,
                expires_at=marker.expires_at,
                remote_etag=etag,
                updated_at=now,
            ),
        )
        return ProcessingLease(
            task_id=marker.task_id,
            lease_id=marker.lease_id,
            processor=marker.processor,
            operation=marker.operation,
            started_at=marker.started_at,
            expires_at=marker.expires_at,
            etag=etag,
        )


class GitDelegationService:
    """Pull, lease, execute constrained Git, push, release, then reply once."""

    def __init__(
        self,
        *,
        storage: ArtifactPort,
        leases: ProcessingLeaseService,
        git: GitClient,
        tasks: TaskReader,
        matrix: MatrixPort,
        supervisor: TaskSupervisorPort,
        cache_root: Path,
        renewal_interval: float = 300,
        events: OperationEventReader | None = None,
    ) -> None:
        if renewal_interval <= 0:
            raise ValueError("lease renewal interval must be positive")
        self._storage = storage
        self._leases = leases
        self._git = git
        self._tasks = tasks
        self._matrix = matrix
        self._supervisor = supervisor
        self._cache_root = cache_root.resolve()
        self._renewal_interval = renewal_interval
        self._events = events

    async def execute(
        self,
        request: GitRequest,
        *,
        context: MutationContext,
        confirmed: bool = False,
    ) -> GitDelegationReceipt:
        if request.requires_confirmation and not confirmed:
            raise ConfirmationRequired(
                "high-risk Git operations require confirmation",
            )
        task = await self._tasks.get(request.task_id)
        if task is None:
            raise NotFoundError(f"task/{request.task_id} does not exist")
        if task.room_id != context.room_id:
            raise ConflictError(
                "Git delegation must originate in the assigned Worker room",
            )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.GIT_DELEGATION,
            target_key=f"task/{request.task_id}/git",
            request={
                "task_id": request.task_id,
                "workspace": str(request.workspace),
                "operations": [
                    item.model_dump(mode="json")
                    for item in request.operations
                ],
                "context": request.context,
            },
        )
        return await self._execute_operation(
            request=request,
            operation=operation,
            task=task,
        )

    async def _execute_operation(
        self,
        *,
        request: GitRequest,
        operation: OperationRecord,
        task: TaskRecord,
    ) -> GitDelegationReceipt:
        if operation.status is OperationStatus.SUCCEEDED:
            return GitDelegationReceipt.model_validate(operation.result)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError("Git delegation previously failed")

        task_cache = (
            self._cache_root / "shared" / "tasks" / request.task_id
        )
        workspace_root = task_cache / "workspace"
        workspace = self._git.validate_workspace(
            workspace_root,
            request.workspace,
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "pull_task_workspace",
                "task_id": request.task_id,
            },
        )
        mirror_down = await self._storage.mirror_down(
            f"shared/tasks/{request.task_id}/",
            task_cache,
        )
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            mirror_down.model_dump(mode="json"),
        )
        workspace = self._git.validate_workspace(
            workspace_root,
            workspace,
        )
        if not workspace.exists():
            workspace.mkdir(parents=True, exist_ok=True)

        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "acquire_processing_lease",
                "task_id": request.task_id,
            },
        )
        lease = await self._leases.acquire(
            request.task_id,
            processor="manager",
            operation=f"git-delegation/{operation.operation_id}",
        )
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": lease.task_id,
                "lease_id": lease.lease_id,
                "expires_at": lease.expires_at.isoformat(),
            },
        )
        local_marker = task_cache / ".processing"
        if local_marker.exists() or local_marker.is_symlink():
            local_marker.unlink()

        git_receipt: GitReceipt | None = None
        mirror_up = None
        failure: Exception | None = None
        stop_renewal = asyncio.Event()
        renewal_failures: list[Exception] = []
        renewal_task = asyncio.create_task(
            self._renew_lease_until_stopped(
                lease,
                stop_renewal,
                renewal_failures,
            ),
        )
        try:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.PROCESS,
                {
                    "operation": "execute_git",
                    "task_id": request.task_id,
                    "command_count": len(request.operations),
                },
            )
            git_receipt = await self._git.run(
                workspace,
                request.operations,
            )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.PROCESS,
                git_receipt.model_dump(mode="json"),
            )
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.STORAGE,
                {
                    "operation": "push_task_workspace",
                    "task_id": request.task_id,
                },
            )
            mirror_up = await self._storage.mirror_up(
                task_cache,
                f"shared/tasks/{request.task_id}/",
            )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.STORAGE,
                mirror_up.model_dump(mode="json"),
            )
        except Exception as exc:
            failure = exc
        finally:
            stop_renewal.set()
            await renewal_task
            if renewal_failures:
                failure = failure or renewal_failures[0]
            try:
                await self._leases.release(lease)
            except Exception as release_error:
                failure = failure or release_error

        success = (
            failure is None
            and git_receipt is not None
            and git_receipt.success
            and mirror_up is not None
        )
        summary = _git_summary(git_receipt, failure)
        transaction_id = matrix_transaction_id(operation.operation_id, 0)
        prefix = "git-result:" if success else "git-failed:"
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "send_git_result",
                "task_id": request.task_id,
                "room_id": task.room_id,
                "txn_id": transaction_id,
                "success": success,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                task.room_id,
                f"@{task.assigned_to} {request.task_id} {prefix}\n"
                f"{summary}\nRun `agentteams-sync` to sync.",
                txn_id=transaction_id,
            )
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.MATRIX,
                type(exc).__name__,
            )
            raise
        receipt = GitDelegationReceipt(
            operation_id=operation.operation_id,
            task_id=request.task_id,
            success=success,
            commands=(
                len(git_receipt.commands)
                if git_receipt is not None
                else 0
            ),
            message_event_id=event_id,
            mirror_manifest_sha256=(
                mirror_up.manifest_sha256
                if mirror_up is not None
                else ""
            ),
            summary=summary,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> GitDelegationReceipt:
        """Resume only while evidence proves no Git process could have run."""
        if operation.kind is not OperationKind.GIT_DELEGATION:
            raise ValueError("operation is not Git delegation")
        if operation.status is OperationStatus.SUCCEEDED:
            return GitDelegationReceipt.model_validate(operation.result)
        if self._events is None:
            raise RecoveryError(
                "Git recovery requires operation event evidence",
            )
        events = await self._events.events_for(operation.operation_id)
        process_planned = any(
            event.event_type == "effect_planned"
            and event.payload.get("effect") == ExternalEffect.PROCESS.value
            for event in events
        )
        if process_planned:
            raise RecoveryError(
                "Git process may have started; refusing blind replay",
            )
        task_id = str(operation.request.get("task_id", ""))
        if not task_id:
            raise RecoveryError("Git request has no task identity")
        lease_was_planned = any(
            event.event_type == "effect_planned"
            and event.payload.get("effect") == ExternalEffect.STORAGE.value
            and isinstance(event.payload.get("request"), dict)
            and event.payload["request"].get("operation")
            == "acquire_processing_lease"
            for event in events
        )
        if lease_was_planned and await self._storage.head(
            _lease_key(task_id),
        ) is not None:
            raise AmbiguousEffectError(
                "Git processing lease is still present",
            )
        try:
            request = GitRequest.model_validate(operation.request)
        except Exception as exc:
            raise RecoveryError(
                "Git request cannot be reconstructed",
            ) from exc
        task = await self._tasks.get(task_id)
        if task is None:
            raise RecoveryError(
                f"Git task {task_id} is no longer present",
            )
        return await self._execute_operation(
            request=request,
            operation=operation,
            task=task,
        )

    async def _renew_lease_until_stopped(
        self,
        lease: ProcessingLease,
        stop: asyncio.Event,
        failures: list[Exception],
    ) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._renewal_interval,
                )
                return
            except TimeoutError:
                try:
                    lease = await self._leases.renew(lease)
                except Exception as exc:
                    failures.append(exc)
                    return


def _lease_key(task_id: str) -> str:
    return f"shared/tasks/{task_id}/.processing"


def _validate_task_id(task_id: str) -> None:
    if re.fullmatch(r"task-[A-Za-z0-9-]+", task_id) is None:
        raise ValueError(f"invalid task ID: {task_id!r}")


def _git_summary(
    receipt: GitReceipt | None,
    failure: Exception | None,
) -> str:
    if failure is not None:
        return (
            f"Git operation failed: {type(failure).__name__}: {failure}"
        )[:1000]
    if receipt is None:
        return "Git operation failed before producing a receipt."
    if receipt.success:
        return f"Completed {len(receipt.commands)} Git operation(s)."
    last = receipt.commands[-1]
    detail = last.stderr.strip() or last.stdout.strip() or "non-zero exit"
    return (
        f"Git operation failed with exit {last.returncode}: {detail}"
    )[:1000]
