"""Recoverable coding CLI delegation over leased task workspaces.

在带 processing lease 的任务 workspace 中执行可恢复 Coding CLI 委托。

workflow 先验证 Task 和 artifact，再取得 lease、记录外部效果意图，最后运行 CLI 并发布
结果。CLI 超时可能留下进程或部分文件，因此不能简单重复；恢复会检查 operation event、
lease 与结果 artifact，判断是已完成、可安全继续还是需要人工关注。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from agentteams_manager.clients.coding_cli import (
    CodingCLIClient,
    CodingCLIError,
    CodingCLIProvider,
    CodingCLIReceipt,
)
from agentteams_manager.domain.errors import (
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
    TaskRecord,
)
from agentteams_manager.domain.ports import MatrixPort
from agentteams_manager.workflows.git_delegation import (
    ProcessingLease,
    ProcessingLeaseService,
)
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskSupervisorPort


class CodingCLIDelegationError(RuntimeError):
    """Base workflow failure for coding CLI delegation."""


class CodingCLIDelegationDisabled(CodingCLIDelegationError):
    """The deployment has not enabled the optional coding CLI boundary."""


class CodingCLIConfirmationRequired(CodingCLIDelegationError):
    """The administrator has not confirmed this code-changing operation."""


class CodingCLIDelegationRequest(BaseModel):
    """Closed request schema persisted without the prompt body."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(pattern=r"^task-[A-Za-z0-9-]+$")
    provider: CodingCLIProvider
    workspace: str = Field(default=".", min_length=1, max_length=512)
    prompt: str = Field(min_length=1, max_length=100_000, repr=False)
    timeout_seconds: float | None = Field(default=None, gt=0, le=3_600)

    @model_validator(mode="after")
    def require_relative_workspace(self) -> Self:
        if "\\" in self.workspace:
            raise ValueError("coding CLI workspace must use POSIX separators")
        path = PurePosixPath(self.workspace)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(
                "coding CLI workspace must stay below task workspace",
            )
        return self


class CodingCLIDelegationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    task_id: str
    provider: CodingCLIProvider
    success: bool
    returncode: int
    message_event_id: str
    mirror_manifest_sha256: str
    summary: str = Field(max_length=2_000)


class TaskReader(Protocol):
    async def get(self, task_id: str) -> TaskRecord | None: ...


class ArtifactMirror(Protocol):
    async def mirror_down(self, prefix: str, destination: Path) -> Any: ...

    async def mirror_up(self, source: Path, prefix: str) -> Any: ...


class OperationEventReader(Protocol):
    async def events_for(
        self,
        operation_id: str,
    ) -> tuple[JournalEvent, ...]: ...


class CodingCLIDelegationService:
    """Lease, mirror, execute, mirror, notify, and release exactly once."""

    def __init__(
        self,
        *,
        enabled: bool,
        admin_room_id: str,
        storage: ArtifactMirror,
        leases: ProcessingLeaseService,
        cli: CodingCLIClient,
        tasks: TaskReader,
        matrix: MatrixPort,
        supervisor: TaskSupervisorPort,
        cache_root: Path,
        renewal_interval: float = 300,
        events: OperationEventReader | None = None,
    ) -> None:
        if renewal_interval <= 0:
            raise ValueError("lease renewal interval must be positive")
        self._enabled = enabled
        self._admin_room_id = admin_room_id
        self._storage = storage
        self._leases = leases
        self._cli = cli
        self._tasks = tasks
        self._matrix = matrix
        self._supervisor = supervisor
        self._cache_root = cache_root.resolve()
        self._renewal_interval = renewal_interval
        self._events = events

    def status(self) -> dict[str, object]:
        return {
            "enabled": self._enabled,
            "providers": self._cli.status(),
        }

    async def execute(
        self,
        request: CodingCLIDelegationRequest,
        *,
        context: MutationContext,
        confirmed: bool = False,
    ) -> CodingCLIDelegationReceipt:
        self._authorize(context, confirmed=confirmed)
        task = await self._tasks.get(request.task_id)
        if task is None:
            raise NotFoundError(f"task/{request.task_id} does not exist")
        prompt_digest = _prompt_digest(request.prompt)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CODING_CLI_DELEGATION,
            target_key=f"task/{request.task_id}/coding-cli",
            request={
                "task_id": request.task_id,
                "provider": request.provider,
                "workspace": request.workspace,
                "prompt_sha256": prompt_digest,
                "timeout_seconds": request.timeout_seconds,
            },
        )
        return await self._execute_operation(
            operation=operation,
            request=request,
            task=task,
        )

    async def _execute_operation(
        self,
        *,
        operation: OperationRecord,
        request: CodingCLIDelegationRequest,
        task: TaskRecord,
    ) -> CodingCLIDelegationReceipt:
        if operation.status is OperationStatus.SUCCEEDED:
            return CodingCLIDelegationReceipt.model_validate(
                operation.result,
            )
        if operation.status is OperationStatus.FAILED:
            raise ConflictError("coding CLI delegation previously failed")

        task_cache = (
            self._cache_root / "shared" / "tasks" / request.task_id
        )
        prefix = f"shared/tasks/{request.task_id}/"
        lease = await self._acquire_lease(operation, request.task_id)
        stop_renewal = asyncio.Event()
        renewal_failures: list[Exception] = []
        renewal_task = asyncio.create_task(
            self._renew_lease_until_stopped(
                lease,
                stop_renewal,
                renewal_failures,
            ),
        )
        cli_receipt: CodingCLIReceipt | None = None
        mirror_up: Any | None = None
        workflow_failure: Exception | None = None
        try:
            await self._mirror_down(operation, prefix, task_cache)
            local_marker = task_cache / ".processing"
            local_marker.unlink(missing_ok=True)
            prompt_path = _prompt_path(task_cache, operation.operation_id)
            prompt_path.parent.mkdir(parents=True, exist_ok=True)
            prompt_path.write_text(request.prompt, encoding="utf-8")
            await self._mirror_prompt(operation, task_cache, prefix)

            workspace_root = task_cache / "workspace"
            workspace = workspace_root.joinpath(
                *PurePosixPath(request.workspace).parts,
            )
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.PROCESS,
                {
                    "operation": "execute_coding_cli",
                    "provider": request.provider,
                    "task_id": request.task_id,
                    "prompt_sha256": _prompt_digest(request.prompt),
                },
            )
            try:
                cli_receipt = await self._cli.run(
                    request.provider,
                    workspace=workspace,
                    prompt=request.prompt,
                    timeout_seconds=request.timeout_seconds,
                )
            except CodingCLIError as exc:
                cli_receipt = CodingCLIReceipt(
                    provider=request.provider,
                    success=False,
                    returncode=-1,
                    stdout="",
                    stderr=f"{type(exc).__name__}: {exc}",
                )
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.PROCESS,
                cli_receipt.model_dump(mode="json"),
            )
            _write_result_log(
                task_cache,
                operation.operation_id,
                cli_receipt,
            )
            mirror_up = await self._mirror_result(
                operation,
                task_cache,
                prefix,
            )
        # This orchestration boundary must preserve any primary failure while
        # still releasing the lease and surfacing renewal failures.
        except Exception as exc:  # noqa: BLE001
            workflow_failure = exc
        finally:
            stop_renewal.set()
            await renewal_task
            if renewal_failures:
                workflow_failure = workflow_failure or renewal_failures[0]
            try:
                await self._leases.release(lease)
            except Exception as exc:  # noqa: BLE001
                workflow_failure = workflow_failure or exc

        if workflow_failure is not None:
            raise workflow_failure
        if cli_receipt is None or mirror_up is None:
            raise RuntimeError(
                "coding CLI delegation produced no durable result",
            )
        return await self._notify(
            operation=operation,
            task=task,
            result=cli_receipt,
            mirror_manifest_sha256=str(mirror_up.manifest_sha256),
        )

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> CodingCLIDelegationReceipt:
        if operation.kind is not OperationKind.CODING_CLI_DELEGATION:
            raise ValueError("operation is not coding CLI delegation")
        if operation.status is OperationStatus.SUCCEEDED:
            return CodingCLIDelegationReceipt.model_validate(operation.result)
        if self._events is None:
            raise RecoveryError(
                "coding CLI recovery requires operation event evidence",
            )
        events = await self._events.events_for(operation.operation_id)
        if any(
            event.event_type == "effect_planned"
            and event.payload.get("effect") == ExternalEffect.PROCESS.value
            for event in events
        ):
            raise RecoveryError(
                "coding CLI process may have started; refusing blind replay",
            )
        task_id = str(operation.request.get("task_id", ""))
        provider = str(operation.request.get("provider", ""))
        workspace = str(operation.request.get("workspace", ""))
        digest = str(operation.request.get("prompt_sha256", ""))
        if not task_id or not provider or not workspace or not digest:
            raise RecoveryError("coding CLI request is incomplete")
        task = await self._tasks.get(task_id)
        if task is None:
            raise RecoveryError(f"coding CLI task {task_id} is missing")
        task_cache = self._cache_root / "shared" / "tasks" / task_id
        try:
            await self._storage.mirror_down(
                f"shared/tasks/{task_id}/",
                task_cache,
            )
            prompt = _prompt_path(
                task_cache,
                operation.operation_id,
            ).read_text(encoding="utf-8")
        except (OSError, ValueError) as exc:
            raise RecoveryError(
                "coding CLI prompt artifact is unavailable",
            ) from exc
        if _prompt_digest(prompt) != digest:
            raise RecoveryError("coding CLI prompt artifact hash mismatch")
        try:
            request = CodingCLIDelegationRequest.model_validate(
                {
                    "task_id": task_id,
                    "provider": provider,
                    "workspace": workspace,
                    "prompt": prompt,
                    "timeout_seconds": operation.request.get(
                        "timeout_seconds",
                    ),
                },
            )
        except (TypeError, ValueError) as exc:
            raise RecoveryError(
                "coding CLI request cannot be reconstructed",
            ) from exc
        return await self._execute_operation(
            operation=operation,
            request=request,
            task=task,
        )

    def _authorize(
        self,
        context: MutationContext,
        *,
        confirmed: bool,
    ) -> None:
        if not self._enabled:
            raise CodingCLIDelegationDisabled(
                "coding CLI delegation is disabled",
            )
        if context.room_id != self._admin_room_id:
            raise PermissionError(
                "coding CLI delegation is restricted to the admin room",
            )
        if not confirmed:
            raise CodingCLIConfirmationRequired(
                "coding CLI delegation requires administrator confirmation",
            )

    async def _acquire_lease(
        self,
        operation: OperationRecord,
        task_id: str,
    ) -> ProcessingLease:
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "acquire_processing_lease",
                "task_id": task_id,
            },
        )
        lease = await self._leases.acquire(
            task_id,
            processor="manager",
            operation=f"coding-cli/{operation.operation_id}",
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
        return lease

    async def _mirror_down(
        self,
        operation: OperationRecord,
        prefix: str,
        task_cache: Path,
    ) -> None:
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "pull_task_workspace",
                "task_id": operation.request["task_id"],
            },
        )
        receipt = await self._storage.mirror_down(prefix, task_cache)
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )

    async def _mirror_prompt(
        self,
        operation: OperationRecord,
        task_cache: Path,
        prefix: str,
    ) -> None:
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "persist_coding_prompt",
                "task_id": operation.request["task_id"],
                "prompt_sha256": operation.request["prompt_sha256"],
            },
        )
        receipt = await self._storage.mirror_up(task_cache, prefix)
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )

    async def _mirror_result(
        self,
        operation: OperationRecord,
        task_cache: Path,
        prefix: str,
    ) -> Any:
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "push_coding_result",
                "task_id": operation.request["task_id"],
            },
        )
        receipt = await self._storage.mirror_up(task_cache, prefix)
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _notify(
        self,
        *,
        operation: OperationRecord,
        task: TaskRecord,
        result: CodingCLIReceipt,
        mirror_manifest_sha256: str,
    ) -> CodingCLIDelegationReceipt:
        summary = _result_summary(result)
        transaction_id = matrix_transaction_id(operation.operation_id, 0)
        prefix = "coding-result:" if result.success else "coding-failed:"
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "send_coding_cli_result",
                "task_id": task.task_id,
                "room_id": task.room_id,
                "txn_id": transaction_id,
                "success": result.success,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                task.room_id,
                f"@{task.assigned_to} {task.task_id} {prefix}\n"
                f"{summary}\nRun `agentteams-sync` to review the workspace.",
                txn_id=transaction_id,
            )
        except Exception as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.MATRIX,
                type(exc).__name__,
            )
            raise
        receipt = CodingCLIDelegationReceipt(
            operation_id=operation.operation_id,
            task_id=task.task_id,
            provider=result.provider,
            success=result.success,
            returncode=result.returncode,
            message_event_id=event_id,
            mirror_manifest_sha256=mirror_manifest_sha256,
            summary=summary,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            receipt.model_dump(mode="json"),
        )
        return receipt

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
                # A lease backend can fail through several adapter-specific
                # exceptions; capture all so the main workflow can stop safely.
                except Exception as exc:  # noqa: BLE001
                    failures.append(exc)
                    return


def _prompt_digest(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def _prompt_path(task_cache: Path, operation_id: str) -> Path:
    return task_cache / "coding-prompts" / f"{operation_id}.txt"


def _write_result_log(
    task_cache: Path,
    operation_id: str,
    receipt: CodingCLIReceipt,
) -> None:
    path = task_cache / "coding-cli-logs" / f"{operation_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            receipt.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _result_summary(receipt: CodingCLIReceipt) -> str:
    if receipt.success:
        detail = receipt.stdout.strip() or "Coding CLI completed."
        return detail[:2_000]
    detail = receipt.stderr.strip() or receipt.stdout.strip()
    if not detail:
        detail = "no diagnostic output"
    return (
        f"{receipt.provider} failed with exit {receipt.returncode}: {detail}"
    )[:2_000]
