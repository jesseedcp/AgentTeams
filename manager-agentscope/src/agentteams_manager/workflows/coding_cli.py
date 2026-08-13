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
        # 逻辑说明：`require_relative_workspace` 把 `workspace` 解析为 POSIX 相对路径，拒绝反斜杠、绝对路径和 `..`，验证成功后返回原请求对象；此纯校验不改状态，非法路径直接抛错以阻止 Coding CLI 越出任务工作区。
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
        # 逻辑说明：`__init__` 校验 lease 续期时间并保存 storage、CLI、Task、Matrix 与 supervisor 等依赖，建立后续委托所需的实例状态；配置非法时立即失败，不启动任何外部操作。
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
        # 逻辑说明：`status` 汇总是否启用 Coding CLI 以及各 provider 的当前可用性并返回字典；这里只读配置与客户端状态，不申请 lease 或执行命令。
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
        # 逻辑说明：`execute` 接收已校验请求和 mutation context，先做授权、读取 Task、隐藏 prompt 原文只保留摘要，再创建可恢复 operation 并进入执行阶段；Task 不存在或未确认时在产生外部效果前失败。
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
        # 逻辑说明：`_execute_operation` 按“lease→mirror down→写 prompt→运行 CLI→mirror up→通知→释放 lease”编排一次委托，并用续期任务保护长运行操作；任何阶段失败都会进入既有恢复/清理路径，避免盲目重复外部命令。
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
        # 逻辑说明：`resume_operation` 根据 operation journal、effect acknowledgement、lease 和结果日志判断 Coding CLI 委托已经完成、可继续还是存在歧义；恢复只补未证明完成的阶段，无法证明时抛 RecoveryError 交由人工处理。
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
        # 逻辑说明：`_authorize` 检查功能开关、管理员身份及显式确认标志，不通过时分别抛出明确错误；该检查必须在 lease、文件和 CLI 副作用之前完成。
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
        # 逻辑说明：`_acquire_lease` 先在 supervisor 中登记 lease 外部效果意图，再为 Task 获取 processing lease 并记录回执；取得 lease 失败会保留可恢复证据，防止两个处理者同时修改同一 workspace。
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
        # 逻辑说明：`_mirror_down` 登记下载效果后把 Task 前缀镜像到本地 cache，并把 manifest 作为 acknowledgement 写入 journal；I/O 失败不会被标记成功。
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
        # 逻辑说明：`_mirror_prompt` 把仅存在于本地的 prompt 文件镜像回 Task 前缀并记录 manifest，使重启后能证明输入已持久化；上传失败沿用 supervisor 的失败语义。
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
        # 逻辑说明：`_mirror_result` 把 CLI 修改后的 workspace 和结果日志镜像回共享存储并记录 manifest；只有远端确认后才承认该阶段完成。
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
        # 逻辑说明：`_notify` 从 CLI receipt 生成脱敏摘要和幂等 Matrix transaction，登记消息效果后通知任务房间并返回最终 receipt；发送结果不确定时保留 journal 供恢复，避免重复通知。
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
        # 逻辑说明：`_renew_lease_until_stopped` 在 stop event 触发前按续期间隔刷新 processing lease，把续期异常收集到 `failures` 后结束；它不吞掉失败，因此主流程能阻止把失去所有权的运行误报成功。
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
    # 逻辑说明：`_prompt_digest` 对 prompt 的 UTF-8 字节计算 SHA-256 并返回十六进制摘要；不保存或返回 prompt 原文，也不产生外部副作用。
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def _prompt_path(task_cache: Path, operation_id: str) -> Path:
    # 逻辑说明：`_prompt_path` 根据 Task cache 与 operation ID 构造本次委托的 prompt 文件路径并返回；只计算路径，不创建目录或文件。
    return task_cache / "coding-prompts" / f"{operation_id}.txt"


def _write_result_log(
    task_cache: Path,
    operation_id: str,
    receipt: CodingCLIReceipt,
) -> None:
    # 逻辑说明：`_write_result_log` 在 Task cache 下创建结果目录，把 CLI receipt 以稳定 JSON 写入 operation 专属日志并返回路径；写盘失败直接传播，避免恢复阶段看到不存在的成功证据。
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
    # 逻辑说明：`_result_summary` 按 stdout、stderr、provider fallback 的优先级挑选非空文本，裁剪后返回适合通知的摘要；不修改 receipt 或外部状态。
    if receipt.success:
        detail = receipt.stdout.strip() or "Coding CLI completed."
        return detail[:2_000]
    detail = receipt.stderr.strip() or receipt.stdout.strip()
    if not detail:
        detail = "no diagnostic output"
    return (
        f"{receipt.provider} failed with exit {receipt.returncode}: {detail}"
    )[:2_000]
