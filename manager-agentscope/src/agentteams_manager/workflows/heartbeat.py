"""Deterministic resource and task reconciliation heartbeat.

周期运行确定性的资源、任务、通知和集成恢复检查。

heartbeat 不是让大模型定时自由行动。每一轮按固定步骤续租/恢复 operation、刷新拓扑、
派发到期 recurring task、检查 Worker 进展、补发终态通知，并记录有界报告。单个子检查
失败会被隔离和上报，不能让整个循环永久停止；重复观察也通过 durable supervision
状态节流。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    RecoveryError,
)
from agentteams_manager.domain.ids import (
    matrix_transaction_id,
    operation_id_for,
)
from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    TaskRecord,
    TopologySnapshot,
    WorkerResource,
)

from .resources import MutationContext, ResourceRecoveryReport
from .tasks import TaskService


class ResourceRecovery(Protocol):
    async def reconcile_pending_resources(self) -> ResourceRecoveryReport: ...


class TopologyRefresh(Protocol):
    async def refresh(self) -> TopologySnapshot: ...


class FailureNotifications(Protocol):
    async def already_sent(self, operation_id: str) -> bool: ...

    async def send_terminal_failure(
        self,
        operation_id: str,
    ) -> None: ...


class TaskOperationReader(Protocol):
    async def list_recoverable(self) -> tuple[OperationRecord, ...]: ...


class OperationResumer(Protocol):
    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> object: ...


class TaskRecoveryReport(BaseModel):
    """Typed result of task, project, and Git operation recovery."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()
    needs_attention: tuple[str, ...] = ()


class TaskRecovery:
    """Route only task-owned operation kinds to deterministic resumers."""

    _TASK_KINDS = frozenset(
        {
            OperationKind.DELEGATE_TASK,
            OperationKind.FILE_SYNC,
        },
    )
    _PROJECT_KINDS = frozenset(
        {
            OperationKind.CREATE_PROJECT,
            OperationKind.UPDATE_PROJECT,
            OperationKind.CLOSE_PROJECT,
        },
    )

    def __init__(
        self,
        *,
        operations: TaskOperationReader,
        tasks: OperationResumer,
        projects: OperationResumer,
        git: OperationResumer,
        files: OperationResumer | None = None,
        coding: OperationResumer | None = None,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `operations`、`tasks`、`projects`、`git`、`files`、`coding`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        self._operations = operations
        self._tasks = tasks
        self._projects = projects
        self._git = git
        self._files = files
        self._coding = coding

    async def reconcile_pending_tasks(self) -> TaskRecoveryReport:
        # 逻辑说明：`reconcile_pending_tasks` 接收 当前服务依赖，核对并恢复 `pending tasks`，依次复用 `list_recoverable`、`append`、`resume_operation`，返回 `TaskRecoveryReport`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        owned = (
            self._TASK_KINDS
            | self._PROJECT_KINDS
            | {
                OperationKind.GIT_DELEGATION,
                OperationKind.CODING_CLI_DELEGATION,
            }
        )
        operations = tuple(
            operation
            for operation in await self._operations.list_recoverable()
            if operation.kind in owned
        )
        reconciled: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        needs_attention: list[str] = []
        for operation in operations:
            if operation.kind is OperationKind.FILE_SYNC:
                if self._files is None:
                    needs_attention.append(operation.operation_id)
                    continue
                resumer = self._files
            elif operation.kind is OperationKind.CODING_CLI_DELEGATION:
                if self._coding is None:
                    needs_attention.append(operation.operation_id)
                    continue
                resumer = self._coding
            elif operation.kind in self._TASK_KINDS:
                resumer = self._tasks
            elif operation.kind in self._PROJECT_KINDS:
                resumer = self._projects
            else:
                resumer = self._git
            try:
                await resumer.resume_operation(operation)
            except AmbiguousEffectError:
                pending.append(operation.operation_id)
            except RecoveryError:
                needs_attention.append(operation.operation_id)
            except Exception:
                failed.append(operation.operation_id)
            else:
                reconciled.append(operation.operation_id)
        return TaskRecoveryReport(
            inspected=len(operations),
            reconciled=tuple(reconciled),
            pending=tuple(pending),
            failed=tuple(failed),
            needs_attention=tuple(needs_attention),
        )


class TaskRecoveryRunner(Protocol):
    async def reconcile_pending_tasks(self) -> TaskRecoveryReport: ...


class RuntimeWatcher(Protocol):
    async def poll_once(self) -> object | None: ...


class IntegrationRecoveryReport(BaseModel):
    """Typed result of model, MCP, and service recovery."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()
    needs_attention: tuple[str, ...] = ()


class IntegrationOperationResumer(Protocol):
    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> object: ...


class IntegrationRecovery:
    """Recover operations owned by integration and gateway boundaries."""

    _KINDS = frozenset(
        {
            OperationKind.SWITCH_MODEL,
            OperationKind.CONFIGURE_MCP,
            OperationKind.CONFIGURE_GATEWAY,
            OperationKind.PUBLISH_SERVICE,
        },
    )

    def __init__(
        self,
        *,
        operations: TaskOperationReader,
        integrations: IntegrationOperationResumer,
        gateways: IntegrationOperationResumer | None = None,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `operations`、`integrations`、`gateways`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        self._operations = operations
        self._integrations = integrations
        self._gateways = gateways

    async def reconcile_pending_integrations(
        self,
    ) -> IntegrationRecoveryReport:
        # 逻辑说明：`reconcile_pending_integrations` 接收 当前服务依赖，核对并恢复 `pending integrations`，依次复用 `list_recoverable`、`RecoveryError`、`resume_operation`，返回 `IntegrationRecoveryReport`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        operations = tuple(
            operation
            for operation in await self._operations.list_recoverable()
            if operation.kind in self._KINDS
        )
        reconciled: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        needs_attention: list[str] = []
        for operation in operations:
            try:
                if operation.kind is OperationKind.CONFIGURE_GATEWAY:
                    if self._gateways is None:
                        raise RecoveryError(
                            "gateway recovery service is unavailable",
                        )
                    await self._gateways.resume_operation(operation)
                else:
                    await self._integrations.resume_operation(operation)
            except AmbiguousEffectError:
                pending.append(operation.operation_id)
            except RecoveryError:
                needs_attention.append(operation.operation_id)
            except Exception:
                failed.append(operation.operation_id)
            else:
                reconciled.append(operation.operation_id)
        return IntegrationRecoveryReport(
            inspected=len(operations),
            reconciled=tuple(reconciled),
            pending=tuple(pending),
            failed=tuple(failed),
            needs_attention=tuple(needs_attention),
        )


class IntegrationRecoveryRunner(Protocol):
    async def reconcile_pending_integrations(
        self,
    ) -> IntegrationRecoveryReport: ...


class NotificationRecoveryReport(BaseModel):
    """Typed result of exactly-once notification recovery."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()
    needs_attention: tuple[str, ...] = ()


class NotificationRecovery:
    """Resume only durable Matrix notification operations."""

    def __init__(
        self,
        *,
        operations: TaskOperationReader,
        notifications: OperationResumer,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `operations`、`notifications`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        self._operations = operations
        self._notifications = notifications

    async def reconcile_pending_notifications(
        self,
    ) -> NotificationRecoveryReport:
        # 逻辑说明：`reconcile_pending_notifications` 接收 当前服务依赖，核对并恢复 `pending notifications`，依次复用 `list_recoverable`、`resume_operation`、`append`，返回 `NotificationRecoveryReport`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        operations = tuple(
            operation
            for operation in await self._operations.list_recoverable()
            if operation.kind is OperationKind.SEND_NOTIFICATION
        )
        reconciled: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        needs_attention: list[str] = []
        for operation in operations:
            try:
                await self._notifications.resume_operation(operation)
            except AmbiguousEffectError:
                pending.append(operation.operation_id)
            except RecoveryError:
                needs_attention.append(operation.operation_id)
            except Exception:
                failed.append(operation.operation_id)
            else:
                reconciled.append(operation.operation_id)
        return NotificationRecoveryReport(
            inspected=len(operations),
            reconciled=tuple(reconciled),
            pending=tuple(pending),
            failed=tuple(failed),
            needs_attention=tuple(needs_attention),
        )


class NotificationRecoveryRunner(Protocol):
    async def reconcile_pending_notifications(
        self,
    ) -> NotificationRecoveryReport: ...


class LeaseReclaimer(Protocol):
    async def reclaim_expired(self, now: datetime) -> object: ...


class DueTaskDispatcher(Protocol):
    async def dispatch_due(self, now: datetime) -> DispatchReport: ...


class CompletionRecovery(Protocol):
    async def reconcile_pending_completions(
        self,
    ) -> CompletionRecoveryReport: ...


class CompletionRecoveryReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()
    needs_attention: tuple[str, ...] = ()


class TaskCompletionRecovery:
    """Recover finite completion and recurring execution operations later."""

    def __init__(
        self,
        *,
        operations: TaskOperationReader,
        tasks: OperationResumer,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `operations`、`tasks`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        self._operations = operations
        self._tasks = tasks

    async def reconcile_pending_completions(
        self,
    ) -> CompletionRecoveryReport:
        # 逻辑说明：`reconcile_pending_completions` 接收 当前服务依赖，核对并恢复 `pending completions`，依次复用 `list_recoverable`、`resume_operation`、`append`，返回 `CompletionRecoveryReport`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        operations = tuple(
            operation
            for operation in await self._operations.list_recoverable()
            if operation.kind is OperationKind.COMPLETE_TASK
        )
        reconciled: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        needs_attention: list[str] = []
        for operation in operations:
            try:
                await self._tasks.resume_operation(operation)
            except AmbiguousEffectError:
                pending.append(operation.operation_id)
            except RecoveryError:
                needs_attention.append(operation.operation_id)
            except Exception:
                failed.append(operation.operation_id)
            else:
                reconciled.append(operation.operation_id)
        return CompletionRecoveryReport(
            inspected=len(operations),
            reconciled=tuple(reconciled),
            pending=tuple(pending),
            failed=tuple(failed),
            needs_attention=tuple(needs_attention),
        )


class SnapshotScheduler(Protocol):
    async def snapshot_if_due(self) -> bool: ...


class SupervisionTaskReader(Protocol):
    async def list_all(self) -> tuple[TaskRecord, ...]: ...


class SupervisionWorkerReader(Protocol):
    async def list_workers(self) -> tuple[WorkerResource, ...]: ...


class SupervisionNotifications(Protocol):
    async def send_once(
        self,
        *,
        source_operation_id: str,
        text: str,
    ) -> object: ...


class SupervisionState(Protocol):
    async def record_ping(
        self,
        *,
        subject_key: str,
        observed_token: str,
        pinged_at: datetime,
    ) -> int: ...


class SupervisionLifecycle(Protocol):
    async def wake_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> WorkerResource: ...


class SupervisionMatrix(Protocol):
    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str: ...


class SupervisionAlert(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str
    subject: str
    source_operation_id: str
    message: str


class SupervisionReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected_tasks: int = Field(ge=0)
    inspected_workers: int = Field(ge=0)
    alerts: tuple[SupervisionAlert, ...] = ()
    notified: int = Field(ge=0)
    pinged: int = Field(default=0, ge=0)
    woken: int = Field(default=0, ge=0)


class SemanticSupervisor:
    """Escalate only durable facts that cross explicit time thresholds."""

    _ACTIVE_TASK_STATES = frozenset(
        {"assigned", "dispatched", "in_progress"},
    )

    def __init__(
        self,
        *,
        tasks: SupervisionTaskReader,
        workers: SupervisionWorkerReader,
        notifications: SupervisionNotifications,
        state: SupervisionState | None = None,
        lifecycle: SupervisionLifecycle | None = None,
        matrix: SupervisionMatrix | None = None,
        overdue_after: timedelta = timedelta(hours=2),
        blocked_after: timedelta = timedelta(minutes=30),
        worker_silence_after: timedelta = timedelta(minutes=45),
    ) -> None:
        # 逻辑说明：`__init__` 接收 `tasks`、`workers`、`notifications`、`state`、`lifecycle`、`matrix`、`overdue_after`、`blocked_after`、`worker_silence_after`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，依次复用 `timedelta`、`ValueError`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        for label, threshold in (
            ("overdue", overdue_after),
            ("blocked", blocked_after),
            ("worker silence", worker_silence_after),
        ):
            if threshold <= timedelta(0):
                raise ValueError(f"{label} threshold must be positive")
        self._tasks = tasks
        self._workers = workers
        self._notifications = notifications
        self._state = state
        self._lifecycle = lifecycle
        self._matrix = matrix
        self._overdue_after = overdue_after
        self._blocked_after = blocked_after
        self._worker_silence_after = worker_silence_after

    async def inspect(self, now: datetime) -> SupervisionReport:
        # 逻辑说明：`inspect` 接收 `now`，检查 `待恢复 operation、定时任务和语义监督巡检`，依次复用 `utcoffset`、`ValueError`、`astimezone`，返回 `SupervisionReport`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("supervision time must be timezone-aware")
        utc_now = now.astimezone(UTC)
        tasks = await self._tasks.list_all()
        workers = await self._workers.list_workers()
        workers_by_name = {worker.name: worker for worker in workers}
        alerts: list[SupervisionAlert] = []
        for task in tasks:
            deadline = _task_deadline(task, self._overdue_after)
            if (
                task.status in self._ACTIVE_TASK_STATES
                and utc_now > deadline
            ):
                alerts.append(
                    _supervision_alert(
                        "task_overdue",
                        task.task_id,
                        task.updated_at.isoformat(),
                        f"[Task Overdue] {task.task_id}: {task.title}; "
                        f"assignee={task.assigned_to}; "
                        f"threshold={deadline.isoformat()}",
                    ),
                )
            if (
                task.status == "blocked"
                and utc_now > task.updated_at + self._blocked_after
            ):
                alerts.append(
                    _supervision_alert(
                        "project_blocker",
                        task.task_id,
                        task.updated_at.isoformat(),
                        f"[Project Blocker] {task.task_id}: {task.title}; "
                        f"assignee={task.assigned_to}",
                    ),
                )
        responsive: set[str] = set()
        for worker in workers:
            heartbeat = _worker_heartbeat(worker)
            is_running = _worker_is_running(worker)
            stale = (
                heartbeat is not None
                and utc_now > heartbeat + self._worker_silence_after
            )
            if is_running and not stale:
                responsive.add(worker.name)
            if is_running and stale:
                assert heartbeat is not None
                alerts.append(
                    _supervision_alert(
                        "worker_nonresponsive",
                        worker.name,
                        heartbeat.isoformat(),
                        f"[Worker Nonresponsive] {worker.name}; "
                        f"last heartbeat={heartbeat.isoformat()}",
                    ),
                )
        waiting = tuple(
            task
            for task in tasks
            if task.status in {"pending", "ready"}
        )
        if waiting and len(waiting) > len(responsive):
            fingerprint = ",".join(
                (
                    *(task.task_id for task in waiting),
                    "|",
                    *sorted(responsive),
                ),
            )
            alerts.append(
                _supervision_alert(
                    "capacity_shortage",
                    "workers",
                    fingerprint,
                    "[Capacity Shortage] "
                    f"{len(waiting)} waiting tasks but only "
                    f"{len(responsive)} responsive Workers.",
                ),
            )
        pinged = 0
        woken = 0
        if (
            self._state is not None
            and self._lifecycle is not None
            and self._matrix is not None
        ):
            for task in tasks:
                if (
                    task.status not in self._ACTIVE_TASK_STATES
                    or utc_now
                    <= _task_deadline(task, self._overdue_after)
                ):
                    continue
                task_pinged, task_woken, task_alert = (
                    await self._supervise_overdue_task(
                        task,
                        workers_by_name.get(task.assigned_to),
                        utc_now,
                    )
                )
                pinged += task_pinged
                woken += task_woken
                if task_alert is not None:
                    alerts.append(task_alert)
        for alert in alerts:
            await self._notifications.send_once(
                source_operation_id=alert.source_operation_id,
                text=alert.message,
            )
        return SupervisionReport(
            inspected_tasks=len(tasks),
            inspected_workers=len(workers),
            alerts=tuple(alerts),
            notified=len(alerts),
            pinged=pinged,
            woken=woken,
        )

    async def _supervise_overdue_task(
        self,
        task: TaskRecord,
        worker: WorkerResource | None,
        now: datetime,
    ) -> tuple[int, int, SupervisionAlert | None]:
        # 逻辑说明：`_supervise_overdue_task` 接收 `task`、`worker`、`now`，监督 `overdue task`，依次复用 `_supervision_alert`、`isoformat`、`_worker_heartbeat`，返回 `tuple[int, int, SupervisionAlert | None]`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        assert self._state is not None
        assert self._lifecycle is not None
        assert self._matrix is not None
        if worker is None:
            return (
                0,
                0,
                _supervision_alert(
                    "worker_unavailable",
                    task.task_id,
                    task.updated_at.isoformat(),
                    f"[Worker Unavailable] {task.assigned_to} is missing "
                    f"while {task.task_id} is active; 请重新分配该任务。",
                ),
            )
        heartbeat = _worker_heartbeat(worker)
        observed_token = "|".join(
            (
                task.updated_at.isoformat(),
                heartbeat.isoformat() if heartbeat is not None else "none",
            ),
        )
        room_id = str(
            task.metadata.get("project_room_id") or task.room_id,
        )
        matrix_user_id = str(
            task.metadata.get("matrix_user_id")
            or worker.matrix_user_id
            or "",
        )
        woken = 0
        phase = (worker.phase or "").casefold()
        if phase in {"failed", "error", "deleting"}:
            return (
                0,
                0,
                _supervision_alert(
                    "worker_unavailable",
                    task.task_id,
                    observed_token,
                    f"[Worker Unavailable] {worker.name} is {phase}; "
                    f"{task.task_id} 需要重新分配。",
                ),
            )
        if not _worker_is_running(worker):
            context = MutationContext(
                room_id=room_id,
                event_id=f"heartbeat:{task.task_id}:{observed_token}",
                tool_call_id=f"wake:{worker.name}",
            )
            try:
                worker = await self._lifecycle.wake_worker(
                    worker.name,
                    context=context,
                )
            except Exception as exc:
                return (
                    0,
                    0,
                    _supervision_alert(
                        "worker_wake_failed",
                        task.task_id,
                        observed_token,
                        f"[Worker Wake Failed] {worker.name}; "
                        f"task={task.task_id}; error={type(exc).__name__}; "
                        "请重新分配或人工介入。",
                    ),
                )
            woken = 1
        message = (
            f"{matrix_user_id} "
            if matrix_user_id
            else ""
        )
        if task.delegated_to_team:
            message += (
                f"团队任务 {task.task_id} 进展如何？"
                "请汇报当前阶段和阻塞项。"
            )
        else:
            message += (
                f"任务 {task.task_id}「{task.title}」进展如何？"
                "如有阻塞请立即说明。"
            )
        ping_operation_id = operation_id_for(
            room_id,
            f"heartbeat:{now.isoformat()}",
            f"progress:{task.task_id}",
        )
        try:
            await self._matrix.send_text(
                room_id,
                message,
                txn_id=matrix_transaction_id(ping_operation_id, 0),
                mentions=(
                    (matrix_user_id,)
                    if matrix_user_id
                    else ()
                ),
            )
        except Exception as exc:
            return (
                0,
                woken,
                _supervision_alert(
                    "worker_ping_failed",
                    task.task_id,
                    observed_token,
                    f"[Worker Ping Failed] task={task.task_id}; "
                    f"error={type(exc).__name__}",
                ),
            )
        missed_cycles = await self._state.record_ping(
            subject_key=f"task:{task.task_id}",
            observed_token=observed_token,
            pinged_at=now,
        )
        if missed_cycles < 1:
            return 1, woken, None
        recommendation = (
            "请使用 reassign_project_task 重新分配。"
            if task.project_id
            else "请重新分配该任务给可用 Worker。"
        )
        return (
            1,
            woken,
            _supervision_alert(
                "task_unresponsive",
                task.task_id,
                observed_token,
                f"[Task Unresponsive] {task.task_id} 已连续 "
                f"{missed_cycles + 1} 个心跳周期无可观察进展；"
                f"assignee={task.assigned_to}。{recommendation}",
            ),
        )


class SemanticSupervisionRunner(Protocol):
    async def inspect(self, now: datetime) -> SupervisionReport: ...


class HeartbeatReport(BaseModel):
    """Typed evidence from one model-free reconciliation pass."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: int = Field(ge=0)
    pending: int = Field(ge=0)
    failed: int = Field(ge=0)
    topology_revision: int = Field(ge=0)
    notifications: int = Field(ge=0)
    task_inspected: int = Field(default=0, ge=0)
    task_reconciled: int = Field(default=0, ge=0)
    task_pending: int = Field(default=0, ge=0)
    task_failed: int = Field(default=0, ge=0)
    task_needs_attention: int = Field(default=0, ge=0)
    leases_reclaimed: int = Field(default=0, ge=0)
    lease_conflicts: int = Field(default=0, ge=0)
    recurring_dispatched: int = Field(default=0, ge=0)
    recurring_pending: int = Field(default=0, ge=0)
    completions_reconciled: int = Field(default=0, ge=0)
    completions_pending: int = Field(default=0, ge=0)
    completions_failed: int = Field(default=0, ge=0)
    runtime_changed: bool = False
    integration_inspected: int = Field(default=0, ge=0)
    integration_reconciled: int = Field(default=0, ge=0)
    integration_pending: int = Field(default=0, ge=0)
    integration_failed: int = Field(default=0, ge=0)
    integration_needs_attention: int = Field(default=0, ge=0)
    notification_inspected: int = Field(default=0, ge=0)
    notification_reconciled: int = Field(default=0, ge=0)
    notification_pending: int = Field(default=0, ge=0)
    notification_failed: int = Field(default=0, ge=0)
    notification_needs_attention: int = Field(default=0, ge=0)
    snapshot_created: bool = False
    supervision_alerts: int = Field(default=0, ge=0)
    supervision_tasks: int = Field(default=0, ge=0)
    supervision_workers: int = Field(default=0, ge=0)
    supervision_pinged: int = Field(default=0, ge=0)
    supervision_woken: int = Field(default=0, ge=0)


class Heartbeat:
    """按固定次序执行一轮 Manager 全局恢复与一致性检查。"""

    def __init__(
        self,
        *,
        recovery: ResourceRecovery,
        topology: TopologyRefresh,
        notifications: FailureNotifications,
        task_recovery: TaskRecoveryRunner | None = None,
        leases: LeaseReclaimer | None = None,
        task_scheduler: DueTaskDispatcher | None = None,
        completions: CompletionRecovery | None = None,
        snapshotter: SnapshotScheduler | None = None,
        runtime_watcher: RuntimeWatcher | None = None,
        integration_recovery: IntegrationRecoveryRunner | None = None,
        notification_recovery: NotificationRecoveryRunner | None = None,
        semantic_supervision: SemanticSupervisionRunner | None = None,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `recovery`、`topology`、`notifications`、`task_recovery`、`leases`、`task_scheduler`、`completions`、`snapshotter`、`runtime_watcher`、`integration_recovery`、`notification_recovery`、`semantic_supervision`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        self._recovery = recovery
        self._topology = topology
        self._notifications = notifications
        self._task_recovery = task_recovery
        self._leases = leases
        self._task_scheduler = task_scheduler
        self._completions = completions
        self._snapshotter = snapshotter
        self._runtime_watcher = runtime_watcher
        self._integration_recovery = integration_recovery
        self._notification_recovery = notification_recovery
        self._semantic_supervision = semantic_supervision

    async def run_once(self) -> HeartbeatReport:
        # 逻辑说明：`run_once` 按固定次序轮询 runtime、恢复资源/Task/集成/通知、刷新拓扑、回收 lease、派发到期任务并执行语义监督，最后把各阶段结果汇总为 HeartbeatReport；单项失败按既有隔离策略记录。
        runtime_change = (
            await self._runtime_watcher.poll_once()
            if self._runtime_watcher is not None
            else None
        )
        recovery = await self._recovery.reconcile_pending_resources()
        snapshot = await self._topology.refresh()
        integration_recovery = (
            await self._integration_recovery.reconcile_pending_integrations()
            if self._integration_recovery is not None
            else IntegrationRecoveryReport(inspected=0)
        )
        task_recovery = (
            await self._task_recovery.reconcile_pending_tasks()
            if self._task_recovery is not None
            else TaskRecoveryReport(inspected=0)
        )
        notification_recovery = (
            await self._notification_recovery
            .reconcile_pending_notifications()
            if self._notification_recovery is not None
            else NotificationRecoveryReport(inspected=0)
        )
        now = datetime.now(UTC)
        lease_report = (
            await self._leases.reclaim_expired(now)
            if self._leases is not None
            else None
        )
        dispatch_report = (
            await self._task_scheduler.dispatch_due(now)
            if self._task_scheduler is not None
            else DispatchReport(inspected=0)
        )
        completion_report = (
            await self._completions.reconcile_pending_completions()
            if self._completions is not None
            else CompletionRecoveryReport(inspected=0)
        )
        supervision_report = (
            await self._semantic_supervision.inspect(now)
            if self._semantic_supervision is not None
            else SupervisionReport(
                inspected_tasks=0,
                inspected_workers=0,
                notified=0,
            )
        )

        notification_count = 0
        for operation_id in dict.fromkeys(
            (
                *recovery.failed,
                *integration_recovery.failed,
                *task_recovery.failed,
                *completion_report.failed,
            ),
        ):
            if await self._notifications.already_sent(operation_id):
                continue
            await self._notifications.send_terminal_failure(operation_id)
            notification_count += 1
        snapshot_created = (
            await self._snapshotter.snapshot_if_due()
            if self._snapshotter is not None
            else False
        )

        return HeartbeatReport(
            inspected=recovery.inspected,
            reconciled=len(recovery.reconciled),
            pending=len(recovery.pending),
            failed=len(recovery.failed),
            topology_revision=snapshot.revision,
            notifications=notification_count,
            task_inspected=task_recovery.inspected,
            task_reconciled=len(task_recovery.reconciled),
            task_pending=len(task_recovery.pending),
            task_failed=len(task_recovery.failed),
            task_needs_attention=len(task_recovery.needs_attention),
            leases_reclaimed=(
                len(getattr(lease_report, "reclaimed", ()))
                if lease_report is not None
                else 0
            ),
            lease_conflicts=(
                len(getattr(lease_report, "conflicted", ()))
                if lease_report is not None
                else 0
            ),
            recurring_dispatched=len(dispatch_report.dispatched),
            recurring_pending=len(dispatch_report.pending),
            completions_reconciled=len(completion_report.reconciled),
            completions_pending=len(completion_report.pending),
            completions_failed=len(completion_report.failed),
            runtime_changed=runtime_change is not None,
            integration_inspected=integration_recovery.inspected,
            integration_reconciled=len(integration_recovery.reconciled),
            integration_pending=len(integration_recovery.pending),
            integration_failed=len(integration_recovery.failed),
            integration_needs_attention=len(
                integration_recovery.needs_attention,
            ),
            notification_inspected=notification_recovery.inspected,
            notification_reconciled=len(
                notification_recovery.reconciled,
            ),
            notification_pending=len(notification_recovery.pending),
            notification_failed=len(notification_recovery.failed),
            notification_needs_attention=len(
                notification_recovery.needs_attention,
            ),
            snapshot_created=snapshot_created,
            supervision_alerts=supervision_report.notified,
            supervision_tasks=supervision_report.inspected_tasks,
            supervision_workers=supervision_report.inspected_workers,
            supervision_pinged=supervision_report.pinged,
            supervision_woken=supervision_report.woken,
        )


class DueTaskReader(Protocol):
    async def due_schedules(
        self,
        now: datetime,
    ) -> tuple[TaskRecord, ...]: ...


class DispatchReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    dispatched: tuple[str, ...] = ()
    already_dispatched: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    late: tuple[str, ...] = ()


class TaskHeartbeat:
    """派发到期 recurring occurrence，并监督有限任务的可观察进展。"""

    def __init__(
        self,
        *,
        tasks: DueTaskReader,
        service: TaskService,
        late_grace: timedelta = timedelta(minutes=30),
    ) -> None:
        # 逻辑说明：`__init__` 接收 `tasks`、`service`、`late_grace`，初始化依赖 `待恢复 operation、定时任务和语义监督巡检`，依次复用 `timedelta`、`ValueError`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        if late_grace < timedelta(0):
            raise ValueError("late grace must not be negative")
        self._tasks = tasks
        self._service = service
        self._late_grace = late_grace

    async def dispatch_due(self, now: datetime) -> DispatchReport:
        # 逻辑说明：`dispatch_due` 接收 `now`，分派 `due`，依次复用 `utcoffset`、`ValueError`、`astimezone`，返回 `DispatchReport`。 它会推进 待恢复 operation、定时任务和语义监督巡检 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("heartbeat time must be timezone-aware")
        utc_now = now.astimezone(UTC)
        due = await self._tasks.due_schedules(utc_now)
        dispatched: list[str] = []
        already: list[str] = []
        pending: list[str] = []
        late: list[str] = []
        for task in due:
            if (
                task.next_scheduled_at is not None
                and utc_now > task.next_scheduled_at + self._late_grace
            ):
                late.append(task.task_id)
            try:
                receipt = await self._service.dispatch_recurring(task)
            except Exception:
                pending.append(task.task_id)
                continue
            if receipt.dispatched:
                dispatched.append(task.task_id)
            else:
                already.append(task.task_id)
        return DispatchReport(
            inspected=len(due),
            dispatched=tuple(dispatched),
            already_dispatched=tuple(already),
            pending=tuple(pending),
            late=tuple(late),
        )


def _task_deadline(
    task: TaskRecord,
    default_after: timedelta,
) -> datetime:
    # 逻辑说明：`_task_deadline` 优先解析 Task metadata 中的 deadline，否则以创建时间加 default_after 计算监督截止点；只返回 UTC 时间，不修改 Task。
    raw = task.metadata.get("deadline") or task.metadata.get("due_at")
    parsed = _parse_timestamp(raw)
    return parsed or task.updated_at + default_after


def _worker_heartbeat(worker: WorkerResource) -> datetime | None:
    # 逻辑说明：`_worker_heartbeat` 从 Worker status/metadata 中按既定字段优先级解析最后心跳时间，找不到合法时间则返回 None；用于判断失联而不更新 Worker。
    for key in (
        "lastHeartbeatAt",
        "lastActiveAt",
        "lastSeenAt",
    ):
        parsed = _parse_timestamp(worker.status.get(key))
        if parsed is not None:
            return parsed
    return None


def _worker_is_running(worker: WorkerResource) -> bool:
    # 逻辑说明：`_worker_is_running` 将 Worker phase/status 统一为小写后判断是否属于运行态集合，返回监督用布尔值；它只读取资源快照。
    return (
        (worker.phase or "").casefold() in {"ready", "running"}
        and str(
            worker.status.get("containerState", "running"),
        ).casefold()
        in {"ready", "running"}
    )


def _parse_timestamp(value: object) -> datetime | None:
    # 逻辑说明：`_parse_timestamp` 接收 `value`，解析 `timestamp`，依次复用 `fromisoformat`、`replace`、`utcoffset`，返回 `datetime | None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _supervision_alert(
    kind: str,
    subject: str,
    version: str,
    message: str,
) -> SupervisionAlert:
    # 逻辑说明：`_supervision_alert` 接收 `kind`、`subject`、`version`、`message`，处理 `alert`，依次复用 `hexdigest`、`sha256`、`encode`，返回 `SupervisionAlert`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
    import hashlib

    fingerprint = hashlib.sha256(
        f"{kind}\0{subject}\0{version}".encode(),
    ).hexdigest()[:24]
    return SupervisionAlert(
        kind=kind,
        subject=subject,
        source_operation_id=f"supervision:{kind}:{fingerprint}",
        message=message,
    )
