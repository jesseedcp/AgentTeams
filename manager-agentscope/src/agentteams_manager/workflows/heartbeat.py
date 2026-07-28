"""Deterministic resource and task reconciliation heartbeat."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    RecoveryError,
)
from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    TaskRecord,
    TopologySnapshot,
    WorkerResource,
)

from .resources import ResourceRecoveryReport
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
        self._operations = operations
        self._tasks = tasks
        self._projects = projects
        self._git = git
        self._files = files
        self._coding = coding

    async def reconcile_pending_tasks(self) -> TaskRecoveryReport:
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
        self._operations = operations
        self._integrations = integrations
        self._gateways = gateways

    async def reconcile_pending_integrations(
        self,
    ) -> IntegrationRecoveryReport:
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
        self._operations = operations
        self._notifications = notifications

    async def reconcile_pending_notifications(
        self,
    ) -> NotificationRecoveryReport:
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
        self._operations = operations
        self._tasks = tasks

    async def reconcile_pending_completions(
        self,
    ) -> CompletionRecoveryReport:
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
        overdue_after: timedelta = timedelta(hours=2),
        blocked_after: timedelta = timedelta(minutes=30),
        worker_silence_after: timedelta = timedelta(minutes=45),
    ) -> None:
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
        self._overdue_after = overdue_after
        self._blocked_after = blocked_after
        self._worker_silence_after = worker_silence_after

    async def inspect(self, now: datetime) -> SupervisionReport:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("supervision time must be timezone-aware")
        utc_now = now.astimezone(UTC)
        tasks = await self._tasks.list_all()
        workers = await self._workers.list_workers()
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
            is_running = (
                (worker.phase or "").casefold() in {"ready", "running"}
                and str(
                    worker.status.get("containerState", "running"),
                ).casefold()
                in {"ready", "running"}
            )
            stale = (
                heartbeat is not None
                and utc_now > heartbeat + self._worker_silence_after
            )
            if is_running and not stale:
                responsive.add(worker.name)
            if is_running and stale:
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


class Heartbeat:
    """Recover operations, refresh topology, then notify new failures."""

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
    """Dispatch one idempotent occurrence for each due recurring task."""

    def __init__(
        self,
        *,
        tasks: DueTaskReader,
        service: TaskService,
        late_grace: timedelta = timedelta(minutes=30),
    ) -> None:
        if late_grace < timedelta(0):
            raise ValueError("late grace must not be negative")
        self._tasks = tasks
        self._service = service
        self._late_grace = late_grace

    async def dispatch_due(self, now: datetime) -> DispatchReport:
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
    raw = task.metadata.get("deadline") or task.metadata.get("due_at")
    parsed = _parse_timestamp(raw)
    return parsed or task.updated_at + default_after


def _worker_heartbeat(worker: WorkerResource) -> datetime | None:
    for key in (
        "lastHeartbeatAt",
        "lastActiveAt",
        "lastSeenAt",
    ):
        parsed = _parse_timestamp(worker.status.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_timestamp(value: object) -> datetime | None:
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
