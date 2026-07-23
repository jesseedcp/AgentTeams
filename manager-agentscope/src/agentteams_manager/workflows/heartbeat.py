"""Deterministic resource and task reconciliation heartbeat."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.models import TaskRecord, TopologySnapshot

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


class HeartbeatReport(BaseModel):
    """Typed evidence from one model-free reconciliation pass."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inspected: int = Field(ge=0)
    reconciled: int = Field(ge=0)
    pending: int = Field(ge=0)
    failed: int = Field(ge=0)
    topology_revision: int = Field(ge=0)
    notifications: int = Field(ge=0)


class Heartbeat:
    """Recover operations, refresh topology, then notify new failures."""

    def __init__(
        self,
        *,
        recovery: ResourceRecovery,
        topology: TopologyRefresh,
        notifications: FailureNotifications,
    ) -> None:
        self._recovery = recovery
        self._topology = topology
        self._notifications = notifications

    async def run_once(self) -> HeartbeatReport:
        recovery = await self._recovery.reconcile_pending_resources()
        snapshot = await self._topology.refresh()

        notification_count = 0
        for operation_id in recovery.failed:
            if await self._notifications.already_sent(operation_id):
                continue
            await self._notifications.send_terminal_failure(operation_id)
            notification_count += 1

        return HeartbeatReport(
            inspected=recovery.inspected,
            reconciled=len(recovery.reconciled),
            pending=len(recovery.pending),
            failed=len(recovery.failed),
            topology_revision=snapshot.revision,
            notifications=notification_count,
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
