"""Deterministic resource reconciliation heartbeat."""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.models import TopologySnapshot

from .resources import ResourceRecoveryReport


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
