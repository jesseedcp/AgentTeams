from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import TopologySnapshot
from agentteams_manager.workflows.heartbeat import Heartbeat
from agentteams_manager.workflows.resources import ResourceRecoveryReport


class Recovery:
    def __init__(self) -> None:
        self.calls = 0

    async def reconcile_pending_resources(self) -> ResourceRecoveryReport:
        self.calls += 1
        return ResourceRecoveryReport(
            inspected=1,
            reconciled=("operation-1",),
        )


class Topology:
    def __init__(self) -> None:
        self.calls = 0

    async def refresh(self) -> TopologySnapshot:
        self.calls += 1
        return TopologySnapshot(
            revision=9,
            refreshed_at=datetime(2026, 7, 23, tzinfo=UTC),
        )


class Notifications:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def already_sent(self, operation_id: str) -> bool:
        return any(item[0] == operation_id for item in self.sent)

    async def send_terminal_failure(
        self,
        operation_id: str,
    ) -> None:
        self.sent.append((operation_id, "failed"))


@pytest.mark.asyncio
async def test_heartbeat_reconciles_without_model_call() -> None:
    recovery = Recovery()
    topology = Topology()
    notifications = Notifications()
    heartbeat = Heartbeat(
        recovery=recovery,
        topology=topology,
        notifications=notifications,
    )

    report = await heartbeat.run_once()

    assert report.reconciled == 1
    assert report.topology_revision == 9
    assert recovery.calls == 1
    assert topology.calls == 1
    assert notifications.sent == []


@pytest.mark.asyncio
async def test_terminal_failure_notification_is_idempotent() -> None:
    class FailedRecovery:
        async def reconcile_pending_resources(
            self,
        ) -> ResourceRecoveryReport:
            return ResourceRecoveryReport(
                inspected=1,
                failed=("operation-failed",),
            )

    notifications = Notifications()
    heartbeat = Heartbeat(
        recovery=FailedRecovery(),
        topology=Topology(),
        notifications=notifications,
    )

    first = await heartbeat.run_once()
    second = await heartbeat.run_once()

    assert first.notifications == 1
    assert second.notifications == 0
    assert notifications.sent == [("operation-failed", "failed")]
