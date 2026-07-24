from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.errors import AmbiguousEffectError
from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.workflows.heartbeat import NotificationRecovery


def operation(
    operation_id: str,
    kind: OperationKind,
) -> OperationRecord:
    now = datetime(2026, 7, 24, tzinfo=UTC)
    return OperationRecord(
        operation_id=operation_id,
        kind=kind,
        target_key="matrix-notification/!admin:example",
        status=OperationStatus.RECONCILING,
        request={"source_operation_id": "f" * 32},
        created_at=now,
        updated_at=now,
    )


class Operations:
    async def list_recoverable(self) -> tuple[OperationRecord, ...]:
        return (
            operation("a" * 32, OperationKind.SEND_NOTIFICATION),
            operation("b" * 32, OperationKind.CREATE_WORKER),
            operation("c" * 32, OperationKind.SEND_NOTIFICATION),
        )


class Notifications:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def resume_operation(self, pending: OperationRecord) -> object:
        self.calls.append(pending.operation_id)
        if pending.operation_id == "c" * 32:
            raise AmbiguousEffectError("Matrix result remains ambiguous")
        return object()


@pytest.mark.asyncio
async def test_notification_recovery_routes_only_notification_operations() -> None:
    notifications = Notifications()
    recovery = NotificationRecovery(
        operations=Operations(),
        notifications=notifications,
    )

    report = await recovery.reconcile_pending_notifications()

    assert report.inspected == 2
    assert report.reconciled == ("a" * 32,)
    assert report.pending == ("c" * 32,)
    assert notifications.calls == ["a" * 32, "c" * 32]
