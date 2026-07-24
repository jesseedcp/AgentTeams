from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    RecoveryError,
)
from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    OperationStatus,
    TopologySnapshot,
)
from agentteams_manager.workflows.heartbeat import (
    Heartbeat,
    IntegrationRecovery,
)
from agentteams_manager.workflows.resources import ResourceRecoveryReport


def _operation(
    operation_id: str,
    kind: OperationKind,
) -> OperationRecord:
    return OperationRecord.new(
        operation_id=operation_id,
        kind=kind,
        target_key=f"integration/{operation_id}",
        request={"action": "test"},
    ).model_copy(update={"status": OperationStatus.RECONCILING})


class Operations:
    def __init__(self, records: tuple[OperationRecord, ...]) -> None:
        self.records = records

    async def list_recoverable(self):
        return self.records


class Resumer:
    def __init__(
        self,
        errors: dict[str, Exception] | None = None,
    ) -> None:
        self.errors = errors or {}
        self.calls: list[str] = []

    async def resume_operation(self, operation: OperationRecord) -> None:
        self.calls.append(operation.operation_id)
        error = self.errors.get(operation.operation_id)
        if error is not None:
            raise error


@pytest.mark.asyncio
async def test_integration_recovery_routes_only_owned_operation_kinds() -> None:
    owned = (
        _operation("1" * 32, OperationKind.SWITCH_MODEL),
        _operation("2" * 32, OperationKind.CONFIGURE_MCP),
        _operation("3" * 32, OperationKind.PUBLISH_SERVICE),
    )
    ignored = _operation("4" * 32, OperationKind.CREATE_WORKER)
    resumer = Resumer()
    recovery = IntegrationRecovery(
        operations=Operations((*owned, ignored)),
        integrations=resumer,
    )

    report = await recovery.reconcile_pending_integrations()

    assert report.inspected == 3
    assert report.reconciled == tuple(
        operation.operation_id
        for operation in owned
    )
    assert resumer.calls == list(report.reconciled)


@pytest.mark.asyncio
async def test_integration_recovery_classifies_retry_attention_and_failure() -> None:
    pending = "5" * 32
    attention = "6" * 32
    failed = "7" * 32
    records = (
        _operation(pending, OperationKind.CONFIGURE_MCP),
        _operation(attention, OperationKind.CONFIGURE_MCP),
        _operation(failed, OperationKind.PUBLISH_SERVICE),
    )
    recovery = IntegrationRecovery(
        operations=Operations(records),
        integrations=Resumer(
            {
                pending: AmbiguousEffectError("not converged"),
                attention: RecoveryError("credential input required"),
                failed: RuntimeError("definite failure"),
            },
        ),
    )

    report = await recovery.reconcile_pending_integrations()

    assert report.pending == (pending,)
    assert report.needs_attention == (attention,)
    assert report.failed == (failed,)


class OrderedRuntime:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def poll_once(self):
        self.order.append("runtime")
        return object()


class OrderedResources:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def reconcile_pending_resources(self):
        self.order.append("resources")
        return ResourceRecoveryReport(inspected=0)


class OrderedTopology:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def refresh(self):
        self.order.append("topology")
        return TopologySnapshot(
            revision=2,
            refreshed_at=datetime(2026, 7, 24, 12, tzinfo=UTC),
        )


class OrderedIntegrations:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def reconcile_pending_integrations(self):
        from agentteams_manager.workflows.heartbeat import (
            IntegrationRecoveryReport,
        )

        self.order.append("integrations")
        return IntegrationRecoveryReport(
            inspected=2,
            reconciled=("8" * 32,),
            pending=("9" * 32,),
        )


class Notifications:
    async def already_sent(self, operation_id: str) -> bool:
        del operation_id
        return False

    async def send_terminal_failure(self, operation_id: str) -> None:
        raise AssertionError(f"unexpected failure: {operation_id}")


@pytest.mark.asyncio
async def test_heartbeat_polls_runtime_before_integration_recovery() -> None:
    order: list[str] = []
    heartbeat = Heartbeat(
        recovery=OrderedResources(order),
        topology=OrderedTopology(order),
        notifications=Notifications(),
        runtime_watcher=OrderedRuntime(order),
        integration_recovery=OrderedIntegrations(order),
    )

    report = await heartbeat.run_once()

    assert order == [
        "runtime",
        "resources",
        "topology",
        "integrations",
    ]
    assert report.runtime_changed is True
    assert report.integration_reconciled == 1
    assert report.integration_pending == 1
