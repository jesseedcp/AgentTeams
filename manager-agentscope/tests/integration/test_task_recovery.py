from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentteams_manager.clients.minio import MinioClient
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
from agentteams_manager.state.database import Database
from agentteams_manager.state.tasks import TaskRepository
from agentteams_manager.workflows.heartbeat import (
    Heartbeat,
    TaskRecovery,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceRecoveryReport,
)
from agentteams_manager.workflows.tasks import TaskService

from tests.fixtures.fake_s3 import FakeS3
from tests.fixtures.task_workflow import (
    FixedClock,
    OrderedTaskRepository,
    TaskController,
    TaskMatrix,
    TaskStorage,
    TaskSupervisor,
)


def _operation(
    operation_id: str,
    kind: OperationKind,
) -> OperationRecord:
    return OperationRecord.new(
        operation_id=operation_id,
        kind=kind,
        target_key=f"target/{operation_id}",
        request={"test": True},
    ).model_copy(update={"status": OperationStatus.RECONCILING})


class Operations:
    def __init__(self, operations: tuple[OperationRecord, ...]) -> None:
        self.operations = operations

    async def list_recoverable(self) -> tuple[OperationRecord, ...]:
        return self.operations


class Resumer:
    def __init__(
        self,
        *,
        error: Exception | None = None,
    ) -> None:
        self.error = error
        self.calls: list[str] = []

    async def resume_operation(self, operation: OperationRecord) -> None:
        self.calls.append(operation.operation_id)
        if self.error is not None:
            raise self.error


@pytest.mark.asyncio
async def test_task_recovery_routes_each_owned_operation_kind() -> None:
    tasks = Resumer()
    projects = Resumer()
    git = Resumer()
    task_id = "1" * 32
    project_id = "2" * 32
    git_id = "3" * 32
    coding_id = "5" * 32
    ignored_id = "4" * 32
    coding = Resumer()
    recovery = TaskRecovery(
        operations=Operations(
            (
                _operation(task_id, OperationKind.DELEGATE_TASK),
                _operation(project_id, OperationKind.CREATE_PROJECT),
                _operation(git_id, OperationKind.GIT_DELEGATION),
                _operation(
                    coding_id,
                    OperationKind.CODING_CLI_DELEGATION,
                ),
                _operation(ignored_id, OperationKind.CREATE_WORKER),
            ),
        ),
        tasks=tasks,
        projects=projects,
        git=git,
        coding=coding,
    )

    report = await recovery.reconcile_pending_tasks()

    assert report.inspected == 4
    assert report.reconciled == (
        task_id,
        project_id,
        git_id,
        coding_id,
    )
    assert tasks.calls == [task_id]
    assert projects.calls == [project_id]
    assert git.calls == [git_id]
    assert coding.calls == [coding_id]


@pytest.mark.asyncio
async def test_task_recovery_distinguishes_pending_attention_and_failure() -> None:
    pending_id = "5" * 32
    attention_id = "6" * 32
    failed_id = "7" * 32
    recovery = TaskRecovery(
        operations=Operations(
            (
                _operation(pending_id, OperationKind.DELEGATE_TASK),
                _operation(attention_id, OperationKind.CREATE_PROJECT),
                _operation(failed_id, OperationKind.GIT_DELEGATION),
            ),
        ),
        tasks=Resumer(error=AmbiguousEffectError("not visible yet")),
        projects=Resumer(error=RecoveryError("proof is incomplete")),
        git=Resumer(error=RuntimeError("definite failure")),
    )

    report = await recovery.reconcile_pending_tasks()

    assert report.pending == (pending_id,)
    assert report.needs_attention == (attention_id,)
    assert report.failed == (failed_id,)


@pytest.mark.asyncio
async def test_recovery_reuses_ambiguous_assignment_transaction(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    order: list[str] = []
    clock = FixedClock()
    supervisor = TaskSupervisor(clock)
    matrix = TaskMatrix(order)
    matrix.timeout_once = True
    service = TaskService(
        tasks=OrderedTaskRepository(TaskRepository(database), order),
        storage=TaskStorage(
            MinioClient(FakeS3(), bucket="agentteams"),
            order,
        ),
        controller=TaskController(),
        matrix=matrix,
        supervisor=supervisor,
        clock=clock,
        cache_root=tmp_path / "cache",
        matrix_domain="example",
    )
    context = MutationContext(
        room_id="!admin:example",
        event_id="$request",
        tool_call_id="create-task",
    )
    with pytest.raises(TimeoutError):
        await service.create_finite(
            title="Recover this",
            spec="The assignment must remain singular.",
            assigned_to="alice",
            context=context,
        )
    operation = supervisor.operations[context.operation_id]
    recovery = TaskRecovery(
        operations=Operations((operation,)),
        tasks=service,
        projects=Resumer(),
        git=Resumer(),
    )

    report = await recovery.reconcile_pending_tasks()

    assert report.reconciled == (context.operation_id,)
    assert len({attempt.txn_id for attempt in matrix.attempts}) == 1
    assert len(matrix.visible) == 1


class OrderedResourceRecovery:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def reconcile_pending_resources(self) -> ResourceRecoveryReport:
        self.order.append("resources")
        return ResourceRecoveryReport(inspected=0)


class OrderedTopology:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def refresh(self) -> TopologySnapshot:
        self.order.append("topology")
        return TopologySnapshot(
            revision=11,
            refreshed_at=datetime(2026, 7, 23, tzinfo=UTC),
        )


class OrderedTaskRecovery:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def reconcile_pending_tasks(self):
        from agentteams_manager.workflows.heartbeat import TaskRecoveryReport

        self.order.append("tasks")
        return TaskRecoveryReport(inspected=1, reconciled=("8" * 32,))


class OrderedLeases:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def reclaim_expired(self, now: datetime):
        from agentteams_manager.workflows.git_delegation import (
            LeaseReclaimReport,
        )

        assert now.tzinfo is not None
        self.order.append("leases")
        return LeaseReclaimReport(inspected=1, reclaimed=("task-1",))


class OrderedScheduler:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def dispatch_due(self, now: datetime):
        from agentteams_manager.workflows.heartbeat import DispatchReport

        assert now.tzinfo is not None
        self.order.append("schedules")
        return DispatchReport(inspected=1, dispatched=("task-2",))


class OrderedCompletions:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def reconcile_pending_completions(self) -> int:
        from agentteams_manager.workflows.heartbeat import (
            CompletionRecoveryReport,
        )

        self.order.append("completions")
        return CompletionRecoveryReport(
            inspected=2,
            reconciled=("9" * 32, "a" * 32),
        )


class OrderedNotifications:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.sent: list[str] = []

    async def already_sent(self, operation_id: str) -> bool:
        self.order.append("notifications")
        return operation_id in self.sent

    async def send_terminal_failure(self, operation_id: str) -> None:
        self.sent.append(operation_id)


class OrderedSnapshot:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def snapshot_if_due(self) -> bool:
        self.order.append("snapshot")
        return True


@pytest.mark.asyncio
async def test_unified_heartbeat_runs_task_effects_in_fixed_order() -> None:
    order: list[str] = []
    heartbeat = Heartbeat(
        recovery=OrderedResourceRecovery(order),
        topology=OrderedTopology(order),
        notifications=OrderedNotifications(order),
        task_recovery=OrderedTaskRecovery(order),
        leases=OrderedLeases(order),
        task_scheduler=OrderedScheduler(order),
        completions=OrderedCompletions(order),
        snapshotter=OrderedSnapshot(order),
    )

    report = await heartbeat.run_once()

    assert order == [
        "resources",
        "topology",
        "tasks",
        "leases",
        "schedules",
        "completions",
        "snapshot",
    ]
    assert report.task_reconciled == 1
    assert report.leases_reclaimed == 1
    assert report.recurring_dispatched == 1
    assert report.completions_reconciled == 2
    assert report.snapshot_created is True
