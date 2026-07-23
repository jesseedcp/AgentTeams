from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import (
    HumanResource,
    OperationKind,
    OperationRecord,
    OperationStatus,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.workflows.resources import (
    ReconcileDisposition,
    ResourceReconciler,
)


class FakeController:
    def __init__(self) -> None:
        self.workers: dict[str, WorkerResource] = {}
        self.teams: dict[str, TeamResource] = {}
        self.humans: dict[str, HumanResource] = {}
        self.get_calls: list[tuple[str, str]] = []

    async def get_worker(self, name: str) -> WorkerResource | None:
        self.get_calls.append(("worker", name))
        return self.workers.get(name)

    async def get_team(self, name: str) -> TeamResource | None:
        self.get_calls.append(("team", name))
        return self.teams.get(name)

    async def get_human(self, name: str) -> HumanResource | None:
        self.get_calls.append(("human", name))
        return self.humans.get(name)


def operation(
    kind: OperationKind,
    target_key: str,
) -> OperationRecord:
    now = datetime.now(UTC)
    return OperationRecord(
        operation_id="a" * 32,
        kind=kind,
        target_key=target_key,
        status=OperationStatus.RECONCILING,
        request={"name": target_key.split("/", 1)[1]},
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_create_worker_is_proved_only_by_controller_fact() -> None:
    controller = FakeController()
    controller.workers["alice"] = WorkerResource(
        name="alice",
        runtime="qwenpaw",
        phase="Running",
        room_id="!alice:example",
    )

    result = await ResourceReconciler(controller).reconcile(
        operation(OperationKind.CREATE_WORKER, "worker/alice"),
    )

    assert result.disposition is ReconcileDisposition.SUCCEEDED
    assert result.receipt["name"] == "alice"
    assert controller.get_calls == [("worker", "alice")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "target"),
    [
        (OperationKind.CREATE_WORKER, "worker/missing"),
        (OperationKind.CREATE_TEAM, "team/missing"),
        (OperationKind.CREATE_HUMAN, "human/missing"),
    ],
)
async def test_absent_create_effect_is_not_reported_as_success(
    kind: OperationKind,
    target: str,
) -> None:
    result = await ResourceReconciler(FakeController()).reconcile(
        operation(kind, target),
    )

    assert result.disposition is ReconcileDisposition.EFFECT_ABSENT
    assert not result.receipt


@pytest.mark.asyncio
async def test_failed_controller_phase_is_terminal_failure() -> None:
    controller = FakeController()
    controller.workers["alice"] = WorkerResource(
        name="alice",
        runtime="copaw",
        phase="Failed",
        status={"message": "image pull failed"},
    )

    result = await ResourceReconciler(controller).reconcile(
        operation(OperationKind.CREATE_WORKER, "worker/alice"),
    )

    assert result.disposition is ReconcileDisposition.FAILED
    assert result.message == "image pull failed"


@pytest.mark.asyncio
async def test_delete_is_proved_only_after_controller_absence() -> None:
    result = await ResourceReconciler(FakeController()).reconcile(
        operation(OperationKind.DELETE_TEAM, "team/alpha"),
    )

    assert result.disposition is ReconcileDisposition.SUCCEEDED
    assert result.receipt == {"name": "alpha", "deleted": True}
