from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.workflows.resources import (
    ReconcileDisposition,
    ResourceReconciler,
)


class LostCreateResultController:
    def __init__(self) -> None:
        self.create_calls = 1
        self.get_calls = 0

    async def get_worker(self, name: str) -> WorkerResource:
        self.get_calls += 1
        return WorkerResource(
            name=name,
            runtime="openhuman",
            phase="Running",
            room_id="!alice:example",
        )

    async def get_team(self, name: str) -> None:
        del name
        return None

    async def get_human(self, name: str) -> None:
        del name
        return None


@pytest.mark.asyncio
async def test_lost_create_result_queries_before_any_retry() -> None:
    now = datetime.now(UTC)
    operation = OperationRecord(
        operation_id="b" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        status=OperationStatus.RECONCILING,
        request={"name": "alice"},
        result={"ambiguous_reason": "process result lost"},
        created_at=now,
        updated_at=now,
    )
    controller = LostCreateResultController()

    result = await ResourceReconciler(controller).reconcile(operation)

    assert result.disposition is ReconcileDisposition.SUCCEEDED
    assert controller.create_calls == 1
    assert controller.get_calls == 1
