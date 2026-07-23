from pathlib import Path

import pytest

from agentteams_manager.domain.models import (
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.operations import OperationRepository


@pytest.mark.asyncio
async def test_operation_transition_is_compare_and_swap(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)
    record = OperationRecord.new(
        operation_id="b" * 32,
        kind="create_worker",
        target_key="worker/alice",
        request={"name": "alice"},
    )
    await repository.create(record)

    changed = await repository.transition(
        record.operation_id,
        expected={OperationStatus.PLANNED},
        target=OperationStatus.PREPARED,
    )
    stale = await repository.transition(
        record.operation_id,
        expected={OperationStatus.PLANNED},
        target=OperationStatus.FAILED,
    )

    assert changed is not None
    assert changed.status is OperationStatus.PREPARED
    assert stale is None


@pytest.mark.asyncio
async def test_matrix_event_is_claimed_once(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)

    assert await repository.claim_matrix_event("!room:a", "$event")
    assert not await repository.claim_matrix_event("!room:a", "$event")


@pytest.mark.asyncio
async def test_next_event_sequence_is_durable(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)
    await repository.create(
        OperationRecord.new(
            operation_id="c" * 32,
            kind="create_team",
            target_key="team/core",
            request={"name": "core"},
        ),
    )

    assert await repository.next_sequence("c" * 32) == 1


@pytest.mark.asyncio
async def test_journal_sequence_is_global_across_operations(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)
    for operation_id in ("d" * 32, "e" * 32):
        await repository.create(
            OperationRecord.new(
                operation_id=operation_id,
                kind="create_worker",
                target_key=f"worker/{operation_id[0]}",
                request={"name": operation_id[0]},
            ),
        )

    first = await repository.next_sequence("d" * 32)
    second = await repository.next_sequence("e" * 32)

    assert (first, second) == (1, 2)
