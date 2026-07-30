from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agentteams_manager.state.database import Database
from agentteams_manager.state.supervision import SupervisionStateRepository


@pytest.mark.asyncio
async def test_supervision_missed_cycles_reset_when_progress_changes(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    state = SupervisionStateRepository(database)
    now = datetime(2026, 7, 30, 8, 0, tzinfo=UTC)

    assert await state.record_ping(
        subject_key="task:one",
        observed_token="revision-1",
        pinged_at=now,
    ) == 0
    assert await state.record_ping(
        subject_key="task:one",
        observed_token="revision-1",
        pinged_at=now + timedelta(minutes=30),
    ) == 1
    assert await state.record_ping(
        subject_key="task:one",
        observed_token="revision-2",
        pinged_at=now + timedelta(hours=1),
    ) == 0
