from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    TeamResource,
    TopologySnapshot,
    WorkerResource,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.topology import TopologyRepository


@pytest.mark.asyncio
async def test_room_cannot_have_worker_and_team_bindings(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TopologyRepository(database)
    snapshot = TopologySnapshot(
        revision=1,
        workers=(
            WorkerResource(
                name="alice",
                runtime="openclaw",
                room_id="!shared:example",
            ),
        ),
        teams=(
            TeamResource(
                name="core",
                leader="alice",
                workers=("alice",),
                room_id="!shared:example",
            ),
        ),
        refreshed_at=datetime.now(UTC),
    )

    with pytest.raises(ConflictError, match="multiple room kinds"):
        await repository.replace_snapshot(snapshot)


@pytest.mark.asyncio
async def test_room_binding_returns_typed_resource(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TopologyRepository(database)
    await repository.replace_snapshot(
        TopologySnapshot(
            revision=2,
            workers=(
                WorkerResource(
                    name="alice",
                    runtime="copaw",
                    room_id="!alice:example",
                    matrix_user_id="@alice:example",
                ),
            ),
            refreshed_at=datetime.now(UTC),
        ),
    )

    binding = await repository.room_binding("!alice:example")

    assert binding is not None
    assert binding.resource_type == "worker"
    assert binding.resource_name == "alice"
    assert binding.matrix_user_id == "@alice:example"
