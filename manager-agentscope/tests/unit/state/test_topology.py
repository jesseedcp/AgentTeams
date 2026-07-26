from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    HumanResource,
    TeamResource,
    TopologySnapshot,
    WorkerResource,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.topology import ActorKind, TopologyRepository


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
                leader="core-lead",
                workers=(),
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


@pytest.mark.asyncio
async def test_human_access_is_indexed_by_matrix_identity(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TopologyRepository(database)
    await repository.replace_snapshot(
        TopologySnapshot(
            revision=3,
            humans=(
                HumanResource(
                    name="reviewer",
                    matrix_user_id="@reviewer:example",
                    permission_level=2,
                    allowed_rooms=("!team:example",),
                ),
            ),
            refreshed_at=datetime.now(UTC),
        ),
    )

    human = await repository.human_for_sender("@reviewer:example")

    assert human is not None
    assert human.permission_level == 2
    assert human.allowed_rooms == ("!team:example",)


@pytest.mark.asyncio
async def test_channel_relationships_survive_resource_refresh(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TopologyRepository(database)
    await repository.set_primary_channel(
        "@reviewer:example",
        "!primary:example",
    )
    await repository.set_trusted_channel(
        "@manager:example",
        "@reviewer:example",
        "!trusted:example",
    )

    await repository.replace_snapshot(
        TopologySnapshot(
            revision=1,
            refreshed_at=datetime.now(UTC),
        ),
    )

    assert (
        await repository.primary_channel("@reviewer:example")
        == "!primary:example"
    )
    assert await repository.trusted_channels("@reviewer:example") == (
        "!trusted:example",
    )


@pytest.mark.asyncio
async def test_actor_index_covers_admin_workers_humans_and_trusted_contacts(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = TopologyRepository(
        database,
        admin_user_id="@admin:example",
    )
    await repository.replace_snapshot(
        TopologySnapshot(
            revision=4,
            workers=(
                WorkerResource(
                    name="solo",
                    runtime="qwenpaw",
                    matrix_user_id="@solo:example",
                ),
                WorkerResource(
                    name="alpha-lead",
                    runtime="qwenpaw",
                    matrix_user_id="@alpha-lead:example",
                ),
                WorkerResource(
                    name="alpha-dev",
                    runtime="hermes",
                    matrix_user_id="@alpha-dev:example",
                ),
            ),
            teams=(
                TeamResource(
                    name="alpha",
                    leader="alpha-lead",
                    workers=("alpha-dev",),
                ),
            ),
            humans=(
                HumanResource(
                    name="reviewer",
                    matrix_user_id="@reviewer:example",
                    permission_level=2,
                ),
            ),
            refreshed_at=datetime.now(UTC),
        ),
    )
    await repository.set_trusted_channel(
        "@manager:example",
        "@external:example",
        "!trusted:example",
    )

    admin = await repository.actor_for_sender("@admin:example")
    solo = await repository.actor_for_sender("@solo:example")
    leader = await repository.actor_for_sender("@alpha-lead:example")
    member = await repository.actor_for_sender("@alpha-dev:example")
    human = await repository.actor_for_sender("@reviewer:example")
    trusted = await repository.actor_for_sender("@external:example")

    assert admin is not None and admin.kind is ActorKind.ADMIN
    assert solo is not None and solo.kind is ActorKind.WORKER
    assert leader is not None and leader.kind is ActorKind.TEAM_LEADER
    assert leader.team_name == "alpha"
    assert member is not None and member.kind is ActorKind.TEAM_WORKER
    assert member.team_name == "alpha"
    assert human is not None and human.kind is ActorKind.HUMAN
    assert trusted is not None and trusted.kind is ActorKind.TRUSTED_CONTACT
