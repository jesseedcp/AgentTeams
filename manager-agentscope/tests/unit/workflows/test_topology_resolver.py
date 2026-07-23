from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    HumanResource,
    RoomKind,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.topology import TopologyRepository
from agentteams_manager.workflows.resources import TopologyResolver


class FakeController:
    def __init__(
        self,
        *,
        workers: tuple[WorkerResource, ...] = (),
        teams: tuple[TeamResource, ...] = (),
        humans: tuple[HumanResource, ...] = (),
    ) -> None:
        self.workers = workers
        self.teams = teams
        self.humans = humans

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        return self.workers

    async def list_teams(self) -> tuple[TeamResource, ...]:
        return self.teams

    async def list_humans(self) -> tuple[HumanResource, ...]:
        return self.humans


class FakeMatrix:
    def __init__(
        self,
        *,
        joined: tuple[str, ...],
        members: dict[str, tuple[str, ...]],
    ) -> None:
        self._joined = joined
        self._members = members

    async def joined_rooms(self) -> tuple[str, ...]:
        return self._joined

    async def members(self, room_id: str) -> tuple[str, ...]:
        return self._members.get(room_id, ())


async def make_repository(tmp_path: Path) -> TopologyRepository:
    database = Database(tmp_path / "manager.db")
    await database.open()
    return TopologyRepository(database)


def facts() -> tuple[
    tuple[WorkerResource, ...],
    tuple[TeamResource, ...],
    tuple[HumanResource, ...],
]:
    workers = (
        WorkerResource(
            name="solo",
            runtime="copaw",
            room_id="!solo:example",
            matrix_user_id="@worker-solo:example",
        ),
        WorkerResource(
            name="alpha-lead",
            runtime="qwenpaw",
            room_id="!leader:example",
            matrix_user_id="@worker-alpha-lead:example",
            team="alpha",
            role="team_leader",
        ),
        WorkerResource(
            name="alpha-dev",
            runtime="hermes",
            room_id="!worker-private:example",
            matrix_user_id="@worker-alpha-dev:example",
            team="alpha",
            role="worker",
        ),
    )
    teams = (
        TeamResource(
            name="alpha",
            leader="alpha-lead",
            workers=("alpha-dev",),
            spec={
                "teamRoomID": "!team:example",
                "leaderDMRoomID": "!leader-dm:example",
            },
        ),
    )
    humans = (
        HumanResource(
            name="reviewer",
            matrix_user_id="@reviewer:example",
            permission_level=2,
            allowed_rooms=("!leader:example",),
        ),
    )
    return workers, teams, humans


def valid_members() -> dict[str, tuple[str, ...]]:
    return {
        "!solo:example": (
            "@manager:example",
            "@worker-solo:example",
        ),
        "!leader:example": (
            "@manager:example",
            "@worker-alpha-lead:example",
            "@reviewer:example",
        ),
        "!team:example": (
            "@worker-alpha-lead:example",
            "@worker-alpha-dev:example",
        ),
        "!leader-dm:example": ("@worker-alpha-lead:example",),
        "!worker-private:example": ("@worker-alpha-dev:example",),
    }


@pytest.mark.asyncio
async def test_refresh_materializes_only_manager_owned_join_targets(
    tmp_path: Path,
) -> None:
    workers, teams, humans = facts()
    repository = await make_repository(tmp_path)
    resolver = TopologyResolver(
        controller=FakeController(
            workers=workers,
            teams=teams,
            humans=humans,
        ),
        matrix=FakeMatrix(
            joined=("!solo:example", "!leader:example"),
            members=valid_members(),
        ),
        topology=repository,
        manager_user_id="@manager:example",
        admin_user_id="@admin:example",
        admin_room_id="!admin:example",
    )

    snapshot = await resolver.refresh()

    assert snapshot.manager_join_targets == (
        "!leader:example",
        "!solo:example",
    )
    assert "!team:example" in snapshot.forbidden_rooms
    assert "!leader-dm:example" in snapshot.forbidden_rooms
    assert "!worker-private:example" in snapshot.forbidden_rooms
    leader = await repository.room_binding("!leader:example")
    assert leader is not None
    assert leader.room_kind is RoomKind.LEADER_ROOM
    assert leader.resource_name == "alpha"


@pytest.mark.asyncio
async def test_manager_membership_in_team_private_room_is_rejected(
    tmp_path: Path,
) -> None:
    workers, teams, humans = facts()
    members = valid_members()
    members["!team:example"] += ("@manager:example",)
    resolver = TopologyResolver(
        controller=FakeController(
            workers=workers,
            teams=teams,
            humans=humans,
        ),
        matrix=FakeMatrix(
            joined=(
                "!solo:example",
                "!leader:example",
                "!team:example",
            ),
            members=members,
        ),
        topology=await make_repository(tmp_path),
        manager_user_id="@manager:example",
        admin_user_id="@admin:example",
        admin_room_id="!admin:example",
    )

    with pytest.raises(ConflictError, match="private Team room"):
        await resolver.refresh()


@pytest.mark.asyncio
async def test_sender_must_match_controller_matrix_identity(
    tmp_path: Path,
) -> None:
    workers, teams, humans = facts()
    repository = await make_repository(tmp_path)
    resolver = TopologyResolver(
        controller=FakeController(
            workers=workers,
            teams=teams,
            humans=humans,
        ),
        matrix=FakeMatrix(
            joined=("!solo:example", "!leader:example"),
            members=valid_members(),
        ),
        topology=repository,
        manager_user_id="@manager:example",
        admin_user_id="@admin:example",
        admin_room_id="!admin:example",
    )
    await resolver.refresh()

    denied = await resolver.policy_for(
        "!solo:example",
        "@worker-impostor:example",
    )
    allowed = await resolver.policy_for(
        "!solo:example",
        "@worker-solo:example",
    )

    assert denied.kind is RoomKind.UNKNOWN
    assert not denied.allowed_tools
    assert allowed.kind is RoomKind.WORKER_ROOM
    assert "delegate_task" in allowed.allowed_tools


@pytest.mark.asyncio
async def test_unknown_admin_room_sender_is_read_only(
    tmp_path: Path,
) -> None:
    resolver = TopologyResolver(
        controller=FakeController(),
        matrix=FakeMatrix(joined=(), members={}),
        topology=await make_repository(tmp_path),
        manager_user_id="@manager:example",
        admin_user_id="@admin:example",
        admin_room_id="!admin:example",
    )

    policy = await resolver.policy_for(
        "!admin:example",
        "@untrusted:example",
    )

    assert policy.kind is RoomKind.ADMIN_DM
    assert "list_workers" in policy.allowed_tools
    assert "create_worker" not in policy.allowed_tools
