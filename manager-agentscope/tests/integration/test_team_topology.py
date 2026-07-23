from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.domain.models import (
    HumanResource,
    RoomKind,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.topology import TopologyRepository
from agentteams_manager.workflows.resources import TopologyResolver


class Controller:
    async def list_workers(self) -> tuple[WorkerResource, ...]:
        return (
            WorkerResource(
                name="alpha-lead",
                runtime="qwenpaw",
                phase="Running",
                room_id="!leader:example",
                matrix_user_id="@alpha-lead:example",
                team="alpha",
                role="team_leader",
            ),
            WorkerResource(
                name="alpha-dev",
                runtime="openclaw",
                phase="Running",
                room_id="!dev:example",
                matrix_user_id="@alpha-dev:example",
                team="alpha",
                role="worker",
            ),
        )

    async def list_teams(self) -> tuple[TeamResource, ...]:
        return (
            TeamResource(
                name="alpha",
                leader="alpha-lead",
                workers=("alpha-dev",),
                phase="Active",
                spec={
                    "teamRoomID": "!team:example",
                    "leaderDMRoomID": "!leader-dm:example",
                },
            ),
        )

    async def list_humans(self) -> tuple[HumanResource, ...]:
        return ()


class Matrix:
    async def joined_rooms(self) -> tuple[str, ...]:
        return ("!leader:example",)

    async def members(self, room_id: str) -> tuple[str, ...]:
        if room_id == "!leader:example":
            return ("@manager:example", "@alpha-lead:example")
        return ()


@pytest.mark.asyncio
async def test_delegation_exists_only_in_team_leader_room(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    resolver = TopologyResolver(
        controller=Controller(),
        matrix=Matrix(),
        topology=TopologyRepository(database),
        manager_user_id="@manager:example",
        admin_user_id="@admin:example",
        admin_room_id="!admin:example",
    )

    snapshot = await resolver.refresh()
    leader = await resolver.policy_for(
        "!leader:example",
        "@alpha-lead:example",
    )
    team = await resolver.policy_for(
        "!team:example",
        "@alpha-lead:example",
    )
    worker = await resolver.policy_for(
        "!dev:example",
        "@alpha-dev:example",
    )

    assert snapshot.manager_join_targets == ("!leader:example",)
    assert leader.kind is RoomKind.LEADER_ROOM
    assert "delegate_team_task" in leader.allowed_tools
    assert "delegate_team_task" not in team.allowed_tools
    assert "delegate_team_task" not in worker.allowed_tools
    assert team.silent
