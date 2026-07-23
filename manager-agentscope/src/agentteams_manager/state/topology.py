"""Materialized Controller/Matrix room topology."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    HumanResource,
    RoomKind,
    TopologySnapshot,
)

from .database import Database


@dataclass(frozen=True, slots=True)
class TopologyBinding:
    resource_type: str
    resource_name: str
    room_kind: RoomKind
    room_id: str
    matrix_user_id: str | None
    payload: dict[str, object]
    refreshed_at: datetime


def _binding_from_row(row: sqlite3.Row) -> TopologyBinding:
    return TopologyBinding(
        resource_type=row["resource_type"],
        resource_name=row["resource_name"],
        room_kind=RoomKind(row["room_kind"]),
        room_id=row["room_id"],
        matrix_user_id=row["matrix_user_id"],
        payload=json.loads(row["payload_json"]),
        refreshed_at=datetime.fromisoformat(row["refreshed_at"]),
    )


class TopologyRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def replace_snapshot(self, snapshot: TopologySnapshot) -> None:
        rows: list[tuple[str, str, str, str, str | None, str, str]] = []
        human_rows: list[tuple[str, str, int, str, str, str]] = []
        kinds_by_room: dict[str, RoomKind] = {}
        teams_by_leader = {team.leader: team for team in snapshot.teams}
        team_worker_names = {
            worker_name
            for team in snapshot.teams
            for worker_name in team.workers
        }

        def add(
            *,
            resource_type: str,
            resource_name: str,
            room_kind: RoomKind,
            room_id: str | None,
            matrix_user_id: str | None,
            payload: str,
        ) -> None:
            if not room_id:
                return
            previous = kinds_by_room.get(room_id)
            if previous is not None and previous is not room_kind:
                raise ConflictError(
                    f"{room_id} has multiple room kinds: "
                    f"{previous.value}, {room_kind.value}",
                )
            kinds_by_room[room_id] = room_kind
            rows.append(
                (
                    resource_type,
                    resource_name,
                    room_kind.value,
                    room_id,
                    matrix_user_id,
                    payload,
                    snapshot.refreshed_at.isoformat(),
                ),
            )

        workers_by_name = {worker.name: worker for worker in snapshot.workers}
        for worker in snapshot.workers:
            if worker.name in teams_by_leader or worker.name in team_worker_names:
                continue
            add(
                resource_type="worker",
                resource_name=worker.name,
                room_kind=RoomKind.WORKER_ROOM,
                room_id=worker.room_id,
                matrix_user_id=worker.matrix_user_id,
                payload=worker.model_dump_json(),
            )
        for team in snapshot.teams:
            leader = workers_by_name.get(team.leader)
            add(
                resource_type="team",
                resource_name=team.name,
                room_kind=RoomKind.LEADER_ROOM,
                room_id=team.room_id,
                matrix_user_id=(
                    leader.matrix_user_id if leader is not None else None
                ),
                payload=team.model_dump_json(),
            )
            team_room_id = team.spec.get("teamRoomID")
            add(
                resource_type="team",
                resource_name=team.name,
                room_kind=RoomKind.TEAM_ROOM,
                room_id=(
                    str(team_room_id)
                    if isinstance(team_room_id, str)
                    else None
                ),
                matrix_user_id=None,
                payload=team.model_dump_json(),
            )
        for human in snapshot.humans:
            human_rows.append(
                (
                    human.name,
                    human.matrix_user_id,
                    human.permission_level,
                    json.dumps(list(human.allowed_rooms)),
                    human.model_dump_json(),
                    snapshot.refreshed_at.isoformat(),
                ),
            )

        def write(connection: sqlite3.Connection) -> None:
            connection.execute("DELETE FROM topology")
            connection.execute("DELETE FROM human_access")
            connection.executemany(
                """
                INSERT INTO topology(
                    resource_type, resource_name, room_kind, room_id,
                    matrix_user_id, payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            connection.executemany(
                """
                INSERT INTO human_access(
                    name, matrix_user_id, permission_level,
                    allowed_rooms_json, payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                human_rows,
            )
            connection.execute(
                """
                INSERT INTO key_values(key, value, updated_at)
                VALUES ('topology_revision', ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (
                    str(snapshot.revision),
                    snapshot.refreshed_at.isoformat(),
                ),
            )

        await self._database.write(write)

    async def revision(self) -> int:
        def read(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                "SELECT value FROM key_values WHERE key='topology_revision'",
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)

    async def room_binding(self, room_id: str) -> TopologyBinding | None:
        def read(connection: sqlite3.Connection) -> TopologyBinding | None:
            row = connection.execute(
                "SELECT * FROM topology WHERE room_id=?",
                (room_id,),
            ).fetchone()
            return _binding_from_row(row) if row else None

        return await self._database.read(read)

    async def human_for_sender(
        self,
        matrix_user_id: str,
    ) -> HumanResource | None:
        """Resolve Controller-declared Human access by Matrix identity."""
        def read(connection: sqlite3.Connection) -> HumanResource | None:
            row = connection.execute(
                """
                SELECT payload_json FROM human_access
                 WHERE matrix_user_id=?
                """,
                (matrix_user_id,),
            ).fetchone()
            if row is None:
                return None
            return HumanResource.model_validate_json(row["payload_json"])

        return await self._database.read(read)

    async def all_bindings(self) -> tuple[TopologyBinding, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[TopologyBinding, ...]:
            rows = connection.execute(
                """
                SELECT * FROM topology
                 ORDER BY resource_type, resource_name
                """,
            ).fetchall()
            return tuple(_binding_from_row(row) for row in rows)

        return await self._database.read(read)
