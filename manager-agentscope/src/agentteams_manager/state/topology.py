"""Materialized Controller/Matrix room topology."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

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


class ActorKind(StrEnum):
    ADMIN = "admin"
    WORKER = "worker"
    TEAM_LEADER = "team_leader"
    TEAM_WORKER = "team_worker"
    HUMAN = "human"
    TRUSTED_CONTACT = "trusted_contact"


@dataclass(frozen=True, slots=True)
class Actor:
    matrix_user_id: str
    kind: ActorKind
    resource_name: str | None = None
    team_name: str | None = None
    payload: dict[str, object] | None = None


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
    def __init__(
        self,
        database: Database,
        *,
        admin_user_id: str | None = None,
    ) -> None:
        self._database = database
        self._admin_user_id = admin_user_id

    async def replace_snapshot(self, snapshot: TopologySnapshot) -> None:
        rows: list[tuple[str, str, str, str, str | None, str, str]] = []
        human_rows: list[tuple[str, str, int, str, str, str]] = []
        actor_rows: list[
            tuple[str, str, str | None, str | None, str, str]
        ] = []
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
        teams_by_worker = {
            worker_name: team
            for team in snapshot.teams
            for worker_name in team.workers
        }
        for worker in snapshot.workers:
            team = teams_by_leader.get(worker.name)
            actor_kind = ActorKind.WORKER
            actor_team_name: str | None = None
            if team is not None:
                actor_kind = ActorKind.TEAM_LEADER
                actor_team_name = team.name
            elif worker.name in teams_by_worker:
                actor_kind = ActorKind.TEAM_WORKER
                actor_team_name = teams_by_worker[worker.name].name
            if worker.matrix_user_id:
                actor_rows.append(
                    (
                        worker.matrix_user_id,
                        actor_kind.value,
                        worker.name,
                        actor_team_name,
                        worker.model_dump_json(),
                        snapshot.refreshed_at.isoformat(),
                    ),
                )
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
            connection.execute(
                "DELETE FROM topology "
                "WHERE resource_type IN ('worker', 'team')",
            )
            connection.execute("DELETE FROM human_access")
            connection.execute(
                "DELETE FROM topology_actors "
                "WHERE actor_kind IN "
                "('worker', 'team_leader', 'team_worker')",
            )
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
            connection.executemany(
                """
                INSERT INTO topology_actors(
                    matrix_user_id, actor_kind, resource_name, team_name,
                    payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(matrix_user_id) DO UPDATE SET
                    actor_kind=excluded.actor_kind,
                    resource_name=excluded.resource_name,
                    team_name=excluded.team_name,
                    payload_json=excluded.payload_json,
                    refreshed_at=excluded.refreshed_at
                """,
                actor_rows,
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

    async def actor_for_sender(
        self,
        matrix_user_id: str,
    ) -> Actor | None:
        if self._admin_user_id and matrix_user_id == self._admin_user_id:
            return Actor(
                matrix_user_id=matrix_user_id,
                kind=ActorKind.ADMIN,
            )

        def read(connection: sqlite3.Connection) -> Actor | None:
            row = connection.execute(
                """
                SELECT actor_kind, resource_name, team_name, payload_json
                  FROM topology_actors
                 WHERE matrix_user_id=?
                """,
                (matrix_user_id,),
            ).fetchone()
            if row is not None:
                return Actor(
                    matrix_user_id=matrix_user_id,
                    kind=ActorKind(row["actor_kind"]),
                    resource_name=row["resource_name"],
                    team_name=row["team_name"],
                    payload=json.loads(row["payload_json"]),
                )

            human = connection.execute(
                """
                SELECT name, payload_json FROM human_access
                 WHERE matrix_user_id=?
                """,
                (matrix_user_id,),
            ).fetchone()
            if human is not None:
                return Actor(
                    matrix_user_id=matrix_user_id,
                    kind=ActorKind.HUMAN,
                    resource_name=human["name"],
                    payload=json.loads(human["payload_json"]),
                )

            trusted = connection.execute(
                """
                SELECT 1 FROM channel_relationships
                 WHERE relationship_kind='trusted'
                   AND (owner_user_id=? OR peer_user_id=?)
                 LIMIT 1
                """,
                (matrix_user_id, matrix_user_id),
            ).fetchone()
            if trusted is not None:
                return Actor(
                    matrix_user_id=matrix_user_id,
                    kind=ActorKind.TRUSTED_CONTACT,
                )
            return None

        return await self._database.read(read)

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

    async def upsert_project(
        self,
        *,
        project_id: str,
        room_id: str,
        payload: dict[str, object],
        refreshed_at: datetime,
    ) -> None:
        _require_channel_key(project_id, "project_id")
        _require_channel_key(room_id, "room_id")

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO topology(
                    resource_type, resource_name, room_kind, room_id,
                    matrix_user_id, payload_json, refreshed_at
                ) VALUES ('project', ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(resource_type, resource_name, room_kind)
                DO UPDATE SET
                    room_id=excluded.room_id,
                    payload_json=excluded.payload_json,
                    refreshed_at=excluded.refreshed_at
                """,
                (
                    project_id,
                    RoomKind.PROJECT_ROOM.value,
                    room_id,
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    refreshed_at.isoformat(),
                ),
            )

        await self._database.write(write)

    async def set_primary_channel(
        self,
        user_id: str,
        room_id: str,
    ) -> None:
        _require_channel_key(user_id, "user_id")
        _require_channel_key(room_id, "room_id")

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO channel_relationships(
                    relationship_kind, owner_user_id, peer_user_id,
                    room_id, updated_at
                ) VALUES ('primary', ?, '', ?, ?)
                ON CONFLICT(
                    relationship_kind, owner_user_id, peer_user_id
                ) DO UPDATE SET
                    room_id=excluded.room_id,
                    updated_at=excluded.updated_at
                """,
                (user_id, room_id, datetime.now(UTC).isoformat()),
            )

        await self._database.write(write)

    async def clear_primary_channel(self, user_id: str) -> None:
        _require_channel_key(user_id, "user_id")

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                DELETE FROM channel_relationships
                 WHERE relationship_kind='primary'
                   AND owner_user_id=?
                """,
                (user_id,),
            )

        await self._database.write(write)

    async def primary_channel(self, user_id: str) -> str | None:
        _require_channel_key(user_id, "user_id")

        def read(connection: sqlite3.Connection) -> str | None:
            row = connection.execute(
                """
                SELECT room_id FROM channel_relationships
                 WHERE relationship_kind='primary'
                   AND owner_user_id=?
                """,
                (user_id,),
            ).fetchone()
            return str(row["room_id"]) if row is not None else None

        return await self._database.read(read)

    async def set_trusted_channel(
        self,
        first_user_id: str,
        second_user_id: str,
        room_id: str,
    ) -> None:
        first, second = _trusted_pair(first_user_id, second_user_id)
        _require_channel_key(room_id, "room_id")

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO channel_relationships(
                    relationship_kind, owner_user_id, peer_user_id,
                    room_id, updated_at
                ) VALUES ('trusted', ?, ?, ?, ?)
                ON CONFLICT(
                    relationship_kind, owner_user_id, peer_user_id
                ) DO UPDATE SET
                    room_id=excluded.room_id,
                    updated_at=excluded.updated_at
                """,
                (
                    first,
                    second,
                    room_id,
                    datetime.now(UTC).isoformat(),
                ),
            )

        await self._database.write(write)

    async def trusted_channels(
        self,
        user_id: str,
    ) -> tuple[str, ...]:
        _require_channel_key(user_id, "user_id")

        def read(connection: sqlite3.Connection) -> tuple[str, ...]:
            rows = connection.execute(
                """
                SELECT room_id FROM channel_relationships
                 WHERE relationship_kind='trusted'
                   AND (owner_user_id=? OR peer_user_id=?)
                 ORDER BY room_id
                """,
                (user_id, user_id),
            ).fetchall()
            return tuple(str(row["room_id"]) for row in rows)

        return await self._database.read(read)

    async def remove_trusted_channel(
        self,
        first_user_id: str,
        second_user_id: str,
    ) -> None:
        first, second = _trusted_pair(first_user_id, second_user_id)

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                DELETE FROM channel_relationships
                 WHERE relationship_kind='trusted'
                   AND owner_user_id=?
                   AND peer_user_id=?
                """,
                (first, second),
            )

        await self._database.write(write)


def _require_channel_key(value: str, label: str) -> None:
    if not value:
        raise ValueError(f"{label} cannot be empty")


def _trusted_pair(first: str, second: str) -> tuple[str, str]:
    _require_channel_key(first, "first_user_id")
    _require_channel_key(second, "second_user_id")
    if first == second:
        raise ValueError("trusted channel users must be distinct")
    ordered = tuple(sorted((first, second)))
    return ordered[0], ordered[1]
