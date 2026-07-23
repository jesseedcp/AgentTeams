"""Materialized Controller/Matrix room topology."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import RoomKind, TopologySnapshot

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
        kinds_by_room: dict[str, RoomKind] = {}

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

        for worker in snapshot.workers:
            add(
                resource_type="worker",
                resource_name=worker.name,
                room_kind=RoomKind.WORKER_ROOM,
                room_id=worker.room_id,
                matrix_user_id=worker.matrix_user_id,
                payload=worker.model_dump_json(),
            )
        for team in snapshot.teams:
            add(
                resource_type="team",
                resource_name=team.name,
                room_kind=RoomKind.LEADER_ROOM,
                room_id=team.room_id,
                matrix_user_id=None,
                payload=team.model_dump_json(),
            )

        def write(connection: sqlite3.Connection) -> None:
            connection.execute("DELETE FROM topology")
            connection.executemany(
                """
                INSERT INTO topology(
                    resource_type, resource_name, room_kind, room_id,
                    matrix_user_id, payload_json, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        await self._database.write(write)

    async def room_binding(self, room_id: str) -> TopologyBinding | None:
        def read(connection: sqlite3.Connection) -> TopologyBinding | None:
            row = connection.execute(
                "SELECT * FROM topology WHERE room_id=?",
                (room_id,),
            ).fetchone()
            return _binding_from_row(row) if row else None

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
