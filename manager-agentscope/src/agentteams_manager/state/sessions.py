"""AgentScope session persistence keyed by Matrix room."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agentscope.state import AgentState

from .database import Database


@dataclass(frozen=True, slots=True)
class StoredSession:
    room_id: str
    state: AgentState
    policy_revision: int
    last_event_id: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SessionSettings:
    room_id: str
    model_override: str | None
    timezone: str
    next_reset_at: datetime
    updated_at: datetime


class SessionRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def load(self, room_id: str) -> StoredSession | None:
        def read(connection: sqlite3.Connection) -> StoredSession | None:
            row = connection.execute(
                "SELECT * FROM sessions WHERE room_id=?",
                (room_id,),
            ).fetchone()
            if row is None:
                return None
            return StoredSession(
                room_id=row["room_id"],
                state=AgentState.model_validate_json(
                    row["agent_state_json"],
                ),
                policy_revision=row["policy_revision"],
                last_event_id=row["last_event_id"],
                updated_at=datetime.fromisoformat(row["updated_at"]),
            )

        return await self._database.read(read)

    async def save(
        self,
        *,
        room_id: str,
        state: AgentState,
        policy_revision: int,
        last_event_id: str | None,
    ) -> StoredSession:
        updated_at = datetime.now(UTC)
        serialized = state.model_dump_json()

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO sessions(
                    room_id, agent_state_json, policy_revision,
                    last_event_id, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(room_id) DO UPDATE SET
                    agent_state_json=excluded.agent_state_json,
                    policy_revision=excluded.policy_revision,
                    last_event_id=excluded.last_event_id,
                    updated_at=excluded.updated_at
                """,
                (
                    room_id,
                    serialized,
                    policy_revision,
                    last_event_id,
                    updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return StoredSession(
            room_id=room_id,
            state=state,
            policy_revision=policy_revision,
            last_event_id=last_event_id,
            updated_at=updated_at,
        )

    async def delete(self, room_id: str) -> bool:
        def write(connection: sqlite3.Connection) -> bool:
            cursor = connection.execute(
                "DELETE FROM sessions WHERE room_id=?",
                (room_id,),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)

    async def settings(
        self,
        room_id: str,
        *,
        now: datetime,
        timezone: str = "UTC",
    ) -> SessionSettings:
        existing = await self._read_settings(room_id)
        if existing is not None:
            return existing
        return await self.configure(
            room_id,
            model_override=None,
            timezone=timezone,
            now=now,
        )

    async def configure(
        self,
        room_id: str,
        *,
        model_override: str | None,
        timezone: str,
        now: datetime,
    ) -> SessionSettings:
        normalized_now = now.astimezone(UTC)
        next_reset = _next_daily_reset(normalized_now, timezone)

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO session_settings(
                    room_id, model_override, timezone,
                    next_reset_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(room_id) DO UPDATE SET
                    model_override=excluded.model_override,
                    timezone=excluded.timezone,
                    next_reset_at=excluded.next_reset_at,
                    updated_at=excluded.updated_at
                """,
                (
                    room_id,
                    model_override,
                    timezone,
                    next_reset.isoformat(),
                    normalized_now.isoformat(),
                ),
            )

        await self._database.write(write)
        return SessionSettings(
            room_id=room_id,
            model_override=model_override,
            timezone=timezone,
            next_reset_at=next_reset,
            updated_at=normalized_now,
        )

    async def due_for_reset(
        self,
        now: datetime,
    ) -> tuple[SessionSettings, ...]:
        normalized_now = now.astimezone(UTC)

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[SessionSettings, ...]:
            rows = connection.execute(
                """
                SELECT * FROM session_settings
                WHERE next_reset_at <= ?
                ORDER BY next_reset_at, room_id
                """,
                (normalized_now.isoformat(),),
            ).fetchall()
            return tuple(_settings_from_row(row) for row in rows)

        return await self._database.read(read)

    async def advance_reset(
        self,
        room_id: str,
        *,
        now: datetime,
    ) -> SessionSettings:
        current = await self.settings(room_id, now=now)
        return await self.configure(
            room_id,
            model_override=current.model_override,
            timezone=current.timezone,
            now=now,
        )

    async def _read_settings(
        self,
        room_id: str,
    ) -> SessionSettings | None:
        def read(
            connection: sqlite3.Connection,
        ) -> SessionSettings | None:
            row = connection.execute(
                "SELECT * FROM session_settings WHERE room_id=?",
                (room_id,),
            ).fetchone()
            return _settings_from_row(row) if row is not None else None

        return await self._database.read(read)


def _settings_from_row(row: sqlite3.Row) -> SessionSettings:
    return SessionSettings(
        room_id=row["room_id"],
        model_override=row["model_override"],
        timezone=row["timezone"],
        next_reset_at=datetime.fromisoformat(row["next_reset_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def _next_daily_reset(now: datetime, timezone: str) -> datetime:
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"unknown session timezone {timezone!r}") from error
    local_now = now.astimezone(zone)
    candidate = datetime.combine(
        local_now.date(),
        time(hour=4),
        tzinfo=zone,
    )
    if candidate <= local_now:
        candidate += timedelta(days=1)
    return candidate.astimezone(UTC)
