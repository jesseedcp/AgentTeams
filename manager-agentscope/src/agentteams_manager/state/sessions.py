"""AgentScope session persistence keyed by Matrix room."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agentscope.state import AgentState

from .database import Database

THINKING_EFFORTS = frozenset(
    {"off", "minimal", "low", "medium", "high", "xhigh"},
)
REASONING_VISIBILITIES = frozenset({"off", "on", "stream"})
VERBOSE_MODES = frozenset({"off", "on", "full"})
ELEVATED_MODES = frozenset({"off", "ask", "full"})
QUEUE_MODES = frozenset({"followup", "collect", "interrupt"})


class _Unset:
    pass


_UNSET = _Unset()
T = TypeVar("T")


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
    thinking_effort: str | None
    reasoning_visibility: str
    verbose_mode: str
    elevated_mode: str
    queue_mode: str
    queue_limit: int
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
        return await self._store_settings(
            room_id,
            model_override=model_override,
            timezone=timezone,
            now=now,
            reset_schedule=True,
        )

    async def update(
        self,
        room_id: str,
        *,
        now: datetime,
        model_override: str | None | _Unset = _UNSET,
        thinking_effort: str | None | _Unset = _UNSET,
        reasoning_visibility: str | _Unset = _UNSET,
        verbose_mode: str | _Unset = _UNSET,
        elevated_mode: str | _Unset = _UNSET,
        queue_mode: str | _Unset = _UNSET,
        queue_limit: int | _Unset = _UNSET,
    ) -> SessionSettings:
        """Update selected controls without resetting unrelated settings."""
        return await self._store_settings(
            room_id,
            model_override=model_override,
            thinking_effort=thinking_effort,
            reasoning_visibility=reasoning_visibility,
            verbose_mode=verbose_mode,
            elevated_mode=elevated_mode,
            queue_mode=queue_mode,
            queue_limit=queue_limit,
            now=now,
            reset_schedule=False,
        )

    async def _store_settings(
        self,
        room_id: str,
        *,
        now: datetime,
        model_override: str | None | _Unset = _UNSET,
        thinking_effort: str | None | _Unset = _UNSET,
        reasoning_visibility: str | _Unset = _UNSET,
        verbose_mode: str | _Unset = _UNSET,
        elevated_mode: str | _Unset = _UNSET,
        queue_mode: str | _Unset = _UNSET,
        queue_limit: int | _Unset = _UNSET,
        timezone: str | _Unset = _UNSET,
        reset_schedule: bool,
    ) -> SessionSettings:
        normalized_now = now.astimezone(UTC)

        def write(connection: sqlite3.Connection) -> SessionSettings:
            row = connection.execute(
                "SELECT * FROM session_settings WHERE room_id=?",
                (room_id,),
            ).fetchone()
            current = (
                _settings_from_row(row) if row is not None else None
            )
            selected_model = _selected(
                model_override,
                current.model_override if current is not None else None,
            )
            selected_thinking = _selected(
                thinking_effort,
                current.thinking_effort if current is not None else None,
            )
            selected_reasoning = _selected(
                reasoning_visibility,
                (
                    current.reasoning_visibility
                    if current is not None
                    else "off"
                ),
            )
            selected_verbose = _selected(
                verbose_mode,
                current.verbose_mode if current is not None else "off",
            )
            selected_elevated = _selected(
                elevated_mode,
                current.elevated_mode if current is not None else "off",
            )
            selected_queue_mode = _selected(
                queue_mode,
                current.queue_mode if current is not None else "followup",
            )
            selected_queue_limit = _selected(
                queue_limit,
                current.queue_limit if current is not None else 20,
            )
            selected_timezone = _selected(
                timezone,
                current.timezone if current is not None else "UTC",
            )
            _validate_settings(
                thinking_effort=selected_thinking,
                reasoning_visibility=selected_reasoning,
                verbose_mode=selected_verbose,
                elevated_mode=selected_elevated,
                queue_mode=selected_queue_mode,
                queue_limit=selected_queue_limit,
            )
            next_reset = (
                _next_daily_reset(normalized_now, selected_timezone)
                if current is None or reset_schedule
                else current.next_reset_at
            )
            connection.execute(
                """
                INSERT INTO session_settings(
                    room_id, model_override, thinking_effort,
                    reasoning_visibility, verbose_mode, elevated_mode,
                    queue_mode, queue_limit, timezone,
                    next_reset_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(room_id) DO UPDATE SET
                    model_override=excluded.model_override,
                    thinking_effort=excluded.thinking_effort,
                    reasoning_visibility=excluded.reasoning_visibility,
                    verbose_mode=excluded.verbose_mode,
                    elevated_mode=excluded.elevated_mode,
                    queue_mode=excluded.queue_mode,
                    queue_limit=excluded.queue_limit,
                    timezone=excluded.timezone,
                    next_reset_at=excluded.next_reset_at,
                    updated_at=excluded.updated_at
                """,
                (
                    room_id,
                    selected_model,
                    selected_thinking,
                    selected_reasoning,
                    selected_verbose,
                    selected_elevated,
                    selected_queue_mode,
                    selected_queue_limit,
                    selected_timezone,
                    next_reset.isoformat(),
                    normalized_now.isoformat(),
                ),
            )
            return SessionSettings(
                room_id=room_id,
                model_override=selected_model,
                thinking_effort=selected_thinking,
                reasoning_visibility=selected_reasoning,
                verbose_mode=selected_verbose,
                elevated_mode=selected_elevated,
                queue_mode=selected_queue_mode,
                queue_limit=selected_queue_limit,
                timezone=selected_timezone,
                next_reset_at=next_reset,
                updated_at=normalized_now,
            )

        return await self._database.write(write)

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
        thinking_effort=row["thinking_effort"],
        reasoning_visibility=row["reasoning_visibility"],
        verbose_mode=row["verbose_mode"],
        elevated_mode=row["elevated_mode"],
        queue_mode=row["queue_mode"],
        queue_limit=row["queue_limit"],
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


def _selected(value: T | _Unset, current: T) -> T:
    return current if isinstance(value, _Unset) else value


def _validate_settings(
    *,
    thinking_effort: str | None,
    reasoning_visibility: str,
    verbose_mode: str,
    elevated_mode: str,
    queue_mode: str,
    queue_limit: int,
) -> None:
    if (
        thinking_effort is not None
        and thinking_effort not in THINKING_EFFORTS
    ):
        raise ValueError(f"unsupported thinking effort {thinking_effort!r}")
    if reasoning_visibility not in REASONING_VISIBILITIES:
        raise ValueError(
            f"unsupported reasoning visibility {reasoning_visibility!r}",
        )
    if verbose_mode not in VERBOSE_MODES:
        raise ValueError(f"unsupported verbose mode {verbose_mode!r}")
    if elevated_mode not in ELEVATED_MODES:
        raise ValueError(f"unsupported elevated mode {elevated_mode!r}")
    if queue_mode not in QUEUE_MODES:
        raise ValueError(f"unsupported queue mode {queue_mode!r}")
    if not 1 <= queue_limit <= 100:
        raise ValueError("queue limit must be between 1 and 100")
