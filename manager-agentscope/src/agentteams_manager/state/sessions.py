"""AgentScope session persistence keyed by Matrix room."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from pydantic import BaseModel, ConfigDict

from .database import Database


@dataclass(frozen=True, slots=True)
class StoredSession:
    room_id: str
    state: AgentState
    policy_revision: int
    last_event_id: str | None
    updated_at: datetime


class PendingConfirmation(BaseModel):
    """Durable continuation data for a parked AgentScope reply."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    reply_id: str
    tool_calls: tuple[ToolCallBlock, ...]
    status: Literal["awaiting", "resolving"] = "awaiting"
    decision: bool | None = None


_PENDING_CONFIRMATION_KEY = "agentteams.matrix.pending_confirmation"


def pending_confirmation(
    state: AgentState,
) -> PendingConfirmation | None:
    raw = state.middle_context.get(_PENDING_CONFIRMATION_KEY)
    if raw is None:
        return None
    return PendingConfirmation.model_validate(raw)


def set_pending_confirmation(
    state: AgentState,
    pending: PendingConfirmation,
) -> None:
    state.middle_context[_PENDING_CONFIRMATION_KEY] = pending.model_dump(
        mode="json",
    )


def clear_pending_confirmation(state: AgentState) -> None:
    state.middle_context.pop(_PENDING_CONFIRMATION_KEY, None)


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
