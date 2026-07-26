"""Durable, cross-room approval requests for AgentScope continuations."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from pydantic import BaseModel, ConfigDict

from agentteams_manager.domain.errors import ConflictError, NotFoundError
from agentteams_manager.domain.ids import operation_id_for
from agentteams_manager.domain.models import RoomPolicy

from .database import Database


class ConfirmationStatus(StrEnum):
    AWAITING = "awaiting"
    RESOLVING = "resolving"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class ConfirmationRequest(BaseModel):
    """One globally addressable approval and its source continuation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    confirmation_id: str
    source_room_id: str
    source_event_id: str
    source_reply_id: str
    requester_id: str
    tool_calls: tuple[ToolCallBlock, ...]
    source_policy: RoomPolicy
    status: ConfirmationStatus = ConfirmationStatus.AWAITING
    decision: bool | None = None
    resolver_id: str | None = None
    created_at: datetime
    expires_at: datetime
    resolved_at: datetime | None = None


def _from_row(row: sqlite3.Row) -> ConfirmationRequest:
    return ConfirmationRequest(
        confirmation_id=row["confirmation_id"],
        source_room_id=row["source_room_id"],
        source_event_id=row["source_event_id"],
        source_reply_id=row["source_reply_id"],
        requester_id=row["requester_id"],
        tool_calls=tuple(
            ToolCallBlock.model_validate(item)
            for item in json.loads(row["tool_calls_json"])
        ),
        source_policy=RoomPolicy.model_validate_json(
            row["source_policy_json"],
        ),
        status=ConfirmationStatus(row["status"]),
        decision=(
            bool(row["decision"])
            if row["decision"] is not None
            else None
        ),
        resolver_id=row["resolver_id"],
        created_at=datetime.fromisoformat(row["created_at"]),
        expires_at=datetime.fromisoformat(row["expires_at"]),
        resolved_at=(
            datetime.fromisoformat(row["resolved_at"])
            if row["resolved_at"]
            else None
        ),
    )


class ConfirmationRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(
        self,
        request: ConfirmationRequest,
    ) -> ConfirmationRequest:
        def write(connection: sqlite3.Connection) -> None:
            _insert_request(connection, request)

        await self._database.write(write)
        stored = await self.get(request.confirmation_id)
        if stored is None:
            raise RuntimeError("confirmation insert did not persist")
        if (
            stored.source_room_id,
            stored.source_event_id,
            stored.source_reply_id,
        ) != (
            request.source_room_id,
            request.source_event_id,
            request.source_reply_id,
        ):
            raise ConflictError(
                f"confirmation/{request.confirmation_id} already exists",
            )
        return stored

    async def migrate_legacy_sessions(
        self,
        *,
        admin_room_id: str,
        admin_user_id: str,
        admin_policy: RoomPolicy,
        now: datetime,
        ttl: timedelta,
    ) -> ConfirmationRequest | None:
        """Move the old room-local admin approval and reset other parks."""

        def write(
            connection: sqlite3.Connection,
        ) -> ConfirmationRequest | None:
            rows = connection.execute(
                "SELECT * FROM sessions",
            ).fetchall()
            migrated: ConfirmationRequest | None = None
            legacy_key = "agentteams.matrix.pending_confirmation"
            for row in rows:
                state = AgentState.model_validate_json(
                    row["agent_state_json"],
                )
                raw = state.middle_context.pop(legacy_key, None)
                if raw is None:
                    continue
                if row["room_id"] != admin_room_id:
                    connection.execute(
                        "DELETE FROM sessions WHERE room_id=?",
                        (row["room_id"],),
                    )
                    continue
                try:
                    reply_id = str(raw["reply_id"])
                    event_id = str(raw["event_id"])
                    tool_calls = tuple(
                        ToolCallBlock.model_validate(item)
                        for item in raw["tool_calls"]
                    )
                except (KeyError, TypeError, ValueError):
                    connection.execute(
                        """
                        UPDATE sessions SET agent_state_json=?
                         WHERE room_id=?
                        """,
                        (state.model_dump_json(), row["room_id"]),
                    )
                    continue
                migrated = ConfirmationRequest(
                    confirmation_id=operation_id_for(
                        admin_room_id,
                        event_id,
                        reply_id,
                    ),
                    source_room_id=admin_room_id,
                    source_event_id=event_id,
                    source_reply_id=reply_id,
                    requester_id=admin_user_id,
                    tool_calls=tool_calls,
                    source_policy=admin_policy,
                    created_at=now,
                    expires_at=now + ttl,
                )
                _insert_request(connection, migrated)
                connection.execute(
                    """
                    UPDATE sessions SET agent_state_json=?
                     WHERE room_id=?
                    """,
                    (state.model_dump_json(), row["room_id"]),
                )
            return migrated

        return await self._database.write(write)

    async def get(
        self,
        confirmation_id: str,
    ) -> ConfirmationRequest | None:
        def read(
            connection: sqlite3.Connection,
        ) -> ConfirmationRequest | None:
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(row) if row is not None else None

        return await self._database.read(read)

    async def pending(self) -> tuple[ConfirmationRequest, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ConfirmationRequest, ...]:
            rows = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE status IN ('awaiting', 'resolving')
                 ORDER BY created_at, confirmation_id
                """,
            ).fetchall()
            return tuple(_from_row(row) for row in rows)

        return await self._database.read(read)

    async def pending_for_room(
        self,
        room_id: str,
    ) -> ConfirmationRequest | None:
        def read(
            connection: sqlite3.Connection,
        ) -> ConfirmationRequest | None:
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE source_room_id=?
                   AND status IN ('awaiting', 'resolving')
                 ORDER BY created_at
                 LIMIT 1
                """,
                (room_id,),
            ).fetchone()
            return _from_row(row) if row is not None else None

        return await self._database.read(read)

    async def begin_resolution(
        self,
        confirmation_id: str,
        *,
        resolver_id: str,
        decision: bool,
        now: datetime,
    ) -> ConfirmationRequest:
        def write(connection: sqlite3.Connection) -> ConfirmationRequest:
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"confirmation/{confirmation_id} does not exist",
                )
            current = _from_row(row)
            if current.expires_at <= now:
                raise ConflictError(
                    f"confirmation/{confirmation_id} has expired",
                )
            if current.status is ConfirmationStatus.RESOLVING:
                if current.decision is decision:
                    return current
                raise ConflictError(
                    f"confirmation/{confirmation_id} is already resolving",
                )
            if current.status is not ConfirmationStatus.AWAITING:
                raise ConflictError(
                    f"confirmation/{confirmation_id} is "
                    f"{current.status.value}",
                )
            connection.execute(
                """
                UPDATE confirmation_requests
                   SET status='resolving', decision=?, resolver_id=?,
                       resolved_at=?
                 WHERE confirmation_id=?
                """,
                (
                    int(decision),
                    resolver_id,
                    now.isoformat(),
                    confirmation_id,
                ),
            )
            updated = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(updated)

        return await self._database.write(write)

    async def complete(
        self,
        confirmation_id: str,
        *,
        now: datetime,
    ) -> ConfirmationRequest:
        def write(connection: sqlite3.Connection) -> ConfirmationRequest:
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"confirmation/{confirmation_id} does not exist",
                )
            current = _from_row(row)
            if current.status is not ConfirmationStatus.RESOLVING:
                raise ConflictError(
                    f"confirmation/{confirmation_id} is not resolving",
                )
            status = (
                ConfirmationStatus.APPROVED
                if current.decision
                else ConfirmationStatus.DENIED
            )
            connection.execute(
                """
                UPDATE confirmation_requests
                   SET status=?, resolved_at=?
                 WHERE confirmation_id=?
                """,
                (status.value, now.isoformat(), confirmation_id),
            )
            updated = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(updated)

        return await self._database.write(write)

    async def cancel(
        self,
        confirmation_id: str,
        *,
        resolver_id: str,
        now: datetime,
    ) -> ConfirmationRequest:
        return await self._terminal_update(
            confirmation_id,
            status=ConfirmationStatus.CANCELLED,
            resolver_id=resolver_id,
            now=now,
        )

    async def expire_due(
        self,
        *,
        now: datetime,
    ) -> tuple[ConfirmationRequest, ...]:
        def write(
            connection: sqlite3.Connection,
        ) -> tuple[ConfirmationRequest, ...]:
            rows = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE status IN ('awaiting', 'resolving')
                   AND expires_at <= ?
                 ORDER BY created_at, confirmation_id
                """,
                (now.isoformat(),),
            ).fetchall()
            if rows:
                connection.execute(
                    """
                    UPDATE confirmation_requests
                       SET status='expired', resolved_at=?
                     WHERE status IN ('awaiting', 'resolving')
                       AND expires_at <= ?
                    """,
                    (now.isoformat(), now.isoformat()),
                )
            return tuple(
                _from_row(row).model_copy(
                    update={
                        "status": ConfirmationStatus.EXPIRED,
                        "resolved_at": now,
                    },
                )
                for row in rows
            )

        return await self._database.write(write)

    async def _terminal_update(
        self,
        confirmation_id: str,
        *,
        status: ConfirmationStatus,
        resolver_id: str,
        now: datetime,
    ) -> ConfirmationRequest:
        def write(connection: sqlite3.Connection) -> ConfirmationRequest:
            cursor = connection.execute(
                """
                UPDATE confirmation_requests
                   SET status=?, resolver_id=?, resolved_at=?
                 WHERE confirmation_id=?
                   AND status IN ('awaiting', 'resolving')
                """,
                (
                    status.value,
                    resolver_id,
                    now.isoformat(),
                    confirmation_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ConflictError(
                    f"confirmation/{confirmation_id} is not pending",
                )
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(row)

        return await self._database.write(write)


class ConfirmationService:
    """Apply lifecycle rules around the durable confirmation repository."""

    def __init__(
        self,
        repository: ConfirmationRepository,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._now = now or (lambda: datetime.now(UTC))

    async def create(
        self,
        request: ConfirmationRequest,
    ) -> ConfirmationRequest:
        return await self._repository.create(request)

    async def get(
        self,
        confirmation_id: str,
    ) -> ConfirmationRequest | None:
        return await self._repository.get(confirmation_id)

    async def pending(self) -> tuple[ConfirmationRequest, ...]:
        await self.expire_due()
        return await self._repository.pending()

    async def pending_for_room(
        self,
        room_id: str,
    ) -> ConfirmationRequest | None:
        await self.expire_due()
        return await self._repository.pending_for_room(room_id)

    async def resolve(
        self,
        confirmation_id: str,
        *,
        admin_id: str,
        decision: bool,
    ) -> ConfirmationRequest:
        await self.expire_due()
        return await self._repository.begin_resolution(
            confirmation_id,
            resolver_id=admin_id,
            decision=decision,
            now=self._now(),
        )

    async def complete(
        self,
        confirmation_id: str,
    ) -> ConfirmationRequest:
        return await self._repository.complete(
            confirmation_id,
            now=self._now(),
        )

    async def cancel(
        self,
        confirmation_id: str,
        *,
        admin_id: str,
    ) -> ConfirmationRequest:
        return await self._repository.cancel(
            confirmation_id,
            resolver_id=admin_id,
            now=self._now(),
        )

    async def expire_due(self) -> tuple[ConfirmationRequest, ...]:
        return await self._repository.expire_due(now=self._now())

    async def migrate_legacy_sessions(
        self,
        *,
        admin_room_id: str,
        admin_user_id: str,
        admin_policy: RoomPolicy,
        ttl: timedelta,
    ) -> ConfirmationRequest | None:
        return await self._repository.migrate_legacy_sessions(
            admin_room_id=admin_room_id,
            admin_user_id=admin_user_id,
            admin_policy=admin_policy,
            now=self._now(),
            ttl=ttl,
        )


def _insert_request(
    connection: sqlite3.Connection,
    request: ConfirmationRequest,
) -> None:
    connection.execute(
        """
        INSERT INTO confirmation_requests(
            confirmation_id, source_room_id, source_event_id,
            source_reply_id, requester_id, tool_calls_json,
            source_policy_json, status, decision, resolver_id,
            created_at, expires_at, resolved_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(confirmation_id) DO NOTHING
        """,
        (
            request.confirmation_id,
            request.source_room_id,
            request.source_event_id,
            request.source_reply_id,
            request.requester_id,
            json.dumps(
                [
                    call.model_dump(mode="json")
                    for call in request.tool_calls
                ],
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            request.source_policy.model_dump_json(),
            request.status.value,
            request.decision,
            request.resolver_id,
            request.created_at.isoformat(),
            request.expires_at.isoformat(),
            (
                request.resolved_at.isoformat()
                if request.resolved_at
                else None
            ),
        ),
    )
