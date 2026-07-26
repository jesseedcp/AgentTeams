from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState

from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.state.confirmations import (
    ConfirmationRepository,
    ConfirmationRequest,
    ConfirmationService,
    ConfirmationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import SessionRepository


def _request(now: datetime) -> ConfirmationRequest:
    return ConfirmationRequest(
        confirmation_id="approval-1",
        source_room_id="!project:local",
        source_event_id="$request",
        source_reply_id="reply-delete",
        requester_id="@worker:local",
        tool_calls=(
            ToolCallBlock(
                id="call-delete",
                name="delete_worker",
                input='{"name":"alice"}',
            ),
        ),
        source_policy=RoomPolicy(
            room_id="!project:local",
            kind=RoomKind.PROJECT_ROOM,
            revision=3,
            allowed_tools=frozenset({"delete_worker"}),
            confirm_tools=frozenset({"delete_worker"}),
            project_id="project-1",
        ),
        status=ConfirmationStatus.AWAITING,
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )


@pytest.mark.asyncio
async def test_confirmation_is_durable_and_resolved_globally(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 26, 8, 0, tzinfo=UTC)
    database = Database(tmp_path / "manager.db")
    await database.open()
    first = ConfirmationService(
        ConfirmationRepository(database),
        now=lambda: now,
    )

    created = await first.create(_request(now))

    restarted = ConfirmationService(
        ConfirmationRepository(database),
        now=lambda: now + timedelta(seconds=1),
    )
    pending = await restarted.pending()
    resolved = await restarted.resolve(
        created.confirmation_id,
        admin_id="@admin:local",
        decision=True,
    )

    assert pending == (created,)
    assert resolved.source_room_id == "!project:local"
    assert resolved.status is ConfirmationStatus.RESOLVING
    assert resolved.decision is True
    assert resolved.resolver_id == "@admin:local"

    completed = await restarted.complete(created.confirmation_id)
    assert completed.status is ConfirmationStatus.APPROVED
    assert await restarted.pending() == ()


@pytest.mark.asyncio
async def test_expired_confirmation_is_removed_from_pending(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 26, 8, 0, tzinfo=UTC)
    database = Database(tmp_path / "manager.db")
    await database.open()
    service = ConfirmationService(
        ConfirmationRepository(database),
        now=lambda: now,
    )
    await service.create(
        _request(now).model_copy(
            update={"expires_at": now + timedelta(seconds=1)},
        ),
    )

    service = ConfirmationService(
        ConfirmationRepository(database),
        now=lambda: now + timedelta(seconds=2),
    )
    expired = await service.expire_due()

    assert expired[0].status is ConfirmationStatus.EXPIRED
    assert await service.pending() == ()


@pytest.mark.asyncio
async def test_legacy_admin_room_confirmation_is_migrated_from_agent_state(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 26, 8, 0, tzinfo=UTC)
    database = Database(tmp_path / "manager.db")
    await database.open()
    sessions = SessionRepository(database)
    state = AgentState(session_id="matrix:!admin:local")
    state.middle_context["agentteams.matrix.pending_confirmation"] = {
        "reply_id": "reply-legacy",
        "event_id": "$legacy",
        "tool_calls": [
            {
                "id": "call-legacy",
                "name": "update_manager_identity",
                "input": '{"name":"管家"}',
            },
        ],
        "status": "awaiting",
        "decision": None,
    }
    await sessions.save(
        room_id="!admin:local",
        state=state,
        policy_revision=3,
        last_event_id="$legacy",
    )
    service = ConfirmationService(
        ConfirmationRepository(database),
        now=lambda: now,
    )

    migrated = await service.migrate_legacy_sessions(
        admin_room_id="!admin:local",
        admin_user_id="@admin:local",
        admin_policy=RoomPolicy(
            room_id="!admin:local",
            kind=RoomKind.ADMIN_DM,
            revision=3,
        ),
        ttl=timedelta(minutes=15),
    )

    assert migrated is not None
    assert migrated.source_reply_id == "reply-legacy"
    assert migrated.tool_calls[0].name == "update_manager_identity"
    stored = await sessions.load("!admin:local")
    assert stored is not None
    assert "agentteams.matrix.pending_confirmation" not in (
        stored.state.middle_context
    )
