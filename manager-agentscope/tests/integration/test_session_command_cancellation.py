from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.message import Msg
from agentscope.state import AgentState
from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.router import EventRouter
from agentteams_manager.matrix.session_runner import MatrixSessionRunner
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.confirmations import (
    ConfirmationRepository,
    ConfirmationService,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import SessionRepository

from tests.integration.test_matrix_agent_turn import RecordingMatrix


class BlockingAgent:
    def __init__(self, room_id: str, started: asyncio.Event) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self._started = started

    async def reply_stream(self, *, inputs):
        if isinstance(inputs, Msg):
            self.state.context.append(inputs)
        self._started.set()
        await asyncio.Event().wait()
        if False:
            yield None


class BlockingFactory:
    runtime_revision = 1

    def __init__(self, started: asyncio.Event) -> None:
        self._started = started

    async def create(
        self,
        room_id,
        policy,
        state=None,
        model_override=None,
        thinking_effort=None,
    ):
        del policy, model_override, thinking_effort
        agent = BlockingAgent(room_id, self._started)
        if state is not None:
            agent.state = state
        return agent


def _event(body: str, event_id: str) -> InboundEvent:
    return InboundEvent(
        room_id="!admin:local",
        event_id=event_id,
        sender="@admin:local",
        body=body,
        timestamp=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
        is_direct=True,
    )


class Claims:
    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool:
        del room_id, event_id
        return True


class Resolver:
    async def resolve(self, event: InboundEvent) -> RoomPolicy:
        return RoomPolicy(
            room_id=event.room_id,
            kind=RoomKind.ADMIN_DM,
            revision=1,
            allowed_senders=frozenset({event.sender_id}),
        )


@pytest.mark.asyncio
async def test_stop_cancels_active_turn_without_persisting_partial_input(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    started = asyncio.Event()
    sessions = RoomSessionManager(
        factory=BlockingFactory(started),
        sessions=repository,
    )
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
    )
    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=runner.handle,
        control_handler=runner.handle_control,
    )
    await router.start()

    await router.submit(_event("long task", "$work"))
    await asyncio.wait_for(started.wait(), timeout=0.5)
    await router.submit(_event("/stop", "$stop"))

    for _ in range(20):
        if any("已停止" in item.text for item in matrix.sent):
            break
        await asyncio.sleep(0)

    assert any("已停止" in item.text for item in matrix.sent)
    stored = await repository.load("!admin:local")
    assert stored is None or stored.state.context == []
    await router.stop()
