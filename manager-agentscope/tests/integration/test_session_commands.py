from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.event import TextBlockDeltaEvent
from agentscope.message import Msg, UserMsg
from agentscope.state import AgentState

from agentteams_manager.domain.models import InboundEvent, RoomKind, RoomPolicy
from agentteams_manager.matrix.session_runner import MatrixSessionRunner
from agentteams_manager.matrix.threads import RoomHistory
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.confirmations import (
    ConfirmationRepository,
    ConfirmationService,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.memory import MemoryRepository
from agentteams_manager.state.sessions import SessionRepository
from tests.integration.test_matrix_agent_turn import RecordingMatrix


class ContextAgent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self.inputs: list[Msg] = []

    async def reply_stream(self, *, inputs):
        self.inputs.append(inputs)
        if isinstance(inputs, Msg):
            self.state.context.append(inputs)
        yield TextBlockDeltaEvent(
            reply_id="reply",
            block_id="text",
            delta="ok",
        )


class ContextFactory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.models: list[str | None] = []
        self.agents: list[ContextAgent] = []

    async def create(
        self,
        room_id,
        policy,
        state=None,
        model_override=None,
    ):
        del policy
        agent = ContextAgent(room_id)
        if state is not None:
            agent.state = state
        self.models.append(model_override)
        self.agents.append(agent)
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


def _policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_senders=frozenset({"@admin:local"}),
    )


async def _runner(tmp_path: Path):
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    factory = ContextFactory()
    matrix = RecordingMatrix()
    history = RoomHistory(limit=5)
    memory = MemoryRepository(database)
    sessions = RoomSessionManager(
        factory=factory,
        sessions=repository,
        session_timezone="Asia/Shanghai",
        now=lambda: datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
        history=history,
        memory=memory,
        now=lambda: datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    return runner, sessions, repository, factory, matrix, history, memory


@pytest.mark.asyncio
async def test_history_projection_is_transient_and_current_message_is_clear(
    tmp_path: Path,
) -> None:
    runner, _, repository, factory, _, history, _ = await _runner(tmp_path)
    history.append(_event("old message", "$old"))

    await runner.handle(_event("new request", "$new"), _policy())

    projected = factory.agents[0].inputs[0].get_text_content() or ""
    assert "[Transient room context]" in projected
    assert "old message" in projected
    stored = await repository.load("!admin:local")
    assert stored is not None
    durable = stored.state.context[0].get_text_content() or ""
    assert durable.startswith("[Current message]")
    assert "Sender ID: @admin:local" in durable
    assert "new request" in durable
    assert "old message" not in durable


@pytest.mark.asyncio
async def test_new_status_compact_and_reset_commands(
    tmp_path: Path,
) -> None:
    (
        runner,
        sessions,
        repository,
        factory,
        matrix,
        _,
        memory,
    ) = await _runner(tmp_path)

    await runner.handle(_event("/new qwen-special", "$new"), _policy())
    await runner.handle(_event("hello", "$hello"), _policy())
    session = await sessions.get_or_create("!admin:local", _policy())
    session.agent.state.context = [
        UserMsg(name="admin", content=f"message-{number}")
        for number in range(12)
    ]
    await sessions.persist("!admin:local")
    await runner.handle(_event("/compact", "$compact"), _policy())
    await runner.handle(_event("/status", "$status"), _policy())

    assert factory.models == ["qwen-special"]
    stored = await repository.load("!admin:local")
    assert stored is not None
    assert len(stored.state.context) == 8
    assert "message-0" in stored.state.summary
    assert await memory.daily(
        "!admin:local",
        datetime(2026, 7, 26, tzinfo=UTC).date(),
    )
    assert any("会话状态" in item.text for item in matrix.sent)

    await runner.handle(_event("/reset", "$reset"), _policy())

    assert await repository.load("!admin:local") is None
    settings = await repository.settings(
        "!admin:local",
        now=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    assert settings.model_override == "qwen-special"
