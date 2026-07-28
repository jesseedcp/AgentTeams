from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.event import (
    TextBlockDeltaEvent,
    ThinkingBlockDeltaEvent,
    ToolCallStartEvent,
    ToolResultEndEvent,
)
from agentscope.message import Msg, ToolResultState, UserMsg
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
        text = inputs.get_text_content() if isinstance(inputs, Msg) else ""
        if text and "observable turn" in text:
            yield ThinkingBlockDeltaEvent(
                reply_id="reply",
                block_id="thinking",
                delta="secret-chain-of-thought",
            )
            yield ToolCallStartEvent(
                reply_id="reply",
                tool_call_id="call-1",
                tool_call_name="lookup_worker",
            )
            yield ToolResultEndEvent(
                reply_id="reply",
                tool_call_id="call-1",
                state=ToolResultState.SUCCESS,
            )
        yield TextBlockDeltaEvent(
            reply_id="reply",
            block_id="text",
            delta="ok",
        )


class ContextFactory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.models: list[str | None] = []
        self.thinking_efforts: list[str | None] = []
        self.agents: list[ContextAgent] = []

    async def create(
        self,
        room_id,
        policy,
        state=None,
        model_override=None,
        thinking_effort=None,
    ):
        del policy
        agent = ContextAgent(room_id)
        if state is not None:
            agent.state = state
        self.models.append(model_override)
        self.thinking_efforts.append(thinking_effort)
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
        known_models={
            "qwen-special": True,
            "qwen-fast": False,
            "openrouter/example/model": True,
        },
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


@pytest.mark.asyncio
async def test_help_models_and_unknown_commands_never_reach_agent(
    tmp_path: Path,
) -> None:
    runner, _, _, factory, matrix, _, _ = await _runner(tmp_path)

    await runner.handle(_event("/help", "$help"), _policy())
    await runner.handle(_event("/commands", "$commands"), _policy())
    await runner.handle(_event("/models", "$models"), _policy())
    await runner.handle(_event("/not-a-command", "$unknown"), _policy())

    assert factory.agents == []
    replies = "\n".join(item.text for item in matrix.sent)
    for command in (
        "/new",
        "/reset",
        "/compact",
        "/status",
        "/model",
        "/models",
        "/help",
        "/commands",
        "/stop",
        "/think",
        "/reasoning",
        "/verbose",
        "/elevated",
        "/queue",
    ):
        assert command in replies
    assert "qwen-special" in replies
    assert "未知命令" in replies


@pytest.mark.asyncio
async def test_model_switch_rebuilds_agent_without_losing_context(
    tmp_path: Path,
) -> None:
    runner, sessions, repository, factory, _, _, _ = await _runner(tmp_path)

    await runner.handle(_event("remember this", "$remember"), _policy())
    original = factory.agents[-1]
    await runner.handle(_event("/model 1", "$model-1"), _policy())

    current = await sessions.get_or_create("!admin:local", _policy())
    assert current.agent is not original
    assert factory.models == [None, "qwen-special"]
    assert (
        current.agent.state.context[0].get_text_content() or ""
    ).endswith("remember this")

    await runner.handle(
        _event("/model openrouter/example/model", "$model-full"),
        _policy(),
    )
    settings = await repository.settings(
        "!admin:local",
        now=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    assert settings.model_override == "openrouter/example/model"
    assert len(factory.agents[-1].state.context) == 1


@pytest.mark.asyncio
async def test_session_controls_parse_persist_and_reject_unsafe_elevation(
    tmp_path: Path,
) -> None:
    runner, sessions, repository, factory, matrix, _, _ = await _runner(
        tmp_path,
    )

    await runner.handle(_event("/think high", "$think"), _policy())
    await runner.handle(_event("/reasoning stream", "$reason"), _policy())
    await runner.handle(_event("/verbose full", "$verbose"), _policy())
    await runner.handle(_event("/elevated ask", "$elevated"), _policy())
    await runner.handle(_event("/queue collect 7", "$queue"), _policy())

    settings = await repository.settings(
        "!admin:local",
        now=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    assert settings.thinking_effort == "high"
    assert settings.reasoning_visibility == "stream"
    assert settings.verbose_mode == "full"
    assert settings.elevated_mode == "ask"
    assert settings.queue_mode == "collect"
    assert settings.queue_limit == 7

    await sessions.get_or_create("!admin:local", _policy())
    assert factory.thinking_efforts[-1] == "high"

    member = _policy().model_copy(
        update={
            "kind": RoomKind.HUMAN_OR_CHANNEL_ROOM,
            "allowed_senders": frozenset({"@member:local"}),
        },
    )
    member_event = _event("/elevated full", "$member").model_copy(
        update={"sender": "@member:local"},
    )
    await runner.handle(member_event, member)
    unchanged = await repository.settings(
        "!admin:local",
        now=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    assert unchanged.elevated_mode == "ask"
    assert any("仅管理员" in item.text for item in matrix.sent)


@pytest.mark.asyncio
async def test_thinking_rejects_a_known_non_reasoning_model(
    tmp_path: Path,
) -> None:
    runner, _, repository, _, matrix, _, _ = await _runner(tmp_path)

    await runner.handle(_event("/model qwen-fast", "$model"), _policy())
    await runner.handle(_event("/think high", "$think"), _policy())

    settings = await repository.settings(
        "!admin:local",
        now=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )
    assert settings.thinking_effort is None
    assert any("不支持思考模式" in item.text for item in matrix.sent)


@pytest.mark.asyncio
async def test_reasoning_and_verbose_modes_expose_only_safe_summaries(
    tmp_path: Path,
) -> None:
    runner, _, _, _, matrix, _, _ = await _runner(tmp_path)

    await runner.handle(_event("/reasoning stream", "$reason"), _policy())
    await runner.handle(_event("/verbose full", "$verbose"), _policy())
    matrix.sent.clear()
    await runner.handle(_event("observable turn", "$turn"), _policy())

    output = "\n".join(item.text for item in matrix.sent)
    assert "模型正在推理" in output
    assert "工具执行" in output
    assert "lookup_worker (success)" in output
    assert "secret-chain-of-thought" not in output
