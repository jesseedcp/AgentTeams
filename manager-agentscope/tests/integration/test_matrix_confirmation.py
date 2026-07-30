from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from agentscope.event import (
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
    UserConfirmResultEvent,
)
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState

from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.session_runner import MatrixSessionRunner
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.database import Database
from agentteams_manager.state.confirmations import (
    ConfirmationRepository,
    ConfirmationService,
    ConfirmationStatus,
)
from agentteams_manager.state.sessions import SessionRepository
from tests.integration.test_matrix_agent_turn import RecordingMatrix


class ConfirmationAgent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self.inputs: list[object] = []
        self.confirmation_results: list[UserConfirmResultEvent] = []

    async def reply_stream(self, *, inputs: object):
        self.inputs.append(inputs)
        if isinstance(inputs, UserConfirmResultEvent):
            self.confirmation_results.append(inputs)
            yield TextBlockDeltaEvent(
                reply_id=inputs.reply_id,
                block_id="text",
                delta="Deleted alice.",
            )
            return
        yield RequireUserConfirmEvent(
            reply_id="reply-delete",
            tool_calls=[
                ToolCallBlock(
                    id="call-delete",
                    name="delete_worker",
                    input='{"name":"alice"}',
                ),
            ],
        )


class ConcurrentConfirmationAgent(ConfirmationAgent):
    async def reply_stream(self, *, inputs: object):
        self.inputs.append(inputs)
        if isinstance(inputs, UserConfirmResultEvent):
            self.confirmation_results.append(inputs)
            yield TextBlockDeltaEvent(
                reply_id=inputs.reply_id,
                block_id="text",
                delta="Listed all resources.",
            )
            return
        for index, name in enumerate(
            ("list_workers", "list_teams", "list_projects"),
        ):
            yield RequireUserConfirmEvent(
                reply_id="reply-list",
                tool_calls=[
                    ToolCallBlock(
                        id=f"call-list-{index}",
                        name=name,
                        input="{}",
                    ),
                ],
            )


class Factory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agent: ConfirmationAgent | None = None

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> ConfirmationAgent:
        del policy
        self.agent = ConfirmationAgent(room_id)
        if state is not None:
            self.agent.state = state
        return self.agent


class ConcurrentFactory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agent: ConcurrentConfirmationAgent | None = None

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> ConcurrentConfirmationAgent:
        del policy
        self.agent = ConcurrentConfirmationAgent(room_id)
        if state is not None:
            self.agent.state = state
        return self.agent


class RestartedConfirmationAgent(ConfirmationAgent):
    async def reply_stream(self, *, inputs: object):
        if isinstance(inputs, UserConfirmResultEvent):
            raise ValueError(
                "Agent is not waiting for user confirmation",
            )
        async for item in super().reply_stream(inputs=inputs):
            yield item


class RestartedFactory:
    runtime_revision = 1

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> RestartedConfirmationAgent:
        del policy
        agent = RestartedConfirmationAgent(room_id)
        if state is not None:
            agent.state = state
        return agent


class MultiRoomFactory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agents: dict[str, ConfirmationAgent] = {}

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> ConfirmationAgent:
        del policy
        agent = ConfirmationAgent(room_id)
        if state is not None:
            agent.state = state
        self.agents[room_id] = agent
        return agent


def _event(body: str, event_id: str, sender: str = "@admin:local") -> InboundEvent:
    return InboundEvent(
        room_id="!admin:local",
        event_id=event_id,
        sender=sender,
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=True,
    )


def _policy(sender: str = "@admin:local") -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_senders=frozenset({sender}),
    )


def _room_event(
    *,
    room_id: str,
    body: str,
    event_id: str,
    sender: str,
    is_direct: bool,
) -> InboundEvent:
    return InboundEvent(
        room_id=room_id,
        event_id=event_id,
        sender=sender,
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=is_direct,
    )


def _project_policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!project:local",
        kind=RoomKind.PROJECT_ROOM,
        revision=1,
        allowed_tools=frozenset({"delete_worker"}),
        confirm_tools=frozenset({"delete_worker"}),
        allowed_senders=frozenset({"@worker:local"}),
        project_id="project-1",
    )


@pytest.mark.asyncio
async def test_project_confirmation_is_approved_from_admin_dm(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    factory = MultiRoomFactory()
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=factory,
            sessions=SessionRepository(database),
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )

    await runner.handle(
        _room_event(
            room_id="!project:local",
            body="delete alice",
            event_id="$project-delete",
            sender="@worker:local",
            is_direct=False,
        ),
        _project_policy(),
    )

    pending = await confirmations.pending()
    assert len(pending) == 1
    approval = pending[0]
    assert approval.source_room_id == "!project:local"
    stored_session = await SessionRepository(database).load(
        "!project:local",
    )
    assert stored_session is not None
    assert "agentteams.matrix.pending_confirmation" not in (
        stored_session.state.middle_context
    )
    assert any(
        item.room_id == "!admin:local"
        and f"/confirm {approval.confirmation_id}" in item.text
        and "delete_worker" in item.text
        for item in matrix.sent
    )
    assert any(
        item.room_id == "!project:local"
        and "已发送给管理员审批" in item.text
        for item in matrix.sent
    )

    await runner.handle(
        _room_event(
            room_id="!admin:local",
            body="确认保存",
            event_id="$approve",
            sender="@admin:local",
            is_direct=True,
        ),
        _policy(),
    )

    source_agent = factory.agents["!project:local"]
    result = source_agent.confirmation_results[0]
    assert result.reply_id == "reply-delete"
    assert result.confirm_results[0].confirmed
    stored = await confirmations.get(approval.confirmation_id)
    assert stored is not None
    assert stored.status is ConfirmationStatus.APPROVED
    assert any(
        item.room_id == "!project:local"
        and item.text == "Deleted alice."
        for item in matrix.sent
    )


@pytest.mark.asyncio
async def test_status_and_reset_release_parked_source_room(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    sessions_repository = SessionRepository(database)
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=MultiRoomFactory(),
            sessions=sessions_repository,
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )
    await runner.handle(
        _room_event(
            room_id="!project:local",
            body="delete alice",
            event_id="$project-delete",
            sender="@worker:local",
            is_direct=False,
        ),
        _project_policy(),
    )
    approval = (await confirmations.pending())[0]

    await runner.handle(
        _event("/status", "$status"),
        _policy(),
    )
    assert approval.confirmation_id in matrix.sent[-1].text

    await runner.handle(
        _event(f"/reset {approval.confirmation_id}", "$reset"),
        _policy(),
    )

    cancelled = await confirmations.get(approval.confirmation_id)
    assert cancelled is not None
    assert cancelled.status is ConfirmationStatus.CANCELLED
    assert await sessions_repository.load("!project:local") is None
    assert "会话已重置" in matrix.sent[-1].text


@pytest.mark.asyncio
async def test_chinese_confirmation_requires_unique_pending_request(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    factory = MultiRoomFactory()
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=factory,
            sessions=SessionRepository(database),
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )
    for index in (1, 2):
        room_id = f"!project-{index}:local"
        await runner.handle(
            _room_event(
                room_id=room_id,
                body="delete alice",
                event_id=f"$delete-{index}",
                sender="@worker:local",
                is_direct=False,
            ),
            _project_policy().model_copy(
                update={"room_id": room_id},
            ),
        )

    await runner.handle(
        _event("确认", "$ambiguous"),
        _policy(),
    )

    assert "多个等待审批" in matrix.sent[-1].text
    assert len(await confirmations.pending()) == 2
    assert all(
        not agent.confirmation_results
        for agent in factory.agents.values()
    )


@pytest.mark.asyncio
async def test_unknown_global_confirmation_id_returns_safe_error(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=MultiRoomFactory(),
            sessions=SessionRepository(database),
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
    )

    await runner.handle(
        _event("/confirm missing", "$missing"),
        _policy(),
    )

    assert "无法处理审批请求" in matrix.sent[-1].text
    assert "missing" in matrix.sent[-1].text


@pytest.mark.asyncio
async def test_expired_confirmation_resets_parked_room(
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 7, 26, 8, 0, tzinfo=UTC)]
    database = Database(tmp_path / "manager.db")
    await database.open()
    sessions_repository = SessionRepository(database)
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
        now=lambda: current[0],
    )
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=MultiRoomFactory(),
            sessions=sessions_repository,
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
        now=lambda: current[0],
        confirmation_ttl=timedelta(seconds=1),
    )
    await runner.handle(
        _room_event(
            room_id="!project:local",
            body="delete alice",
            event_id="$project-delete",
            sender="@worker:local",
            is_direct=False,
        ),
        _project_policy(),
    )
    approval = (await confirmations.pending())[0]
    current[0] += timedelta(seconds=2)

    await runner.handle(_event("/status", "$status"), _policy())

    expired = await confirmations.get(approval.confirmation_id)
    assert expired is not None
    assert expired.status is ConfirmationStatus.EXPIRED
    assert await sessions_repository.load("!project:local") is None
    assert any("审批请求已过期" in item.text for item in matrix.sent)


@pytest.mark.asyncio
async def test_confirmation_continues_same_reply(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    factory = Factory()
    sessions = RoomSessionManager(factory=factory, sessions=repository)
    matrix = RecordingMatrix()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )

    await asyncio.wait_for(
        runner.handle(
            _event("delete alice", "$delete"),
            _policy(),
        ),
        timeout=1,
    )

    approval = (await confirmations.pending())[0]
    prompt = matrix.sent[-1]
    assert f"/confirm {approval.confirmation_id}" in prompt.text
    stored = await repository.load("!admin:local")
    assert stored is not None
    assert "agentteams.matrix.pending_confirmation" not in (
        stored.state.middle_context
    )

    await runner.handle(
        _event(f"/confirm {approval.confirmation_id}", "$confirm"),
        _policy(),
    )

    assert factory.agent is not None
    result = factory.agent.confirmation_results[0]
    assert result.reply_id == "reply-delete"
    assert result.confirm_results[0].confirmed
    assert any(item.text == "Deleted alice." for item in matrix.sent)


@pytest.mark.asyncio
async def test_confirmation_interrupted_by_restart_is_cancelled_cleanly(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    matrix = RecordingMatrix()
    first_runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=Factory(),
            sessions=repository,
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )
    await first_runner.handle(
        _event("delete alice", "$delete-before-restart"),
        _policy(),
    )
    approval = (await confirmations.pending())[0]

    restarted_runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=RestartedFactory(),
            sessions=repository,
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )
    await restarted_runner.handle(
        _event(
            f"/confirm {approval.confirmation_id}",
            "$confirm-after-restart",
        ),
        _policy(),
    )

    recovered = await confirmations.get(approval.confirmation_id)
    assert recovered is not None
    assert recovered.status is ConfirmationStatus.CANCELLED
    assert await repository.load("!admin:local") is None
    assert any(
        "Manager 重启后无法安全恢复" in item.text
        for item in matrix.sent
    )


@pytest.mark.asyncio
async def test_parallel_confirmation_items_are_approved_together(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = ConcurrentFactory()
    matrix = RecordingMatrix()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=factory,
            sessions=SessionRepository(database),
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )

    await runner.handle(
        _event("list workers, teams, and projects", "$list"),
        _policy(),
    )

    pending = await confirmations.pending()
    assert len(pending) == 1
    assert {
        call.name for call in pending[0].tool_calls
    } == {"list_workers", "list_teams", "list_projects"}

    await runner.handle(
        _event(
            f"/confirm {pending[0].confirmation_id}",
            "$confirm-list",
        ),
        _policy(),
    )

    assert factory.agent is not None
    result = factory.agent.confirmation_results[0]
    assert len(result.confirm_results) == 3
    assert any(item.text == "Listed all resources." for item in matrix.sent)


@pytest.mark.asyncio
async def test_pending_confirmation_blocks_ordinary_message(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    factory = Factory()
    sessions = RoomSessionManager(factory=factory, sessions=repository)
    matrix = RecordingMatrix()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )

    await runner.handle(_event("delete alice", "$delete"), _policy())
    await runner.handle(
        _event("告诉我你的名字", "$ordinary"),
        _policy(),
    )

    assert factory.agent is not None
    assert len(factory.agent.inputs) == 1
    reminder = matrix.sent[-1].text
    assert "这条消息尚未交给模型处理" in reminder
    approval = (await confirmations.pending())[0]
    assert f"/confirm {approval.confirmation_id}" in reminder
    assert f"/deny {approval.confirmation_id}" in reminder
    stored = await repository.load("!admin:local")
    assert stored is not None
    assert "agentteams.matrix.pending_confirmation" not in (
        stored.state.middle_context
    )


@pytest.mark.asyncio
async def test_pending_confirmation_rejects_mismatched_id(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    factory = Factory()
    sessions = RoomSessionManager(factory=factory, sessions=repository)
    matrix = RecordingMatrix()
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )

    await runner.handle(_event("delete alice", "$delete"), _policy())
    await runner.handle(
        _event("/confirm wrong-reply", "$wrong"),
        _policy(),
    )

    assert factory.agent is not None
    assert len(factory.agent.inputs) == 1
    assert factory.agent.confirmation_results == []
    assert "无法处理审批请求" in matrix.sent[-1].text
    assert len(await confirmations.pending()) == 1


@pytest.mark.asyncio
async def test_non_admin_cannot_resolve_confirmation(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    sessions = RoomSessionManager(
        factory=factory,
        sessions=SessionRepository(database),
    )
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )
    await runner.handle(_event("delete alice", "$delete"), _policy())
    approval = (await confirmations.pending())[0]

    with pytest.raises(PermissionError, match="admin"):
        await runner.handle(
            _event(
                f"/confirm {approval.confirmation_id}",
                "$intruder",
                sender="@intruder:local",
            ),
            _policy("@intruder:local"),
        )
