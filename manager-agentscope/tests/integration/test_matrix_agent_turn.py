from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from agentscope.event import TextBlockDeltaEvent
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
)
from agentteams_manager.state.sessions import SessionRepository


class RecordingMatrix:
    def __init__(self) -> None:
        self.sent: list[SimpleNamespace] = []

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        self.sent.append(
            SimpleNamespace(
                kind="send",
                room_id=room_id,
                text=text,
                txn_id=txn_id,
                thread_id=thread_id,
                mentions=mentions,
            ),
        )
        return "$message"

    async def edit_text(
        self,
        room_id: str,
        event_id: str,
        text: str,
        *,
        txn_id: str,
    ) -> str:
        self.sent.append(
            SimpleNamespace(
                kind="edit",
                room_id=room_id,
                event_id=event_id,
                text=text,
                txn_id=txn_id,
            ),
        )
        return "$edit"


class ReplyAgent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self.reply_stream_inputs: list[object] = []

    async def reply_stream(self, *, inputs: object):
        self.reply_stream_inputs.append(inputs)
        yield TextBlockDeltaEvent(
            reply_id="reply",
            block_id="text",
            delta="There are ",
        )
        yield TextBlockDeltaEvent(
            reply_id="reply",
            block_id="text",
            delta="2 workers.",
        )


class Factory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agents: list[ReplyAgent] = []

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> ReplyAgent:
        del policy
        agent = ReplyAgent(room_id)
        if state is not None:
            agent.state = state
        self.agents.append(agent)
        return agent


class ProtocolTasks:
    def __init__(self, *, status: str = "dispatched") -> None:
        self.status = status

    async def get(self, task_id: str):
        return SimpleNamespace(
            task_id=task_id,
            project_id="project-e2e",
            status=self.status,
        )


class ProtocolProjects:
    def __init__(self) -> None:
        self.blocked: list[dict[str, str]] = []
        self.completed: list[dict[str, object]] = []

    async def report_blocked(self, **kwargs):
        self.blocked.append(kwargs)
        return SimpleNamespace(status="blocked")

    async def complete_task(self, **kwargs):
        self.completed.append(kwargs)
        return SimpleNamespace(status="completed")


class MemoryProjection:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def projection(self, **kwargs) -> str:
        self.calls.append(kwargs)
        return "[Durable Manager memory]\n- Prefer concise Chinese."


def _event(body: str = "list workers") -> InboundEvent:
    return InboundEvent(
        room_id="!admin:local",
        event_id="$request",
        sender="@admin:local",
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=True,
    )


def _policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_senders=frozenset({"@admin:local"}),
    )


@pytest.mark.asyncio
async def test_runner_calls_reply_stream_directly(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    sessions = RoomSessionManager(
        factory=factory,
        sessions=SessionRepository(database),
    )
    matrix = RecordingMatrix()
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
        monotonic=iter((0.0, 0.1)).__next__,
    )

    await runner.handle(_event(), _policy())

    message = factory.agents[0].reply_stream_inputs[0]
    assert message.name == "@admin:local"
    assert message.metadata["event_id"] == "$request"
    assert [item.kind for item in matrix.sent] == ["send", "edit"]
    assert matrix.sent[-1].text == "There are 2 workers."
    stored = await SessionRepository(database).load("!admin:local")
    assert stored is not None
    assert stored.last_event_id == "$request"


@pytest.mark.asyncio
async def test_cold_admin_session_receives_private_memory_projection(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    memory = MemoryProjection()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=factory,
            sessions=SessionRepository(database),
        ),
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
        memory_service=memory,
    )

    await runner.handle(_event("continue the project"), _policy())

    message = factory.agents[0].reply_stream_inputs[0]
    assert "[Durable Manager memory]" in message.content[0].text
    assert memory.calls == [
        {
            "room_id": "!admin:local",
            "include_private": True,
            "project_id": None,
        },
    ]


@pytest.mark.asyncio
async def test_structured_project_blocked_bypasses_model_turn(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    matrix = RecordingMatrix()
    projects = ProtocolProjects()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=factory,
            sessions=SessionRepository(database),
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
        task_reader=ProtocolTasks(),
        project_workflow=projects,
    )
    event = InboundEvent(
        room_id="!leader:local",
        event_id="$blocked",
        sender="@manual-lead:local",
        body=(
            "@manager:local TASK_BLOCKED: "
            "task-20260729-204759-7c954e - "
            "等待管理员提供颜色代码. Result: "
            "shared/tasks/task-20260729-204759-7c954e/result.md"
        ),
        timestamp=datetime.now(UTC),
    )
    policy = RoomPolicy(
        room_id=event.room_id,
        kind=RoomKind.LEADER_ROOM,
        revision=1,
        allowed_senders=frozenset({event.sender_id}),
        team_name="manual-qa",
        allowed_worker_names=frozenset({"manual-lead"}),
    )

    await runner.handle(event, policy)

    assert factory.agents == []
    assert projects.blocked == [
        {
            "project_id": "project-e2e",
            "task_id": "task-20260729-204759-7c954e",
            "sender_id": "@manual-lead:local",
            "reason": (
                "等待管理员提供颜色代码. Result: "
                "shared/tasks/task-20260729-204759-7c954e/result.md"
            ),
        },
    ]
    assert len(matrix.sent) == 1
    assert "已记录任务 task-20260729-204759-7c954e 为 blocked" in (
        matrix.sent[0].text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["dispatched", "completed"])
async def test_structured_project_completed_requires_agent_result_review(
    tmp_path: Path,
    task_status: str,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = Factory()
    matrix = RecordingMatrix()
    projects = ProtocolProjects()
    runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=factory,
            sessions=SessionRepository(database),
        ),
        matrix=matrix,
        admin_user_id="@admin:local",
        confirmations=ConfirmationService(
            ConfirmationRepository(database),
        ),
        task_reader=ProtocolTasks(status=task_status),
        project_workflow=projects,
    )
    event = InboundEvent(
        room_id="!project:local",
        event_id="$completed",
        sender="@worker-alice:local",
        body=(
            "@manager:local TASK_COMPLETED: "
            "task-20260729-204759-7c954e - "
            "BLUE-2026 result verified. Result: "
            "shared/tasks/task-20260729-204759-7c954e/result.md"
        ),
        timestamp=datetime.now(UTC),
    )
    policy = RoomPolicy(
        room_id=event.room_id,
        kind=RoomKind.PROJECT_ROOM,
        revision=1,
        allowed_senders=frozenset({event.sender_id}),
        allowed_worker_names=frozenset({"alice"}),
    )

    await runner.handle(event, policy)

    assert len(factory.agents) == 1
    assert projects.completed == []
    assert [item.kind for item in matrix.sent] == ["send", "edit"]
    assert matrix.sent[-1].text == "There are 2 workers."
