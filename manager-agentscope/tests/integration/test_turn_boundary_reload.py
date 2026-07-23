from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.state import AgentState

from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import SessionRepository


class BlockingAgent:
    def __init__(
        self,
        *,
        model: str,
        state: AgentState,
        started: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self.model = model
        self.state = state
        self.started = started
        self.release = release

    async def reply_stream(self, *, inputs):
        self.state.summary = (
            f"{self.model}:{inputs.get_text_content() or ''}"
        )
        self.started.set()
        await self.release.wait()
        if False:
            yield None


class ReloadFactory:
    def __init__(self) -> None:
        self.runtime_revision = 1
        self.model = "old"
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.created: list[BlockingAgent] = []
        self.retired: list[BlockingAgent] = []

    async def create(self, room_id, policy, state=None):
        del policy
        agent = BlockingAgent(
            model=self.model,
            state=state
            or AgentState(session_id=f"matrix:{room_id}"),
            started=self.started,
            release=self.release,
        )
        self.created.append(agent)
        return agent

    async def retire(self, agent) -> None:
        self.retired.append(agent)


def _policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!room:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
    )


def _event(event_id: str, body: str) -> InboundEvent:
    return InboundEvent(
        room_id="!room:example",
        event_id=event_id,
        sender="@admin:example",
        body=body,
        timestamp=datetime.now(UTC),
    )


async def _run(manager, event) -> None:
    async for _ in manager.run(event, _policy()):
        pass


@pytest.mark.asyncio
async def test_active_turn_finishes_before_runtime_replacement(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    factory = ReloadFactory()
    manager = RoomSessionManager(
        factory=factory,
        sessions=SessionRepository(database),
    )
    first = asyncio.create_task(
        _run(manager, _event("$one", "first")),
    )
    await factory.started.wait()
    old_agent = factory.created[0]
    factory.runtime_revision = 2
    factory.model = "new"
    factory.started = asyncio.Event()
    factory.release = asyncio.Event()
    second = asyncio.create_task(
        _run(manager, _event("$two", "second")),
    )
    await asyncio.sleep(0)

    assert len(factory.created) == 1
    assert old_agent.model == "old"

    old_agent.release.set()
    await first
    await factory.started.wait()
    new_agent = factory.created[1]

    assert new_agent.model == "new"
    assert new_agent.state is not old_agent.state
    assert new_agent.state.summary == "new:second"
    assert factory.retired == [old_agent]

    new_agent.release.set()
    await second
