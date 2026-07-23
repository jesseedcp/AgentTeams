"""One serialized AgentScope session per Matrix room."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol

from agentscope.event import AgentEvent
from agentscope.message import UserMsg
from agentscope.state import AgentState

from agentteams_manager.domain.models import InboundEvent, RoomPolicy
from agentteams_manager.state.sessions import SessionRepository


class AgentFactoryPort(Protocol):
    @property
    def runtime_revision(self) -> int: ...

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> Any: ...


@dataclass(slots=True)
class RoomSession:
    agent: Any
    lock: asyncio.Lock
    policy_revision: int
    runtime_revision: int
    last_event_id: str | None = None


class RoomSessionManager:
    def __init__(
        self,
        *,
        factory: AgentFactoryPort,
        sessions: SessionRepository,
    ) -> None:
        self._factory = factory
        self._sessions = sessions
        self._cache: dict[str, RoomSession] = {}
        self._cache_guard = asyncio.Lock()

    async def get_or_create(
        self,
        room_id: str,
        policy: RoomPolicy,
    ) -> RoomSession:
        cached = self._cache.get(room_id)
        runtime_revision = getattr(
            self._factory,
            "runtime_revision",
            0,
        )
        if (
            cached is not None
            and cached.policy_revision == policy.revision
            and cached.runtime_revision == runtime_revision
        ):
            return cached

        async with self._cache_guard:
            cached = self._cache.get(room_id)
            if (
                cached is not None
                and cached.policy_revision == policy.revision
                and cached.runtime_revision == runtime_revision
            ):
                return cached
            stored = await self._sessions.load(room_id)
            state = (
                cached.agent.state
                if cached is not None
                else stored.state if stored is not None else None
            )
            agent = await self._factory.create(
                room_id,
                policy,
                state=state,
            )
            session = RoomSession(
                agent=agent,
                lock=cached.lock if cached is not None else asyncio.Lock(),
                policy_revision=policy.revision,
                runtime_revision=runtime_revision,
                last_event_id=(
                    cached.last_event_id
                    if cached is not None
                    else stored.last_event_id if stored else None
                ),
            )
            self._cache[room_id] = session
            return session

    async def run(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> AsyncIterator[AgentEvent]:
        session = await self.get_or_create(event.room_id, policy)
        async with session.lock:
            message = UserMsg(
                name=event.sender,
                content=event.body,
                id=event.event_id,
                created_at=event.timestamp.isoformat(),
                metadata={
                    "room_id": event.room_id,
                    "thread_id": event.thread_id,
                    "mentions": list(event.mentions),
                },
            )
            try:
                async for item in session.agent.reply_stream(message):
                    yield item
            finally:
                session.last_event_id = event.event_id
                await self._sessions.save(
                    room_id=event.room_id,
                    state=session.agent.state,
                    policy_revision=policy.revision,
                    last_event_id=event.event_id,
                )

    async def save_all(self) -> None:
        for room_id, session in tuple(self._cache.items()):
            async with session.lock:
                await self._sessions.save(
                    room_id=room_id,
                    state=session.agent.state,
                    policy_revision=session.policy_revision,
                    last_event_id=session.last_event_id,
                )
