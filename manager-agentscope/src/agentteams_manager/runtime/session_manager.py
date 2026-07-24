"""One serialized AgentScope session per Matrix room."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from agentscope.event import AgentEvent
from agentscope.message import UserMsg
from agentscope.state import AgentState

from agentteams_manager.domain.models import InboundEvent, RoomPolicy
from agentteams_manager.state.sessions import SessionRepository
from agentteams_manager.tools.base import bind_matrix_turn


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
        self._room_locks: dict[str, asyncio.Lock] = {}
        self._cache_guard = asyncio.Lock()

    async def get_or_create(
        self,
        room_id: str,
        policy: RoomPolicy,
    ) -> RoomSession:
        lock = await self._lock_for(room_id)
        async with lock:
            return await self._get_or_create_locked(
                room_id,
                policy,
                lock,
            )

    async def _get_or_create_locked(
        self,
        room_id: str,
        policy: RoomPolicy,
        lock: asyncio.Lock,
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

        stored = await self._sessions.load(room_id)
        state = (
            cached.agent.state.model_copy(deep=True)
            if cached is not None
            else (
                stored.state.model_copy(deep=True)
                if stored is not None
                else None
            )
        )
        agent = await self._factory.create(
            room_id,
            policy,
            state=state,
        )
        session = RoomSession(
            agent=agent,
            lock=lock,
            policy_revision=policy.revision,
            runtime_revision=runtime_revision,
            last_event_id=(
                cached.last_event_id
                if cached is not None
                else stored.last_event_id if stored else None
            ),
        )
        self._cache[room_id] = session
        if cached is not None:
            retire = getattr(self._factory, "retire", None)
            if retire is not None:
                result = retire(cached.agent)
                if inspect.isawaitable(result):
                    await result
        return session

    async def run(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> AsyncIterator[AgentEvent]:
        message = UserMsg(
            name=event.sender,
            content=event.body,
            id=event.event_id,
            created_at=event.timestamp.isoformat(),
            metadata={
                "room_id": event.room_id,
                "event_id": event.event_id,
                "sender_id": event.sender_id,
                "thread_id": event.thread_id,
                "mentions": list(event.mentions),
            },
        )
        async for item in self.run_input(event, policy, message):
            yield item

    async def run_input(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        inputs: Any,
        *,
        tool_event_id: str | None = None,
        on_event: Callable[[AgentEvent, AgentState], None] | None = None,
    ) -> AsyncIterator[AgentEvent]:
        """Run one native AgentScope input under the room session lock."""
        lock = await self._lock_for(event.room_id)
        async with lock:
            session = await self._get_or_create_locked(
                event.room_id,
                policy,
                lock,
            )
            try:
                with bind_matrix_turn(
                    event.room_id,
                    tool_event_id or event.event_id,
                ):
                    async for item in session.agent.reply_stream(
                        inputs=inputs,
                    ):
                        if on_event is not None:
                            on_event(item, session.agent.state)
                        yield item
            finally:
                session.last_event_id = event.event_id
                await self._save(event.room_id, session)

    async def _lock_for(self, room_id: str) -> asyncio.Lock:
        lock = self._room_locks.get(room_id)
        if lock is not None:
            return lock
        async with self._cache_guard:
            return self._room_locks.setdefault(
                room_id,
                asyncio.Lock(),
            )

    async def persist(self, room_id: str) -> None:
        """Persist a cached session after out-of-stream state changes."""
        session = self._cache.get(room_id)
        if session is None:
            return
        async with session.lock:
            await self._save(room_id, session)

    async def _save(self, room_id: str, session: RoomSession) -> None:
        await self._sessions.save(
            room_id=room_id,
            state=session.agent.state,
            policy_revision=session.policy_revision,
            last_event_id=session.last_event_id,
        )

    async def save_all(self) -> None:
        for room_id, session in tuple(self._cache.items()):
            async with session.lock:
                await self._save(room_id, session)

    async def close_all(self) -> None:
        """Persist and retire every cached Agent generation."""
        for room_id, session in tuple(self._cache.items()):
            async with session.lock:
                await self._save(room_id, session)
                retire = getattr(self._factory, "retire", None)
                if retire is not None:
                    result = retire(session.agent)
                    if inspect.isawaitable(result):
                        await result
        self._cache.clear()
