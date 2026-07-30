"""One serialized AgentScope session per Matrix room."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from agentscope.event import AgentEvent
from agentscope.message import Msg, TextBlock, UserMsg
from agentscope.state import AgentState

from agentteams_manager.domain.models import InboundEvent, RoomPolicy
from agentteams_manager.runtime.messages import current_message_text
from agentteams_manager.state.sessions import (
    SessionRepository,
    SessionSettings,
)
from agentteams_manager.tools.base import bind_matrix_turn


class AgentFactoryPort(Protocol):
    @property
    def runtime_revision(self) -> int: ...

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
        model_override: str | None = None,
        thinking_effort: str | None = None,
    ) -> Any: ...


@dataclass(slots=True)
class RoomSession:
    agent: Any
    lock: asyncio.Lock
    policy_revision: int
    runtime_revision: int
    model_override: str | None = None
    thinking_effort: str | None = None
    elevated_mode: str = "off"
    last_event_id: str | None = None


@dataclass(frozen=True, slots=True)
class RoomSessionStatus:
    room_id: str
    session_id: str | None
    model_override: str | None
    thinking_effort: str | None
    reasoning_visibility: str
    verbose_mode: str
    elevated_mode: str
    queue_mode: str
    queue_limit: int
    context_messages: int
    summary_characters: int
    summary: str
    next_reset_at: datetime


class RoomSessionManager:
    def __init__(
        self,
        *,
        factory: AgentFactoryPort,
        sessions: SessionRepository,
        session_timezone: str = "UTC",
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._factory = factory
        self._sessions = sessions
        self._cache: dict[str, RoomSession] = {}
        self._room_locks: dict[str, asyncio.Lock] = {}
        self._cache_guard = asyncio.Lock()
        self._active_tasks: dict[str, asyncio.Task[Any]] = {}
        self._session_timezone = session_timezone
        self._now = now or (lambda: datetime.now(UTC))

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
        settings = await self._sessions.settings(
            room_id,
            now=self._now(),
            timezone=self._session_timezone,
        )
        if (
            cached is not None
            and cached.policy_revision == policy.revision
            and cached.runtime_revision == runtime_revision
            and cached.model_override == settings.model_override
            and cached.thinking_effort == settings.thinking_effort
            and cached.elevated_mode == settings.elevated_mode
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
        create_options: dict[str, Any] = {"state": state}
        if settings.model_override is not None:
            create_options["model_override"] = settings.model_override
        if settings.thinking_effort is not None:
            create_options["thinking_effort"] = settings.thinking_effort
        agent = await self._factory.create(
            room_id,
            _effective_policy(policy, settings.elevated_mode),
            **create_options,
        )
        session = RoomSession(
            agent=agent,
            lock=lock,
            policy_revision=policy.revision,
            runtime_revision=runtime_revision,
            model_override=settings.model_override,
            thinking_effort=settings.thinking_effort,
            elevated_mode=settings.elevated_mode,
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
            content=current_message_text(event),
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
        transient_context: str = "",
    ) -> AsyncIterator[AgentEvent]:
        """Run one native AgentScope input under the room session lock."""
        lock = await self._lock_for(event.room_id)
        async with lock:
            session = await self._get_or_create_locked(
                event.room_id,
                policy,
                lock,
            )
            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("AgentScope turn has no asyncio task")
            self._active_tasks[event.room_id] = task
            state_before = session.agent.state.model_copy(deep=True)
            cancelled = False
            canonical_input = inputs if isinstance(inputs, Msg) else None
            projected_input = canonical_input
            if (
                canonical_input is not None
                and transient_context.strip()
                and not session.agent.state.context
            ):
                projected_input = canonical_input.model_copy(
                    update={
                        "content": [
                            TextBlock(
                                text=(
                                    "[Transient room context]\n"
                                    f"{transient_context.strip()}\n\n"
                                ),
                            ),
                            *canonical_input.content,
                        ],
                    },
                )
            try:
                with bind_matrix_turn(
                    event.room_id,
                    tool_event_id or event.event_id,
                ):
                    async for item in session.agent.reply_stream(
                        inputs=(
                            projected_input
                            if canonical_input is not None
                            else inputs
                        ),
                    ):
                        if on_event is not None:
                            on_event(item, session.agent.state)
                        yield item
            except asyncio.CancelledError:
                cancelled = True
                session.agent.state = state_before
                raise
            finally:
                if self._active_tasks.get(event.room_id) is task:
                    self._active_tasks.pop(event.room_id, None)
                if canonical_input is not None and not cancelled:
                    session.agent.state.context = [
                        (
                            canonical_input
                            if message.id == canonical_input.id
                            else message
                        )
                        for message in session.agent.state.context
                    ]
                if not cancelled:
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

    async def reset(self, room_id: str) -> None:
        """Drop one parked continuation and its persisted room state."""
        await self._drop_state(room_id)

    async def settings(self, room_id: str) -> SessionSettings:
        return await self._sessions.settings(
            room_id,
            now=self._now(),
            timezone=self._session_timezone,
        )

    async def queue_settings(self, room_id: str) -> tuple[str, int]:
        settings = await self.settings(room_id)
        return settings.queue_mode, settings.queue_limit

    async def update_settings(
        self,
        room_id: str,
        **changes: Any,
    ) -> SessionSettings:
        return await self._sessions.update(
            room_id,
            now=self._now(),
            **changes,
        )

    async def switch_model(
        self,
        room_id: str,
        policy: RoomPolicy,
        model_override: str | None,
    ) -> RoomSessionStatus:
        """Change only the room model and rebuild while preserving state."""
        await self.update_settings(
            room_id,
            model_override=model_override,
        )
        await self.get_or_create(room_id, policy)
        return await self.status(room_id)

    async def cancel(self, room_id: str) -> bool:
        """Cancel the in-flight turn for a room and wait for rollback."""
        task = self._active_tasks.get(room_id)
        if task is None or task.done():
            return False
        if task is asyncio.current_task():
            task.cancel()
            return True
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return True

    async def new(
        self,
        room_id: str,
        *,
        model_override: str | None = None,
        now: datetime | None = None,
    ) -> RoomSessionStatus:
        """Start a clean room session and optionally pin its model."""
        timestamp = (now or self._now()).astimezone(UTC)
        await self._drop_state(room_id)
        await self._sessions.configure(
            room_id,
            model_override=model_override,
            timezone=self._session_timezone,
            now=timestamp,
        )
        return await self.status(room_id, now=timestamp)

    async def compact(
        self,
        room_id: str,
        *,
        keep_messages: int = 8,
        summary_limit: int = 8_000,
    ) -> RoomSessionStatus:
        """Fold older AgentScope context into a bounded durable summary."""
        if keep_messages < 0:
            raise ValueError("kept context messages cannot be negative")
        if summary_limit <= 0:
            raise ValueError("summary limit must be positive")
        lock = await self._lock_for(room_id)
        async with lock:
            session = self._cache.get(room_id)
            stored = None
            if session is None:
                stored = await self._sessions.load(room_id)
                if stored is None:
                    return await self.status(room_id)
                state = stored.state
            else:
                state = session.agent.state
            older = (
                state.context[:-keep_messages]
                if keep_messages
                else state.context
            )
            retained = (
                state.context[-keep_messages:]
                if keep_messages
                else []
            )
            if older:
                fragments = [
                    _context_line(message)
                    for message in older
                    if message.get_text_content()
                ]
                existing = (
                    state.summary
                    if isinstance(state.summary, str)
                    else "\n".join(
                        block.text
                        for block in state.summary
                        if hasattr(block, "text")
                    )
                )
                state.summary = "\n".join(
                    part
                    for part in (existing, *fragments)
                    if part
                )[-summary_limit:]
                state.context = list(retained)
            if session is not None:
                await self._save(room_id, session)
            elif stored is not None:
                await self._sessions.save(
                    room_id=room_id,
                    state=state,
                    policy_revision=stored.policy_revision,
                    last_event_id=stored.last_event_id,
                )
        return await self.status(room_id)

    async def status(
        self,
        room_id: str,
        *,
        now: datetime | None = None,
    ) -> RoomSessionStatus:
        timestamp = (now or self._now()).astimezone(UTC)
        settings = await self._sessions.settings(
            room_id,
            now=timestamp,
            timezone=self._session_timezone,
        )
        cached = self._cache.get(room_id)
        stored = (
            None
            if cached is not None
            else await self._sessions.load(room_id)
        )
        state = (
            cached.agent.state
            if cached is not None
            else stored.state if stored is not None else None
        )
        summary = state.summary if state is not None else ""
        return RoomSessionStatus(
            room_id=room_id,
            session_id=state.session_id if state is not None else None,
            model_override=settings.model_override,
            thinking_effort=settings.thinking_effort,
            reasoning_visibility=settings.reasoning_visibility,
            verbose_mode=settings.verbose_mode,
            elevated_mode=settings.elevated_mode,
            queue_mode=settings.queue_mode,
            queue_limit=settings.queue_limit,
            context_messages=len(state.context) if state is not None else 0,
            summary_characters=(
                len(summary)
                if isinstance(summary, str)
                else sum(
                    len(getattr(block, "text", ""))
                    for block in summary
                )
            ),
            summary=(
                summary
                if isinstance(summary, str)
                else "\n".join(
                    getattr(block, "text", "")
                    for block in summary
                )
            ),
            next_reset_at=settings.next_reset_at,
        )

    async def reset_due(
        self,
        now: datetime | None = None,
        *,
        exclude_rooms: frozenset[str] = frozenset(),
    ) -> tuple[str, ...]:
        """Reset every room whose persisted local 04:00 boundary passed."""
        timestamp = (now or self._now()).astimezone(UTC)
        due = await self._sessions.due_for_reset(timestamp)
        reset_rooms: list[str] = []
        for settings in due:
            if settings.room_id in exclude_rooms:
                continue
            await self._drop_state(settings.room_id)
            await self._sessions.advance_reset(
                settings.room_id,
                now=timestamp,
            )
            reset_rooms.append(settings.room_id)
        return tuple(reset_rooms)

    async def _drop_state(self, room_id: str) -> None:
        lock = await self._lock_for(room_id)
        async with lock:
            session = self._cache.pop(room_id, None)
            if session is not None:
                await self._retire(session.agent)
            await self._sessions.delete(room_id)

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
                await self._retire(session.agent)
        self._cache.clear()

    async def _retire(self, agent: Any) -> None:
        retire = getattr(self._factory, "retire", None)
        if retire is None:
            return
        result = retire(agent)
        if inspect.isawaitable(result):
            await result


def _context_line(message: Any) -> str:
    text = message.get_text_content() or ""
    return f"{message.role}/{message.name}: {text}"[:2_000]


def _effective_policy(
    policy: RoomPolicy,
    elevated_mode: str,
) -> RoomPolicy:
    if elevated_mode == "ask":
        return policy.model_copy(
            update={
                "confirm_tools": policy.allowed_tools,
                "confirmation_mode": "ask",
            },
        )
    if elevated_mode == "full":
        return policy.model_copy(
            update={
                "confirm_tools": frozenset(),
                "confirmation_mode": "full",
            },
        )
    return policy.model_copy(
        update={"confirmation_mode": "off"},
    )
