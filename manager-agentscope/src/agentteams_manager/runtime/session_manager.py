"""One serialized AgentScope session per Matrix room.

为每个 Matrix room 保存一个串行化的 AgentScope 会话。

同一房间的上下文、模型覆盖和 elevated 设置互相关联，不能并发改写；因此每个 room
有独立 ``asyncio.Lock``，不同 room 仍可并行。每个 turn 结束后把 ``AgentState`` 写入
SQLite；取消时回滚到 turn 前快照，避免保存半段工具循环。runtime 或 policy revision
变化时重建 Agent，但复用经过复制的会话状态。
"""

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
    """协调房间锁、Agent cache、持久化 state 和 turn 边界热更新。

    ``get_or_create`` 比较 policy/runtime revision 与会话设置；只有边界变化才重建 Agent。
    ``run_input`` 在持锁期间流式执行，取消则恢复深拷贝 state，正常结束才保存。这是同一
    room 不出现两条回复互相覆盖上下文的核心保证。
    """
    def __init__(
        self,
        *,
        factory: AgentFactoryPort,
        sessions: SessionRepository,
        session_timezone: str = "UTC",
        now: Callable[[], datetime] | None = None,
    ) -> None:
        # 逻辑说明：保存 Agent factory、会话仓库、时区和时钟，并初始化房间 session/lock cache、cache guard 与活动 turn 任务表；构造阶段不加载持久化 state 或创建 Agent。
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
        # 逻辑说明：取得 room_id 对应的唯一锁并在锁内委托 _get_or_create_locked，使同房间并发调用串行地复用或重建同一 RoomSession；创建失败原样传播且锁自动释放。
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
        # 逻辑说明：比较缓存 session 与当前 policy/runtime revision 及持久化设置；完全一致则复用，否则从缓存或仓库深拷贝 state 创建新 Agent，成功写入 cache 后才 retire 旧 Agent，创建失败保留旧缓存。
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
        # 逻辑说明：把 InboundEvent 的已验证发送者、房间、线程、mentions 与 canonical 当前正文封装成 UserMsg，再把 run_input 产生的 AgentEvent 原序转发；构造或下游异常直接传播。
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
        """在 room lock 内运行一次 AgentScope 输入并原子保存结果。

        ``transient_context`` 只在冷会话首轮临时附加，结束后会把 canonical 用户消息恢复
        到 state，避免历史/记忆副本被永久重复保存。发生取消时恢复 ``state_before``，
        因而 ``/stop`` 不会留下半个 tool call 的上下文。
        """
        # 逻辑说明：在 room lock 内取得 Agent、登记当前 task 并保存 turn 前深拷贝；冷会话可临时前置房间上下文后流式 yield reply，取消时回滚 state，非取消退出时恢复 canonical 输入、记录 event_id 并持久化，回调或流异常也按 finally 保存已形成状态。
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
        # 逻辑说明：快速返回已存在的 room lock；首次访问时用全局 cache_guard 和 setdefault 原子创建唯一 asyncio.Lock，保证并发初始化同一 room_id 不会得到两把不同的串行锁。
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
        # 逻辑说明：若 room_id 已缓存则在该 session 锁内保存当前 Agent state、policy revision 与 last_event_id；冷房间为空操作，仓库写入失败向上传播且不移除缓存。
        session = self._cache.get(room_id)
        if session is None:
            return
        async with session.lock:
            await self._save(room_id, session)

    async def reset(self, room_id: str) -> None:
        """Drop one parked continuation and its persisted room state."""
        # 逻辑说明：通过 _drop_state 删除指定房间的缓存 Agent 和持久化会话，并在存在旧 Agent 时释放其 generation；清理或仓库删除失败由调用方处理。
        await self._drop_state(room_id)

    async def settings(self, room_id: str) -> SessionSettings:
        # 逻辑说明：使用注入时钟和 session_timezone 向仓库读取 room_id 的完整 SessionSettings；时间计算或仓库异常直接传播，本方法不修改 cache。
        return await self._sessions.settings(
            room_id,
            now=self._now(),
            timezone=self._session_timezone,
        )

    async def queue_settings(self, room_id: str) -> tuple[str, int]:
        # 逻辑说明：复用 settings(room_id) 读取持久化配置，并只投影 queue_mode 与 queue_limit 组成元组返回；读取失败不提供猜测默认值。
        settings = await self.settings(room_id)
        return settings.queue_mode, settings.queue_limit

    async def update_settings(
        self,
        room_id: str,
        **changes: Any,
    ) -> SessionSettings:
        # 逻辑说明：把 room_id、当前 UTC 时刻和显式 changes 交给 SessionRepository.update，并返回仓库校验后的 SessionSettings；本方法不主动重建缓存 Agent，非法变更由仓库拒绝。
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
        # 逻辑说明：先持久化 room_id 的 model_override，再调用 get_or_create 依设置重建 Agent 且保留会话 state，最后返回最新 status；设置成功后若重建失败，持久化覆盖仍保留供下次重试。
        await self.update_settings(
            room_id,
            model_override=model_override,
        )
        await self.get_or_create(room_id, policy)
        return await self.status(room_id)

    async def cancel(self, room_id: str) -> bool:
        """Cancel the in-flight turn for a room and wait for rollback."""
        # 逻辑说明：查找 room_id 正在执行且未结束的 asyncio task，缺失时返回 False；存在时发出 cancel，若非当前 task 则等待其 run_input 回滚完成并吞掉预期 CancelledError，最终返回 True。
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
        # 逻辑说明：按给定或当前时间先删除房间旧 state/Agent，再用可选 model_override 和时区建立全新持久化设置，最后读取空会话 status；删除后配置失败不会恢复旧会话。
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
        # 逻辑说明：校验保留条数与摘要上限后在 room lock 内读取缓存或持久化 state，将较老消息格式化并追加到限长 summary、仅保留尾部 context，再按来源保存；无会话或无旧消息时保持原状态并返回 status。
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
        # 逻辑说明：用指定时刻读取房间设置，并优先从 cache、否则从仓库取得 AgentState，投影 session/model/reasoning/queue、context 数量、字符串或块摘要及下次重置时间；只读过程中不创建 Agent。
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
        # 逻辑说明：查询给定 UTC 时刻已越过本地 04:00 边界的设置，跳过 exclude_rooms，其余逐房间删除 state 并推进下一重置点；返回实际完成的 room_id，途中失败会停止且不把未完成房间列入结果。
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
        # 逻辑说明：取得 room lock 后从 cache 移除 session，若存在则先 retire 其 Agent generation，再删除仓库中的持久化 state；释放或删除失败直接传播，防止调用方误认为重置完整成功。
        lock = await self._lock_for(room_id)
        async with lock:
            session = self._cache.pop(room_id, None)
            if session is not None:
                await self._retire(session.agent)
            await self._sessions.delete(room_id)

    async def _save(self, room_id: str, session: RoomSession) -> None:
        # 逻辑说明：将 RoomSession 的当前 AgentState、policy_revision 和 last_event_id 作为一组写入 SessionRepository.save；仓库失败直接传播，内存 session 不被本方法改写。
        await self._sessions.save(
            room_id=room_id,
            state=session.agent.state,
            policy_revision=session.policy_revision,
            last_event_id=session.last_event_id,
        )

    async def save_all(self) -> None:
        # 逻辑说明：遍历当前 cache 快照并逐个获取对应 session lock，将每个房间的 AgentState、policy revision 和 last_event_id 写入仓库；任一保存失败立即传播，已保存房间保持提交且 cache 不变。
        for room_id, session in tuple(self._cache.items()):
            async with session.lock:
                await self._save(room_id, session)

    async def close_all(self) -> None:
        """Persist and retire every cached Agent generation."""
        # 逻辑说明：遍历 cache 快照并逐房间持锁，依次保存 state 后 retire Agent，全部成功才清空 cache；任一保存或释放失败即传播并保留 cache 供后续重试。
        for room_id, session in tuple(self._cache.items()):
            async with session.lock:
                await self._save(room_id, session)
                await self._retire(session.agent)
        self._cache.clear()

    async def _retire(self, agent: Any) -> None:
        # 逻辑说明：动态查找 factory 的可选 retire 方法，缺失时为空操作；存在时调用并兼容同步或 awaitable 返回值，释放异常不吞掉以避免隐藏 generation 泄漏。
        retire = getattr(self._factory, "retire", None)
        if retire is None:
            return
        result = retire(agent)
        if inspect.isawaitable(result):
            await result


def _context_line(message: Any) -> str:
    # 逻辑说明：读取消息文本并格式化为 role/name: text 的单行摘要，最后硬截断至 2000 字符供 compact 持久化；空文本保留身份前缀，消息访问错误直接传播。
    text = message.get_text_content() or ""
    return f"{message.role}/{message.name}: {text}"[:2_000]


def _effective_policy(
    policy: RoomPolicy,
    elevated_mode: str,
) -> RoomPolicy:
    # 逻辑说明：依据 elevated_mode 仅调整传入 RoomPolicy 的确认集合与 confirmation_mode：ask 要求所有已允许工具确认，full 清空确认，其他值设为 off；始终 model_copy，不扩大 allowed_tools 或修改原 policy。
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
