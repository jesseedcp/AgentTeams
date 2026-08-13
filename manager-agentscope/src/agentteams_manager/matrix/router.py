"""Room-serialized, cross-room concurrent Matrix event routing.

对 Matrix 事件去重，并实现“同房间串行、不同房间并行”。

同一 room 的两条消息共享 AgentScope 会话，必须按顺序处理；不同 room 则可以各自运行，
避免一个慢请求卡住全系统。事件先通过 SQLite claim 保证只消费一次，再进入房间队列。
``interrupt`` 可取消当前 turn，``collect`` 可合并积压输入；失败事件进入可恢复状态，
而不是提前标成已处理。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from agentteams_manager.domain.models import InboundEvent, RoomPolicy

logger = logging.getLogger(__name__)


class EventClaims(Protocol):
    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool: ...


class PolicyResolver(Protocol):
    async def resolve(self, event: InboundEvent) -> RoomPolicy: ...


EventHandler = Callable[[InboundEvent, RoomPolicy], Awaitable[None]]
DeadLetterHandler = Callable[[InboundEvent, str], Awaitable[None]]
QueueSettingsProvider = Callable[
    [str],
    Awaitable[tuple[str, int]],
]
InterruptHandler = Callable[[str], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class RoutedEvent:
    event: InboundEvent
    policy: RoomPolicy
    queue_mode: str = "followup"


class EventRouter:
    """对事件做 durable claim，并用每房间队列调度处理。

    一个 room 始终只有一个 worker task 修改其 AgentState；队列字典允许其他 room 拥有
    自己的 worker task 并行运行。只有 handler 成功后才完成 claim，异常会记录 dead
    letter/retry 信息，因此 Matrix 重放不是简单丢弃。
    """

    def __init__(
        self,
        *,
        claims: EventClaims,
        resolver: PolicyResolver,
        handler: EventHandler,
        control_handler: EventHandler | None = None,
        queue_settings: QueueSettingsProvider | None = None,
        interrupt_handler: InterruptHandler | None = None,
        idle_timeout_seconds: float = 300,
        shutdown_grace_seconds: float = 5.0,
        retry_delays: tuple[float, ...] = (1.0, 3.0),
        max_attempts: int = 3,
        dead_letter_handler: DeadLetterHandler | None = None,
    ) -> None:
        # 逻辑说明：校验重试次数、退避和停机宽限后保存 claim/策略/处理器依赖，并初始化每房间队列、任务表和互斥锁；构造时不启动后台任务。
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if any(delay < 0 for delay in retry_delays):
            raise ValueError("retry_delays must not be negative")
        if shutdown_grace_seconds < 0:
            raise ValueError("shutdown_grace_seconds must not be negative")
        self._claims = claims
        self._resolver = resolver
        self._handler = handler
        self._control_handler = control_handler
        self._queue_settings = queue_settings
        self._interrupt_handler = interrupt_handler
        self._idle_timeout = idle_timeout_seconds
        self._shutdown_grace = shutdown_grace_seconds
        self._retry_delays = retry_delays
        self._max_attempts = max_attempts
        self._dead_letter_handler = dead_letter_handler
        self._queues: dict[str, asyncio.Queue[RoutedEvent]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._active_handlers: dict[str, asyncio.Task[None]] = {}
        self._guard = asyncio.Lock()
        self._running = False

    async def start(self) -> None:
        # 逻辑说明：先标记路由器可接收事件，再从 durable claim 存储恢复未完成事件；恢复失败由下层记录，正常新消息随后可进入 submit。
        self._running = True
        await self._recover_durable_events()

    async def stop(self) -> None:
        # 逻辑说明：停止接收新事件，给正在运行的 handler 一段完成时间，再在锁内清空房间引用并取消 drain 任务；gather 吸收取消异常以完成幂等收尾。
        self._running = False
        active = tuple(self._active_handlers.values())
        if active and self._shutdown_grace:
            await asyncio.wait(
                active,
                timeout=self._shutdown_grace,
            )
        async with self._guard:
            tasks = tuple(self._tasks.values())
            self._tasks.clear()
            self._queues.clear()
            self._active_handlers.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def submit(self, event: InboundEvent) -> bool:
        # 逻辑说明：解析事件权限并做 durable claim；/stop 绕过普通队列，其他消息按房间 queue_mode 入队，interrupt 会先取消活跃 turn，返回值表示是否真正接纳。
        if not self._running:
            raise RuntimeError("Matrix event router is not running")
        policy = await self._resolver.resolve(event)
        if policy.silent:
            return False
        if (
            self._control_handler is not None
            and _is_stop_command(event.body)
        ):
            if not await self._claim_event(event):
                return False
            async with self._guard:
                discarded = self._discard_waiting_events_locked(
                    event.room_id,
                )
            await self._cancel_events(
                discarded,
                reason="cancelled by /stop",
            )
            await self._process_routed(
                RoutedEvent(event=event, policy=policy),
                handler=self._control_handler,
            )
            return True

        queue_mode, queue_limit = await self._room_queue_settings(
            event.room_id,
        )
        interrupt_active = False
        async with self._guard:
            queue = self._queues.get(event.room_id)
            if (
                queue is not None
                and queue_limit > 0
                and queue.qsize() >= queue_limit
            ):
                return False
            if not await self._claim_event(event):
                return False
            interrupt_active = (
                queue_mode == "interrupt"
                and event.room_id in self._active_handlers
                and self._interrupt_handler is not None
            )
            if not interrupt_active:
                await self._enqueue_locked(event, policy, queue_mode)
        if interrupt_active:
            assert self._interrupt_handler is not None
            await self._interrupt_handler(event.room_id)
            async with self._guard:
                await self._enqueue_locked(event, policy, queue_mode)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return True

    def _discard_waiting_events_locked(
        self,
        room_id: str,
    ) -> tuple[RoutedEvent, ...]:
        """Drop already-claimed follow-up work when `/stop` is received."""
        # 逻辑说明：调用方已持有 router guard，本函数立即取空指定房间队列并为每项配对 task_done；返回已 claim 但尚未处理的事件供 /stop 持久化取消。

        queue = self._queues.get(room_id)
        if queue is None:
            return ()
        discarded: list[RoutedEvent] = []
        while True:
            try:
                discarded.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                return tuple(discarded)
            queue.task_done()

    async def _room_queue_settings(
        self,
        room_id: str,
    ) -> tuple[str, int]:
        # 逻辑说明：读取房间会话的队列模式与上限，非法模式记录警告后回退 followup，上限钳制到 1..100；未配置 provider 时用无限 followup。
        if self._queue_settings is None:
            return "followup", 0
        mode, limit = await self._queue_settings(room_id)
        if mode not in {"followup", "collect", "interrupt"}:
            logger.warning(
                "Invalid room queue mode; using followup",
                extra={"room_id": room_id, "queue_mode": mode},
            )
            mode = "followup"
        return mode, max(1, min(limit, 100))

    async def _enqueue_locked(
        self,
        event: InboundEvent,
        policy: RoomPolicy,
        queue_mode: str,
    ) -> None:
        # 逻辑说明：在 guard 内为首次出现的 room 创建队列和唯一 drain task，再放入携带权限快照的 RoutedEvent，确保同房间只有一个消费者。
        queue = self._queues.get(event.room_id)
        if queue is None:
            queue = asyncio.Queue()
            self._queues[event.room_id] = queue
            self._tasks[event.room_id] = asyncio.create_task(
                self._drain(event.room_id, queue),
                name=f"matrix-room:{event.room_id}",
            )
        await queue.put(
            RoutedEvent(
                event=event,
                policy=policy,
                queue_mode=queue_mode,
            ),
        )

    async def _drain(
        self,
        room_id: str,
        queue: asyncio.Queue[RoutedEvent],
    ) -> None:
        # 逻辑说明：持续串行消费一个房间的队列；collect 合并连续消息，空闲超时安全移除任务，处理异常仅记录且始终为每个原始队列项调用 task_done。
        deferred: RoutedEvent | None = None
        while self._running:
            if deferred is not None:
                routed = deferred
                deferred = None
            else:
                try:
                    routed = await asyncio.wait_for(
                        queue.get(),
                        timeout=self._idle_timeout,
                    )
                except TimeoutError:
                    async with self._guard:
                        if (
                            queue.empty()
                            and self._queues.get(room_id) is queue
                        ):
                            self._queues.pop(room_id, None)
                            self._tasks.pop(room_id, None)
                            return
                    continue
            batch = [routed]
            if routed.queue_mode == "collect":
                while True:
                    try:
                        queued = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if queued.queue_mode != "collect":
                        deferred = queued
                        break
                    batch.append(queued)
            routed = _collect_events(batch)
            handler_task: asyncio.Task[None] | None = None
            try:
                handler_task = asyncio.create_task(
                    self._process_routed(routed, batch=batch),
                    name=(
                        "matrix-event:"
                        f"{routed.event.room_id}:"
                        f"{routed.event.event_id}"
                    ),
                )
                self._active_handlers[room_id] = handler_task
                await handler_task
            except asyncio.CancelledError:
                if not self._running:
                    raise
            except Exception:
                logger.exception(
                    "Matrix event routing failed outside retry boundary",
                    extra={
                        "room_id": routed.event.room_id,
                        "event_id": routed.event.event_id,
                    },
                )
            finally:
                if (
                    handler_task is not None
                    and self._active_handlers.get(room_id)
                    is handler_task
                ):
                    self._active_handlers.pop(room_id, None)
                for _ in batch:
                    queue.task_done()

    async def _invoke_handler(self, routed: RoutedEvent) -> None:
        # 逻辑说明：把已解析好 policy 的事件交给默认业务 handler；这个窄适配点不做 claim 或完成标记，异常原样交给外层重试边界。
        await self._handler(routed.event, routed.policy)

    async def _process_routed(
        self,
        routed: RoutedEvent,
        *,
        batch: list[RoutedEvent] | None = None,
        handler: EventHandler | None = None,
    ) -> None:
        """Run one event batch with bounded retry and durable completion."""
        # 逻辑说明：对一个事件或 collect 批次执行有限次 handler 重试；每次失败写入 durable 状态并按退避重开 claim，达到上限进入 dead letter，成功才逐项标记完成。

        deliveries = batch or [routed]
        selected_handler = handler or self._handler
        attempt = 1
        while True:
            try:
                await selected_handler(routed.event, routed.policy)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = str(exc).strip() or type(exc).__name__
                dead_letter = attempt >= self._max_attempts
                for delivery in deliveries:
                    dead_letter = (
                        await self._fail_event(
                            delivery.event,
                            error=error,
                            attempt=attempt,
                        )
                        or dead_letter
                    )
                if dead_letter:
                    logger.exception(
                        "Matrix event moved to dead letter",
                        extra={
                            "room_id": routed.event.room_id,
                            "event_id": routed.event.event_id,
                            "attempt": attempt,
                        },
                    )
                    await self._report_dead_letter(routed.event, error)
                    return
                logger.warning(
                    "Matrix event processing failed; retrying",
                    extra={
                        "room_id": routed.event.room_id,
                        "event_id": routed.event.event_id,
                        "attempt": attempt,
                    },
                    exc_info=True,
                )
                delay = self._retry_delay(attempt)
                if delay:
                    await asyncio.sleep(delay)
                for delivery in deliveries:
                    await self._begin_retry(delivery.event)
                attempt += 1
                continue
            for delivery in deliveries:
                await self._complete_event(delivery.event)
            return

    async def _claim_event(self, event: InboundEvent) -> bool:
        # 逻辑说明：优先调用能保存完整 InboundEvent 的新版 durable claim，旧实现则按 room_id/event_id 去重；布尔值决定 submit 是否继续入队。
        durable_claim = getattr(
            self._claims,
            "claim_inbound_event",
            None,
        )
        if callable(durable_claim):
            return bool(await durable_claim(event))
        return await self._claims.claim_matrix_event(
            event.room_id,
            event.event_id,
        )

    async def _complete_event(self, event: InboundEvent) -> None:
        # 逻辑说明：handler 成功后若 claim 存储实现完成接口，就把该 Matrix 事件推进到 completed；旧的仅去重存储无需额外动作。
        complete = getattr(
            self._claims,
            "complete_matrix_event",
            None,
        )
        if callable(complete):
            await complete(event.room_id, event.event_id)

    async def _fail_event(
        self,
        event: InboundEvent,
        *,
        error: str,
        attempt: int,
    ) -> bool:
        # 逻辑说明：记录当前处理错误并询问 durable 存储是否已达最大尝试次数；旧存储无法计数时由传入 attempt 在本地判定 dead letter。
        fail = getattr(self._claims, "fail_matrix_event", None)
        if callable(fail):
            return bool(
                await fail(
                    event.room_id,
                    event.event_id,
                    error=error,
                    max_attempts=self._max_attempts,
                ),
            )
        return attempt >= self._max_attempts

    async def _begin_retry(self, event: InboundEvent) -> None:
        # 逻辑说明：下一次调用 handler 前把支持重试的 durable claim 从 failed 重新置为 processing；不支持该接口的旧实现保持兼容空操作。
        retry = getattr(self._claims, "retry_matrix_event", None)
        if callable(retry):
            await retry(event.room_id, event.event_id)

    async def _cancel_events(
        self,
        events: tuple[RoutedEvent, ...],
        *,
        reason: str,
    ) -> None:
        # 逻辑说明：把 /stop 丢弃的每个已 claim 队列事件持久化为 cancelled，并保存统一原因；存储未实现取消接口时安全跳过。
        cancel = getattr(self._claims, "cancel_matrix_event", None)
        if not callable(cancel):
            return
        for routed in events:
            await cancel(
                routed.event.room_id,
                routed.event.event_id,
                reason=reason,
            )

    async def _recover_durable_events(self) -> None:
        # 逻辑说明：启动时重新解析 durable 存储中的可恢复事件，策略拒绝者标为取消，其余按当前房间模式重新入队；单条恢复失败只记录，不阻断其他房间。
        recover = getattr(
            self._claims,
            "recoverable_matrix_events",
            None,
        )
        if not callable(recover):
            return
        events = await recover()
        for event in events:
            try:
                policy = await self._resolver.resolve(event)
                if policy.silent:
                    cancel = getattr(
                        self._claims,
                        "cancel_matrix_event",
                        None,
                    )
                    if callable(cancel):
                        await cancel(
                            event.room_id,
                            event.event_id,
                            reason=(
                                "room policy rejected event during recovery"
                            ),
                        )
                    continue
                queue_mode, _ = await self._room_queue_settings(
                    event.room_id,
                )
                async with self._guard:
                    await self._enqueue_locked(
                        event,
                        policy,
                        queue_mode,
                    )
            except Exception:
                logger.exception(
                    "Failed to restore durable Matrix event",
                    extra={
                        "room_id": event.room_id,
                        "event_id": event.event_id,
                    },
                )

    def _retry_delay(self, attempt: int) -> float:
        # 逻辑说明：按从 1 开始的尝试次数选择退避秒数，超过配置长度后重复最后一个值；空退避表直接返回零，不做睡眠。
        if not self._retry_delays:
            return 0
        index = min(attempt - 1, len(self._retry_delays) - 1)
        return self._retry_delays[index]

    async def _report_dead_letter(
        self,
        event: InboundEvent,
        error: str,
    ) -> None:
        # 逻辑说明：事件最终进入 dead letter 时调用可选通知器；通知本身失败只记录日志，不能让已经终止重试的事件重新污染房间队列。
        if self._dead_letter_handler is None:
            return
        try:
            await self._dead_letter_handler(event, error)
        except Exception:
            logger.exception(
                "Failed to report Matrix dead letter",
                extra={
                    "room_id": event.room_id,
                    "event_id": event.event_id,
                },
            )


def _is_stop_command(body: str) -> bool:
    # 逻辑说明：去除两端空白并忽略大小写精确识别 /stop，避免含有该词的普通聊天误走队列旁路控制路径。
    return body.strip().lower() == "/stop"


def _collect_events(batch: list[RoutedEvent]) -> RoutedEvent:
    # 逻辑说明：collect 模式把连续事件正文和媒体按序合并、提及去重，并采用最后一条事件的元数据与 policy；单元素批次保持原对象。
    if len(batch) == 1:
        return batch[0]
    last = batch[-1]
    event = last.event.model_copy(
        update={
            "body": "\n\n".join(item.event.body for item in batch),
            "media": tuple(
                media
                for item in batch
                for media in item.event.media
            ),
            "mentions": tuple(
                dict.fromkeys(
                    mention
                    for item in batch
                    for mention in item.event.mentions
                ),
            ),
        },
    )
    return RoutedEvent(
        event=event,
        policy=last.policy,
        queue_mode=last.queue_mode,
    )
