"""Room-serialized, cross-room concurrent Matrix event routing."""

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
    """Claim once, serialize per room, and process rooms concurrently."""

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
    ) -> None:
        self._claims = claims
        self._resolver = resolver
        self._handler = handler
        self._control_handler = control_handler
        self._queue_settings = queue_settings
        self._interrupt_handler = interrupt_handler
        self._idle_timeout = idle_timeout_seconds
        self._queues: dict[str, asyncio.Queue[RoutedEvent]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._active_handlers: dict[str, asyncio.Task[None]] = {}
        self._guard = asyncio.Lock()
        self._running = False

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False
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
        if not self._running:
            raise RuntimeError("Matrix event router is not running")
        policy = await self._resolver.resolve(event)
        if policy.silent:
            return False
        if (
            self._control_handler is not None
            and _is_stop_command(event.body)
        ):
            if not await self._claims.claim_matrix_event(
                event.room_id,
                event.event_id,
            ):
                return False
            await self._control_handler(event, policy)
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
            if not await self._claims.claim_matrix_event(
                event.room_id,
                event.event_id,
            ):
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

    async def _room_queue_settings(
        self,
        room_id: str,
    ) -> tuple[str, int]:
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
                    self._invoke_handler(routed),
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
                    "Matrix event processing failed",
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
        await self._handler(routed.event, routed.policy)


def _is_stop_command(body: str) -> bool:
    return body.strip().lower() == "/stop"


def _collect_events(batch: list[RoutedEvent]) -> RoutedEvent:
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
