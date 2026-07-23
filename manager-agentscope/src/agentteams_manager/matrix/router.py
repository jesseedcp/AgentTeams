"""Room-serialized, cross-room concurrent Matrix event routing."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
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


class EventRouter:
    """Claim once, serialize per room, and process rooms concurrently."""

    def __init__(
        self,
        *,
        claims: EventClaims,
        resolver: PolicyResolver,
        handler: EventHandler,
        idle_timeout_seconds: float = 300,
    ) -> None:
        self._claims = claims
        self._resolver = resolver
        self._handler = handler
        self._idle_timeout = idle_timeout_seconds
        self._queues: dict[str, asyncio.Queue[InboundEvent]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
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
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def submit(self, event: InboundEvent) -> bool:
        if not self._running:
            raise RuntimeError("Matrix event router is not running")
        if not await self._claims.claim_matrix_event(
            event.room_id,
            event.event_id,
        ):
            return False

        async with self._guard:
            queue = self._queues.get(event.room_id)
            if queue is None:
                queue = asyncio.Queue()
                self._queues[event.room_id] = queue
                self._tasks[event.room_id] = asyncio.create_task(
                    self._drain(event.room_id, queue),
                    name=f"matrix-room:{event.room_id}",
                )
            await queue.put(event)
        await asyncio.sleep(0)
        return True

    async def _drain(
        self,
        room_id: str,
        queue: asyncio.Queue[InboundEvent],
    ) -> None:
        try:
            while self._running:
                try:
                    event = await asyncio.wait_for(
                        queue.get(),
                        timeout=self._idle_timeout,
                    )
                except TimeoutError:
                    async with self._guard:
                        if queue.empty() and self._queues.get(room_id) is queue:
                            self._queues.pop(room_id, None)
                            self._tasks.pop(room_id, None)
                            return
                    continue
                try:
                    policy = await self._resolver.resolve(event)
                    if not policy.silent:
                        await self._handler(event, policy)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Matrix event processing failed",
                        extra={
                            "room_id": event.room_id,
                            "event_id": event.event_id,
                        },
                    )
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            raise
