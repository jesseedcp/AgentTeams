from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.router import EventRouter


def _event(
    room_id: str,
    event_id: str,
    body: str = "work",
) -> InboundEvent:
    return InboundEvent(
        room_id=room_id,
        event_id=event_id,
        sender="@admin:local",
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=True,
    )


@pytest.mark.asyncio
async def test_same_room_is_serial_but_rooms_are_parallel() -> None:
    started: list[tuple[str, str]] = []
    releases: dict[tuple[str, str], asyncio.Event] = {}

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id, event_id
            return True

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=1,
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del policy
        key = (event.room_id, event.event_id)
        started.append(key)
        release = releases.setdefault(key, asyncio.Event())
        await release.wait()

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
        idle_timeout_seconds=60,
    )
    await router.start()

    await router.submit(_event("!a:local", "$1"))
    await router.submit(_event("!a:local", "$2"))
    await router.submit(_event("!b:local", "$3"))

    for _ in range(5):
        if ("!b:local", "$3") in started:
            break
        await asyncio.sleep(0)
    assert started == [("!a:local", "$1"), ("!b:local", "$3")]
    releases[("!a:local", "$1")].set()
    for _ in range(5):
        if ("!a:local", "$2") in started:
            break
        await asyncio.sleep(0)
    assert ("!a:local", "$2") in started

    for release in releases.values():
        release.set()
    await router.stop()


@pytest.mark.asyncio
async def test_duplicate_is_rejected_before_model_invocation() -> None:
    handled: list[str] = []
    resolved: list[str] = []

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id, event_id
            return False

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            resolved.append(event.event_id)
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=1,
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del policy
        handled.append(event.event_id)

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
    )
    await router.start()

    accepted = await router.submit(_event("!a:local", "$same"))
    await router.stop()

    assert not accepted
    assert resolved == ["$same"]
    assert handled == []


@pytest.mark.asyncio
async def test_silent_event_is_filtered_before_durable_claim() -> None:
    claimed: list[str] = []

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id
            claimed.append(event_id)
            return True

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.PROJECT_ROOM,
                revision=1,
                silent=True,
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        raise AssertionError(f"must not handle {event.event_id}: {policy}")

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
    )
    await router.start()

    accepted = await router.submit(_event("!project:local", "$silent"))
    await router.stop()

    assert not accepted
    assert claimed == []


@pytest.mark.asyncio
async def test_stop_uses_control_path_before_room_queue_drains() -> None:
    work_started = asyncio.Event()
    release_work = asyncio.Event()
    control_seen = asyncio.Event()
    handled: list[str] = []

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id, event_id
            return True

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=1,
                allowed_senders=frozenset({event.sender_id}),
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del policy
        handled.append(event.event_id)
        work_started.set()
        await release_work.wait()

    async def control_handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del event, policy
        control_seen.set()

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
        control_handler=control_handler,
    )
    await router.start()

    await router.submit(_event("!a:local", "$work"))
    await work_started.wait()
    await router.submit(_event("!a:local", "$stop", "/stop"))
    await asyncio.wait_for(control_seen.wait(), timeout=0.25)

    assert handled == ["$work"]
    release_work.set()
    await router.stop()


@pytest.mark.asyncio
async def test_queue_limit_rejects_before_claiming_excess_event() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    claimed: list[str] = []

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id
            claimed.append(event_id)
            return True

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=1,
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del event, policy
        started.set()
        await release.wait()

    async def queue_settings(room_id: str) -> tuple[str, int]:
        del room_id
        return "followup", 1

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
        queue_settings=queue_settings,
    )
    await router.start()

    assert await router.submit(_event("!a:local", "$active"))
    await started.wait()
    assert await router.submit(_event("!a:local", "$queued"))
    assert not await router.submit(_event("!a:local", "$excess"))

    assert claimed == ["$active", "$queued"]
    release.set()
    await router.stop()


@pytest.mark.asyncio
async def test_collect_mode_combines_waiting_messages_in_order() -> None:
    active_started = asyncio.Event()
    release_active = asyncio.Event()
    handled: list[tuple[str, str]] = []

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id, event_id
            return True

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=1,
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del policy
        handled.append((event.event_id, event.body))
        if event.event_id == "$active":
            active_started.set()
            await release_active.wait()

    async def queue_settings(room_id: str) -> tuple[str, int]:
        del room_id
        return "collect", 10

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
        queue_settings=queue_settings,
    )
    await router.start()

    await router.submit(_event("!a:local", "$active", "active"))
    await active_started.wait()
    await router.submit(_event("!a:local", "$two", "second"))
    await router.submit(_event("!a:local", "$three", "third"))
    release_active.set()
    for _ in range(10):
        if len(handled) == 2:
            break
        await asyncio.sleep(0)

    assert handled == [
        ("$active", "active"),
        ("$three", "second\n\nthird"),
    ]
    await router.stop()


@pytest.mark.asyncio
async def test_interrupt_mode_cancels_active_before_admitting_message() -> None:
    active_started = asyncio.Event()
    release_active = asyncio.Event()
    interrupted = asyncio.Event()
    handled: list[str] = []

    class Claims:
        async def claim_matrix_event(
            self,
            room_id: str,
            event_id: str,
        ) -> bool:
            del room_id, event_id
            return True

    class Resolver:
        async def resolve(self, event: InboundEvent) -> RoomPolicy:
            return RoomPolicy(
                room_id=event.room_id,
                kind=RoomKind.ADMIN_DM,
                revision=1,
            )

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del policy
        handled.append(event.event_id)
        if event.event_id == "$active":
            active_started.set()
            await release_active.wait()

    async def queue_settings(room_id: str) -> tuple[str, int]:
        del room_id
        return "interrupt", 10

    async def interrupt(room_id: str) -> bool:
        del room_id
        interrupted.set()
        release_active.set()
        return True

    router = EventRouter(
        claims=Claims(),
        resolver=Resolver(),
        handler=handler,
        queue_settings=queue_settings,
        interrupt_handler=interrupt,
    )
    await router.start()

    await router.submit(_event("!a:local", "$active"))
    await active_started.wait()
    await router.submit(_event("!a:local", "$replacement"))
    assert interrupted.is_set()
    for _ in range(10):
        if "$replacement" in handled:
            break
        await asyncio.sleep(0)

    assert handled == ["$active", "$replacement"]
    await router.stop()
