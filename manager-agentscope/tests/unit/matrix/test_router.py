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


def _event(room_id: str, event_id: str) -> InboundEvent:
    return InboundEvent(
        room_id=room_id,
        event_id=event_id,
        sender="@admin:local",
        body="work",
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

    assert started == [("!a:local", "$1"), ("!b:local", "$3")]
    releases[("!a:local", "$1")].set()
    await asyncio.sleep(0)
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
