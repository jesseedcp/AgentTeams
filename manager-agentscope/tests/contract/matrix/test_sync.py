from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from agentteams_manager.matrix.client import MatrixClient, MatrixClientConfig


class FakeState:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get_value(self, key: str) -> str | None:
        return self.values.get(key)

    async def set_value(self, key: str, value: str) -> None:
        self.values[key] = value

    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool:
        del room_id, event_id
        return True


class FakeNio:
    def __init__(self) -> None:
        self.sync_calls: list[dict[str, Any]] = []
        self.joined_rooms: list[str] = []
        self.next_sync = self.response()
        self.unknown_token_failures = 0
        self.login_calls = 0
        self.olm = object()
        self.should_upload_keys = False
        self.should_query_keys = False
        self.should_claim_keys = False
        self.rooms: dict[str, object] = {}

    @staticmethod
    def response(
        *,
        invite: tuple[str, ...] = (),
        joined: dict[str, object] | None = None,
        next_batch: str = "next-token",
    ) -> object:
        return SimpleNamespace(
            next_batch=next_batch,
            rooms=SimpleNamespace(
                invite={room_id: object() for room_id in invite},
                join=joined or {},
            ),
        )

    async def sync(self, **kwargs: Any) -> object:
        self.sync_calls.append(kwargs)
        if self.unknown_token_failures:
            self.unknown_token_failures -= 1
            raise RuntimeError("M_UNKNOWN_TOKEN")
        return self.next_sync

    async def join(self, room_id: str) -> None:
        self.joined_rooms.append(room_id)

    async def login(
        self,
        password: str,
        *,
        device_name: str,
    ) -> object:
        del password, device_name
        self.login_calls += 1
        return SimpleNamespace(
            access_token=f"refreshed-{self.login_calls}",
            user_id="@manager:local",
            device_id="DEVICE",
        )

    async def send_to_device_messages(self) -> None:
        return None

    def fail_sync_with_unknown_token(self, *, times: int) -> None:
        self.unknown_token_failures = times


def _config(tmp_path: Path, *, password: str | None = None) -> MatrixClientConfig:
    return MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:local",
        access_token=SecretStr("token"),
        password=SecretStr(password) if password else None,
        device_name="agentteams-manager",
        crypto_store=tmp_path / "matrix-e2ee",
        media_dir=tmp_path / "media",
    )


@pytest.mark.asyncio
async def test_sync_resumes_from_persisted_token(tmp_path: Path) -> None:
    state = FakeState()
    state.values["matrix.sync_token"] = "saved-token"
    nio = FakeNio()
    client = MatrixClient(_config(tmp_path), state, nio_client=nio)

    await client.sync_once()

    assert nio.sync_calls[0]["since"] == "saved-token"
    assert nio.sync_calls[0]["full_state"] is True
    assert state.values["matrix.sync_token"] == "next-token"
    assert client.ready.is_set()

    await client.sync_once()

    assert nio.sync_calls[1]["full_state"] is False


@pytest.mark.asyncio
async def test_invites_are_joined_before_timeline_dispatch(
    tmp_path: Path,
) -> None:
    state = FakeState()
    nio = FakeNio()
    event = SimpleNamespace(
        event_id="$one",
        sender="@alice:local",
        body="hello",
        server_timestamp=1_700_000_000_000,
        source={"content": {"body": "hello"}},
    )
    nio.next_sync = nio.response(
        invite=("!worker:local",),
        joined={
            "!worker:local": SimpleNamespace(
                timeline=SimpleNamespace(events=[event]),
            ),
        },
    )
    order: list[str] = []
    original_join = nio.join

    async def join(room_id: str) -> None:
        await original_join(room_id)
        order.append("join")

    async def handler(inbound: object) -> None:
        del inbound
        order.append("dispatch")

    nio.join = join
    client = MatrixClient(_config(tmp_path), state, nio_client=nio)
    client.bind_handler(handler)

    await client.sync_once()

    assert nio.joined_rooms == ["!worker:local"]
    assert order == ["join", "dispatch"]


@pytest.mark.asyncio
async def test_two_member_room_is_normalized_as_direct(
    tmp_path: Path,
) -> None:
    state = FakeState()
    nio = FakeNio()
    nio.rooms["!dm:local"] = SimpleNamespace(
        users={
            "@manager:local": object(),
            "@admin:local": object(),
        },
    )
    event = SimpleNamespace(
        event_id="$dm",
        sender="@admin:local",
        body="status",
        server_timestamp=1_700_000_000_000,
        source={"content": {"body": "status"}},
    )
    nio.next_sync = nio.response(
        joined={
            "!dm:local": SimpleNamespace(
                timeline=SimpleNamespace(events=[event]),
            ),
        },
    )
    received: list[object] = []

    async def handler(inbound: object) -> None:
        received.append(inbound)

    client = MatrixClient(_config(tmp_path), state, nio_client=nio)
    client.bind_handler(handler)

    await client.sync_once()

    assert received[0].is_direct


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    [
        {
            "type": "m.room.redaction",
            "content": {"body": "redacted"},
        },
        {
            "type": "m.room.message",
            "content": {
                "body": "* edited",
                "m.relates_to": {
                    "rel_type": "m.replace",
                    "event_id": "$original",
                },
            },
        },
        {
            "type": "m.room.message",
            "content": {
                "body": "ack",
                "io.agentteams.acknowledgement": True,
            },
        },
    ],
)
async def test_non_actionable_timeline_events_are_not_dispatched(
    tmp_path: Path,
    source: dict[str, object],
) -> None:
    state = FakeState()
    nio = FakeNio()
    event = SimpleNamespace(
        event_id="$ignored",
        sender="@worker:local",
        body=source["content"]["body"],
        server_timestamp=1_700_000_000_000,
        source=source,
    )
    nio.next_sync = nio.response(
        joined={
            "!room:local": SimpleNamespace(
                timeline=SimpleNamespace(events=[event]),
            ),
        },
    )
    received: list[object] = []

    async def handler(inbound: object) -> None:
        received.append(inbound)

    client = MatrixClient(_config(tmp_path), state, nio_client=nio)
    client.bind_handler(handler)

    await client.sync_once()

    assert received == []


@pytest.mark.asyncio
async def test_unknown_token_refresh_is_bounded(tmp_path: Path) -> None:
    state = FakeState()
    nio = FakeNio()
    nio.fail_sync_with_unknown_token(times=4)
    delays: list[float] = []

    async def record_delay(seconds: float) -> None:
        delays.append(seconds)

    client = MatrixClient(
        _config(tmp_path, password="manager-password"),
        state,
        nio_client=nio,
        sleeper=record_delay,
    )

    with pytest.raises(RuntimeError, match="three token refresh attempts"):
        await client.run_sync_loop()

    assert nio.login_calls == 3
    assert delays == [5, 10, 20]
