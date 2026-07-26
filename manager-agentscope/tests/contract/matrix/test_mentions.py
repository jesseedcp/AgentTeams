from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from agentteams_manager.matrix.client import MatrixClient, MatrixClientConfig


class State:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get_value(self, key: str) -> str | None:
        return self.values.get(key)

    async def set_value(self, key: str, value: str) -> None:
        self.values[key] = value


class Nio:
    def __init__(self, *, timeout_once: bool = False) -> None:
        self.sent: list[dict[str, Any]] = []
        self.timeout_once = timeout_once
        self.typing: list[tuple[str, bool, int]] = []
        self.read: list[tuple[str, str, str]] = []

    async def room_send(self, **kwargs: Any) -> object:
        self.sent.append(kwargs)
        if self.timeout_once:
            self.timeout_once = False
            raise TimeoutError("ambiguous Matrix send")
        return SimpleNamespace(event_id="$sent")

    async def room_typing(self, room_id, typing, *, timeout):
        self.typing.append((room_id, typing, timeout))
        return SimpleNamespace()

    async def room_read_markers(
        self,
        room_id,
        *,
        fully_read_event,
        read_event,
    ):
        self.read.append((room_id, fully_read_event, read_event))
        return SimpleNamespace()


def _client(tmp_path: Path, nio: Nio, state: State | None = None) -> MatrixClient:
    config = MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:local",
        access_token=SecretStr("token"),
        device_name="agentteams-manager",
        crypto_store=tmp_path / "matrix-e2ee",
        media_dir=tmp_path / "media",
    )
    return MatrixClient(config, state or State(), nio_client=nio)


@pytest.mark.asyncio
async def test_structured_mention_is_always_emitted(
    tmp_path: Path,
) -> None:
    nio = Nio()
    client = _client(tmp_path, nio)

    event_id = await client.send_text(
        "!room:local",
        "Review complete.",
        txn_id="txn-mention",
        mentions=("@alice:local",),
    )

    content = nio.sent[0]["content"]
    assert event_id == "$sent"
    assert content["m.mentions"]["user_ids"] == ["@alice:local"]
    assert content["format"] == "org.matrix.custom.html"
    assert content["formatted_body"] == "<p>Review complete.</p>"


@pytest.mark.asyncio
async def test_timeout_retry_reuses_transaction_id(tmp_path: Path) -> None:
    state = State()
    nio = Nio(timeout_once=True)
    client = _client(tmp_path, nio, state)

    await client.send_text(
        "!room:local",
        "Exactly once.",
        txn_id="txn-stable",
    )

    assert [call["tx_id"] for call in nio.sent] == [
        "txn-stable",
        "txn-stable",
    ]
    assert "txn-stable" in state.values["matrix.txn.txn-stable"]


@pytest.mark.asyncio
async def test_typing_and_read_receipts_use_matrix_client(tmp_path: Path) -> None:
    nio = Nio()
    client = _client(tmp_path, nio)

    await client.set_typing("!room:local", typing=True, timeout_ms=5000)
    await client.mark_read("!room:local", "$event")

    assert nio.typing == [("!room:local", True, 5000)]
    assert nio.read == [("!room:local", "$event", "$event")]
