from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from agentteams_manager.domain.models import InboundEvent
from agentteams_manager.matrix.client import MatrixClient, MatrixClientConfig
from agentteams_manager.matrix.threads import RoomHistory, ThreadProjector


def _event(number: int) -> InboundEvent:
    return InboundEvent(
        room_id="!room:local",
        event_id=f"${number}",
        sender="@alice:local",
        body=f"message-{number}",
        timestamp=datetime.now(UTC),
    )


def test_thread_relation_matches_matrix_contract() -> None:
    assert ThreadProjector.relation("$root") == {
        "rel_type": "m.thread",
        "event_id": "$root",
        "is_falling_back": True,
        "m.in_reply_to": {"event_id": "$root"},
    }


def test_history_limit_evicts_oldest_entry() -> None:
    history = RoomHistory(limit=50)
    for number in range(51):
        history.append(_event(number))

    entries = history.entries("!room:local")

    assert len(entries) == 50
    assert entries[0].event_id == "$1"
    assert entries[-1].event_id == "$50"


@pytest.mark.asyncio
async def test_stream_edit_uses_replace_relation(tmp_path: Path) -> None:
    sent: list[dict[str, Any]] = []

    class State:
        async def get_value(self, key: str) -> str | None:
            del key
            return None

        async def set_value(self, key: str, value: str) -> None:
            del key, value

    class Nio:
        async def room_send(self, **kwargs: Any) -> object:
            sent.append(kwargs)
            return SimpleNamespace(event_id="$edit")

    config = MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:local",
        access_token=SecretStr("token"),
        device_name="agentteams-manager",
        crypto_store=tmp_path / "matrix-e2ee",
        media_dir=tmp_path / "media",
    )
    client = MatrixClient(config, State(), nio_client=Nio())

    await client.edit_text(
        "!room:local",
        "$original",
        "final",
        txn_id="txn-edit",
    )

    content = sent[0]["content"]
    assert content["m.new_content"]["body"] == "final"
    assert content["m.relates_to"] == {
        "rel_type": "m.replace",
        "event_id": "$original",
    }
