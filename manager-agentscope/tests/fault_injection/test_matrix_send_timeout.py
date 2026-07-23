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


@pytest.mark.asyncio
async def test_accepted_send_timeout_retries_one_matrix_event(
    tmp_path: Path,
) -> None:
    events_by_transaction: dict[str, str] = {}
    calls: list[str] = []

    class AmbiguousNio:
        async def room_send(self, **kwargs: Any) -> object:
            transaction = kwargs["tx_id"]
            calls.append(transaction)
            event_id = events_by_transaction.setdefault(
                transaction,
                "$accepted",
            )
            if len(calls) == 1:
                raise TimeoutError("response lost after accept")
            return SimpleNamespace(event_id=event_id)

    config = MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:local",
        access_token=SecretStr("token"),
        device_name="agentteams-manager",
        crypto_store=tmp_path / "matrix-e2ee",
        media_dir=tmp_path / "media",
    )
    client = MatrixClient(config, State(), nio_client=AmbiguousNio())

    event_id = await client.send_text(
        "!room:local",
        "singular effect",
        txn_id="stable-transaction",
    )

    assert event_id == "$accepted"
    assert calls == ["stable-transaction", "stable-transaction"]
    assert events_by_transaction == {
        "stable-transaction": "$accepted",
    }
