from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest
from pydantic import SecretStr

from agentteams_manager.matrix.client import MatrixClient, MatrixClientConfig
from agentteams_manager.matrix.crypto import CryptoStore, maintain_e2ee


def test_crypto_store_is_private_and_never_recreated(tmp_path: Path) -> None:
    path = tmp_path / "matrix-e2ee"
    store = CryptoStore(path)

    prepared = store.prepare()
    sentinel = prepared / "keys.db"
    sentinel.write_bytes(b"persisted-megolm-keys")
    store.prepare()

    assert sentinel.read_bytes() == b"persisted-megolm-keys"
    if os.name != "nt":
        assert stat.S_IMODE(prepared.stat().st_mode) == 0o700


@pytest.mark.asyncio
async def test_e2ee_maintenance_uploads_and_flushes_keys() -> None:
    calls: list[str] = []

    class FakeNio:
        olm = object()
        should_upload_keys = True
        should_query_keys = True
        should_claim_keys = True

        async def keys_upload(self) -> None:
            calls.append("upload")

        async def keys_query(self) -> None:
            calls.append("query")

        def get_users_for_key_claiming(self) -> set[str]:
            return {"@alice:local"}

        async def keys_claim(self, users: set[str]) -> None:
            assert users == {"@alice:local"}
            calls.append("claim")

        async def send_to_device_messages(self) -> None:
            calls.append("flush")

    await maintain_e2ee(FakeNio(), enabled=True)

    assert calls == ["upload", "query", "claim", "flush"]


@pytest.mark.asyncio
async def test_start_reuses_crypto_store_and_loads_device_keys(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    crypto_path = tmp_path / "matrix-e2ee"
    crypto_path.mkdir()
    (crypto_path / "keys.db").write_bytes(b"existing")

    class FakeState:
        async def get_value(self, key: str) -> str | None:
            del key
            return None

        async def set_value(self, key: str, value: str) -> None:
            del key, value

    class FakeNio:
        access_token = ""
        user_id = ""
        user = ""
        device_id = None
        store_path = str(crypto_path)
        olm = None

        async def whoami(self) -> object:
            return type(
                "Whoami",
                (),
                {
                    "user_id": "@manager:local",
                    "device_id": "DEVICE",
                },
            )()

        def load_store(self) -> None:
            calls.append("load")
            self.olm = object()

        async def sync(self, **kwargs: object) -> object:
            del kwargs
            raise RuntimeError("stop-test-loop")

        async def send_to_device_messages(self) -> None:
            calls.append("flush")

        async def close(self) -> None:
            calls.append("close")

    config = MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:local",
        access_token=SecretStr("token"),
        device_name="agentteams-manager",
        crypto_store=crypto_path,
        media_dir=tmp_path / "media",
    )
    client = MatrixClient(config, FakeState(), nio_client=FakeNio())

    async def handler(event: object) -> None:
        del event

    await client.start(handler)
    await client.stop()

    assert (crypto_path / "keys.db").read_bytes() == b"existing"
    assert "load" in calls
    assert "close" in calls
