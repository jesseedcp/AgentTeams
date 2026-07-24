from __future__ import annotations

import inspect
from datetime import UTC
from pathlib import Path

import pytest
from pydantic import SecretStr

from agentteams_manager.application import ManagerApplication
from agentteams_manager.bootstrap import (
    MinioJournalStore,
    SystemClock,
    build_application,
    create_application,
)
from agentteams_manager.config import ManagerConfig


class FakeMinio:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
    ):
        del content_type
        self.objects[key] = data

        class Receipt:
            etag = "etag-created"

        return Receipt()

    async def put_bytes_if_version(
        self,
        key: str,
        data: bytes,
        *,
        expected_etag: str | None,
        content_type: str,
    ):
        del content_type
        if expected_etag is None and key in self.objects:
            raise RuntimeError("version conflict")
        self.objects[key] = data

        class Receipt:
            etag = "etag-versioned"

        return Receipt()

    async def get_bytes(self, key: str) -> bytes:
        if key not in self.objects:
            raise KeyError(key)
        return self.objects[key]

    async def list_prefix(self, prefix: str):
        class Receipt:
            def __init__(self, key: str) -> None:
                self.key = key

        return tuple(
            Receipt(key)
            for key in sorted(self.objects)
            if key.startswith(prefix)
        )


def test_bootstrap_factory_is_async() -> None:
    assert inspect.iscoroutinefunction(create_application)


def test_system_clock_returns_utc() -> None:
    assert SystemClock().now().tzinfo is UTC


@pytest.mark.asyncio
async def test_journal_store_uses_bucket_relative_object_keys() -> None:
    minio = FakeMinio()
    store = MinioJournalStore(minio)

    etag = await store.put(
        "manager/journal/op/0001.json",
        b"event",
        content_type="application/json",
        if_none_match=True,
    )

    assert etag == "etag-versioned"
    assert tuple(minio.objects) == (
        "manager/journal/op/0001.json",
    )
    assert await store.get("manager/journal/op/0001.json") == b"event"
    assert await store.list("manager/journal/") == (
        "manager/journal/op/0001.json",
    )


@pytest.mark.asyncio
async def test_production_composition_root_constructs_every_service(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[3]
    monkeypatch.setenv(
        "AGENTTEAMS_MANAGER_ASSET_ROOT",
        str(repository / "manager" / "agent"),
    )
    monkeypatch.setenv(
        "AGENTTEAMS_KNOWN_MODELS_PATH",
        str(repository / "manager" / "configs" / "known-models.json"),
    )
    config = ManagerConfig(
        manager_name="manager",
        manager_user_id="@manager:example.test",
        matrix_url="http://matrix.test",
        matrix_domain="example.test",
        matrix_access_token=SecretStr("matrix-token"),
        controller_url="http://controller.test",
        controller_auth_token=None,
        ai_gateway_url="http://gateway.test",
        gateway_key=SecretStr("gateway-key"),
        fs_endpoint="http://minio.test",
        fs_bucket="agentteams-storage",
        fs_access_key="access",
        fs_secret_key=SecretStr("storage-secret"),
        storage_prefix="agentteams/agentteams-storage",
        default_model="qwen3.6-plus",
        workspace=tmp_path,
        runtime_document_path=tmp_path / "agentscope-manager.json",
        runtime_document_key="manager/agentscope-manager.json",
        session_database=tmp_path / "state" / "manager.db",
        health_port=0,
        admin_user_id="@admin:example.test",
        manager_admin_room_id="!admin:example.test",
    )

    application = build_application(config, storage=FakeMinio())

    assert isinstance(application, ManagerApplication)
    assert application.readiness.ready is False
    await application.stop()
