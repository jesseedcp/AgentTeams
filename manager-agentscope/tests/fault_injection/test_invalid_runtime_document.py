from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.config import PromptSources, RuntimeDocument
from agentteams_manager.runtime.config_watcher import (
    ConfigWatcher,
    RuntimeRegistry,
)
from tests.fixtures.fake_s3 import FakeS3


def _initial() -> RuntimeDocument:
    return RuntimeDocument(
        revision=3,
        manager_name="manager",
        model="stable",
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


@pytest.mark.asyncio
async def test_invalid_or_rollback_document_never_replaces_cache(
    tmp_path: Path,
) -> None:
    s3 = FakeS3()
    storage = MinioClient(s3, bucket="agentteams")
    key = "manager/agentscope-manager.json"
    cache = tmp_path / "runtime.json"
    cache.write_text(_initial().model_dump_json(), encoding="utf-8")
    registry = RuntimeRegistry(_initial())
    watcher = ConfigWatcher(
        storage=storage,
        key=key,
        cache_path=cache,
        registry=registry,
    )
    first = await storage.put_bytes_if_version(
        key,
        b'{"schema_version":2,"revision":4}',
        expected_etag=None,
        content_type="application/json",
    )

    assert await watcher.poll_once() is None
    assert RuntimeDocument.load(cache).model == "stable"
    assert registry.revision == 3

    await storage.put_json_if_version(
        key,
        _initial().model_copy(
            update={"revision": 2, "model": "rollback"},
        ).model_dump(mode="json"),
        expected_etag=first.etag,
    )
    assert await watcher.poll_once() is None
    assert registry.revision == 3
    assert RuntimeDocument.load(cache).model == "stable"
