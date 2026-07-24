from __future__ import annotations

from pathlib import Path

import pytest

from agentteams_manager.clients.minio import MinioClient
from agentteams_manager.config import (
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.runtime.config_watcher import (
    ConfigWatcher,
    RuntimeRegistry,
)
from tests.fixtures.fake_s3 import FakeS3


def _runtime(revision: int, model: str) -> RuntimeDocument:
    return RuntimeDocument(
        revision=revision,
        manager_name="manager",
        model=model,
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


@pytest.mark.asyncio
async def test_new_revision_is_downloaded_validated_and_activated(
    tmp_path: Path,
) -> None:
    storage = MinioClient(FakeS3(), bucket="agentteams")
    initial = _runtime(1, "old")
    registry = RuntimeRegistry(initial)
    watcher = ConfigWatcher(
        storage=storage,
        key="manager/agentscope-manager.json",
        cache_path=tmp_path / "agentscope-manager.json",
        registry=registry,
    )
    remote = _runtime(2, "new")
    await storage.put_json_if_version(
        "manager/agentscope-manager.json",
        remote.model_dump(mode="json"),
        expected_etag=None,
    )

    change = await watcher.poll_once()

    assert change is not None
    assert change.revision == 2
    assert registry.current.document.model == "new"
    assert RuntimeDocument.load(
        tmp_path / "agentscope-manager.json",
    ) == remote
    assert registry.degraded is False


@pytest.mark.asyncio
async def test_same_etag_is_not_downloaded_twice(tmp_path: Path) -> None:
    class CountingStorage:
        def __init__(self, storage) -> None:
            self.storage = storage
            self.gets = 0

        async def head(self, key):
            return await self.storage.head(key)

        async def get_bytes(self, key):
            self.gets += 1
            return await self.storage.get_bytes(key)

    base = MinioClient(FakeS3(), bucket="agentteams")
    remote = _runtime(2, "new")
    await base.put_json_if_version(
        "manager/agentscope-manager.json",
        remote.model_dump(mode="json"),
        expected_etag=None,
    )
    storage = CountingStorage(base)
    watcher = ConfigWatcher(
        storage=storage,
        key="manager/agentscope-manager.json",
        cache_path=tmp_path / "runtime.json",
        registry=RuntimeRegistry(_runtime(1, "old")),
    )

    assert await watcher.poll_once() is not None
    assert await watcher.poll_once() is None
    assert storage.gets == 1


@pytest.mark.asyncio
async def test_prepare_failure_keeps_previous_generation(
    tmp_path: Path,
) -> None:
    storage = MinioClient(FakeS3(), bucket="agentteams")
    registry = RuntimeRegistry(_runtime(1, "old"))
    await storage.put_json_if_version(
        "manager/agentscope-manager.json",
        _runtime(2, "broken").model_dump(mode="json"),
        expected_etag=None,
    )

    async def reject(document: RuntimeDocument) -> None:
        del document
        raise RuntimeError("secret diagnostic must be redacted")

    watcher = ConfigWatcher(
        storage=storage,
        key="manager/agentscope-manager.json",
        cache_path=tmp_path / "runtime.json",
        registry=registry,
        prepare=reject,
    )

    assert await watcher.poll_once() is None
    assert registry.revision == 1
    assert registry.current.document.model == "old"
    assert registry.degraded is True
    assert registry.last_error == "RuntimeError"
    assert not (tmp_path / "runtime.json").exists()


@pytest.mark.asyncio
async def test_watcher_lifecycle_requires_initial_remote_generation(
    tmp_path: Path,
) -> None:
    storage = MinioClient(FakeS3(), bucket="agentteams")
    registry = RuntimeRegistry(_runtime(0, "fallback"))
    watcher = ConfigWatcher(
        storage=storage,
        key="manager/agentscope-manager.json",
        cache_path=tmp_path / "agentscope-manager.json",
        registry=registry,
        poll_interval_seconds=0.01,
        initial_timeout_seconds=0.05,
    )

    with pytest.raises(RuntimeError, match="initial runtime document"):
        await watcher.start()

    assert not watcher.ready
    await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_lifecycle_polls_and_stops(
    tmp_path: Path,
) -> None:
    storage = MinioClient(FakeS3(), bucket="agentteams")
    remote = _runtime(1, "qwen-next")
    await storage.put_json_if_version(
        "manager/agentscope-manager.json",
        remote.model_dump(mode="json"),
        expected_etag=None,
    )
    registry = RuntimeRegistry(_runtime(0, "fallback"))
    watcher = ConfigWatcher(
        storage=storage,
        key="manager/agentscope-manager.json",
        cache_path=tmp_path / "agentscope-manager.json",
        registry=registry,
        poll_interval_seconds=0.01,
        initial_timeout_seconds=0.1,
    )

    await watcher.start()

    assert watcher.ready
    assert registry.revision == 1
    await watcher.stop()
    assert not watcher.ready
