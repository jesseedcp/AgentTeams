"""Verified Controller runtime polling and monotonic activation.

轮询 Controller 发布的 runtime document，并原子激活新配置。

远端对象只有 ETag 改变时才下载；字节数、SHA-256、Pydantic 结构和递增 revision 全部
通过后，才预热 MCP 等依赖并用原子替换写入本地缓存。无效新版本只把 registry 标为
degraded，当前可用 generation 保持不变，因此一次坏配置不会破坏正在进行的会话。
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from agentteams_manager.config import RuntimeDocument
from agentteams_manager.domain.models import ObjectReceipt

logger = logging.getLogger(__name__)


class RuntimeObjectStorage(Protocol):
    async def head(self, key: str) -> ObjectReceipt | None: ...

    async def get_bytes(self, key: str) -> bytes: ...


RuntimePrepare = Callable[
    [RuntimeDocument],
    None | Awaitable[None],
]


@dataclass(frozen=True, slots=True)
class RuntimeGeneration:
    revision: int
    digest: str
    document: RuntimeDocument
    activated_at: datetime


@dataclass(frozen=True, slots=True)
class ConfigChange:
    revision: int
    digest: str
    document: RuntimeDocument
    etag: str


class RuntimeRegistry:
    """Hold one immutable, verified runtime generation."""

    def __init__(self, initial: RuntimeDocument) -> None:
        self._current = RuntimeGeneration(
            revision=initial.revision,
            digest=_runtime_digest(initial),
            document=initial,
            activated_at=datetime.now(UTC),
        )
        self._degraded = False
        self._last_error: str | None = None

    @property
    def current(self) -> RuntimeGeneration:
        return self._current

    @property
    def revision(self) -> int:
        return self._current.revision

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def activate(self, change: ConfigChange) -> RuntimeGeneration:
        if change.revision <= self.revision:
            raise ValueError(
                "runtime revision must increase monotonically",
            )
        expected = _runtime_digest(change.document)
        if change.digest != expected:
            raise ValueError("runtime document digest does not match")
        generation = RuntimeGeneration(
            revision=change.revision,
            digest=change.digest,
            document=change.document,
            activated_at=datetime.now(UTC),
        )
        self._current = generation
        self._degraded = False
        self._last_error = None
        return generation

    def mark_degraded(self, error: BaseException) -> None:
        self._degraded = True
        self._last_error = type(error).__name__


class ConfigWatcher:
    """只下载变化的 ETag，并只激活完整验证且 revision 递增的文档。

    首次 ``start`` 会等待至少一个远端 generation 成功激活，防止 Manager 用镜像内占位
    配置接收消息。后续轮询失败时保留旧 generation 并标记 degraded，恢复后可自动继续。
    """

    def __init__(
        self,
        *,
        storage: RuntimeObjectStorage,
        key: str,
        cache_path: Path,
        registry: RuntimeRegistry,
        prepare: RuntimePrepare | None = None,
        poll_interval_seconds: float = 5,
        initial_timeout_seconds: float = 60,
    ) -> None:
        if not key:
            raise ValueError("runtime document key must not be empty")
        if poll_interval_seconds <= 0:
            raise ValueError("poll interval must be positive")
        if initial_timeout_seconds <= 0:
            raise ValueError("initial timeout must be positive")
        self._storage = storage
        self._key = key
        self._cache_path = cache_path.resolve()
        self._registry = registry
        self._prepare = prepare
        self._poll_interval = poll_interval_seconds
        self._initial_timeout = initial_timeout_seconds
        self._initial_revision = registry.revision
        self._etag: str | None = None
        self._poll_lock = asyncio.Lock()
        self._poll_task: asyncio.Task[None] | None = None
        self._ready = False

    @property
    def observed_etag(self) -> str | None:
        return self._etag

    @property
    def ready(self) -> bool:
        return self._ready

    async def start(self) -> None:
        """Activate one Controller generation before serving Matrix turns."""
        if self._poll_task is not None:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._initial_timeout
        while True:
            await self.poll_once()
            if (
                self._registry.revision > self._initial_revision
                and not self._registry.degraded
            ):
                self._ready = True
                break
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RuntimeError(
                    "initial runtime document was not activated",
                )
            await asyncio.sleep(min(self._poll_interval, remaining))
        self._poll_task = asyncio.create_task(
            self._run(),
            name="manager-runtime-config",
        )

    async def stop(self) -> None:
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._ready = False

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval)
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Manager runtime polling failed")

    async def poll_once(self) -> ConfigChange | None:
        async with self._poll_lock:
            try:
                receipt = await self._storage.head(self._key)
            except Exception as exc:
                self._registry.mark_degraded(exc)
                return None
            if receipt is None:
                self._registry.mark_degraded(
                    FileNotFoundError(self._key),
                )
                return None
            if receipt.etag == self._etag:
                return None
            try:
                data = await self._storage.get_bytes(self._key)
                if (
                    len(data) != receipt.size
                    or hashlib.sha256(data).hexdigest()
                    != receipt.sha256
                ):
                    raise ValueError(
                        "runtime object checksum or size mismatch",
                    )
                document = RuntimeDocument.model_validate_json(data)
                if document.revision <= self._registry.revision:
                    raise ValueError(
                        "remote runtime revision is not newer",
                    )
                digest = _runtime_digest(document)
                if self._prepare is not None:
                    prepared = self._prepare(document)
                    if inspect.isawaitable(prepared):
                        await prepared
                change = ConfigChange(
                    revision=document.revision,
                    digest=digest,
                    document=document,
                    etag=receipt.etag,
                )
                _atomic_write(self._cache_path, _canonical(document))
                self._registry.activate(change)
            except Exception as exc:
                self._registry.mark_degraded(exc)
                return None
            self._etag = receipt.etag
            return change


def _canonical(document: RuntimeDocument) -> bytes:
    return json.dumps(
        document.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _runtime_digest(document: RuntimeDocument) -> str:
    return hashlib.sha256(_canonical(document)).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
