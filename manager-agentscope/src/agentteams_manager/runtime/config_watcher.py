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
        # 逻辑说明：把初始 RuntimeDocument 封装为带 canonical digest 和激活时间的首个 generation，并将 degraded/last_error 初始化为健康状态；这里只计算内存状态，不访问远端存储。
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
        # 逻辑说明：先要求 ConfigChange revision 严格递增且 digest 与 document 重算值一致，再一次性替换 current generation 并清除降级错误；校验失败抛错且保留旧 generation。
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
        # 逻辑说明：把 registry 标记为 degraded，并仅记录异常类型名作为 last_error；当前可用 generation 保持不变，避免坏配置或存储故障清空服务配置。
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
        # 逻辑说明：校验对象 key、轮询周期和首次等待时限后保存存储/缓存/预热依赖，记录启动时 revision，并初始化 ETag、轮询锁、任务句柄和 ready 标志；构造阶段不下载配置。
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
        # 逻辑说明：若尚未启动则反复 poll_once，直到激活高于启动基线且未降级的 generation 才置 ready 并创建轮询任务；超时明确失败且不会发布假就绪任务，重复 start 为空操作。
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
        # 逻辑说明：原子取出并清空后台轮询任务句柄，取消后等待其退出并吞掉预期 CancelledError，最后清除 ready；未启动时也可安全调用。
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._ready = False

    async def _run(self) -> None:
        # 逻辑说明：按 poll_interval 永久休眠并触发 poll_once；取消必须重新抛出以结束后台任务，其他单轮异常只记录日志后继续下一轮，使意外的适配器错误不终止配置观察器。
        while True:
            await asyncio.sleep(self._poll_interval)
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Manager runtime polling failed")

    async def poll_once(self) -> ConfigChange | None:
        # 逻辑说明：在单次轮询锁内先用 HEAD 比较 ETag，再下载并核验大小、SHA-256、Pydantic 结构和递增 revision；预热成功后原子写缓存并激活 registry，任一故障只标记 degraded 并返回 None，不覆盖旧 generation 或已观察 ETag。
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
    # 逻辑说明：把 RuntimeDocument 的 JSON 模型按 UTF-8、排序键、紧凑分隔符和禁用 NaN 的规则序列化为稳定字节；不可序列化值直接报错，供摘要和缓存共享同一表示。
    return json.dumps(
        document.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _runtime_digest(document: RuntimeDocument) -> str:
    # 逻辑说明：对 _canonical 产生的稳定 runtime 字节计算 SHA-256 十六进制摘要；canonical 序列化失败会原样传播，避免为无效文档生成可激活 digest。
    return hashlib.sha256(_canonical(document)).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    # 逻辑说明：在目标目录写入同盘临时文件、flush 并 fsync 后用 os.replace 原子发布 data；无论写入或替换是否失败都清理残留临时文件，原目标不会暴露半写内容。
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
