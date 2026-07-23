"""Verified Controller runtime polling and monotonic activation."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from agentteams_manager.config import RuntimeDocument
from agentteams_manager.domain.models import ObjectReceipt


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
    """Download only changed ETags and activate only verified revisions."""

    def __init__(
        self,
        *,
        storage: RuntimeObjectStorage,
        key: str,
        cache_path: Path,
        registry: RuntimeRegistry,
        prepare: RuntimePrepare | None = None,
    ) -> None:
        if not key:
            raise ValueError("runtime document key must not be empty")
        self._storage = storage
        self._key = key
        self._cache_path = cache_path.resolve()
        self._registry = registry
        self._prepare = prepare
        self._etag: str | None = None
        self._poll_lock = asyncio.Lock()

    @property
    def observed_etag(self) -> str | None:
        return self._etag

    async def poll_once(self) -> ConfigChange | None:
        async with self._poll_lock:
            receipt = await self._storage.head(self._key)
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
                self._etag = receipt.etag
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
