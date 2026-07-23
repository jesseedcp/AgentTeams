"""Versioned task artifacts stored in the shared AgentTeams filesystem."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import NotFoundError
from agentteams_manager.domain.models import (
    MirrorReceipt,
    ObjectReceipt,
    TaskMetadata,
    TaskRecord,
)
from agentteams_manager.workflows.git_delegation import (
    ProcessingLease,
    ProcessingLeaseService,
)


class VersionedObjectStorage(Protocol):
    async def put_bytes_if_version(
        self,
        key: str,
        data: bytes,
        *,
        expected_etag: str | None,
        content_type: str = "application/octet-stream",
    ) -> ObjectReceipt: ...

    async def head(self, key: str) -> ObjectReceipt | None: ...

    async def get_bytes(self, key: str) -> bytes: ...

    async def mirror_down(
        self,
        prefix: str,
        destination: Path,
    ) -> MirrorReceipt: ...

    async def put_json_if_version(
        self,
        key: str,
        value: Any,
        *,
        expected_etag: str | None,
    ) -> ObjectReceipt: ...


class TaskArtifactSet:
    """Writes the immutable prepared form of a task in publish order."""

    def __init__(
        self,
        *,
        storage: VersionedObjectStorage,
        metadata: TaskMetadata,
        specification: str,
    ) -> None:
        self._storage = storage
        self._metadata = metadata
        self._specification = specification

    async def write_prepared(self) -> tuple[ObjectReceipt, ObjectReceipt]:
        if self._metadata.status != "prepared":
            raise ValueError("new task metadata must have prepared status")
        if not self._specification.strip():
            raise ValueError("task specification must not be empty")
        prefix = f"shared/tasks/{self._metadata.task_id}"
        metadata_receipt = await self._storage.put_json_if_version(
            f"{prefix}/meta.json",
            self._metadata.model_dump(mode="json"),
            expected_etag=None,
        )
        specification_receipt = await self._storage.put_bytes_if_version(
            f"{prefix}/spec.md",
            self._specification.encode("utf-8"),
            expected_etag=None,
            content_type="text/markdown",
        )
        return metadata_receipt, specification_receipt


class SyncTaskReader(Protocol):
    async def get(self, task_id: str) -> TaskRecord | None: ...


class FileSyncReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str
    prefix: str
    files: int = Field(ge=0)
    bytes_transferred: int = Field(ge=0)
    manifest_sha256: str
    instruction: str = "Run `agentteams-sync` before reading the files."


class FileSyncService:
    """Make remote pull and protected Worker push explicit."""

    def __init__(
        self,
        *,
        storage: VersionedObjectStorage,
        leases: ProcessingLeaseService,
        tasks: SyncTaskReader,
        cache_root: Path,
    ) -> None:
        self._storage = storage
        self._leases = leases
        self._tasks = tasks
        self._cache_root = cache_root.resolve()

    async def pull_task(self, task_id: str) -> Path:
        await self._require_task(task_id)
        destination = self._task_root(task_id)
        await self._storage.mirror_down(
            f"shared/tasks/{task_id}/",
            destination,
        )
        return destination

    async def push_task(
        self,
        task_id: str,
        *,
        processor: str,
        lease: ProcessingLease | None = None,
    ) -> FileSyncReceipt:
        root = self._task_root(task_id)
        if not root.is_dir():
            raise FileNotFoundError(root)
        paths = tuple(
            path
            for path in sorted(root.rglob("*"))
            if path.is_file()
            and not path.is_symlink()
            and not _manager_owned(path.relative_to(root))
        )
        return await self.push_files(
            task_id,
            paths=paths,
            processor=processor,
            lease=lease,
        )

    async def push_files(
        self,
        task_id: str,
        *,
        paths: tuple[Path, ...],
        processor: str,
        lease: ProcessingLease | None = None,
    ) -> FileSyncReceipt:
        await self._require_task(task_id)
        root = self._task_root(task_id)
        if not root.is_dir():
            raise FileNotFoundError(root)
        owned_lease = lease is None
        active_lease = lease or await self._leases.acquire(
            task_id,
            processor=processor,
            operation="file-sync",
        )
        if active_lease.task_id != task_id:
            raise ValueError("processing lease belongs to another task")

        entries: list[dict[str, object]] = []
        transferred = 0
        try:
            for path in sorted(set(paths)):
                if path.is_symlink():
                    raise ValueError(f"sync path must not be a symlink: {path}")
                absolute = (
                    path.resolve(strict=True)
                    if path.is_absolute()
                    else (root / path).resolve(strict=True)
                )
                if not absolute.is_relative_to(root):
                    raise ValueError(f"sync path escapes task root: {path}")
                relative = absolute.relative_to(root)
                if _manager_owned(relative):
                    raise PermissionError(
                        f"Worker sync cannot mutate {relative.as_posix()}",
                    )
                if not absolute.is_file():
                    raise ValueError(f"sync path is not a file: {path}")
                data = absolute.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                key = (
                    f"shared/tasks/{task_id}/"
                    f"{relative.as_posix()}"
                )
                current = await self._storage.head(key)
                if (
                    current is None
                    or current.sha256 != digest
                    or current.size != len(data)
                ):
                    receipt = await self._storage.put_bytes_if_version(
                        key,
                        data,
                        expected_etag=(
                            current.etag if current is not None else None
                        ),
                        content_type=_content_type(absolute),
                    )
                    transferred += len(data)
                else:
                    receipt = current
                entries.append(
                    {
                        "etag": receipt.etag,
                        "key": key,
                        "sha256": receipt.sha256,
                        "size": receipt.size,
                    },
                )
        finally:
            if owned_lease:
                await self._leases.release(active_lease)
        encoded = json.dumps(
            entries,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return FileSyncReceipt(
            task_id=task_id,
            prefix=f"shared/tasks/{task_id}/",
            files=len(entries),
            bytes_transferred=transferred,
            manifest_sha256=hashlib.sha256(encoded).hexdigest(),
        )

    async def _require_task(self, task_id: str) -> TaskRecord:
        task = await self._tasks.get(task_id)
        if task is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        return task

    def _task_root(self, task_id: str) -> Path:
        if (
            not task_id.startswith("task-")
            or "/" in task_id
            or "\\" in task_id
            or task_id in {"task-.", "task-.."}
        ):
            raise ValueError(f"invalid task ID: {task_id!r}")
        return self._cache_root / "shared" / "tasks" / task_id


def _manager_owned(relative: Path) -> bool:
    if not relative.parts:
        return True
    return relative.parts[0] in {
        ".processing",
        "base",
        "meta.json",
        "spec.md",
    }


def _content_type(path: Path) -> str:
    return {
        ".json": "application/json",
        ".md": "text/markdown",
        ".txt": "text/plain",
        ".yaml": "application/yaml",
        ".yml": "application/yaml",
    }.get(path.suffix.casefold(), "application/octet-stream")
