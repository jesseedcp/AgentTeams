"""Versioned task artifacts stored in the shared AgentTeams filesystem."""

from __future__ import annotations

from typing import Any, Protocol

from agentteams_manager.domain.models import ObjectReceipt, TaskMetadata


class VersionedObjectStorage(Protocol):
    async def put_bytes_if_version(
        self,
        key: str,
        data: bytes,
        *,
        expected_etag: str | None,
        content_type: str = "application/octet-stream",
    ) -> ObjectReceipt: ...

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
