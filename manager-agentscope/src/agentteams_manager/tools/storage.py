"""Versioned task artifacts stored in the shared AgentTeams filesystem."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator

from agentteams_manager.domain.models import ObjectReceipt


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


class TaskMetadata(BaseModel):
    """Canonical task metadata shared by Manager and Workers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    task_id: str = Field(
        pattern=r"^task-\d{8}-\d{6}-[a-z0-9]{6}$",
    )
    task_type: Literal["finite", "infinite"]
    status: Literal[
        "prepared",
        "assigned",
        "active",
        "completed",
        "failed",
        "cancelled",
    ]
    title: str = Field(min_length=1)
    assigned_to: str = Field(min_length=1)
    room_id: str = Field(min_length=1)
    project_id: str | None = None
    schedule: str | None = None
    timezone: str | None = None
    created_at: datetime
    completed_at: datetime | None = None

    @model_validator(mode="after")
    def validate_schedule(self) -> Self:
        if self.task_type == "infinite" and (
            not self.schedule or not self.timezone
        ):
            raise ValueError(
                "infinite tasks require both schedule and timezone",
            )
        if (self.schedule is None) != (self.timezone is None):
            raise ValueError("schedule and timezone must be provided together")
        if self.timezone is not None:
            try:
                ZoneInfo(self.timezone)
            except ZoneInfoNotFoundError as exc:
                raise ValueError(
                    f"unknown task timezone: {self.timezone}",
                ) from exc
        if self.status == "completed" and self.completed_at is None:
            raise ValueError("completed tasks require completed_at")
        if self.status != "completed" and self.completed_at is not None:
            raise ValueError("only completed tasks may have completed_at")
        return self


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
