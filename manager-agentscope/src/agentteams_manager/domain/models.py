"""Strict domain records for durable Manager workflows."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    """Base model that rejects accidental contract expansion."""

    model_config = ConfigDict(extra="forbid")


class FrozenStrictModel(StrictModel):
    """Immutable value object."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class RoomKind(StrEnum):
    ADMIN_DM = "admin_dm"
    WORKER_ROOM = "worker_room"
    LEADER_ROOM = "leader_room"
    TEAM_ROOM = "team_room"
    HUMAN_OR_CHANNEL_ROOM = "human_or_channel_room"
    PROJECT_ROOM = "project_room"
    UNKNOWN = "unknown"


class OperationStatus(StrEnum):
    PLANNED = "planned"
    PREPARED = "prepared"
    DISPATCHED = "dispatched"
    ACKNOWLEDGED = "acknowledged"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    RECONCILING = "reconciling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NEEDS_ATTENTION = "needs_attention"


class ExternalEffect(StrEnum):
    CONTROLLER = "controller"
    MATRIX = "matrix"
    STORAGE = "storage"
    PROCESS = "process"
    HIGRESS = "higress"


class OperationKind(StrEnum):
    CREATE_WORKER = "create_worker"
    UPDATE_WORKER = "update_worker"
    DELETE_WORKER = "delete_worker"
    CREATE_TEAM = "create_team"
    UPDATE_TEAM = "update_team"
    DELETE_TEAM = "delete_team"
    CREATE_HUMAN = "create_human"
    UPDATE_HUMAN = "update_human"
    DELETE_HUMAN = "delete_human"
    DELEGATE_TASK = "delegate_task"
    COMPLETE_TASK = "complete_task"
    CREATE_PROJECT = "create_project"
    GIT_DELEGATION = "git_delegation"
    FILE_SYNC = "file_sync"
    CONFIGURE_MCP = "configure_mcp"
    SWITCH_MODEL = "switch_model"
    PUBLISH_SERVICE = "publish_service"


_TRANSITIONS: dict[OperationStatus, frozenset[OperationStatus]] = {
    OperationStatus.PLANNED: frozenset(
        {OperationStatus.PREPARED, OperationStatus.FAILED},
    ),
    OperationStatus.PREPARED: frozenset(
        {
            OperationStatus.DISPATCHED,
            OperationStatus.RECONCILING,
            OperationStatus.RETRY_WAIT,
            OperationStatus.FAILED,
        },
    ),
    OperationStatus.DISPATCHED: frozenset(
        {
            OperationStatus.ACKNOWLEDGED,
            OperationStatus.RUNNING,
            OperationStatus.RECONCILING,
            OperationStatus.RETRY_WAIT,
        },
    ),
    OperationStatus.ACKNOWLEDGED: frozenset(
        {OperationStatus.RUNNING, OperationStatus.SUCCEEDED},
    ),
    OperationStatus.RUNNING: frozenset(
        {
            OperationStatus.SUCCEEDED,
            OperationStatus.RETRY_WAIT,
            OperationStatus.RECONCILING,
            OperationStatus.FAILED,
            OperationStatus.NEEDS_ATTENTION,
        },
    ),
    OperationStatus.RETRY_WAIT: frozenset(
        {OperationStatus.PREPARED, OperationStatus.RECONCILING},
    ),
    OperationStatus.RECONCILING: frozenset(
        {
            OperationStatus.RUNNING,
            OperationStatus.SUCCEEDED,
            OperationStatus.RETRY_WAIT,
            OperationStatus.FAILED,
            OperationStatus.NEEDS_ATTENTION,
        },
    ),
    OperationStatus.SUCCEEDED: frozenset(),
    OperationStatus.FAILED: frozenset(),
    OperationStatus.NEEDS_ATTENTION: frozenset(
        {OperationStatus.RECONCILING, OperationStatus.FAILED},
    ),
}


class OperationRecord(StrictModel):
    operation_id: str = Field(min_length=32, max_length=32)
    kind: OperationKind
    target_key: str = Field(min_length=1)
    status: OperationStatus
    request: dict[str, Any]
    result: dict[str, Any] = Field(default_factory=dict)
    retry_count: int = Field(default=0, ge=0)
    created_at: datetime
    updated_at: datetime

    @classmethod
    def new(
        cls,
        *,
        operation_id: str,
        kind: OperationKind | str,
        target_key: str,
        request: dict[str, Any],
    ) -> Self:
        now = datetime.now(UTC)
        return cls(
            operation_id=operation_id,
            kind=kind,
            target_key=target_key,
            status=OperationStatus.PLANNED,
            request=request,
            created_at=now,
            updated_at=now,
        )

    def can_transition_to(self, status: OperationStatus) -> bool:
        return status in _TRANSITIONS[self.status]


class RoomPolicy(FrozenStrictModel):
    room_id: str = Field(min_length=1)
    kind: RoomKind
    revision: int = Field(ge=0)
    allowed_tools: frozenset[str] = frozenset()
    confirm_tools: frozenset[str] = frozenset()
    allowed_senders: frozenset[str] = frozenset()
    resource_name: str | None = None
    team_name: str | None = None
    project_id: str | None = None
    silent: bool = False


class MediaReference(FrozenStrictModel):
    mxc_uri: str
    media_type: str
    filename: str | None = None
    size: int | None = Field(default=None, ge=0)
    encryption_key: str | None = None
    encryption_hash: str | None = None
    encryption_iv: str | None = None


class InboundEvent(FrozenStrictModel):
    room_id: str
    event_id: str
    sender: str
    body: str
    timestamp: datetime
    is_direct: bool = False
    thread_id: str | None = None
    mentions: tuple[str, ...] = ()
    media: tuple[MediaReference, ...] = ()

    @property
    def sender_id(self) -> str:
        """Expose the sender with the explicit name used at transport edges."""
        return self.sender


class WorkerResource(FrozenStrictModel):
    name: str
    runtime: str
    model: str | None = None
    phase: str | None = None
    room_id: str | None = None
    matrix_user_id: str | None = None
    team: str | None = None
    role: str | None = None
    skills: tuple[str, ...] = ()
    spec: dict[str, Any] = Field(default_factory=dict)
    status: dict[str, Any] = Field(default_factory=dict)


class TeamResource(FrozenStrictModel):
    name: str
    leader: str
    workers: tuple[str, ...]
    room_id: str | None = None
    phase: str | None = None
    spec: dict[str, Any] = Field(default_factory=dict)
    status: dict[str, Any] = Field(default_factory=dict)


class HumanResource(FrozenStrictModel):
    name: str
    matrix_user_id: str
    permission_level: int = Field(ge=1, le=3)
    allowed_rooms: tuple[str, ...] = ()
    spec: dict[str, Any] = Field(default_factory=dict)
    status: dict[str, Any] = Field(default_factory=dict)


class TaskRecord(StrictModel):
    task_id: str
    task_type: str
    status: str
    title: str
    assigned_to: str
    room_id: str
    project_id: str | None = None
    delegated_to_team: str | None = None
    schedule: str | None = None
    timezone: str | None = None
    last_executed_at: datetime | None = None
    next_scheduled_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class ProjectRecord(StrictModel):
    project_id: str
    name: str
    room_id: str
    status: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class JournalEvent(FrozenStrictModel):
    operation_id: str = Field(min_length=32, max_length=32)
    sequence: int = Field(ge=1)
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime

    @classmethod
    def example(
        cls,
        *,
        operation_id: str,
        sequence: int,
        event_type: str,
    ) -> Self:
        return cls(
            operation_id=operation_id,
            sequence=sequence,
            event_type=event_type,
            created_at=datetime.now(UTC),
        )


class ObjectReceipt(FrozenStrictModel):
    key: str
    etag: str
    sha256: str
    size: int = Field(ge=0)
    content_type: str | None = None


class MirrorReceipt(FrozenStrictModel):
    prefix: str
    files: int = Field(ge=0)
    bytes_transferred: int = Field(ge=0)
    manifest_sha256: str


class RecoveryReport(FrozenStrictModel):
    snapshot_sequence: int = Field(default=0, ge=0)
    replayed_events: int = Field(default=0, ge=0)
    reconciled_operations: int = Field(default=0, ge=0)
    needs_attention: tuple[str, ...] = ()


class TopologySnapshot(FrozenStrictModel):
    revision: int = Field(ge=0)
    workers: tuple[WorkerResource, ...] = ()
    teams: tuple[TeamResource, ...] = ()
    humans: tuple[HumanResource, ...] = ()
    manager_join_targets: tuple[str, ...] = ()
    forbidden_rooms: tuple[str, ...] = ()
    refreshed_at: datetime
