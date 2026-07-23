"""Crash-safe finite and recurring task workflows."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
)
from agentteams_manager.domain.ids import (
    matrix_transaction_id,
    operation_id_for,
)
from agentteams_manager.domain.models import (
    ExternalEffect,
    ObjectReceipt,
    OperationKind,
    OperationRecord,
    OperationStatus,
    TaskRecord,
    TaskMetadata,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.domain.ports import ArtifactPort, Clock, MatrixPort
from agentteams_manager.workflows.resources import MutationContext


class TaskError(RuntimeError):
    """Base failure for a task lifecycle request."""


class TaskResultMissing(TaskError):
    """A completion report has neither an artifact nor structured result."""


class TaskRepositoryPort(Protocol):
    async def create(self, task: TaskRecord) -> TaskRecord: ...

    async def get(self, task_id: str) -> TaskRecord | None: ...

    async def transition(
        self,
        task_id: str,
        *,
        expected: set[str],
        target: str,
        last_executed_at: datetime | None = None,
        next_scheduled_at: datetime | None = None,
        metadata: dict[str, object] | None = None,
    ) -> TaskRecord | None: ...


class TaskControllerPort(Protocol):
    async def get_worker(self, name: str) -> WorkerResource | None: ...

    async def get_team(self, name: str) -> TeamResource | None: ...


class TaskSupervisorPort(Protocol):
    async def begin(
        self,
        *,
        operation_id: str,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> OperationRecord: ...

    async def before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> object: ...

    async def effect_acknowledged(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...

    async def effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...

    async def effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord: ...

    async def effect_failed(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord: ...


class CompletionNotificationPort(Protocol):
    async def send_completion(
        self,
        *,
        operation_id: str,
        task: TaskRecord,
        summary: str,
    ) -> object: ...


class TaskCreateRequest(BaseModel):
    """Durable request saved in the operation journal."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str = Field(min_length=1)
    specification: str = Field(min_length=1)
    assigned_to: str = Field(min_length=1)
    delegated_to_team: str | None = None
    project_id: str | None = None
    project_room_id: str | None = None
    requester_room_id: str = Field(min_length=1)


class TaskReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    task_id: str
    status: str
    assigned_to: str
    room_id: str
    assignment_event_id: str | None = None
    summary: str | None = None


class TaskMessageFormatter:
    """Wire-compatible Worker messages from the original AgentTeams."""

    @staticmethod
    def assignment(
        *,
        task_id: str,
        title: str,
        matrix_user_id: str,
    ) -> str:
        return (
            f"{matrix_user_id} New task [{task_id}]: {title}. "
            "Use your file-sync skill to pull the spec: "
            f"shared/tasks/{task_id}/spec.md. "
            "@mention me when complete."
        )

    @staticmethod
    def completion(
        *,
        task_id: str,
        title: str,
        assigned_to: str,
        summary: str,
    ) -> str:
        return (
            f"[Task Completed] {task_id}: {title} — "
            f"assigned to {assigned_to}. {summary}"
        )


class TaskService:
    """Prepare durable task state before dispatching any Worker message."""

    _ASSIGNMENT_EFFECT_SEQUENCE = 0

    def __init__(
        self,
        *,
        tasks: TaskRepositoryPort,
        storage: ArtifactPort,
        controller: TaskControllerPort,
        matrix: MatrixPort,
        supervisor: TaskSupervisorPort,
        clock: Clock,
        cache_root: Path,
        matrix_domain: str,
        notifications: CompletionNotificationPort | None = None,
    ) -> None:
        if not matrix_domain.strip():
            raise ValueError("matrix_domain must not be empty")
        self._tasks = tasks
        self.storage = storage
        self._controller = controller
        self._matrix = matrix
        self._supervisor = supervisor
        self._clock = clock
        self._cache_root = cache_root.resolve()
        self._matrix_domain = matrix_domain
        self._notifications = notifications

    async def create_finite(
        self,
        *,
        title: str,
        spec: str,
        assigned_to: str,
        context: MutationContext,
        delegated_to_team: str | None = None,
        project_id: str | None = None,
        project_room_id: str | None = None,
    ) -> TaskReceipt:
        request = TaskCreateRequest(
            title=title,
            specification=spec,
            assigned_to=assigned_to,
            delegated_to_team=delegated_to_team,
            project_id=project_id,
            project_room_id=project_room_id,
            requester_room_id=context.room_id,
        )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.DELEGATE_TASK,
            target_key=f"task-request/{context.operation_id}",
            request=request.model_dump(mode="json"),
        )
        return await self.dispatch(operation)

    async def dispatch(self, operation: OperationRecord) -> TaskReceipt:
        """Resume a finite dispatch from facts in SQLite, MinIO, and Matrix."""

        if operation.kind is not OperationKind.DELEGATE_TASK:
            raise ValueError("operation is not a task delegation")
        request = TaskCreateRequest.model_validate(operation.request)
        task_id = _task_id_for(operation)
        if operation.status is OperationStatus.FAILED:
            raise TaskError(f"task delegation {task_id} previously failed")

        task = await self._tasks.get(task_id)
        if task is None:
            room_id, matrix_user_id = await self._resolve_assignment(request)
            now = self._clock.now().astimezone(UTC)
            task = TaskRecord(
                task_id=task_id,
                task_type="finite",
                status="prepared",
                title=request.title,
                assigned_to=request.assigned_to,
                room_id=room_id,
                project_id=request.project_id,
                delegated_to_team=request.delegated_to_team,
                metadata={
                    "operation_id": operation.operation_id,
                    "matrix_user_id": matrix_user_id,
                    "project_room_id": request.project_room_id,
                    "requester_room_id": request.requester_room_id,
                },
                created_at=now,
                updated_at=now,
            )
            try:
                task = await self._tasks.create(task)
            except Exception:
                raced = await self._tasks.get(task_id)
                if raced is None:
                    raise
                task = raced
        self._verify_task_request(task, operation, request)

        if task.status in {"completed", "failed", "cancelled"}:
            return _task_receipt(operation.operation_id, task)

        durable_status = (
            "prepared" if task.status == "prepared" else task.status
        )
        prepared = _task_metadata(task, status=durable_status)
        await self._ensure_json(
            operation,
            f"shared/tasks/{task_id}/meta.json",
            prepared.model_dump(mode="json"),
            operation_name="write_task_metadata",
        )
        await self._ensure_bytes(
            operation,
            f"shared/tasks/{task_id}/spec.md",
            request.specification.encode("utf-8"),
            content_type="text/markdown",
            operation_name="write_task_specification",
        )

        if task.status == "prepared":
            transaction_id = matrix_transaction_id(
                operation.operation_id,
                self._ASSIGNMENT_EFFECT_SEQUENCE,
            )
            matrix_user_id = str(task.metadata["matrix_user_id"])
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {
                    "operation": "send_task_assignment",
                    "task_id": task_id,
                    "room_id": task.room_id,
                    "txn_id": transaction_id,
                },
            )
            try:
                event_id = await self._matrix.send_text(
                    task.room_id,
                    TaskMessageFormatter.assignment(
                        task_id=task_id,
                        title=task.title,
                        matrix_user_id=matrix_user_id,
                    ),
                    txn_id=transaction_id,
                    mentions=(matrix_user_id,),
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    exc,
                )
                raise
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {
                    "task_id": task_id,
                    "event_id": event_id,
                    "txn_id": transaction_id,
                },
            )
            local_metadata = {
                **task.metadata,
                "assignment_event_id": event_id,
                "assignment_txn_id": transaction_id,
            }
            changed = await self._tasks.transition(
                task_id,
                expected={"prepared"},
                target="assigned",
                metadata=local_metadata,
            )
            task = changed or await self._require_task(task_id)

        if task.status != "assigned":
            raise ConflictError(
                f"task {task_id} cannot dispatch from {task.status}",
            )
        assigned_metadata = _task_metadata(task, status="assigned")
        receipt = await self._ensure_json(
            operation,
            f"shared/tasks/{task_id}/meta.json",
            assigned_metadata.model_dump(mode="json"),
            operation_name="publish_assigned_task",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": task_id,
                "status": "assigned",
                "metadata_etag": receipt.etag,
                "assignment_event_id": task.metadata.get(
                    "assignment_event_id",
                ),
            },
        )
        return _task_receipt(operation.operation_id, task)

    async def record_completion(
        self,
        *,
        task_id: str,
        worker_event_id: str,
        structured_result: dict[str, Any] | None = None,
    ) -> TaskReceipt:
        task = await self._require_task(task_id)
        operation_id = operation_id_for(
            task.room_id,
            worker_event_id,
            f"complete:{task_id}",
        )
        if task.status == "completed":
            return _task_receipt(
                operation_id,
                task,
                summary=_completion_summary(task),
            )
        if task.status not in {"assigned", "active"}:
            raise ConflictError(
                f"task {task_id} cannot complete from {task.status}",
            )
        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.COMPLETE_TASK,
            target_key=f"task/{task_id}",
            request={
                "task_id": task_id,
                "worker_event_id": worker_event_id,
                "structured_result": structured_result,
            },
        )
        destination = self._cache_root / "shared" / "tasks" / task_id
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "pull_task_for_completion",
                "task_id": task_id,
            },
        )
        try:
            mirror = await self.storage.mirror_down(
                f"shared/tasks/{task_id}/",
                destination,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.STORAGE,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation_id,
            ExternalEffect.STORAGE,
            mirror.model_dump(mode="json"),
        )

        result_path = destination / "result.md"
        if result_path.is_file():
            summary = _summarize(result_path.read_text(encoding="utf-8"))
        elif structured_result is not None:
            summary = _summarize(
                json.dumps(
                    structured_result,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        else:
            await self._supervisor.effect_failed(
                operation_id,
                ExternalEffect.STORAGE,
                "result.md or structured result is required",
            )
            raise TaskResultMissing(
                f"task {task_id} has no result.md or structured result",
            )

        current_remote = await self._read_task_metadata(task_id)
        if current_remote.status == "completed":
            completed_remote = current_remote
            completed_at = current_remote.completed_at
            if completed_at is None:
                raise ConflictError(
                    f"completed metadata for {task_id} has no timestamp",
                )
        else:
            completed_at = self._clock.now().astimezone(UTC)
            completed_remote = TaskMetadata.model_validate(
                {
                    **current_remote.model_dump(mode="json"),
                    "status": "completed",
                    "completed_at": completed_at,
                },
            )
        receipt = await self._replace_task_metadata(
            operation,
            completed_remote,
            operation_name="publish_completed_task",
        )
        local_metadata = {
            **task.metadata,
            "completion_event_id": worker_event_id,
            "completed_at": completed_at.isoformat(),
            "completion_summary": summary,
            "completion_metadata_etag": receipt.etag,
        }
        changed = await self._tasks.transition(
            task_id,
            expected={"assigned", "active"},
            target="completed",
            metadata=local_metadata,
        )
        task = changed or await self._require_task(task_id)
        if task.status != "completed":
            raise ConflictError(
                f"task {task_id} completion did not converge",
            )
        if self._notifications is not None:
            await self._notifications.send_completion(
                operation_id=operation_id,
                task=task,
                summary=summary,
            )
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": task_id,
                "status": "completed",
                "metadata_etag": receipt.etag,
                "summary": summary,
            },
        )
        return _task_receipt(operation_id, task, summary=summary)

    async def cancel(
        self,
        *,
        task_id: str,
        context: MutationContext,
    ) -> TaskReceipt:
        task = await self._require_task(task_id)
        if task.status == "cancelled":
            return _task_receipt(context.operation_id, task)
        if task.status not in {"prepared", "assigned", "active"}:
            raise ConflictError(
                f"task {task_id} cannot cancel from {task.status}",
            )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.COMPLETE_TASK,
            target_key=f"task/{task_id}",
            request={"task_id": task_id, "action": "cancel"},
        )
        remote = await self._read_task_metadata(task_id)
        cancelled = TaskMetadata.model_validate(
            {
                **remote.model_dump(mode="json"),
                "status": "cancelled",
                "completed_at": None,
            },
        )
        receipt = await self._replace_task_metadata(
            operation,
            cancelled,
            operation_name="publish_cancelled_task",
        )
        changed = await self._tasks.transition(
            task_id,
            expected={"prepared", "assigned", "active"},
            target="cancelled",
            metadata={
                **task.metadata,
                "cancelled_by_operation": operation.operation_id,
            },
        )
        task = changed or await self._require_task(task_id)
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": task_id,
                "status": "cancelled",
                "metadata_etag": receipt.etag,
            },
        )
        return _task_receipt(operation.operation_id, task)

    async def _resolve_assignment(
        self,
        request: TaskCreateRequest,
    ) -> tuple[str, str]:
        if request.delegated_to_team is not None:
            team = await self._controller.get_team(
                request.delegated_to_team,
            )
            if team is None:
                raise NotFoundError(
                    f"team/{request.delegated_to_team} does not exist",
                )
            if not team.room_id:
                raise ConflictError(
                    f"team/{team.name} has no authoritative Leader Room",
                )
            leader = await self._controller.get_worker(team.leader)
            matrix_user_id = (
                leader.matrix_user_id
                if leader is not None
                else None
            )
            return (
                team.room_id,
                matrix_user_id
                or f"@worker-{team.leader}:{self._matrix_domain}",
            )
        worker = await self._controller.get_worker(request.assigned_to)
        if worker is None:
            raise NotFoundError(
                f"worker/{request.assigned_to} does not exist",
            )
        if not worker.room_id:
            raise ConflictError(
                f"worker/{request.assigned_to} has no authoritative room",
            )
        return (
            worker.room_id,
            worker.matrix_user_id
            or f"@worker-{request.assigned_to}:{self._matrix_domain}",
        )

    async def _ensure_json(
        self,
        operation: OperationRecord,
        key: str,
        value: dict[str, Any],
        *,
        operation_name: str,
    ) -> ObjectReceipt:
        existing = await self.storage.head(key)
        if existing is not None:
            current = await self.storage.get_json(key)
            if current == value:
                return existing
            if (
                isinstance(current, dict)
                and current.get("task_id") == value.get("task_id")
                and _task_status_rank(str(current.get("status", "")))
                < _task_status_rank(str(value.get("status", "")))
            ):
                return await self._replace_json(
                    operation,
                    key,
                    value,
                    expected_etag=existing.etag,
                    operation_name=operation_name,
                )
            raise ConflictError(f"object {key} has incompatible contents")
        return await self._replace_json(
            operation,
            key,
            value,
            expected_etag=None,
            operation_name=operation_name,
        )

    async def _ensure_bytes(
        self,
        operation: OperationRecord,
        key: str,
        value: bytes,
        *,
        content_type: str,
        operation_name: str,
    ) -> ObjectReceipt:
        existing = await self.storage.head(key)
        if existing is not None:
            if await self.storage.get_bytes(key) != value:
                raise ConflictError(f"object {key} has incompatible contents")
            return existing
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {"operation": operation_name, "key": key},
        )
        try:
            receipt = await self.storage.put_bytes_if_version(
                key,
                value,
                expected_etag=None,
                content_type=content_type,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.STORAGE,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _replace_json(
        self,
        operation: OperationRecord,
        key: str,
        value: dict[str, Any],
        *,
        expected_etag: str | None,
        operation_name: str,
    ) -> ObjectReceipt:
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {"operation": operation_name, "key": key},
        )
        try:
            receipt = await self.storage.put_json_if_version(
                key,
                value,
                expected_etag=expected_etag,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.STORAGE,
                exc,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _read_task_metadata(self, task_id: str) -> TaskMetadata:
        key = f"shared/tasks/{task_id}/meta.json"
        if await self.storage.head(key) is None:
            raise NotFoundError(f"task metadata does not exist: {task_id}")
        return TaskMetadata.model_validate(await self.storage.get_json(key))

    async def _replace_task_metadata(
        self,
        operation: OperationRecord,
        metadata: TaskMetadata,
        *,
        operation_name: str,
    ) -> ObjectReceipt:
        key = f"shared/tasks/{metadata.task_id}/meta.json"
        current_receipt = await self.storage.head(key)
        if current_receipt is None:
            raise NotFoundError(
                f"task metadata does not exist: {metadata.task_id}",
            )
        current = await self.storage.get_json(key)
        target = metadata.model_dump(mode="json")
        if current == target:
            return current_receipt
        return await self._replace_json(
            operation,
            key,
            target,
            expected_etag=current_receipt.etag,
            operation_name=operation_name,
        )

    async def _record_external_failure(
        self,
        operation_id: str,
        effect: ExternalEffect,
        exc: Exception,
    ) -> None:
        if _ambiguous_exception(exc):
            await self._supervisor.effect_ambiguous(
                operation_id,
                effect,
                type(exc).__name__,
            )
            return
        await self._supervisor.effect_failed(
            operation_id,
            effect,
            _safe_reason(exc),
        )

    async def _require_task(self, task_id: str) -> TaskRecord:
        task = await self._tasks.get(task_id)
        if task is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        return task

    @staticmethod
    def _verify_task_request(
        task: TaskRecord,
        operation: OperationRecord,
        request: TaskCreateRequest,
    ) -> None:
        expected = {
            "title": request.title,
            "assigned_to": request.assigned_to,
            "project_id": request.project_id,
            "delegated_to_team": request.delegated_to_team,
            "operation_id": operation.operation_id,
        }
        actual = {
            "title": task.title,
            "assigned_to": task.assigned_to,
            "project_id": task.project_id,
            "delegated_to_team": task.delegated_to_team,
            "operation_id": task.metadata.get("operation_id"),
        }
        if actual != expected:
            raise ConflictError(
                f"task {task.task_id} does not match its journal request",
            )


def _task_id_for(operation: OperationRecord) -> str:
    timestamp = operation.created_at.astimezone(UTC)
    return (
        f"task-{timestamp:%Y%m%d-%H%M%S}-"
        f"{operation.operation_id[:6]}"
    )


def _task_metadata(task: TaskRecord, *, status: str) -> TaskMetadata:
    completed_at_raw = task.metadata.get("completed_at")
    return TaskMetadata(
        task_id=task.task_id,
        task_type=(
            "infinite"
            if task.task_type in {"infinite", "recurring"}
            else "finite"
        ),
        status=status,
        title=task.title,
        assigned_to=task.assigned_to,
        room_id=task.room_id,
        project_id=task.project_id,
        schedule=task.schedule,
        timezone=task.timezone,
        created_at=task.created_at,
        completed_at=(
            datetime.fromisoformat(str(completed_at_raw))
            if completed_at_raw
            else None
        ),
    )


def _task_receipt(
    operation_id: str,
    task: TaskRecord,
    *,
    summary: str | None = None,
) -> TaskReceipt:
    return TaskReceipt(
        operation_id=operation_id,
        task_id=task.task_id,
        status=task.status,
        assigned_to=task.assigned_to,
        room_id=task.room_id,
        assignment_event_id=(
            str(task.metadata["assignment_event_id"])
            if task.metadata.get("assignment_event_id")
            else None
        ),
        summary=summary,
    )


def _task_status_rank(status: str) -> int:
    return {
        "prepared": 0,
        "assigned": 1,
        "active": 1,
        "completed": 2,
        "failed": 2,
        "cancelled": 2,
    }.get(status, -1)


def _summarize(value: str) -> str:
    normalized = " ".join(value.split())
    if not normalized:
        raise TaskResultMissing("task result must not be empty")
    return normalized[:500]


def _completion_summary(task: TaskRecord) -> str | None:
    value = task.metadata.get("completion_summary")
    return str(value) if value is not None else None


def _ambiguous_exception(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            TimeoutError,
            ConnectionError,
            BrokenPipeError,
        ),
    )


def _safe_reason(exc: Exception) -> str:
    if isinstance(exc, AmbiguousEffectError):
        return type(exc).__name__
    text = str(exc).strip()
    return text[:500] if text else type(exc).__name__
