"""Crash-safe finite and recurring task workflows.

实现可崩溃恢复的有限任务、周期任务、派发、验收与返修流程。

有限任务先创建版本化 spec artifact，再向 Worker 或 Team Room 发送带稳定 task ID 的
assignment。Worker 的 ``TASK_COMPLETED`` 仅触发结果检查；必须读取 result artifact、
验证 digest 和状态后才能接受，未达标则创建关联 revision task。周期任务由 heartbeat
根据 cron 生成稳定 occurrence，重启或重复 tick 不会重复派发同一次执行。
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.cron import CronSchedule
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
    RecoveryError,
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
    TaskMetadata,
    TaskRecord,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.domain.ports import ArtifactPort, Clock, MatrixPort
from agentteams_manager.workflows.resources import MutationContext

TaskDocumentStatus = Literal[
    "prepared",
    "assigned",
    "active",
    "pending",
    "ready",
    "dispatched",
    "in_progress",
    "blocked",
    "revision_needed",
    "completed",
    "failed",
    "cancelled",
]


class TaskError(RuntimeError):
    """Base failure for a task lifecycle request."""


class TaskResultMissing(TaskError):
    """A completion report has neither an artifact nor structured result."""


class TaskResultInvalid(TaskError):
    """A submitted result does not satisfy the Worker result contract."""


class TaskAcceptanceRequired(TaskError):
    """A successful candidate result still needs Manager acceptance."""


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

    async def update_routing(
        self,
        task_id: str,
        *,
        room_id: str,
        metadata: dict[str, object],
    ) -> TaskRecord: ...


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


class ProjectGraphPort(Protocol):
    async def transition(
        self,
        task_id: str,
        *,
        expected: set[Any],
        target: Any,
        actor_id: str,
        reason: str | None = None,
    ) -> TaskRecord: ...


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
    defer_dispatch: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class RecurringTaskCreateRequest(TaskCreateRequest):
    task_mode: Literal["recurring"] = "recurring"
    schedule: str = Field(min_length=1, max_length=128)
    timezone: str = Field(min_length=1)


class TaskReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    task_id: str
    status: str
    assigned_to: str
    room_id: str
    assignment_event_id: str | None = None
    summary: str | None = None
    last_executed_at: datetime | None = None
    next_scheduled_at: datetime | None = None
    result_status: str | None = None
    deliverables: tuple[str, ...] = ()


class TaskResultSubmission(BaseModel):
    """Validated candidate result inspected before Manager acceptance."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str
    status: Literal[
        "SUCCESS",
        "SUCCESS_WITH_NOTES",
        "REVISION_NEEDED",
        "BLOCKED",
        "INTERRUPTED",
        "FAILED",
        "PARTIAL",
    ]
    summary: str = Field(min_length=1, max_length=20_000)
    deliverables: tuple[str, ...]
    result_path: str
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class RecurringDispatchReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    task_id: str
    scheduled_at: datetime
    txn_id: str
    event_id: str | None = None
    dispatched: bool


class TaskMessageFormatter:
    """Wire-compatible Worker messages from the original AgentTeams."""

    @staticmethod
    def assignment(
        *,
        task_id: str,
        title: str,
        matrix_user_id: str,
        completion_user_id: str | None = None,
    ) -> str:
        message = (
            f"{matrix_user_id} New task [{task_id}]: {title}. "
            "Use your file-sync skill to pull the spec: "
            f"shared/tasks/{task_id}/spec.md. "
            "@mention me when complete."
        )
        if completion_user_id is None:
            return message
        return (
            f"{message} This is a Team parent task. After publishing "
            f"shared/projects/{task_id}/result.md, call "
            f"`projectflow complete_project` for project `{task_id}`. "
            "TeamHarness will mirror the report, publish the submitted parent "
            "metadata, and deterministically send the Manager completion "
            "notification in this format: "
            f"{completion_user_id} TASK_COMPLETED: {task_id}. "
            "Do not hand-edit the parent metadata. Do not send a duplicate "
            "completion reply or send the result directly to the Admin room."
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
    """编排 Task 的 artifact、状态、派发、结果验收与恢复。

    repository 保证本地事务，supervisor 保证外部效果顺序，Controller/Matrix/MinIO ports
    提供真实 I/O。本类把它们组合成不可跳步的生命周期；模型不能直接把一条 Worker
    文本当作完成事实。
    """

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
        manager_user_id: str | None = None,
        notifications: CompletionNotificationPort | None = None,
        project_graph: ProjectGraphPort | None = None,
    ) -> None:
        if not matrix_domain.strip():
            raise ValueError("matrix_domain must not be empty")
        resolved_manager_user_id = (
            manager_user_id or f"@manager:{matrix_domain}"
        ).strip()
        if (
            not resolved_manager_user_id.startswith("@")
            or ":" not in resolved_manager_user_id
        ):
            raise ValueError("manager_user_id must be a Matrix user ID")
        self._tasks = tasks
        self.storage = storage
        self._controller = controller
        self._matrix = matrix
        self._supervisor = supervisor
        self._clock = clock
        self._cache_root = cache_root.resolve()
        self._matrix_domain = matrix_domain
        self._manager_user_id = resolved_manager_user_id
        self._notifications = notifications
        self._project_graph = project_graph

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
        defer_dispatch: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> TaskReceipt:
        request = TaskCreateRequest(
            title=title,
            specification=spec,
            assigned_to=assigned_to,
            delegated_to_team=delegated_to_team,
            project_id=project_id,
            project_room_id=project_room_id,
            requester_room_id=context.room_id,
            defer_dispatch=defer_dispatch,
            metadata=metadata or {},
        )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.DELEGATE_TASK,
            target_key=f"task-request/{context.operation_id}",
            request=request.model_dump(mode="json"),
        )
        return await self.dispatch(operation)

    async def create_recurring(
        self,
        *,
        title: str,
        spec: str,
        assigned_to: str,
        schedule: str,
        timezone: str,
        context: MutationContext,
        delegated_to_team: str | None = None,
        project_id: str | None = None,
        project_room_id: str | None = None,
    ) -> TaskReceipt:
        CronSchedule.parse(schedule)
        request = RecurringTaskCreateRequest(
            title=title,
            specification=spec,
            assigned_to=assigned_to,
            delegated_to_team=delegated_to_team,
            project_id=project_id,
            project_room_id=project_room_id,
            requester_room_id=context.room_id,
            schedule=schedule,
            timezone=timezone,
        )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.DELEGATE_TASK,
            target_key=f"task-request/{context.operation_id}",
            request=request.model_dump(mode="json"),
        )
        return await self._resume_recurring_creation(operation)

    async def _resume_recurring_creation(
        self,
        operation: OperationRecord,
    ) -> TaskReceipt:
        request = RecurringTaskCreateRequest.model_validate(
            operation.request,
        )
        parsed = CronSchedule.parse(request.schedule)
        task_id = _task_id_for(operation)
        if operation.status is OperationStatus.FAILED:
            raise TaskError(f"task delegation {task_id} previously failed")
        task = await self._tasks.get(task_id)
        if task is None:
            (
                room_id,
                matrix_user_id,
                storage_team_name,
            ) = await self._resolve_assignment(request)
            now = self._clock.now().astimezone(UTC)
            task = TaskRecord(
                task_id=task_id,
                task_type="infinite",
                status="active",
                title=request.title,
                assigned_to=request.assigned_to,
                room_id=room_id,
                project_id=request.project_id,
                delegated_to_team=request.delegated_to_team,
                schedule=request.schedule,
                timezone=request.timezone,
                next_scheduled_at=parsed.next_after(now, request.timezone),
                metadata={
                    "operation_id": operation.operation_id,
                    "matrix_user_id": matrix_user_id,
                    "project_room_id": request.project_room_id,
                    "requester_room_id": request.requester_room_id,
                    "defer_dispatch": request.defer_dispatch,
                    "storage_team_name": storage_team_name,
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
        self._verify_recurring_request(task, operation, request)
        metadata = _task_metadata(
            task,
            status="active",
            manager_user_id=self._manager_user_id,
        )
        task_root = _task_storage_root(task)
        await self._ensure_json(
            operation,
            f"{task_root}/meta.json",
            metadata.model_dump(mode="json"),
            operation_name="write_recurring_task_metadata",
        )
        specification_receipt = await self._ensure_bytes(
            operation,
            f"{task_root}/spec.md",
            _task_specification(
                request,
                task_id=task_id,
                manager_user_id=self._manager_user_id,
            ).encode("utf-8"),
            content_type="text/markdown",
            operation_name="write_recurring_task_specification",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": task_id,
                "status": "active",
                "specification_etag": specification_receipt.etag,
                "next_scheduled_at": (
                    task.next_scheduled_at.isoformat()
                    if task.next_scheduled_at is not None
                    else None
                ),
            },
        )
        return _task_receipt(operation.operation_id, task)

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
            (
                room_id,
                matrix_user_id,
                storage_team_name,
            ) = await self._resolve_assignment(request)
            now = self._clock.now().astimezone(UTC)
            task = TaskRecord(
                task_id=task_id,
                task_type="finite",
                status=(
                    "pending" if request.defer_dispatch else "prepared"
                ),
                title=request.title,
                assigned_to=request.assigned_to,
                room_id=room_id,
                project_id=request.project_id,
                delegated_to_team=request.delegated_to_team,
                metadata={
                    **request.metadata,
                    "operation_id": operation.operation_id,
                    "matrix_user_id": matrix_user_id,
                    "project_room_id": request.project_room_id,
                    "requester_room_id": request.requester_room_id,
                    "defer_dispatch": request.defer_dispatch,
                    "storage_team_name": storage_team_name,
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

        durable_status = _task_document_status(
            "prepared" if task.status == "prepared" else task.status
        )
        prepared = _task_metadata(
            task,
            status=durable_status,
            manager_user_id=self._manager_user_id,
        )
        task_root = _task_storage_root(task)
        await self._ensure_json(
            operation,
            f"{task_root}/meta.json",
            prepared.model_dump(mode="json"),
            operation_name="write_task_metadata",
        )
        await self._ensure_bytes(
            operation,
            f"{task_root}/spec.md",
            _task_specification(
                request,
                task_id=task_id,
                manager_user_id=self._manager_user_id,
            ).encode("utf-8"),
            content_type="text/markdown",
            operation_name="write_task_specification",
        )

        if task.status == "pending":
            await self._supervisor.effect_succeeded(
                operation.operation_id,
                ExternalEffect.STORAGE,
                {
                    "task_id": task_id,
                    "status": "pending",
                },
            )
            return _task_receipt(operation.operation_id, task)

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
                        completion_user_id=(
                            self._manager_user_id
                            if task.delegated_to_team
                            else None
                        ),
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
        assigned_metadata = _task_metadata(
            task,
            status="assigned",
            manager_user_id=self._manager_user_id,
        )
        receipt = await self._ensure_json(
            operation,
            f"{task_root}/meta.json",
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

    async def dispatch_ready(
        self,
        *,
        task_id: str,
        context: MutationContext,
    ) -> TaskReceipt:
        if self._project_graph is None:
            raise RuntimeError("project graph is not configured")
        await self._require_task(task_id)
        operation_id = operation_id_for(
            context.room_id,
            context.event_id,
            f"{context.tool_call_id}:dispatch:{task_id}",
        )
        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.DELEGATE_TASK,
            target_key=f"project-task/{task_id}/dispatch",
            request={
                "action": "dispatch_project_ready",
                "task_id": task_id,
            },
        )
        return await self._resume_ready_dispatch(operation)

    async def prepare_reassignment_storage(
        self,
        *,
        task_id: str,
        storage_team_name: str | None,
        operation: OperationRecord,
    ) -> None:
        """Copy task artifacts before changing an assignee's storage scope."""

        task = await self._require_task(task_id)
        target_team_name = str(storage_team_name or "").strip() or None
        old_root = _task_storage_root(task)
        new_root = _task_storage_root_for(
            task_id=task.task_id,
            team_name=target_team_name,
        )
        if old_root == new_root:
            return
        old_prefix = f"{old_root}/"
        for receipt in await self.storage.list_prefix(old_prefix):
            if not receipt.key.startswith(old_prefix):
                raise RecoveryError(
                    f"task storage listing escaped {old_prefix}",
                )
            relative = receipt.key.removeprefix(old_prefix)
            if not relative:
                continue
            await self._ensure_bytes(
                operation,
                f"{new_root}/{relative}",
                await self.storage.get_bytes(receipt.key),
                content_type=(
                    receipt.content_type or "application/octet-stream"
                ),
                operation_name="migrate_reassigned_task_artifact",
            )

    async def _resume_ready_dispatch(
        self,
        operation: OperationRecord,
    ) -> TaskReceipt:
        from agentteams_manager.state.tasks import ProjectTaskState

        project_graph = self._project_graph
        if project_graph is None:
            raise RuntimeError("project graph is not configured")
        task_id = str(operation.request.get("task_id", ""))
        if not task_id:
            raise RecoveryError("ready dispatch is missing task identity")
        task = await self._require_task(task_id)
        operation_id = operation.operation_id
        txn_id = matrix_transaction_id(operation_id, 0)
        if operation.status is OperationStatus.SUCCEEDED:
            current = await self._require_task(task_id)
            return _task_receipt(operation_id, current)
        if task.status == ProjectTaskState.DISPATCHED:
            await self._supervisor.effect_succeeded(
                operation_id,
                ExternalEffect.MATRIX,
                {
                    "task_id": task_id,
                    "status": "dispatched",
                    "event_id": task.metadata.get("assignment_event_id"),
                },
            )
            return _task_receipt(operation_id, task)
        if task.status != ProjectTaskState.READY:
            raise ConflictError(
                f"task {task_id} is not ready for dispatch",
            )
        (
            target_room_id,
            matrix_user_id,
            resolved_storage_team,
        ) = await self._resolve_destination(
            assigned_to=task.assigned_to,
            delegated_to_team=task.delegated_to_team,
            project_room_id=(
                str(task.metadata.get("project_room_id") or "").strip()
                or None
            ),
        )
        target_storage_team = (
            task.delegated_to_team or resolved_storage_team
        )
        target_storage_team = (
            str(target_storage_team or "").strip() or None
        )
        current_storage_team = (
            str(task.metadata.get("storage_team_name") or "").strip()
            or None
        )
        updated_metadata = {
            **task.metadata,
            "matrix_user_id": matrix_user_id,
        }
        if target_storage_team is None:
            updated_metadata.pop("storage_team_name", None)
        else:
            updated_metadata["storage_team_name"] = target_storage_team
        if target_storage_team != current_storage_team:
            await self.prepare_reassignment_storage(
                task_id=task_id,
                storage_team_name=target_storage_team,
                operation=operation,
            )
        if (
            target_room_id != task.room_id
            or updated_metadata != task.metadata
        ):
            task = await self._tasks.update_routing(
                task_id,
                room_id=target_room_id,
                metadata=updated_metadata,
            )
        await self._replace_task_metadata(
            operation,
            _task_metadata(
                task,
                status="ready",
                manager_user_id=self._manager_user_id,
            ),
            operation_name="prepare_ready_task_dispatch",
        )
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "dispatch_project_ready_task",
                "task_id": task_id,
                "room_id": task.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                task.room_id,
                TaskMessageFormatter.assignment(
                    task_id=task_id,
                    title=task.title,
                    matrix_user_id=matrix_user_id,
                    completion_user_id=(
                        self._manager_user_id
                        if task.delegated_to_team
                        else None
                    ),
                ),
                txn_id=txn_id,
                mentions=(matrix_user_id,),
            )
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.MATRIX,
                exc,
            )
            raise
        updated = await project_graph.transition(
            task_id,
            expected={ProjectTaskState.READY},
            target=ProjectTaskState.DISPATCHED,
            actor_id="@manager:system",
            reason="ready task dispatched",
        )
        changed = await self._tasks.transition(
            task_id,
            expected={"dispatched"},
            target="dispatched",
            metadata={
                **updated.metadata,
                "assignment_event_id": event_id,
                "assignment_txn_id": txn_id,
            },
        )
        task = changed or updated
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "task_id": task_id,
                "status": "dispatched",
                "event_id": event_id,
                "txn_id": txn_id,
            },
        )
        return _task_receipt(operation_id, task)

    async def inspect_result(
        self,
        *,
        task_id: str,
        structured_result: dict[str, Any] | None = None,
    ) -> TaskResultSubmission:
        """Pull and validate a Worker result without accepting it."""

        task = await self._require_task(task_id)
        destination = self._cache_root / "shared" / "tasks" / task_id
        await self.storage.mirror_down(
            f"{_task_storage_root(task)}/",
            destination,
        )
        return await self._result_submission(
            task,
            destination=destination,
            structured_result=structured_result,
        )

    async def record_completion(
        self,
        *,
        task_id: str,
        worker_event_id: str,
        structured_result: dict[str, Any] | None = None,
        actor_id: str = "@manager:system",
        accepted: bool = False,
        result_digest: str | None = None,
    ) -> TaskReceipt:
        """Process one submitted result after an explicit Manager decision."""

        task = await self._require_task(task_id)
        operation_id = operation_id_for(
            task.room_id,
            worker_event_id,
            f"complete:{task_id}",
        )
        if task.status == "completed":
            canonical_operation_id = str(
                task.metadata.get("completion_operation_id")
                or operation_id
            )
            summary = _completion_summary(task)
            if self._notifications is not None and summary is not None:
                await self._notifications.send_completion(
                    operation_id=canonical_operation_id,
                    task=task,
                    summary=summary,
                )
            if not task.metadata.get("completion_finalized"):
                await self._supervisor.effect_succeeded(
                    canonical_operation_id,
                    ExternalEffect.STORAGE,
                    {
                        "task_id": task_id,
                        "status": "completed",
                        "summary": summary,
                        "recovered": True,
                    },
                )
                changed = await self._tasks.transition(
                    task_id,
                    expected={"completed"},
                    target="completed",
                    metadata={
                        **task.metadata,
                        "completion_finalized": True,
                    },
                )
                task = changed or task
            return _task_receipt(
                canonical_operation_id,
                task,
                summary=summary,
            )
        if task.status not in {
            "assigned",
            "active",
            "dispatched",
            "in_progress",
            "blocked",
            "revision_needed",
        }:
            raise ConflictError(
                f"task {task_id} cannot process a result from {task.status}",
            )

        candidate = await self.inspect_result(
            task_id=task_id,
            structured_result=structured_result,
        )
        if result_digest is not None and result_digest != candidate.digest:
            raise TaskResultInvalid(
                f"task {task_id} result changed after inspection",
            )
        if candidate.status in {"SUCCESS", "SUCCESS_WITH_NOTES"}:
            if not accepted:
                raise TaskAcceptanceRequired(
                    f"task {task_id} result requires Manager acceptance",
                )
            if result_digest is None:
                raise TaskAcceptanceRequired(
                    f"task {task_id} acceptance requires the inspected digest",
                )

        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.COMPLETE_TASK,
            target_key=f"task/{task_id}",
            request={
                "task_id": task_id,
                "worker_event_id": worker_event_id,
                "structured_result": structured_result,
                "accepted": accepted,
                "result_digest": candidate.digest,
            },
        )
        destination = self._cache_root / "shared" / "tasks" / task_id
        task_root = _task_storage_root(task)
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "pull_task_for_result_decision",
                "task_id": task_id,
                "accepted_digest": candidate.digest,
            },
        )
        try:
            mirror = await self.storage.mirror_down(
                f"{task_root}/",
                destination,
            )
            submission = await self._result_submission(
                task,
                destination=destination,
                structured_result=structured_result,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.STORAGE,
                exc,
            )
            raise
        if submission.digest != candidate.digest:
            await self._supervisor.effect_failed(
                operation_id,
                ExternalEffect.STORAGE,
                "result changed while the acceptance decision was applied",
            )
            raise TaskResultInvalid(
                f"task {task_id} result changed while being accepted",
            )
        await self._supervisor.effect_acknowledged(
            operation_id,
            ExternalEffect.STORAGE,
            {
                **mirror.model_dump(mode="json"),
                "result_digest": submission.digest,
                "result_status": submission.status,
            },
        )

        successful = submission.status in {
            "SUCCESS",
            "SUCCESS_WITH_NOTES",
        }
        target_status = (
            "completed"
            if successful
            else (
                "revision_needed"
                if submission.status == "REVISION_NEEDED"
                else "blocked"
            )
        )
        completed_at = (
            self._clock.now().astimezone(UTC)
            if successful
            else None
        )
        current_remote = await self._read_task_metadata(task_id)
        decided_remote = TaskMetadata.model_validate(
            {
                **current_remote.model_dump(mode="json"),
                "status": target_status,
                "completed_at": completed_at,
                "deliverables": submission.deliverables,
                "result_path": submission.result_path,
                "result_status": submission.status,
                "submitted_by_role": (
                    current_remote.submitted_by_role or "worker"
                ),
                "summary": submission.summary,
            },
        )
        receipt = await self._replace_task_metadata(
            operation,
            decided_remote,
            operation_name="publish_task_result_decision",
        )
        local_metadata: dict[str, object] = {
            **task.metadata,
            "completion_operation_id": operation_id,
            "completion_event_id": worker_event_id,
            "completion_summary": submission.summary,
            "completion_metadata_etag": receipt.etag,
            "result_status": submission.status,
            "result_path": submission.result_path,
            "result_deliverables": list(submission.deliverables),
            "result_digest": submission.digest,
            "result_decided_by": actor_id,
        }
        if completed_at is not None:
            local_metadata.update(
                {
                    "completed_at": completed_at.isoformat(),
                    "accepted_at": completed_at.isoformat(),
                    "accepted_by": actor_id,
                },
            )

        if task.project_id and self._project_graph is not None:
            from agentteams_manager.state.tasks import ProjectTaskState

            target = ProjectTaskState(target_status)
            current = ProjectTaskState(task.status)
            if current is not target:
                task = await self._project_graph.transition(
                    task_id,
                    expected={current},
                    target=target,
                    actor_id=actor_id,
                    reason=(
                        "result accepted"
                        if successful
                        else submission.summary
                    ),
                )
            changed = await self._tasks.transition(
                task_id,
                expected={target.value},
                target=target.value,
                metadata=local_metadata,
            )
            task = changed or task
        else:
            changed = await self._tasks.transition(
                task_id,
                expected={
                    "assigned",
                    "active",
                    "blocked",
                    "revision_needed",
                },
                target=target_status,
                metadata=local_metadata,
            )
            task = changed or await self._require_task(task_id)
        if task.status != target_status:
            raise ConflictError(
                f"task {task_id} result decision did not converge",
            )

        if successful and self._notifications is not None:
            await self._notifications.send_completion(
                operation_id=operation_id,
                task=task,
                summary=submission.summary,
            )
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": task_id,
                "status": target_status,
                "result_status": submission.status,
                "result_digest": submission.digest,
                "metadata_etag": receipt.etag,
                "summary": submission.summary,
            },
        )
        changed = await self._tasks.transition(
            task_id,
            expected={target_status},
            target=target_status,
            metadata={
                **task.metadata,
                "completion_finalized": successful,
                "result_decision_finalized": True,
            },
        )
        task = changed or task
        return _task_receipt(
            operation_id,
            task,
            summary=submission.summary,
        )

    async def _result_submission(
        self,
        task: TaskRecord,
        *,
        destination: Path,
        structured_result: dict[str, Any] | None,
    ) -> TaskResultSubmission:
        current_remote = await self._read_task_metadata(task.task_id)
        result_file = destination / "result.md"
        result_text = (
            result_file.read_text(encoding="utf-8")
            if result_file.is_file()
            else ""
        )
        return _parse_task_result_submission(
            task,
            current_remote,
            result_text=result_text,
            structured_result=structured_result,
            destination=destination,
        )

    async def dispatch_recurring(
        self,
        task: TaskRecord,
    ) -> RecurringDispatchReceipt:
        if (
            task.task_type not in {"infinite", "recurring"}
            or task.status != "active"
            or task.next_scheduled_at is None
        ):
            raise ConflictError(
                f"task {task.task_id} is not an active recurring task",
            )
        scheduled_at = task.next_scheduled_at.astimezone(UTC)
        operation_id = operation_id_for(
            task.room_id,
            scheduled_at.isoformat(),
            f"recurring:{task.task_id}",
        )
        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.DELEGATE_TASK,
            target_key=f"task/{task.task_id}/occurrence/"
            f"{scheduled_at.isoformat()}",
            request={
                "task_id": task.task_id,
                "scheduled_at": scheduled_at.isoformat(),
                "action": "dispatch_recurring",
            },
        )
        transaction_id = matrix_transaction_id(operation_id, 0)
        if operation.status is OperationStatus.SUCCEEDED:
            return RecurringDispatchReceipt(
                operation_id=operation_id,
                task_id=task.task_id,
                scheduled_at=scheduled_at,
                txn_id=transaction_id,
                event_id=(
                    str(operation.result["event_id"])
                    if operation.result.get("event_id")
                    else None
                ),
                dispatched=False,
            )
        if operation.status is OperationStatus.FAILED:
            raise TaskError(
                f"recurring dispatch for {task.task_id} previously failed",
            )
        matrix_user_id = str(
            task.metadata.get("matrix_user_id")
            or f"@worker-{task.assigned_to}:{self._matrix_domain}",
        )
        text = (
            f"{matrix_user_id} Execute recurring task {task.task_id}: "
            f"{task.title}. Report back with \"executed\" when done."
        )
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "dispatch_recurring_task",
                "task_id": task.task_id,
                "scheduled_at": scheduled_at.isoformat(),
                "room_id": task.room_id,
                "txn_id": transaction_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                task.room_id,
                text,
                txn_id=transaction_id,
                mentions=(matrix_user_id,),
            )
        except Exception as exc:
            await self._record_external_failure(
                operation_id,
                ExternalEffect.MATRIX,
                exc,
            )
            raise
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "task_id": task.task_id,
                "scheduled_at": scheduled_at.isoformat(),
                "event_id": event_id,
                "txn_id": transaction_id,
            },
        )
        return RecurringDispatchReceipt(
            operation_id=operation_id,
            task_id=task.task_id,
            scheduled_at=scheduled_at,
            txn_id=transaction_id,
            event_id=event_id,
            dispatched=True,
        )

    async def record_execution(
        self,
        *,
        task_id: str,
        worker_event_id: str,
    ) -> TaskReceipt:
        task = await self._require_task(task_id)
        if (
            task.task_type not in {"infinite", "recurring"}
            or task.status != "active"
            or not task.schedule
            or not task.timezone
        ):
            raise ConflictError(
                f"task {task_id} is not an active recurring task",
            )
        seen = tuple(task.metadata.get("execution_event_ids", ()))
        operation_id = operation_id_for(
            task.room_id,
            worker_event_id,
            f"executed:{task_id}",
        )
        if worker_event_id in seen:
            return _task_receipt(operation_id, task)
        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.COMPLETE_TASK,
            target_key=f"task/{task_id}/execution",
            request={
                "task_id": task_id,
                "worker_event_id": worker_event_id,
                "action": "record_execution",
            },
        )
        remote = await self._read_task_metadata(task_id)
        if remote.last_execution_event_id == worker_event_id:
            executed_at = remote.last_executed_at
            next_at = remote.next_scheduled_at
            if executed_at is None or next_at is None:
                raise ConflictError(
                    f"execution metadata for {task_id} is incomplete",
                )
            receipt = await self.storage.head(
                f"{_task_storage_root(task)}/meta.json",
            )
            if receipt is None:
                raise NotFoundError(
                    f"task metadata does not exist: {task_id}",
                )
        else:
            executed_at = self._clock.now().astimezone(UTC)
            next_at = CronSchedule.parse(task.schedule).next_after(
                executed_at,
                task.timezone,
            )
            updated_remote = TaskMetadata.model_validate(
                {
                    **remote.model_dump(mode="json"),
                    "last_executed_at": executed_at,
                    "next_scheduled_at": next_at,
                    "last_execution_event_id": worker_event_id,
                },
            )
            receipt = await self._replace_task_metadata(
                operation,
                updated_remote,
                operation_name="record_recurring_execution",
            )
        changed = await self._tasks.transition(
            task_id,
            expected={"active"},
            target="active",
            last_executed_at=executed_at,
            next_scheduled_at=next_at,
            metadata={
                **task.metadata,
                "execution_event_ids": [*seen, worker_event_id],
                "last_execution_event_id": worker_event_id,
                "last_execution_metadata_etag": receipt.etag,
            },
        )
        task = changed or await self._require_task(task_id)
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.STORAGE,
            {
                "task_id": task_id,
                "status": "active",
                "last_executed_at": executed_at.isoformat(),
                "next_scheduled_at": next_at.isoformat(),
                "metadata_etag": receipt.etag,
            },
        )
        return _task_receipt(operation_id, task)

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
        return await self._resume_cancel(operation)

    async def _resume_cancel(
        self,
        operation: OperationRecord,
    ) -> TaskReceipt:
        request = operation.request
        task_id = str(request.get("task_id", ""))
        if (
            operation.kind is not OperationKind.COMPLETE_TASK
            or request.get("action") != "cancel"
            or not task_id
        ):
            raise ValueError("operation is not task cancellation")
        task = await self._require_task(task_id)
        if task.status == "cancelled":
            return _task_receipt(operation.operation_id, task)
        if task.status not in {"prepared", "assigned", "active"}:
            raise ConflictError(
                f"task {task_id} cannot cancel from {task.status}",
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

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> object:
        """Resume one task-owned operation from its durable request."""
        if operation.kind is OperationKind.DELEGATE_TASK:
            if operation.request.get("action") == "dispatch_project_ready":
                return await self._resume_ready_dispatch(operation)
            if operation.request.get("action") == "dispatch_recurring":
                task_id = str(operation.request.get("task_id", ""))
                task = await self._require_task(task_id)
                return await self.dispatch_recurring(task)
            if "schedule" in operation.request:
                return await self._resume_recurring_creation(operation)
            return await self.dispatch(operation)
        if operation.kind is OperationKind.COMPLETE_TASK:
            action = operation.request.get("action")
            task_id = str(operation.request.get("task_id", ""))
            if action == "cancel":
                return await self._resume_cancel(operation)
            worker_event_id = str(
                operation.request.get("worker_event_id", ""),
            )
            if not task_id or not worker_event_id:
                raise RecoveryError(
                    "task completion request is missing durable identity",
                )
            if action == "record_execution":
                return await self.record_execution(
                    task_id=task_id,
                    worker_event_id=worker_event_id,
                )
            structured = operation.request.get("structured_result")
            if structured is not None and not isinstance(structured, dict):
                raise RecoveryError(
                    "structured task result is not an object",
                )
            return await self.record_completion(
                task_id=task_id,
                worker_event_id=worker_event_id,
                structured_result=structured,
            )
        raise RecoveryError(
            f"TaskService cannot recover {operation.kind.value}",
        )

    async def _resolve_assignment(
        self,
        request: TaskCreateRequest,
    ) -> tuple[str, str, str | None]:
        return await self._resolve_destination(
            assigned_to=request.assigned_to,
            delegated_to_team=request.delegated_to_team,
            project_room_id=request.project_room_id,
        )

    async def _resolve_destination(
        self,
        *,
        assigned_to: str,
        delegated_to_team: str | None,
        project_room_id: str | None = None,
    ) -> tuple[str, str, str | None]:
        if delegated_to_team is not None:
            team = await self._controller.get_team(
                delegated_to_team,
            )
            if team is None:
                raise NotFoundError(
                    f"team/{delegated_to_team} does not exist",
                )
            leader = await self._controller.get_worker(team.leader)
            room_id = leader.room_id if leader is not None else None
            if not room_id:
                raise ConflictError(
                    f"team/{team.name} has no Manager-facing Leader Room",
                )
            matrix_user_id = (
                leader.matrix_user_id
                if leader is not None
                else None
            )
            return (
                room_id,
                matrix_user_id
                or f"@worker-{team.leader}:{self._matrix_domain}",
                team.name,
            )
        worker = await self._controller.get_worker(assigned_to)
        if worker is None:
            raise NotFoundError(
                f"worker/{assigned_to} does not exist",
            )
        room_id = worker.room_id
        if not room_id:
            raise ConflictError(
                f"worker/{assigned_to} has no authoritative room",
            )
        return (
            project_room_id or room_id,
            worker.matrix_user_id
            or f"@worker-{assigned_to}:{self._matrix_domain}",
            worker.team,
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
        task = await self._require_task(task_id)
        key = f"{_task_storage_root(task)}/meta.json"
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
        task = await self._require_task(metadata.task_id)
        key = f"{_task_storage_root(task)}/meta.json"
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
            "defer_dispatch": request.defer_dispatch,
            "metadata": request.metadata,
        }
        actual = {
            "title": task.title,
            "assigned_to": task.assigned_to,
            "project_id": task.project_id,
            "delegated_to_team": task.delegated_to_team,
            "operation_id": task.metadata.get("operation_id"),
            "defer_dispatch": bool(
                task.metadata.get("defer_dispatch", False),
            ),
            "metadata": {
                key: task.metadata.get(key)
                for key in request.metadata
            },
        }
        if actual != expected:
            raise ConflictError(
                f"task {task.task_id} does not match its journal request",
            )

    @staticmethod
    def _verify_recurring_request(
        task: TaskRecord,
        operation: OperationRecord,
        request: RecurringTaskCreateRequest,
    ) -> None:
        TaskService._verify_task_request(task, operation, request)
        if (
            task.task_type not in {"infinite", "recurring"}
            or task.schedule != request.schedule
            or task.timezone != request.timezone
            or task.status != "active"
        ):
            raise ConflictError(
                f"recurring task {task.task_id} does not match its request",
            )


def _task_id_for(operation: OperationRecord) -> str:
    timestamp = operation.created_at.astimezone(UTC)
    return (
        f"task-{timestamp:%Y%m%d-%H%M%S}-"
        f"{operation.operation_id[:6]}"
    )


def _task_storage_root(task: TaskRecord) -> str:
    team_name = str(
        task.delegated_to_team
        or task.metadata.get("storage_team_name")
        or "",
    ).strip() or None
    return _task_storage_root_for(
        task_id=task.task_id,
        team_name=team_name,
    )


def _task_storage_root_for(
    *,
    task_id: str,
    team_name: str | None,
) -> str:
    if not team_name:
        return f"shared/tasks/{task_id}"
    if team_name in {".", ".."} or "/" in team_name or "\\" in team_name:
        raise RecoveryError(
            f"task {task_id} has an invalid storage Team name",
        )
    return f"teams/{team_name}/shared/tasks/{task_id}"


def _task_specification(
    request: TaskCreateRequest,
    *,
    task_id: str,
    manager_user_id: str,
) -> str:
    specification = (
        f"{request.specification.rstrip()}\n\n"
        "## AgentTeams result contract (required)\n\n"
        "Before reporting completion, publish `result.md` and update "
        "`meta.json` through the task-management/file-sync tools. The "
        "result must contain these fields:\n\n"
        "```text\n"
        "STATUS: SUCCESS | SUCCESS_WITH_NOTES | REVISION_NEEDED | "
        "BLOCKED | INTERRUPTED\n"
        "SUMMARY: <concise factual outcome>\n"
        "DELIVERABLES:\n"
        f"- shared/tasks/{task_id}/result.md\n"
        "```\n\n"
        "A SUCCESS result is only a candidate. The Manager must inspect and "
        "accept it before the task becomes completed or dependent work starts."
    )
    if not request.delegated_to_team:
        return specification
    return (
        f"{specification}\n\n"
        "## AgentTeams parent-task completion protocol (required)\n\n"
        f"This task was delegated to Team `{request.delegated_to_team}` by "
        f"`{manager_user_id}`. After coordinating the Team and accepting its "
        "Worker results:\n\n"
        f"1. Use Project Work with project id `{task_id}`. Child Worker task "
        f"ids must be distinct from the parent task id.\n"
        f"2. Write the final project report to "
        f"`shared/projects/{task_id}/result.md`.\n"
        f"3. Call `projectflow complete_project` for `{task_id}`. TeamHarness "
        f"mirrors the report to `shared/tasks/{task_id}/result.md`, publishes "
        "it and the submitted parent `meta.json` with file sync, and sends the "
        "structured Manager notification "
        f"`{manager_user_id} TASK_COMPLETED: {task_id}`.\n"
        "4. Verify `parentTaskCompletion.synced` is true and "
        "`parentTaskCompletion.notification.status` is `sent`.\n"
        "5. Do not hand-edit or re-push the parent metadata. Do not send a "
        "duplicate completion reply or send the result directly to the Admin "
        "room. The Manager owns the Admin notification after it records "
        "completion.\n"
    )


def _task_metadata(
    task: TaskRecord,
    *,
    status: TaskDocumentStatus,
    manager_user_id: str,
) -> TaskMetadata:
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
        source_room_id=(
            str(task.metadata["requester_room_id"])
            if task.metadata.get("requester_room_id")
            else None
        ),
        coordinator_matrix_user_id=(
            str(task.metadata["coordinator_matrix_user_id"])
            if task.metadata.get("coordinator_matrix_user_id")
            else manager_user_id
        ),
        project_id=task.project_id,
        schedule=task.schedule,
        timezone=task.timezone,
        last_executed_at=task.last_executed_at,
        next_scheduled_at=task.next_scheduled_at,
        last_execution_event_id=(
            str(task.metadata["last_execution_event_id"])
            if task.metadata.get("last_execution_event_id")
            else None
        ),
        created_at=task.created_at,
        completed_at=(
            datetime.fromisoformat(str(completed_at_raw))
            if completed_at_raw
            else None
        ),
    )


def _task_document_status(status: str) -> TaskDocumentStatus:
    allowed = {
        "prepared",
        "assigned",
        "active",
        "pending",
        "ready",
        "dispatched",
        "in_progress",
        "blocked",
        "revision_needed",
        "completed",
        "failed",
        "cancelled",
    }
    if status not in allowed:
        raise RecoveryError(f"invalid durable task status {status!r}")
    return cast(TaskDocumentStatus, status)


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
        summary=summary or _completion_summary(task),
        last_executed_at=task.last_executed_at,
        next_scheduled_at=task.next_scheduled_at,
        result_status=(
            str(task.metadata["result_status"])
            if task.metadata.get("result_status")
            else None
        ),
        deliverables=tuple(
            str(item)
            for item in task.metadata.get("result_deliverables", ())
        ),
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


def _parse_task_result_submission(
    task: TaskRecord,
    remote: TaskMetadata,
    *,
    result_text: str,
    structured_result: dict[str, Any] | None,
    destination: Path,
) -> TaskResultSubmission:
    structured = structured_result or {}
    if not result_text and not structured and remote.result_status is None:
        raise TaskResultMissing(
            f"task {task.task_id} has no result.md or structured result",
        )

    status_value = (
        remote.result_status
        or structured.get("result_status")
        or structured.get("status")
        or structured.get("outcome")
        or _markdown_field(result_text, "STATUS")
        or _markdown_field(result_text, "OUTCOME")
    )
    status = str(status_value or "").strip().upper()
    allowed_statuses = {
        "SUCCESS",
        "SUCCESS_WITH_NOTES",
        "REVISION_NEEDED",
        "BLOCKED",
        "INTERRUPTED",
        "FAILED",
        "PARTIAL",
    }
    if status not in allowed_statuses:
        raise TaskResultInvalid(
            f"task {task.task_id} result status is missing or invalid",
        )

    summary_value = (
        remote.summary
        or structured.get("summary")
        or _markdown_field(result_text, "SUMMARY")
    )
    if summary_value is None and result_text:
        summary_value = _summarize(result_text)
    summary = str(summary_value or "").strip()
    if not summary:
        raise TaskResultInvalid(
            f"task {task.task_id} result summary is missing",
        )

    deliverable_values: object = (
        remote.deliverables
        or structured.get("deliverables")
        or _markdown_deliverables(result_text)
    )
    raw_deliverables: tuple[str, ...]
    if isinstance(deliverable_values, str):
        raw_deliverables = (deliverable_values,)
    elif isinstance(deliverable_values, (list, tuple)):
        raw_deliverables = tuple(str(item) for item in deliverable_values)
    else:
        raw_deliverables = ()
    default_result_path = f"shared/tasks/{task.task_id}/result.md"
    if not raw_deliverables and result_text:
        raw_deliverables = (default_result_path,)
    deliverables = tuple(
        dict.fromkeys(
            _normalize_result_path(task, value)
            for value in raw_deliverables
            if value.strip()
        ),
    )
    if not deliverables:
        raise TaskResultInvalid(
            f"task {task.task_id} result deliverables are missing",
        )

    result_path = _normalize_result_path(
        task,
        str(
            remote.result_path
            or structured.get("result_path")
            or structured.get("resultPath")
            or (
                default_result_path
                if result_text
                else deliverables[0]
            )
        ),
    )
    if result_path not in deliverables:
        deliverables = (result_path, *deliverables)

    artifact_paths = tuple(
        _local_result_artifact(
            task,
            path,
            destination=destination,
        )
        for path in deliverables
    )
    missing = tuple(
        path
        for path, local in zip(
            deliverables,
            artifact_paths,
            strict=True,
        )
        if not local.exists()
    )
    if missing:
        raise TaskResultInvalid(
            f"task {task.task_id} deliverables do not exist: "
            f"{', '.join(missing)}",
        )

    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "task_id": task.task_id,
                "status": status,
                "summary": summary,
                "deliverables": deliverables,
                "result_path": result_path,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8"),
    )
    for relative, local in zip(
        deliverables,
        artifact_paths,
        strict=True,
    ):
        digest.update(b"\0")
        digest.update(relative.encode("utf-8"))
        if local.is_dir():
            files = tuple(
                sorted(
                    item
                    for item in local.rglob("*")
                    if item.is_file()
                ),
            )
        else:
            files = (local,)
        for item in files:
            digest.update(b"\0")
            digest.update(
                item.relative_to(destination).as_posix().encode("utf-8"),
            )
            with item.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
    return TaskResultSubmission(
        task_id=task.task_id,
        status=status,  # type: ignore[arg-type]
        summary=summary,
        deliverables=deliverables,
        result_path=result_path,
        digest=digest.hexdigest(),
    )


def _markdown_field(body: str, name: str) -> str | None:
    match = re.search(
        rf"(?im)^\s*(?:#{{1,6}}\s*)?(?:[-*]\s+)?"
        rf"(?:\*\*|__)?{re.escape(name)}(?:\*\*|__)?"
        rf"\s*:\s*(.+?)\s*$",
        body,
    )
    if match is None:
        return None
    value = match.group(1).strip()
    for marker in ("`", "**", "__"):
        if value.startswith(marker) and value.endswith(marker):
            value = value[len(marker) : -len(marker)].strip()
    return value or None


def _markdown_deliverables(body: str) -> tuple[str, ...]:
    match = re.search(
        r"(?ims)^\s*(?:#{1,6}\s*)?"
        r"(?:\*\*|__)?DELIVERABLES(?:\*\*|__)?\s*:\s*$"
        r"(?P<body>.*?)(?=^\s*(?:#{1,6}\s*)?"
        r"(?:\*\*|__)?[A-Z][A-Z_ ]+(?:\*\*|__)?\s*:|\Z)",
        body,
    )
    if match is None:
        return ()
    values: list[str] = []
    for line in match.group("body").splitlines():
        value = re.sub(r"^\s*[-*]\s+", "", line).strip().strip("`")
        if value:
            values.append(value)
    return tuple(values)


def _normalize_result_path(task: TaskRecord, value: str) -> str:
    normalized = value.strip().strip("`").replace("\\", "/")
    normalized = normalized.removeprefix("./")
    if not normalized or normalized.startswith("/") or ":" in normalized:
        raise TaskResultInvalid(
            f"task {task.task_id} has an invalid deliverable path",
        )
    parts = tuple(part for part in normalized.split("/") if part)
    if ".." in parts:
        raise TaskResultInvalid(
            f"task {task.task_id} deliverable escapes its task directory",
        )
    canonical_root = f"shared/tasks/{task.task_id}"
    storage_root = _task_storage_root(task)
    if normalized == canonical_root or normalized == storage_root:
        return normalized
    for root in (canonical_root, storage_root):
        prefix = f"{root}/"
        if normalized.startswith(prefix):
            relative = normalized.removeprefix(prefix)
            if relative:
                return f"{canonical_root}/{relative}"
    raise TaskResultInvalid(
        f"task {task.task_id} deliverable is outside its task directory",
    )


def _local_result_artifact(
    task: TaskRecord,
    value: str,
    *,
    destination: Path,
) -> Path:
    canonical_root = f"shared/tasks/{task.task_id}"
    relative = value.removeprefix(f"{canonical_root}/")
    candidate = (destination / relative).resolve()
    try:
        candidate.relative_to(destination.resolve())
    except ValueError as exc:
        raise TaskResultInvalid(
            f"task {task.task_id} deliverable escapes its cache",
        ) from exc
    return candidate


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
