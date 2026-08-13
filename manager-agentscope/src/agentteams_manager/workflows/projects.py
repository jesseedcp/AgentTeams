"""Durable projects spanning SQLite, MinIO, Matrix, and task workflows.

协调跨 SQLite、MinIO、Matrix 与 Task workflow 的完整 Project 生命周期。

创建 Project 先保存 planning 记录与计划 artifact，再创建 Project Room；只有管理员后来
明确确认计划，才进入 active 并开放 DAG 首批任务。参与者变更、计划修订、任务返修、
重分配和关闭都保留版本与决策证据。``/elevated full`` 只跳过 tool 确认，不替代项目
计划确认；只有显式 YOLO policy 可自动确认。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

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
    ProjectMetadata,
    ProjectPlan,
    ProjectRecord,
    TaskRecord,
    WorkerResource,
)
from agentteams_manager.domain.ports import (
    ArtifactPort,
    Clock,
    MatrixAdministrationPort,
    MatrixPort,
)
from agentteams_manager.state.tasks import ProjectPlanRevision
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import (
    TaskControllerPort,
    TaskReceipt,
    TaskService,
    TaskSupervisorPort,
)

ProjectStatus = Literal["planning", "active", "completed", "cancelled"]


class ProjectMatrixPort(
    MatrixPort,
    MatrixAdministrationPort,
    Protocol,
):
    """Combined Matrix surface required by project workflows."""


class ProjectRepositoryPort(Protocol):
    async def create(self, project: ProjectRecord) -> ProjectRecord: ...

    async def get(self, project_id: str) -> ProjectRecord | None: ...

    async def update(
        self,
        project_id: str,
        *,
        expected: set[str],
        status: str,
        room_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ProjectRecord | None: ...


class ProjectTaskReader(Protocol):
    async def list_by_project(
        self,
        project_id: str,
    ) -> tuple[TaskRecord, ...]: ...


class ProjectTopologyPort(Protocol):
    async def upsert_project(
        self,
        *,
        project_id: str,
        room_id: str,
        payload: dict[str, object],
        refreshed_at: datetime,
    ) -> None: ...


class ProjectGraphPort(Protocol):
    async def add_participant(
        self,
        project_id: str,
        worker_name: str,
        *,
        now: datetime,
    ) -> None: ...

    async def append_plan_revision(
        self,
        project_id: str,
        *,
        body: str,
        change_kind: str,
        created_by: str,
        now: datetime,
    ) -> ProjectPlanRevision: ...

    async def update_participants(
        self,
        project_id: str,
        *,
        add: tuple[str, ...],
        remove: tuple[str, ...],
        worker_users: dict[str, str],
        now: datetime,
    ) -> tuple[str, ...]: ...

    async def revise_plan(
        self,
        project_id: str,
        *,
        body: str,
        change_kind: str,
        reason: str,
        created_by: str,
        now: datetime,
    ) -> ProjectPlanRevision: ...

    async def set_dependencies(
        self,
        task_id: str,
        dependencies: tuple[str, ...],
    ) -> None: ...

    async def promote_ready(
        self,
        project_id: str,
    ) -> tuple[TaskRecord, ...]: ...

    async def transition(
        self,
        task_id: str,
        *,
        expected: set[Any],
        target: Any,
        actor_id: str,
        reason: str | None = None,
    ) -> TaskRecord: ...

    async def reassign(
        self,
        task_id: str,
        *,
        assigned_to: str,
        room_id: str,
        matrix_user_id: str,
        storage_team_name: str | None,
        actor_id: str,
        reason: str,
        operation_id: str,
    ) -> TaskRecord: ...


class ProjectMemoryPort(Protocol):
    async def record_project_decision(
        self,
        *,
        room_id: str,
        source_event_id: str,
        project_id: str,
        decision: str,
        rationale: str,
        visibility: Literal["private", "project"] = "private",
    ) -> object: ...


class ProjectCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    plan: str = Field(min_length=1)
    participants: tuple[str, ...] = Field(min_length=1)
    requester_room_id: str = Field(min_length=1)


class ProjectReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    project_id: str
    title: str
    status: str
    room_id: str
    participants: tuple[str, ...]
    task_ids: tuple[str, ...] = ()
    confirmed_at: datetime | None = None
    confirmed_by: str | None = None


class ProjectService:
    """维护 Project plan、参与者、DAG 和任务结果之间的一致性。

    每次变更都保留 revision/decision；依赖节点只有在上游 accepted 后才 ready。需要返修时
    创建关联替代 Task 并保持下游关闭，而不是篡改已经提交的原 Task 历史。
    """

    def __init__(
        self,
        *,
        projects: ProjectRepositoryPort,
        tasks: ProjectTaskReader,
        task_service: TaskService,
        storage: ArtifactPort,
        controller: TaskControllerPort,
        matrix: ProjectMatrixPort,
        topology: ProjectTopologyPort,
        graph: ProjectGraphPort,
        supervisor: TaskSupervisorPort,
        clock: Clock,
        admin_user_id: str,
        manager_user_id: str,
        memory: ProjectMemoryPort | None = None,
    ) -> None:
        if not admin_user_id or not manager_user_id:
            raise ValueError("admin and Manager Matrix IDs are required")
        self._projects = projects
        self._tasks = tasks
        self._task_service = task_service
        self._storage = storage
        self._controller = controller
        self._matrix = matrix
        self._topology = topology
        self._graph = graph
        self._supervisor = supervisor
        self._clock = clock
        self._admin_user_id = admin_user_id
        self._manager_user_id = manager_user_id
        self._memory = memory

    async def create(
        self,
        *,
        title: str,
        description: str,
        plan: str,
        participants: tuple[str, ...],
        context: MutationContext,
    ) -> ProjectReceipt:
        normalized_participants = tuple(dict.fromkeys(participants))
        request = ProjectCreateRequest(
            title=title,
            description=description,
            plan=plan,
            participants=normalized_participants,
            requester_room_id=context.room_id,
        )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CREATE_PROJECT,
            target_key=f"project-request/{context.operation_id}",
            request=request.model_dump(mode="json"),
        )
        return await self.resume_create(operation)

    async def resume_create(
        self,
        operation: OperationRecord,
    ) -> ProjectReceipt:
        if operation.kind is not OperationKind.CREATE_PROJECT:
            raise ValueError("operation is not project creation")
        if operation.status is OperationStatus.FAILED:
            raise ConflictError("project creation previously failed")
        request = ProjectCreateRequest.model_validate(operation.request)
        project_id = _project_id_for(operation)
        project = await self._projects.get(project_id)
        worker_users: dict[str, str] = {}
        for name in request.participants:
            worker = await self._controller.get_worker(name)
            if worker is None:
                raise NotFoundError(f"worker/{name} does not exist")
            worker_users[name] = (
                worker.matrix_user_id
                or _fallback_worker_user(worker)
            )

        if project is None:
            now = self._clock.now().astimezone(UTC)
            project = ProjectRecord(
                project_id=project_id,
                name=request.title,
                room_id="",
                status="planning",
                metadata={
                    "operation_id": operation.operation_id,
                    "description": request.description,
                    "plan": request.plan,
                    "participants": list(request.participants),
                    "worker_users": worker_users,
                    "requester_room_id": request.requester_room_id,
                    "task_ids": [],
                    "plan_revision": 1,
                    "plan_confirmation_status": "pending",
                },
                created_at=now,
                updated_at=now,
            )
            try:
                project = await self._projects.create(project)
            except Exception:
                raced = await self._projects.get(project_id)
                if raced is None:
                    raise
                project = raced
        self._verify_project_request(project, operation, request)
        for participant in request.participants:
            await self._graph.add_participant(
                project_id,
                participant,
                now=project.created_at,
            )
        await self._graph.append_plan_revision(
            project_id,
            body=request.plan,
            change_kind="initial",
            created_by=self._admin_user_id,
            now=project.created_at,
        )
        if operation.status is OperationStatus.SUCCEEDED:
            if project.status not in {"planning", "active"} or not project.room_id:
                raise ConflictError(
                    f"succeeded project {project_id} lacks prepared state",
                )
            return _project_receipt(operation.operation_id, project)

        initial_metadata = _project_metadata(project)
        await self._ensure_json(
            operation,
            f"shared/projects/{project_id}/meta.json",
            initial_metadata.model_dump(mode="json"),
            operation_name="write_project_metadata",
        )
        if project.status == "planning":
            initial_plan = _project_plan(project).render().encode("utf-8")
            await self._ensure_bytes(
                operation,
                f"shared/projects/{project_id}/plan.md",
                initial_plan,
                operation_name="write_project_plan",
            )

        room_id = project.room_id or await self._find_project_room(project_id)
        expected_members = tuple(
            dict.fromkeys(
                (
                    self._admin_user_id,
                    self._manager_user_id,
                    *worker_users.values(),
                ),
            ),
        )
        if not room_id:
            if operation.status is OperationStatus.RECONCILING:
                raise AmbiguousEffectError(
                    f"project room for {project_id} has no visible marker",
                )
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {
                    "operation": "create_project_room",
                    "project_id": project_id,
                    "invite": [],
                    "ensure_members": list(expected_members),
                },
            )
            try:
                room_id = await self._matrix.create_private_room(
                    name=request.title,
                    topic=f"AgentTeams project {project_id}",
                    invite=(),
                    creation_marker={
                        "kind": "project",
                        "operation_id": operation.operation_id,
                        "m.agentteams.project_id": project_id,
                        "m.agentteams.schema_version": 1,
                    },
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    exc,
                )
                room_id = await self._find_project_room(project_id)
                if not room_id:
                    raise AmbiguousEffectError(
                        f"project room creation for {project_id} "
                        "has no recoverable marker",
                    ) from exc
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {"project_id": project_id, "room_id": room_id},
            )

        await self._ensure_members(
            operation,
            room_id,
            expected_members,
        )
        prepared_metadata_values = {
            **project.metadata,
            "room_id": room_id,
            "plan_confirmation_status": (
                "confirmed"
                if project.status == "active"
                else "pending"
            ),
        }
        if project.room_id != room_id:
            changed = await self._projects.update(
                project_id,
                expected={"planning", "active"},
                status=project.status,
                room_id=room_id,
                metadata=prepared_metadata_values,
            )
            project = changed or await self._require_project(project_id)
        if (
            project.status not in {"planning", "active"}
            or project.room_id != room_id
        ):
            raise ConflictError(
                f"project {project_id} did not converge to prepared",
            )

        await self._topology.upsert_project(
            project_id=project_id,
            room_id=room_id,
            payload=_project_metadata(project).model_dump(mode="json"),
            refreshed_at=self._clock.now().astimezone(UTC),
        )
        metadata_receipt = await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="publish_prepared_project",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="publish_prepared_project_plan",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "project_id": project_id,
                "room_id": room_id,
                "status": project.status,
                "metadata_etag": metadata_receipt.etag,
            },
        )
        return _project_receipt(operation.operation_id, project)

    async def confirm_plan(
        self,
        *,
        project_id: str,
        confirmed_by: str,
        context: MutationContext,
        auto_confirmed: bool = False,
    ) -> ProjectReceipt:
        """Activate a prepared project after the plan decision is explicit."""

        project = await self._require_project(project_id)
        confirmation_policy = "yolo" if auto_confirmed else "manual"
        if confirmed_by != self._admin_user_id:
            raise ConflictError("only the administrator may confirm a plan")
        if not project.room_id:
            raise ConflictError(
                f"project {project_id} has no prepared Matrix room",
            )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}/plan-confirmation",
            request={
                "action": "confirm_project_plan",
                "project_id": project_id,
                "confirmed_by": confirmed_by,
                "auto_confirmed": auto_confirmed,
                "confirmation_policy": confirmation_policy,
                "source_room_id": context.room_id,
                "source_event_id": context.event_id,
                "source_tool_call_id": context.tool_call_id,
            },
        )
        if project.status == "active":
            if (
                project.metadata.get("plan_confirmation_operation_id")
                != operation.operation_id
                and operation.status is not OperationStatus.SUCCEEDED
            ):
                raise ConflictError(
                    f"project {project_id} was confirmed by another request",
                )
        elif project.status != "planning":
            raise ConflictError(
                f"project {project_id} cannot be confirmed from "
                f"{project.status}",
            )

        if operation.status is OperationStatus.SUCCEEDED:
            await self._remember_project_decision(
                operation,
                project,
                decision=(
                    "Confirmed project plan revision "
                    f"{project.metadata.get('plan_revision', 1)}"
                ),
                rationale=(
                    "Automatically confirmed by configured YOLO policy."
                    if auto_confirmed
                    else "Explicitly confirmed by the administrator."
                ),
            )
            return _project_receipt(operation.operation_id, project)

        if project.status == "planning":
            confirmed_at = self._clock.now().astimezone(UTC)
            changed = await self._projects.update(
                project_id,
                expected={"planning"},
                status="active",
                room_id=project.room_id,
                metadata={
                    **project.metadata,
                    "confirmed_at": confirmed_at.isoformat(),
                    "confirmed_by": confirmed_by,
                    "plan_confirmation_status": "confirmed",
                    "plan_confirmation_operation_id": operation.operation_id,
                    "plan_auto_confirmed": auto_confirmed,
                    "plan_confirmation_policy": confirmation_policy,
                },
            )
            project = changed or await self._require_project(project_id)
        if project.status != "active":
            raise ConflictError(
                f"project {project_id} did not converge to active",
            )

        metadata_receipt = await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="confirm_project_plan",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="confirm_project_plan_document",
        )
        await self._topology.upsert_project(
            project_id=project_id,
            room_id=project.room_id,
            payload=_project_metadata(project).model_dump(mode="json"),
            refreshed_at=self._clock.now().astimezone(UTC),
        )
        txn_id = matrix_transaction_id(operation.operation_id, 0)
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "announce_project_plan_confirmation",
                "project_id": project_id,
                "room_id": project.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                project.room_id,
                "[Project Plan Confirmed]\n\n"
                f"{_project_plan(project).render()}",
                txn_id=txn_id,
                mentions=(self._admin_user_id,),
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.MATRIX,
                exc,
            )
            raise
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "room_id": project.room_id,
                "status": "active",
                "metadata_etag": metadata_receipt.etag,
                "event_id": event_id,
            },
        )
        await self._remember_project_decision(
            operation,
            project,
            decision=(
                "Confirmed project plan revision "
                f"{project.metadata.get('plan_revision', 1)}"
            ),
            rationale=(
                "Automatically confirmed by configured YOLO policy."
                if auto_confirmed
                else "Explicitly confirmed by the administrator."
            ),
        )
        return _project_receipt(operation.operation_id, project)

    async def add_task(
        self,
        *,
        project_id: str,
        title: str,
        specification: str,
        assigned_to: str,
        context: MutationContext,
        delegated_to_team: str | None = None,
        dependencies: tuple[str, ...] = (),
        metadata: dict[str, Any] | None = None,
    ) -> TaskReceipt:
        project = await self._require_project(project_id)
        if project.status != "active":
            raise ConflictError(
                f"project {project_id} is not active",
            )
        participants = tuple(project.metadata.get("participants", ()))
        if assigned_to not in participants and delegated_to_team is None:
            raise ConflictError(
                f"worker/{assigned_to} is not a project participant",
            )
        task = await self._task_service.create_finite(
            title=title,
            spec=specification,
            assigned_to=assigned_to,
            delegated_to_team=delegated_to_team,
            project_id=project_id,
            project_room_id=project.room_id,
            context=context,
            defer_dispatch=True,
            metadata=metadata,
        )
        await self._graph.set_dependencies(task.task_id, dependencies)
        for ready in await self._graph.promote_ready(project_id):
            dispatched = await self._task_service.dispatch_ready(
                task_id=ready.task_id,
                context=MutationContext(
                    room_id=project.room_id,
                    event_id=context.event_id,
                    tool_call_id=(
                        f"{context.tool_call_id}:ready:{ready.task_id}"
                    ),
                ),
            )
            if ready.task_id == task.task_id:
                task = dispatched
        update_operation_id = operation_id_for(
            context.room_id,
            context.event_id,
            f"{context.tool_call_id}:project-index",
        )
        operation = await self._supervisor.begin(
            operation_id=update_operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}",
            request={
                "action": "add_task",
                "project_id": project_id,
                "task_id": task.task_id,
            },
        )
        indexed_metadata = await self._project_task_index(project)
        changed = await self._projects.update(
            project_id,
            expected={"active"},
            status="active",
            metadata=indexed_metadata,
        )
        project = changed or await self._require_project(project_id)
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="add_project_task",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="update_project_plan",
        )
        transaction_id = matrix_transaction_id(update_operation_id, 0)
        await self._supervisor.before_effect(
            update_operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "announce_project_task",
                "project_id": project_id,
                "task_id": task.task_id,
                "room_id": project.room_id,
                "txn_id": transaction_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                project.room_id,
                f"[Project Task Assigned] {task.task_id}: {title} "
                f"to {assigned_to}.",
                txn_id=transaction_id,
            )
        except Exception as exc:
            await self._record_external_failure(
                update_operation_id,
                ExternalEffect.MATRIX,
                exc,
            )
            raise
        await self._supervisor.effect_succeeded(
            update_operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "task_id": task.task_id,
                "event_id": event_id,
            },
        )
        return task

    async def request_revision(
        self,
        *,
        project_id: str,
        task_id: str,
        feedback: str,
        assigned_to: str | None,
        triggered_by_task_id: str | None,
        context: MutationContext,
    ) -> TaskReceipt:
        """Create a linked rework task without erasing the original task."""

        from agentteams_manager.state.tasks import ProjectTaskState

        if not feedback.strip():
            raise ValueError("revision feedback must not be empty")
        project = await self._require_project(project_id)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}/revision/{task_id}",
            request={
                "action": "request_revision",
                "project_id": project_id,
                "task_id": task_id,
                "feedback": feedback.strip(),
                "assigned_to": assigned_to,
                "triggered_by_task_id": triggered_by_task_id,
                "source_room_id": context.room_id,
                "source_event_id": context.event_id,
                "source_tool_call_id": context.tool_call_id,
            },
        )
        project_tasks = await self._tasks.list_by_project(project_id)
        original = next(
            (
                task
                for task in project_tasks
                if task.task_id == task_id
            ),
            None,
        )
        if original is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        if operation.status is OperationStatus.SUCCEEDED:
            revision = next(
                (
                    item
                    for item in project_tasks
                    if item.metadata.get("revision_request_operation_id")
                    == operation.operation_id
                ),
                None,
            )
            if revision is None:
                raise RecoveryError(
                    "succeeded revision request has no linked task",
                )
            return _task_receipt_from_record(
                operation.operation_id,
                revision,
            )
        revision_assignee = assigned_to or original.assigned_to
        participants = tuple(project.metadata.get("participants", ()))
        if revision_assignee not in participants:
            raise ConflictError(
                f"worker/{revision_assignee} is not a project participant",
            )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "request_project_task_revision",
                "project_id": project_id,
                "task_id": task_id,
                "assigned_to": revision_assignee,
            },
        )
        revision = next(
            (
                item
                for item in project_tasks
                if (
                    item.metadata.get("revision_request_operation_id")
                    == operation.operation_id
                    or (
                        item.metadata.get("is_revision_for")
                        == task_id
                        and item.metadata.get("revision_feedback")
                        == feedback.strip()
                        and item.assigned_to == revision_assignee
                    )
                )
                and item.status
                not in {"completed", "cancelled", "failed"}
            ),
            None,
        )
        if revision is not None:
            await self._supervisor.effect_succeeded(
                operation.operation_id,
                ExternalEffect.STORAGE,
                {
                    "project_id": project_id,
                    "task_id": task_id,
                    "revision_task_id": revision.task_id,
                    "reused": True,
                },
            )
            return _task_receipt_from_record(
                operation.operation_id,
                revision,
            )
        if original.status != ProjectTaskState.REVISION_NEEDED:
            await self._graph.transition(
                task_id,
                expected={
                    ProjectTaskState.DISPATCHED,
                    ProjectTaskState.IN_PROGRESS,
                    ProjectTaskState.BLOCKED,
                },
                target=ProjectTaskState.REVISION_NEEDED,
                actor_id=self._admin_user_id,
                reason=feedback,
            )
        try:
            receipt = await self.add_task(
                project_id=project_id,
                title=f"Revision: {original.title}",
                specification=(
                    f"Revise task {task_id} using this feedback:\n\n"
                    f"{feedback.strip()}"
                ),
                assigned_to=revision_assignee,
                context=MutationContext(
                    room_id=context.room_id,
                    event_id=context.event_id,
                    tool_call_id=f"{context.tool_call_id}:revision-task",
                ),
                metadata={
                    "is_revision_for": task_id,
                    "triggered_by_task_id": triggered_by_task_id,
                    "revision_feedback": feedback.strip(),
                    "revision_request_operation_id": operation.operation_id,
                    "revision_source_event_id": context.event_id,
                },
            )
        except Exception:
            revision = next(
                (
                    item
                    for item in await self._tasks.list_by_project(project_id)
                    if item.metadata.get("revision_request_operation_id")
                    == operation.operation_id
                ),
                None,
            )
            current_project = await self._require_project(project_id)
            indexed_ids = set(
                current_project.metadata.get("task_ids", ()),
            )
            if revision is None or revision.task_id not in indexed_ids:
                raise
            receipt = _task_receipt_from_record(
                operation.operation_id,
                revision,
            )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "project_id": project_id,
                "task_id": task_id,
                "revision_task_id": receipt.task_id,
            },
        )
        return receipt

    async def reassign_task(
        self,
        *,
        project_id: str,
        task_id: str,
        assigned_to: str,
        reason: str,
        context: MutationContext,
    ) -> TaskReceipt:
        """Move one live project task and revoke the previous assignee."""

        if not reason.strip():
            raise ValueError("reassignment reason must not be empty")
        project = await self._require_project(project_id)
        participants = tuple(project.metadata.get("participants", ()))
        if assigned_to not in participants:
            raise ConflictError(
                f"worker/{assigned_to} is not a project participant",
            )
        task = next(
            (
                item
                for item in await self._tasks.list_by_project(project_id)
                if item.task_id == task_id
            ),
            None,
        )
        if task is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        worker = await self._controller.get_worker(assigned_to)
        if worker is None:
            raise NotFoundError(f"worker/{assigned_to} does not exist")
        if not worker.room_id:
            raise ConflictError(
                f"worker/{assigned_to} has no authoritative room",
            )
        room_id = project.room_id
        matrix_user_id = worker.matrix_user_id or _fallback_worker_user(worker)
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}/task/{task_id}/assignee",
            request={
                "action": "reassign_task",
                "project_id": project_id,
                "task_id": task_id,
                "assigned_to": assigned_to,
                "previous_assigned_to": task.assigned_to,
                "storage_team_name": worker.team,
                "reason": reason.strip(),
                "source_room_id": context.room_id,
                "source_event_id": context.event_id,
                "source_tool_call_id": context.tool_call_id,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            current = next(
                (
                    item
                    for item in await self._tasks.list_by_project(project_id)
                    if item.task_id == task_id
                ),
                None,
            )
            if current is None:
                raise RecoveryError(
                    "succeeded reassignment has no durable task",
                )
            return _task_receipt_from_record(
                operation.operation_id,
                current,
            )
        storage_team_name = (
            str(operation.request.get("storage_team_name") or "").strip()
            or None
        )
        await self._task_service.prepare_reassignment_storage(
            task_id=task_id,
            storage_team_name=storage_team_name,
            operation=operation,
        )
        if (
            task.assigned_to == assigned_to
            and task.metadata.get("reassignment_operation_id")
            == operation.operation_id
            and task.status in {"ready", "dispatched"}
        ):
            changed = task
        else:
            changed = await self._graph.reassign(
                task_id,
                assigned_to=assigned_to,
                room_id=room_id,
                matrix_user_id=matrix_user_id,
                storage_team_name=storage_team_name,
                actor_id=self._admin_user_id,
                reason=reason.strip(),
                operation_id=operation.operation_id,
            )
        if changed.status == "pending":
            promoted = await self._graph.promote_ready(project_id)
            changed = next(
                (item for item in promoted if item.task_id == task_id),
                changed,
            )
        if changed.status != "ready":
            raise ConflictError(
                f"task/{task_id} cannot be dispatched after reassignment",
            )
        receipt = await self._task_service.dispatch_ready(
            task_id=task_id,
            context=MutationContext(
                room_id=project.room_id,
                event_id=context.event_id,
                tool_call_id=f"{context.tool_call_id}:reassign",
            ),
        )
        operation_id = operation.operation_id
        indexed_metadata = await self._project_task_index(project)
        updated = await self._projects.update(
            project_id,
            expected={"active"},
            status="active",
            metadata=indexed_metadata,
        )
        project = updated or await self._require_project(project_id)
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="reassign_project_task",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="reassign_project_task_plan",
        )
        txn_id = matrix_transaction_id(operation_id, 0)
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "announce_project_task_reassignment",
                "project_id": project_id,
                "task_id": task_id,
                "room_id": project.room_id,
                "txn_id": txn_id,
            },
        )
        event_id = await self._matrix.send_text(
            project.room_id,
            f"[Project Task Reassigned] {task_id}: "
            f"{operation.request.get('previous_assigned_to')} -> "
            f"{assigned_to}. {reason.strip()}",
            txn_id=txn_id,
        )
        await self._supervisor.effect_succeeded(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "task_id": task_id,
                "event_id": event_id,
            },
        )
        return receipt

    async def update_participants(
        self,
        *,
        project_id: str,
        add: tuple[str, ...],
        remove: tuple[str, ...],
        reason: str,
        context: MutationContext,
        _recovering: bool = False,
        _recovery_worker_users: dict[str, str] | None = None,
    ) -> ProjectReceipt:
        """Synchronize durable participants with Matrix membership."""

        additions = tuple(dict.fromkeys(add))
        removals = tuple(dict.fromkeys(remove))
        if not reason.strip():
            raise ValueError("participant change reason must not be empty")
        overlap = set(additions) & set(removals)
        if overlap:
            raise ValueError(
                "participants cannot be added and removed together: "
                + ", ".join(sorted(overlap)),
            )
        if not additions and not removals:
            raise ValueError("participant change must add or remove a worker")
        project = await self._require_project(project_id)
        if project.status not in {"planning", "active"}:
            raise ConflictError(
                f"project {project_id} cannot update participants from "
                f"{project.status}",
            )
        current = tuple(project.metadata.get("participants", ()))
        worker_users = dict(project.metadata.get("worker_users", {}))
        worker_users.update(_recovery_worker_users or {})
        for worker_name in additions:
            worker = await self._controller.get_worker(worker_name)
            if worker is None:
                raise NotFoundError(f"worker/{worker_name} does not exist")
            worker_users[worker_name] = (
                worker.matrix_user_id or _fallback_worker_user(worker)
            )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}/participants",
            request={
                "action": "update_participants",
                "project_id": project_id,
                "add": list(additions),
                "remove": list(removals),
                "reason": reason.strip(),
                "worker_users": worker_users,
                "source_room_id": context.room_id,
                "source_event_id": context.event_id,
                "source_tool_call_id": context.tool_call_id,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            current_project = await self._require_project(project_id)
            await self._remember_project_decision(
                operation,
                current_project,
                decision=(
                    "Changed project participants: "
                    f"added={list(additions)}, removed={list(removals)}"
                ),
                rationale=reason.strip(),
            )
            return _project_receipt(
                operation.operation_id,
                current_project,
            )
        unknown_removals = tuple(
            name for name in removals if name not in current
        )
        if unknown_removals and not _recovering:
            raise ConflictError(
                "project participants do not include: "
                + ", ".join(unknown_removals),
            )
        changed_names = await self._graph.update_participants(
            project_id,
            add=additions,
            remove=removals,
            worker_users=worker_users,
            now=self._clock.now().astimezone(UTC),
        )
        project = await self._require_project(project_id)
        await self._graph.append_plan_revision(
            project_id,
            body=str(project.metadata.get("plan", "")),
            change_kind="major_participants",
            created_by=self._admin_user_id,
            now=self._clock.now().astimezone(UTC),
        )
        members = await self._known_room_members(project.room_id)
        for worker_name in additions:
            user_id = worker_users[worker_name]
            if user_id not in members:
                await self._supervisor.before_effect(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    {
                        "operation": "invite_project_participant",
                        "project_id": project_id,
                        "room_id": project.room_id,
                        "user_id": user_id,
                    },
                )
                try:
                    await self._matrix.invite_user(
                        project.room_id,
                        user_id,
                    )
                except Exception as exc:
                    refreshed = await self._known_room_members(
                        project.room_id,
                    )
                    if user_id not in refreshed:
                        await self._record_external_failure(
                            operation.operation_id,
                            ExternalEffect.MATRIX,
                            exc,
                        )
                        raise
                    members.update(refreshed)
                await self._supervisor.effect_acknowledged(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    {
                        "project_id": project_id,
                        "room_id": project.room_id,
                        "user_id": user_id,
                    },
                )
        for worker_name in removals:
            user_id = worker_users.get(worker_name)
            if user_id and user_id in members:
                await self._supervisor.before_effect(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    {
                        "operation": "kick_project_participant",
                        "project_id": project_id,
                        "room_id": project.room_id,
                        "user_id": user_id,
                    },
                )
                await self._matrix.kick_user(
                    project.room_id,
                    user_id,
                    reason=reason.strip(),
                )
                await self._supervisor.effect_acknowledged(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    {
                        "project_id": project_id,
                        "room_id": project.room_id,
                        "user_id": user_id,
                    },
                )
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="update_project_participants",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="update_project_participants_plan",
        )
        txn_id = matrix_transaction_id(operation.operation_id, 0)
        event_id = await self._matrix.send_text(
            project.room_id,
            "[Project Participants Changed] "
            f"added={list(additions)}, removed={list(removals)}. "
            f"{reason.strip()}",
            txn_id=txn_id,
            mentions=tuple(
                worker_users[name]
                for name in additions
                if name in worker_users
            ),
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "participants": list(changed_names),
                "event_id": event_id,
            },
        )
        await self._remember_project_decision(
            operation,
            project,
            decision=(
                "Changed project participants: "
                f"added={list(additions)}, removed={list(removals)}"
            ),
            rationale=reason.strip(),
        )
        return _project_receipt(operation.operation_id, project)

    async def revise_plan(
        self,
        *,
        project_id: str,
        plan: str,
        change_kind: str,
        reason: str,
        context: MutationContext,
    ) -> ProjectReceipt:
        """Persist a minor or confirmed-major plan revision."""

        if change_kind not in {"minor", "major"}:
            raise ValueError("plan change kind must be minor or major")
        if not plan.strip() or not reason.strip():
            raise ValueError("plan and revision reason must not be empty")
        project = await self._require_project(project_id)
        if project.status not in {"planning", "active"}:
            raise ConflictError(
                f"project {project_id} cannot revise its plan from "
                f"{project.status}",
            )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}/plan",
            request={
                "action": "revise_plan",
                "project_id": project_id,
                "plan": plan.strip(),
                "change_kind": change_kind,
                "reason": reason.strip(),
                "source_room_id": context.room_id,
                "source_event_id": context.event_id,
                "source_tool_call_id": context.tool_call_id,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            current_project = await self._require_project(project_id)
            await self._remember_project_decision(
                operation,
                current_project,
                decision=(
                    "Revised project plan to revision "
                    f"{current_project.metadata.get('plan_revision', 1)} "
                    f"({change_kind})"
                ),
                rationale=reason.strip(),
            )
            return _project_receipt(
                operation.operation_id,
                current_project,
            )
        revision = await self._graph.revise_plan(
            project_id,
            body=plan.strip(),
            change_kind=change_kind,
            reason=reason.strip(),
            created_by=self._admin_user_id,
            now=self._clock.now().astimezone(UTC),
        )
        project = await self._require_project(project_id)
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="revise_project_plan_metadata",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="revise_project_plan",
        )
        txn_id = matrix_transaction_id(operation.operation_id, 0)
        event_id = await self._matrix.send_text(
            project.room_id,
            f"[Project Plan Revised] revision={revision.revision}, "
            f"kind={change_kind}. {reason.strip()}",
            txn_id=txn_id,
            mentions=(self._admin_user_id,),
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "plan_revision": revision.revision,
                "change_kind": change_kind,
                "event_id": event_id,
            },
        )
        await self._remember_project_decision(
            operation,
            project,
            decision=(
                "Revised project plan to revision "
                f"{revision.revision} ({change_kind})"
            ),
            rationale=reason.strip(),
        )
        return _project_receipt(operation.operation_id, project)

    async def complete_task(
        self,
        *,
        project_id: str,
        task_id: str,
        worker_event_id: str,
        sender_id: str,
        structured_result: dict[str, Any] | None = None,
        accepted: bool = False,
        result_digest: str | None = None,
    ) -> TaskReceipt:
        project = await self._require_project(project_id)
        await self._require_task_assignee(
            project_id=project_id,
            task_id=task_id,
            sender_id=sender_id,
        )
        task = await self._task_service.record_completion(
            task_id=task_id,
            worker_event_id=worker_event_id,
            structured_result=structured_result,
            actor_id=sender_id,
            accepted=accepted,
            result_digest=result_digest,
        )
        result_status = str(task.result_status or "")
        completed_result = task.status == "completed"
        revision_receipt: TaskReceipt | None = None
        if result_status == "REVISION_NEEDED":
            revision_receipt = await self.request_revision(
                project_id=project_id,
                task_id=task_id,
                feedback=task.summary or "Worker requested revision.",
                assigned_to=None,
                triggered_by_task_id=None,
                context=MutationContext(
                    room_id=project.room_id,
                    event_id=worker_event_id,
                    tool_call_id=f"result-revision:{task_id}",
                ),
            )

        completed_record = next(
            (
                item
                for item in await self._tasks.list_by_project(project_id)
                if item.task_id == task_id
            ),
            None,
        )
        revision_target = (
            completed_record.metadata.get("is_revision_for")
            if completed_result and completed_record is not None
            else None
        )
        if revision_target:
            from agentteams_manager.state.tasks import ProjectTaskState

            original = next(
                (
                    item
                    for item in await self._tasks.list_by_project(project_id)
                    if item.task_id == str(revision_target)
                ),
                None,
            )
            if original is None:
                raise NotFoundError(
                    f"task/{revision_target} does not exist",
                )
            if original.status == ProjectTaskState.REVISION_NEEDED:
                await self._graph.transition(
                    str(revision_target),
                    expected={ProjectTaskState.REVISION_NEEDED},
                    target=ProjectTaskState.COMPLETED,
                    actor_id=sender_id,
                    reason=f"revision task {task_id} completed",
                )
            elif original.status != ProjectTaskState.COMPLETED:
                raise ConflictError(
                    f"revision target task/{revision_target} cannot be "
                    f"completed from {original.status}",
                )
        if completed_result:
            await self._graph.promote_ready(project_id)
            for ready in await self._tasks.list_by_project(project_id):
                if ready.status != "ready":
                    continue
                await self._task_service.dispatch_ready(
                    task_id=ready.task_id,
                    context=MutationContext(
                        room_id=project.room_id,
                        event_id=worker_event_id,
                        tool_call_id=f"dependency-ready:{ready.task_id}",
                    ),
                )
        operation_id = operation_id_for(
            project.room_id,
            worker_event_id,
            f"project-result-decision:{task_id}",
        )
        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}",
            request={
                "action": "process_task_result",
                "project_id": project_id,
                "task_id": task_id,
                "worker_event_id": worker_event_id,
                "result_status": result_status,
                "result_digest": result_digest,
                "accepted": accepted,
                "revision_task_id": (
                    revision_receipt.task_id
                    if revision_receipt is not None
                    else None
                ),
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            await self._synchronize_project_task_index(project_id)
            await self._remember_project_decision(
                operation,
                project,
                decision=(
                    f"Applied task result decision for {task_id}: "
                    f"{result_status or task.status}"
                ),
                rationale=(
                    task.summary
                    or (
                        "Result accepted."
                        if accepted
                        else "Result did not satisfy completion."
                    )
                ),
            )
            if completed_result:
                await self._close_project_if_terminal(
                    project_id=project_id,
                    worker_event_id=worker_event_id,
                    task_id=task_id,
                )
            return task
        project = await self._synchronize_project_task_index(project_id)
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="process_project_task_result",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="process_project_task_result_plan",
        )
        if completed_result:
            label = "Project Task Completed"
            detail = task.summary or "Completed."
        elif result_status == "REVISION_NEEDED":
            label = "Project Task Revision Requested"
            detail = (
                f"{task.summary or 'Revision required.'} "
                f"Replacement task: "
                f"{revision_receipt.task_id if revision_receipt else 'pending'}"
            )
        else:
            label = "Project Task Blocked"
            detail = task.summary or result_status or "Blocked."
        txn_id = matrix_transaction_id(operation_id, 0)
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "announce_project_task_result",
                "project_id": project_id,
                "task_id": task_id,
                "result_status": result_status,
                "room_id": project.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                project.room_id,
                f"[{label}] {task_id}: {detail}",
                txn_id=txn_id,
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
                "project_id": project_id,
                "task_id": task_id,
                "result_status": result_status,
                "event_id": event_id,
            },
        )
        await self._remember_project_decision(
            operation,
            project,
            decision=(
                f"Applied task result decision for {task_id}: "
                f"{result_status or task.status}"
            ),
            rationale=(
                task.summary
                or (
                    "Result accepted."
                    if accepted
                    else "Result did not satisfy completion."
                )
            ),
        )
        await self._synchronize_project_task_index(project_id)
        if completed_result:
            await self._close_project_if_terminal(
                project_id=project_id,
                worker_event_id=worker_event_id,
                task_id=task_id,
            )
        return task

    async def report_blocked(
        self,
        *,
        project_id: str,
        task_id: str,
        sender_id: str,
        reason: str,
    ) -> TaskRecord:
        from agentteams_manager.state.tasks import ProjectTaskState

        task = await self._require_task_assignee(
            project_id=project_id,
            task_id=task_id,
            sender_id=sender_id,
        )
        return await self._graph.transition(
            task.task_id,
            expected={
                ProjectTaskState.DISPATCHED,
                ProjectTaskState.IN_PROGRESS,
            },
            target=ProjectTaskState.BLOCKED,
            actor_id=sender_id,
            reason=reason,
        )

    async def _require_task_assignee(
        self,
        *,
        project_id: str,
        task_id: str,
        sender_id: str,
    ) -> TaskRecord:
        task_record = next(
            (
                item
                for item in await self._tasks.list_by_project(project_id)
                if item.task_id == task_id
            ),
            None,
        )
        if task_record is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        assignee = await self._controller.get_worker(
            task_record.assigned_to,
        )
        assignee_user_id = (
            assignee.matrix_user_id
            if assignee is not None and assignee.matrix_user_id
            else (
                _fallback_worker_user(assignee)
                if assignee is not None
                else None
            )
        )
        if sender_id not in {self._admin_user_id, assignee_user_id}:
            raise ConflictError(
                f"sender {sender_id} is not task/{task_id} assignee",
            )
        return task_record

    async def _project_task_index(
        self,
        project: ProjectRecord,
    ) -> dict[str, Any]:
        """Derive the project task index from durable task rows.

        Project metadata is a projection, not a second source of truth.  A
        full rebuild prevents revision transitions, dependency promotion, or
        generic task delegation from leaving stale or invisible entries.
        """

        tasks = await self._tasks.list_by_project(project.project_id)
        task_ids = tuple(dict.fromkeys(task.task_id for task in tasks))
        tasks_by_id = {task.task_id: task for task in tasks}
        task_statuses = {
            task_id: tasks_by_id[task_id].status
            for task_id in task_ids
            if task_id in tasks_by_id
        }
        task_assignments = {
            task_id: tasks_by_id[task_id].assigned_to
            for task_id in task_ids
            if task_id in tasks_by_id
        }
        return {
            **project.metadata,
            "task_ids": list(task_ids),
            "task_statuses": task_statuses,
            "task_assignments": task_assignments,
        }

    async def _synchronize_project_task_index(
        self,
        project_id: str,
    ) -> ProjectRecord:
        """Refresh the project task projection from authoritative task rows."""

        project = await self._require_project(project_id)
        indexed_metadata = await self._project_task_index(project)
        if indexed_metadata == project.metadata:
            return project
        changed = await self._projects.update(
            project_id,
            expected={project.status},
            status=project.status,
            metadata=indexed_metadata,
        )
        return changed or await self._require_project(project_id)

    async def _close_project_if_terminal(
        self,
        *,
        project_id: str,
        worker_event_id: str,
        task_id: str,
    ) -> None:
        remaining = tuple(
            item
            for item in await self._tasks.list_by_project(project_id)
            if item.status not in {"completed", "failed", "cancelled"}
        )
        if remaining:
            return
        project = await self._require_project(project_id)
        if project.status != "active":
            return
        await self.close(
            project_id=project_id,
            force=False,
            context=MutationContext(
                room_id=project.room_id,
                event_id=worker_event_id,
                tool_call_id=f"auto-close:{task_id}",
            ),
        )

    async def close(
        self,
        *,
        project_id: str,
        force: bool,
        context: MutationContext,
    ) -> ProjectReceipt:
        project = await self._require_project(project_id)
        if project.status in {"completed", "cancelled"}:
            return _project_receipt(context.operation_id, project)
        tasks = await self._tasks.list_by_project(project_id)
        nonterminal = tuple(
            task.task_id
            for task in tasks
            if task.status not in {"completed", "failed", "cancelled"}
        )
        if nonterminal and not force:
            raise ConflictError(
                "project has nonterminal tasks: " + ", ".join(nonterminal),
            )
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CLOSE_PROJECT,
            target_key=f"project/{project_id}",
            request={
                "project_id": project_id,
                "force": force,
                "nonterminal_tasks": list(nonterminal),
                "source_room_id": context.room_id,
                "source_event_id": context.event_id,
                "source_tool_call_id": context.tool_call_id,
            },
        )
        return await self._resume_close(operation)

    async def _resume_close(
        self,
        operation: OperationRecord,
    ) -> ProjectReceipt:
        if operation.kind is not OperationKind.CLOSE_PROJECT:
            raise ValueError("operation is not project closure")
        project_id = str(operation.request.get("project_id", ""))
        if not project_id:
            raise RecoveryError("project closure has no project ID")
        project = await self._synchronize_project_task_index(project_id)
        if operation.status is OperationStatus.SUCCEEDED:
            await self._remember_project_decision(
                operation,
                project,
                decision=f"Project {project.status}",
                rationale=(
                    "Project was force-closed."
                    if bool(operation.request.get("force", False))
                    else "All project tasks reached terminal state."
                ),
            )
            return _project_receipt(operation.operation_id, project)
        force = bool(operation.request.get("force", False))
        nonterminal = tuple(
            str(task_id)
            for task_id in operation.request.get(
                "nonterminal_tasks",
                (),
            )
        )
        if project.status == "active":
            completed_at = self._clock.now().astimezone(UTC)
            changed = await self._projects.update(
                project_id,
                expected={"active"},
                status="completed",
                metadata={
                    **project.metadata,
                    "completed_at": completed_at.isoformat(),
                    "forced_close": force,
                },
            )
            project = changed or await self._require_project(project_id)
        elif project.status == "planning" and force and not nonterminal:
            cancelled_at = self._clock.now().astimezone(UTC)
            changed = await self._projects.update(
                project_id,
                expected={"planning"},
                status="cancelled",
                metadata={
                    **project.metadata,
                    "cancelled_at": cancelled_at.isoformat(),
                    "forced_close": True,
                },
            )
            project = changed or await self._require_project(project_id)
        if project.status not in {"completed", "cancelled"}:
            raise ConflictError(
                f"project {project_id} cannot close from {project.status}",
            )
        receipt = await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="close_project",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="close_project_plan",
        )
        requester_room_id = str(
            project.metadata.get("requester_room_id") or "",
        )
        completion_rooms = tuple(
            dict.fromkeys(
                room_id
                for room_id in (project.room_id, requester_room_id)
                if room_id
            ),
        )
        event_ids: dict[str, str] = {}
        lifecycle_label = (
            "Cancelled"
            if project.status == "cancelled"
            else "Completed"
        )
        for sequence, room_id in enumerate(completion_rooms):
            txn_id = matrix_transaction_id(
                operation.operation_id,
                sequence,
            )
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {
                    "operation": "announce_project_completion",
                    "room_id": room_id,
                    "txn_id": txn_id,
                },
            )
            try:
                event_ids[room_id] = await self._matrix.send_text(
                    room_id,
                    f"[Project {lifecycle_label}] {project.project_id}: "
                    f"{project.name}.",
                    txn_id=txn_id,
                    mentions=(self._admin_user_id,),
                )
            except Exception as exc:
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    exc,
                )
                raise
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "status": project.status,
                "metadata_etag": receipt.etag,
                "event_ids": event_ids,
            },
        )
        await self._remember_project_decision(
            operation,
            project,
            decision=f"Project {project.status}",
            rationale=(
                "Project was force-closed."
                if force
                else "All project tasks reached terminal state."
            ),
        )
        return _project_receipt(operation.operation_id, project)

    async def _resume_project_task_index(
        self,
        operation: OperationRecord,
    ) -> TaskRecord:
        request = operation.request
        action = request.get("action")
        if action not in {"add_task", "complete_task"}:
            raise RecoveryError(
                f"unknown project update action: {action!r}",
            )
        project_id = str(request.get("project_id", ""))
        task_id = str(request.get("task_id", ""))
        if not project_id or not task_id:
            raise RecoveryError(
                "project update is missing project or task identity",
            )
        project = await self._require_project(project_id)
        tasks = await self._tasks.list_by_project(project_id)
        task = next(
            (item for item in tasks if item.task_id == task_id),
            None,
        )
        if task is None:
            raise RecoveryError(
                f"project task {task_id} is not durably indexed",
            )
        if operation.status is OperationStatus.SUCCEEDED:
            return task
        project = await self._synchronize_project_task_index(project_id)
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name=f"recover_{action}",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name=f"recover_{action}_plan",
        )
        txn_id = matrix_transaction_id(operation.operation_id, 0)
        if action == "add_task":
            text = (
                f"[Project Task Assigned] {task.task_id}: "
                f"{task.title} to {task.assigned_to}."
            )
        elif action == "complete_task":
            summary = str(
                task.metadata.get("completion_summary") or "Completed.",
            )
            text = (
                f"[Project Task Completed] {task.task_id}: {summary}"
            )
        else:
            text = (
                f"[Project Task Reassigned] {task.task_id}: "
                f"assigned to {task.assigned_to}. "
                f"{request.get('reason', '')}"
            )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": f"recover_project_{action}",
                "project_id": project_id,
                "task_id": task_id,
                "room_id": project.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                project.room_id,
                text,
                txn_id=txn_id,
            )
        except Exception as exc:
            await self._record_external_failure(
                operation.operation_id,
                ExternalEffect.MATRIX,
                exc,
            )
            raise
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "project_id": project_id,
                "task_id": task_id,
                "event_id": event_id,
                "recovered": True,
            },
        )
        if action == "complete_task":
            await self._synchronize_project_task_index(project_id)
            await self._close_project_if_terminal(
                project_id=project_id,
                worker_event_id=str(
                    request.get("worker_event_id") or operation.operation_id,
                ),
                task_id=task_id,
            )
        return task

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> object:
        """Resume one project operation from durable request facts."""
        if operation.kind is OperationKind.CREATE_PROJECT:
            return await self.resume_create(operation)
        if operation.kind is OperationKind.UPDATE_PROJECT:
            action = str(operation.request.get("action") or "")
            if action in {
                "request_revision",
                "reassign_task",
                "update_participants",
                "revise_plan",
            }:
                context = _context_from_operation(operation)
                if action == "request_revision":
                    return await self.request_revision(
                        project_id=str(operation.request["project_id"]),
                        task_id=str(operation.request["task_id"]),
                        feedback=str(operation.request["feedback"]),
                        assigned_to=(
                            str(operation.request["assigned_to"])
                            if operation.request.get("assigned_to")
                            else None
                        ),
                        triggered_by_task_id=(
                            str(operation.request["triggered_by_task_id"])
                            if operation.request.get("triggered_by_task_id")
                            else None
                        ),
                        context=context,
                    )
                if action == "reassign_task":
                    return await self.reassign_task(
                        project_id=str(operation.request["project_id"]),
                        task_id=str(operation.request["task_id"]),
                        assigned_to=str(operation.request["assigned_to"]),
                        reason=str(operation.request["reason"]),
                        context=context,
                    )
                if action == "update_participants":
                    return await self.update_participants(
                        project_id=str(operation.request["project_id"]),
                        add=tuple(operation.request.get("add", ())),
                        remove=tuple(operation.request.get("remove", ())),
                        reason=str(operation.request["reason"]),
                        context=context,
                        _recovering=True,
                        _recovery_worker_users={
                            str(name): str(user_id)
                            for name, user_id in dict(
                                operation.request.get(
                                    "worker_users",
                                    {},
                                ),
                            ).items()
                        },
                    )
                return await self.revise_plan(
                    project_id=str(operation.request["project_id"]),
                    plan=str(operation.request["plan"]),
                    change_kind=str(operation.request["change_kind"]),
                    reason=str(operation.request["reason"]),
                    context=context,
                )
            return await self._resume_project_task_index(operation)
        if operation.kind is OperationKind.CLOSE_PROJECT:
            return await self._resume_close(operation)
        raise RecoveryError(
            f"ProjectService cannot recover {operation.kind.value}",
        )

    async def _ensure_members(
        self,
        operation: OperationRecord,
        room_id: str,
        expected: tuple[str, ...],
    ) -> None:
        current = await self._known_room_members(room_id)
        for user_id in expected:
            if user_id in current:
                continue
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {
                    "operation": "invite_project_member",
                    "room_id": room_id,
                    "user_id": user_id,
                },
            )
            try:
                await self._matrix.invite_user(room_id, user_id)
            except Exception as exc:
                refreshed = await self._known_room_members(room_id)
                if user_id not in refreshed:
                    await self._record_external_failure(
                        operation.operation_id,
                        ExternalEffect.MATRIX,
                        exc,
                    )
                    raise
                current.update(refreshed)
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {"room_id": room_id, "user_id": user_id, "invited": True},
            )
            current.add(user_id)

    async def _known_room_members(self, room_id: str) -> set[str]:
        """Return joined and invited Matrix users for idempotent membership."""
        known = set(await self._matrix.members(room_id))
        for event in await self._matrix.room_state(room_id):
            if event.get("type") != "m.room.member":
                continue
            content = event.get("content")
            user_id = event.get("state_key")
            if (
                isinstance(content, dict)
                and content.get("membership") in {"invite", "join"}
                and isinstance(user_id, str)
                and user_id
            ):
                known.add(user_id)
        return known

    async def _find_project_room(self, project_id: str) -> str | None:
        matches: list[str] = []
        for room_id in await self._matrix.joined_rooms():
            for event in await self._matrix.room_state(room_id):
                content = event.get("content")
                if (
                    event.get("type") == "io.agentteams.creation"
                    and event.get("state_key", "") == ""
                    and isinstance(content, dict)
                    and content.get("m.agentteams.project_id")
                    == project_id
                    and content.get("m.agentteams.schema_version") == 1
                ):
                    matches.append(room_id)
                    break
        if len(matches) > 1:
            raise ConflictError(
                f"multiple Matrix rooms claim project {project_id}",
            )
        return matches[0] if matches else None

    async def _ensure_json(
        self,
        operation: OperationRecord,
        key: str,
        value: dict[str, Any],
        *,
        operation_name: str,
    ) -> ObjectReceipt:
        existing = await self._storage.head(key)
        if existing is not None:
            current = await self._storage.get_json(key)
            if current == value:
                return existing
            if (
                isinstance(current, dict)
                and current.get("project_id") == value.get("project_id")
                and _project_status_rank(
                    str(current.get("status", "")),
                )
                <= _project_status_rank(str(value.get("status", "")))
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
        operation_name: str,
    ) -> ObjectReceipt:
        existing = await self._storage.head(key)
        if existing is not None:
            if await self._storage.get_bytes(key) != value:
                raise ConflictError(f"object {key} has incompatible contents")
            return existing
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {"operation": operation_name, "key": key},
        )
        try:
            receipt = await self._storage.put_bytes_if_version(
                key,
                value,
                expected_etag=None,
                content_type="text/markdown",
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
            receipt = await self._storage.put_json_if_version(
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

    async def _replace_project_metadata(
        self,
        operation: OperationRecord,
        metadata: ProjectMetadata,
        *,
        operation_name: str,
    ) -> ObjectReceipt:
        return await self._ensure_json(
            operation,
            f"shared/projects/{metadata.project_id}/meta.json",
            metadata.model_dump(mode="json"),
            operation_name=operation_name,
        )

    async def _replace_project_plan(
        self,
        operation: OperationRecord,
        project: ProjectRecord,
        *,
        operation_name: str,
    ) -> ObjectReceipt:
        key = f"shared/projects/{project.project_id}/plan.md"
        target = _project_plan(project).render().encode("utf-8")
        current = await self._storage.head(key)
        if current is not None and await self._storage.get_bytes(key) == target:
            return current
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {"operation": operation_name, "key": key},
        )
        try:
            receipt = await self._storage.put_bytes_if_version(
                key,
                target,
                expected_etag=current.etag if current is not None else None,
                content_type="text/markdown",
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

    async def _remember_project_decision(
        self,
        operation: OperationRecord,
        project: ProjectRecord,
        *,
        decision: str,
        rationale: str,
    ) -> None:
        if self._memory is None:
            return
        room_id = str(
            operation.request.get("source_room_id")
            or project.metadata.get("requester_room_id")
            or project.room_id,
        )
        if not room_id:
            return
        await self._memory.record_project_decision(
            room_id=room_id,
            source_event_id=f"operation:{operation.operation_id}",
            project_id=project.project_id,
            decision=decision,
            rationale=rationale,
            visibility="project",
        )

    async def _record_external_failure(
        self,
        operation_id: str,
        effect: ExternalEffect,
        exc: Exception,
    ) -> None:
        if isinstance(exc, (TimeoutError, ConnectionError, BrokenPipeError)):
            await self._supervisor.effect_ambiguous(
                operation_id,
                effect,
                type(exc).__name__,
            )
        else:
            await self._supervisor.effect_failed(
                operation_id,
                effect,
                type(exc).__name__,
            )

    async def _require_project(self, project_id: str) -> ProjectRecord:
        project = await self._projects.get(project_id)
        if project is None:
            raise NotFoundError(f"project/{project_id} does not exist")
        return project

    @staticmethod
    def _verify_project_request(
        project: ProjectRecord,
        operation: OperationRecord,
        request: ProjectCreateRequest,
    ) -> None:
        if (
            project.name != request.title
            or project.metadata.get("operation_id")
            != operation.operation_id
            or project.metadata.get("description") != request.description
            or project.metadata.get("plan") != request.plan
            or tuple(project.metadata.get("participants", ()))
            != request.participants
        ):
            raise ConflictError(
                f"project {project.project_id} does not match its request",
            )


def _project_id_for(operation: OperationRecord) -> str:
    timestamp = operation.created_at.astimezone(UTC)
    return (
        f"project-{timestamp:%Y%m%d-%H%M%S}-"
        f"{operation.operation_id[:6]}"
    )


def _project_metadata(project: ProjectRecord) -> ProjectMetadata:
    completed_at = project.metadata.get("completed_at")
    confirmed_at = project.metadata.get("confirmed_at")
    confirmed_by = project.metadata.get("confirmed_by")
    if project.status in {"active", "completed"} and not confirmed_at:
        # Compatibility for projects created before the explicit plan gate.
        confirmed_at = project.created_at.isoformat()
        confirmed_by = str(
            project.metadata.get("requester_user_id")
            or "legacy-migration"
        )
    return ProjectMetadata(
        project_id=project.project_id,
        title=project.name,
        description=str(project.metadata["description"]),
        status=_project_status(project),
        room_id=project.room_id or None,
        participants=tuple(project.metadata.get("participants", ())),
        task_ids=tuple(project.metadata.get("task_ids", ())),
        created_at=project.created_at,
        updated_at=project.updated_at,
        confirmed_at=(
            datetime.fromisoformat(str(confirmed_at))
            if confirmed_at
            else None
        ),
        confirmed_by=(
            str(confirmed_by)
            if confirmed_by
            else None
        ),
        plan_revision=int(project.metadata.get("plan_revision", 1)),
        completed_at=(
            datetime.fromisoformat(str(completed_at))
            if completed_at
            else None
        ),
    )


def _project_plan(project: ProjectRecord) -> ProjectPlan:
    return ProjectPlan(
        project_id=project.project_id,
        title=project.name,
        description=str(project.metadata["description"]),
        status=_project_status(project),
        body=str(project.metadata["plan"]),
        task_ids=tuple(project.metadata.get("task_ids", ())),
        task_statuses=dict(project.metadata.get("task_statuses", {})),
        updated_at=project.updated_at,
    )


def _project_status(project: ProjectRecord) -> ProjectStatus:
    if project.status not in {
        "planning",
        "active",
        "completed",
        "cancelled",
    }:
        raise RecoveryError(
            f"project/{project.project_id} has invalid status "
            f"{project.status!r}",
        )
    return cast(ProjectStatus, project.status)


def _project_receipt(
    operation_id: str,
    project: ProjectRecord,
) -> ProjectReceipt:
    return ProjectReceipt(
        operation_id=operation_id,
        project_id=project.project_id,
        title=project.name,
        status=project.status,
        room_id=project.room_id,
        participants=tuple(project.metadata.get("participants", ())),
        task_ids=tuple(project.metadata.get("task_ids", ())),
        confirmed_at=(
            datetime.fromisoformat(str(project.metadata["confirmed_at"]))
            if project.metadata.get("confirmed_at")
            else None
        ),
        confirmed_by=(
            str(project.metadata["confirmed_by"])
            if project.metadata.get("confirmed_by")
            else None
        ),
    )


def _task_receipt_from_record(
    operation_id: str,
    task: TaskRecord,
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
        summary=(
            str(task.metadata["completion_summary"])
            if task.metadata.get("completion_summary")
            else None
        ),
        last_executed_at=task.last_executed_at,
        next_scheduled_at=task.next_scheduled_at,
    )


def _context_from_operation(
    operation: OperationRecord,
) -> MutationContext:
    room_id = str(operation.request.get("source_room_id") or "")
    event_id = str(operation.request.get("source_event_id") or "")
    tool_call_id = str(
        operation.request.get("source_tool_call_id") or "",
    )
    if not room_id or not event_id or not tool_call_id:
        raise RecoveryError(
            f"operation {operation.operation_id} lacks source context",
        )
    context = MutationContext(
        room_id=room_id,
        event_id=event_id,
        tool_call_id=tool_call_id,
    )
    if context.operation_id != operation.operation_id:
        raise RecoveryError(
            f"operation {operation.operation_id} source context changed",
        )
    return context


def _fallback_worker_user(worker: WorkerResource) -> str:
    domain = "example"
    if worker.room_id and ":" in worker.room_id:
        domain = worker.room_id.rsplit(":", 1)[1]
    return f"@worker-{worker.name}:{domain}"


def _project_status_rank(status: str) -> int:
    return {
        "planning": 0,
        "active": 1,
        "completed": 2,
        "cancelled": 2,
    }.get(status, -1)
