"""Durable projects spanning SQLite, MinIO, Matrix, and task workflows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

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
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import (
    TaskControllerPort,
    TaskReceipt,
    TaskService,
    TaskSupervisorPort,
)


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


class ProjectService:
    """Prepare project artifacts before room creation and invitations."""

    def __init__(
        self,
        *,
        projects: ProjectRepositoryPort,
        tasks: ProjectTaskReader,
        task_service: TaskService,
        storage: ArtifactPort,
        controller: TaskControllerPort,
        matrix: MatrixAdministrationPort | MatrixPort,
        topology: ProjectTopologyPort,
        graph: ProjectGraphPort,
        supervisor: TaskSupervisorPort,
        clock: Clock,
        admin_user_id: str,
        manager_user_id: str,
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
            if project.status != "active" or not project.room_id:
                raise ConflictError(
                    f"succeeded project {project_id} lacks active state",
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
                    "invite": list(expected_members),
                },
            )
            try:
                room_id = await self._matrix.create_private_room(
                    name=request.title,
                    topic=f"AgentTeams project {project_id}",
                    invite=tuple(
                        user_id
                        for user_id in expected_members
                        if user_id != self._manager_user_id
                    ),
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
        active_metadata_values = {
            **project.metadata,
            "room_id": room_id,
        }
        if project.status == "planning" or project.room_id != room_id:
            changed = await self._projects.update(
                project_id,
                expected={"planning", "active"},
                status="active",
                room_id=room_id,
                metadata=active_metadata_values,
            )
            project = changed or await self._require_project(project_id)
        if project.status != "active" or project.room_id != room_id:
            raise ConflictError(
                f"project {project_id} did not converge to active",
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
            operation_name="publish_active_project",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="publish_active_project_plan",
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "project_id": project_id,
                "room_id": room_id,
                "status": "active",
                "metadata_etag": metadata_receipt.etag,
            },
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
        )
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
        task_ids = tuple(
            dict.fromkeys(
                (
                    *project.metadata.get("task_ids", ()),
                    task.task_id,
                ),
            ),
        )
        task_statuses = {
            **dict(project.metadata.get("task_statuses", {})),
            task.task_id: task.status,
        }
        changed = await self._projects.update(
            project_id,
            expected={"active"},
            status="active",
            metadata={
                **project.metadata,
                "task_ids": list(task_ids),
                "task_statuses": task_statuses,
            },
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

    async def complete_task(
        self,
        *,
        project_id: str,
        task_id: str,
        worker_event_id: str,
        structured_result: dict[str, Any] | None = None,
    ) -> TaskReceipt:
        project = await self._require_project(project_id)
        task = await self._task_service.record_completion(
            task_id=task_id,
            worker_event_id=worker_event_id,
            structured_result=structured_result,
        )
        operation_id = operation_id_for(
            project.room_id,
            worker_event_id,
            f"project-completion:{task_id}",
        )
        operation = await self._supervisor.begin(
            operation_id=operation_id,
            kind=OperationKind.UPDATE_PROJECT,
            target_key=f"project/{project_id}",
            request={
                "action": "complete_task",
                "project_id": project_id,
                "task_id": task_id,
                "worker_event_id": worker_event_id,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return task
        task_statuses = {
            **dict(project.metadata.get("task_statuses", {})),
            task_id: task.status,
        }
        changed = await self._projects.update(
            project_id,
            expected={"active"},
            status="active",
            metadata={
                **project.metadata,
                "task_statuses": task_statuses,
            },
        )
        project = changed or await self._require_project(project_id)
        await self._replace_project_metadata(
            operation,
            _project_metadata(project),
            operation_name="complete_project_task",
        )
        await self._replace_project_plan(
            operation,
            project,
            operation_name="complete_project_task_plan",
        )
        txn_id = matrix_transaction_id(operation_id, 0)
        await self._supervisor.before_effect(
            operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "announce_project_task_completion",
                "project_id": project_id,
                "task_id": task_id,
                "room_id": project.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                project.room_id,
                f"[Project Task Completed] {task_id}: "
                f"{task.summary or 'Completed.'}",
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
                "event_id": event_id,
            },
        )
        return task

    async def close(
        self,
        *,
        project_id: str,
        force: bool,
        context: MutationContext,
    ) -> ProjectReceipt:
        project = await self._require_project(project_id)
        if project.status == "completed":
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
        project = await self._require_project(project_id)
        if operation.status is OperationStatus.SUCCEEDED:
            return _project_receipt(operation.operation_id, project)
        force = bool(operation.request.get("force", False))
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
        if project.status != "completed":
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
        txn_id = matrix_transaction_id(operation.operation_id, 0)
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "announce_project_completion",
                "room_id": project.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                project.room_id,
                f"[Project Completed] {project.project_id}: {project.name}.",
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
                "status": "completed",
                "metadata_etag": receipt.etag,
                "event_id": event_id,
            },
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
        task_ids = tuple(
            dict.fromkeys(
                (*project.metadata.get("task_ids", ()), task_id),
            ),
        )
        statuses = {
            **dict(project.metadata.get("task_statuses", {})),
            task_id: task.status,
        }
        changed = await self._projects.update(
            project_id,
            expected={"active"},
            status="active",
            metadata={
                **project.metadata,
                "task_ids": list(task_ids),
                "task_statuses": statuses,
            },
        )
        project = changed or await self._require_project(project_id)
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
        else:
            summary = str(
                task.metadata.get("completion_summary") or "Completed.",
            )
            text = (
                f"[Project Task Completed] {task.task_id}: {summary}"
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
        return task

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> object:
        """Resume one project operation from durable request facts."""
        if operation.kind is OperationKind.CREATE_PROJECT:
            return await self.resume_create(operation)
        if operation.kind is OperationKind.UPDATE_PROJECT:
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
        current = set(await self._matrix.members(room_id))
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
                await self._record_external_failure(
                    operation.operation_id,
                    ExternalEffect.MATRIX,
                    exc,
                )
                raise
            await self._supervisor.effect_acknowledged(
                operation.operation_id,
                ExternalEffect.MATRIX,
                {"room_id": room_id, "user_id": user_id, "invited": True},
            )
            current.add(user_id)

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
    return ProjectMetadata(
        project_id=project.project_id,
        title=project.name,
        description=str(project.metadata["description"]),
        status=project.status,
        room_id=project.room_id or None,
        participants=tuple(project.metadata.get("participants", ())),
        task_ids=tuple(project.metadata.get("task_ids", ())),
        created_at=project.created_at,
        updated_at=project.updated_at,
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
        status=project.status,
        body=str(project.metadata["plan"]),
        task_ids=tuple(project.metadata.get("task_ids", ())),
        task_statuses=dict(project.metadata.get("task_statuses", {})),
        updated_at=project.updated_at,
    )


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
    )


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
