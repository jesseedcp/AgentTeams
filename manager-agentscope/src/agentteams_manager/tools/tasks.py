"""Policy-bound AgentScope tools for task and project workflows.

向 AgentScope 暴露有限任务、周期任务和 Project 生命周期工具。

这些 tool 只收集结构化请求并绑定当前房间/事件；TaskService 与 ProjectService 才负责
artifact、DAG、Matrix 通知、状态转换和恢复。``TASK_COMPLETED`` 只是唤醒信号，必须先
inspect result 并比较 digest，接受或返修后才改变任务终态。
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Literal, Protocol, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from agentteams_manager.clients.git import GitRequest, GitRequestParser
from agentteams_manager.domain.errors import (
    NotFoundError,
    PermissionDeniedError,
)
from agentteams_manager.domain.models import (
    ProjectRecord,
    RoomKind,
    RoomPolicy,
    TaskRecord,
)
from agentteams_manager.tools.base import (
    ManagerTool,
    current_tool_invocation,
)
from agentteams_manager.workflows.git_delegation import (
    GitDelegationReceipt,
    GitDelegationService,
)
from agentteams_manager.workflows.projects import (
    ProjectReceipt,
    ProjectService,
)
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskReceipt, TaskService

TASK_TOOL_NAMES = frozenset(
    {
        "list_tasks",
        "get_task",
        "create_task",
        "update_task",
        "delete_task",
        "delegate_task",
        "delegate_team_task",
        "inspect_task_result",
        "complete_task",
        "schedule_task",
        "create_project",
        "confirm_project_plan",
        "list_projects",
        "get_project",
        "update_project",
        "request_project_revision",
        "reassign_project_task",
        "report_project_blocked",
        "revise_project_plan",
        "revise_project_plan_major",
        "update_project_participants",
        "delete_project",
        "sync_files",
        "read_task_file",
        "inspect_git_request",
        "git_delegate",
        "git_delegate_high_risk",
    },
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _EmptyInput(_Input):
    pass


class _TaskIdInput(_Input):
    task_id: str = Field(pattern=r"^task-[A-Za-z0-9-]+$")


class _ProjectIdInput(_Input):
    project_id: str = Field(pattern=r"^project-[A-Za-z0-9-]+$")


class CreateFiniteTaskInput(_Input):
    title: str = Field(min_length=1, max_length=500)
    specification: str = Field(min_length=1, max_length=100_000)
    assigned_to: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    delegated_to_team: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    project_id: str | None = Field(
        default=None,
        pattern=r"^project-[A-Za-z0-9-]+$",
    )
    project_room_id: str | None = None


class DelegateTeamTaskInput(_Input):
    title: str = Field(min_length=1, max_length=500)
    specification: str = Field(min_length=1, max_length=100_000)
    leader: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    team_name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    project_id: str | None = Field(
        default=None,
        pattern=r"^project-[A-Za-z0-9-]+$",
    )
    project_room_id: str | None = None


class CompleteTaskInput(_TaskIdInput):
    result: dict[str, Any] | None = None
    accepted: bool = False
    result_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @model_validator(mode="after")
    def validate_acceptance(self) -> CompleteTaskInput:
        if self.accepted and self.result_digest is None:
            raise ValueError(
                "accepted results require result_digest from "
                "inspect_task_result",
            )
        return self


class CreateRecurringTaskInput(CreateFiniteTaskInput):
    schedule: str = Field(min_length=1, max_length=128)
    timezone: str = Field(min_length=1, max_length=128)


class RecordTaskExecutionInput(_TaskIdInput):
    pass


class CancelTaskInput(_TaskIdInput):
    pass


class UpdateTaskInput(CompleteTaskInput):
    action: Literal["complete", "record_execution", "cancel"]


class CreateProjectInput(_Input):
    title: str = Field(min_length=1, max_length=500)
    description: str = Field(min_length=1, max_length=20_000)
    plan: str = Field(min_length=1, max_length=100_000)
    participants: tuple[str, ...] = Field(min_length=1)


class ConfirmProjectPlanInput(_ProjectIdInput):
    pass


class AddProjectTaskInput(_Input):
    project_id: str = Field(pattern=r"^project-[A-Za-z0-9-]+$")
    title: str = Field(min_length=1, max_length=500)
    specification: str = Field(min_length=1, max_length=100_000)
    assigned_to: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    delegated_to_team: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    dependencies: tuple[str, ...] = ()


class UpdateProjectInput(_ProjectIdInput):
    action: Literal["add_task", "complete_task"]
    task_id: str | None = Field(
        default=None,
        pattern=r"^task-[A-Za-z0-9-]+$",
    )
    title: str | None = Field(default=None, max_length=500)
    specification: str | None = Field(
        default=None,
        max_length=100_000,
    )
    assigned_to: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    delegated_to_team: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    result: dict[str, Any] | None = None
    accepted: bool = False
    result_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    dependencies: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_action_fields(self) -> UpdateProjectInput:
        if self.action == "add_task" and not (
            self.title and self.specification and self.assigned_to
        ):
            raise ValueError(
                "add_task requires title, specification, and assigned_to",
            )
        if self.action == "complete_task" and not self.task_id:
            raise ValueError("complete_task requires task_id")
        if (
            self.action == "complete_task"
            and self.accepted
            and self.result_digest is None
        ):
            raise ValueError(
                "accepted results require result_digest from "
                "inspect_task_result",
            )
        return self


class CloseProjectInput(_ProjectIdInput):
    force: bool = False


class RequestProjectRevisionInput(_ProjectIdInput):
    task_id: str = Field(pattern=r"^task-[A-Za-z0-9-]+$")
    feedback: str = Field(min_length=1, max_length=20_000)
    assigned_to: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    triggered_by_task_id: str | None = Field(
        default=None,
        pattern=r"^task-[A-Za-z0-9-]+$",
    )


class ReassignProjectTaskInput(_ProjectIdInput):
    task_id: str = Field(pattern=r"^task-[A-Za-z0-9-]+$")
    assigned_to: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    reason: str = Field(min_length=1, max_length=20_000)


class ReportProjectBlockedInput(_ProjectIdInput):
    task_id: str = Field(pattern=r"^task-[A-Za-z0-9-]+$")
    reason: str = Field(min_length=1, max_length=20_000)


class ReviseProjectPlanInput(_ProjectIdInput):
    plan: str = Field(min_length=1, max_length=100_000)
    reason: str = Field(min_length=1, max_length=20_000)


class UpdateProjectParticipantsInput(_ProjectIdInput):
    add: tuple[str, ...] = ()
    remove: tuple[str, ...] = ()
    reason: str = Field(min_length=1, max_length=20_000)

    @model_validator(mode="after")
    def validate_changes(self) -> UpdateProjectParticipantsInput:
        if not self.add and not self.remove:
            raise ValueError("add or remove must contain a worker")
        overlap = set(self.add) & set(self.remove)
        if overlap:
            raise ValueError(
                "the same worker cannot be added and removed",
            )
        return self


class SyncFilesInput(_Input):
    direction: Literal["pull", "push"]
    root: Literal[
        "task_artifacts",
        "worker_workspace",
        "shared_knowledge",
    ] = "task_artifacts"
    task_id: str | None = None
    worker_name: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )

    @model_validator(mode="after")
    def validate_target(self) -> SyncFilesInput:
        if self.root == "task_artifacts":
            if not self.task_id or self.worker_name is not None:
                raise ValueError(
                    "task_artifacts requires only task_id",
                )
        elif self.root == "worker_workspace":
            if not self.worker_name or self.task_id is not None:
                raise ValueError(
                    "worker_workspace requires only worker_name",
                )
        elif self.task_id is not None or self.worker_name is not None:
            raise ValueError("shared_knowledge has no target")
        return self


class ReadTaskFileInput(_TaskIdInput):
    path: str = Field(min_length=1, max_length=1_024)
    max_bytes: int = Field(
        default=256 * 1024,
        ge=1,
        le=1024 * 1024,
    )


class GitDelegationInput(_Input):
    message: str = Field(min_length=1, max_length=100_000)


class TaskCollectionReceipt(_Input):
    tool: str
    items: tuple[dict[str, Any], ...]
    total: int = Field(ge=0)


class LookupReceipt(_Input):
    tool: str
    status: Literal["found", "not_found"]
    item: dict[str, Any] | None = None


class SyncReceipt(_Input):
    tool: Literal["sync_files"] = "sync_files"
    direction: Literal["pull", "push"]
    root: str = "task_artifacts"
    task_id: str | None = None
    worker_name: str | None = None
    result: dict[str, Any]


class TaskReader(Protocol):
    async def list_all(self) -> tuple[TaskRecord, ...]: ...

    async def get(self, task_id: str) -> TaskRecord | None: ...


class ProjectReader(Protocol):
    async def list_all(self) -> tuple[ProjectRecord, ...]: ...

    async def get(self, project_id: str) -> ProjectRecord | None: ...


class FileSyncPort(Protocol):
    async def pull_task(self, task_id: str) -> object: ...

    async def read_task_file(
        self,
        task_id: str,
        path: str,
        *,
        max_bytes: int,
    ) -> object: ...

    async def push_task(
        self,
        task_id: str,
        *,
        processor: str,
    ) -> object: ...

    async def sync_task(
        self,
        task_id: str,
        *,
        direction: Literal["pull", "push"],
        processor: str,
        context: MutationContext,
    ) -> object: ...

    async def pull_root(
        self,
        root: Literal["worker_workspace", "shared_knowledge"],
        *,
        worker_name: str | None = None,
    ) -> object: ...

    async def push_root(
        self,
        root: Literal["worker_workspace", "shared_knowledge"],
        *,
        processor: str,
        worker_name: str | None = None,
    ) -> object: ...


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]


class TaskTools:
    """Typed task facade retained for direct workflow composition."""

    def __init__(self, service: TaskService) -> None:
        self._service = service

    async def create_finite(
        self,
        request: CreateFiniteTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.create_finite(
            title=request.title,
            spec=request.specification,
            assigned_to=request.assigned_to,
            delegated_to_team=request.delegated_to_team,
            project_id=request.project_id,
            project_room_id=request.project_room_id,
            context=context,
        )

    async def complete(
        self,
        request: CompleteTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.record_completion(
            task_id=request.task_id,
            worker_event_id=context.event_id,
            structured_result=request.result,
            accepted=request.accepted,
            result_digest=request.result_digest,
        )

    async def create_recurring(
        self,
        request: CreateRecurringTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.create_recurring(
            title=request.title,
            spec=request.specification,
            assigned_to=request.assigned_to,
            schedule=request.schedule,
            timezone=request.timezone,
            delegated_to_team=request.delegated_to_team,
            project_id=request.project_id,
            project_room_id=request.project_room_id,
            context=context,
        )

    async def record_execution(
        self,
        request: RecordTaskExecutionInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.record_execution(
            task_id=request.task_id,
            worker_event_id=context.event_id,
        )

    async def cancel(
        self,
        request: CancelTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.cancel(
            task_id=request.task_id,
            context=context,
        )


class ProjectTools:
    """Typed project facade retained for direct workflow composition."""

    def __init__(self, service: ProjectService) -> None:
        self._service = service

    async def create(
        self,
        request: CreateProjectInput,
        *,
        context: MutationContext,
    ) -> ProjectReceipt:
        return await self._service.create(
            title=request.title,
            description=request.description,
            plan=request.plan,
            participants=request.participants,
            context=context,
        )

    async def confirm(
        self,
        request: ConfirmProjectPlanInput,
        *,
        context: MutationContext,
        confirmed_by: str,
        auto_confirmed: bool = False,
    ) -> ProjectReceipt:
        return await self._service.confirm_plan(
            project_id=request.project_id,
            confirmed_by=confirmed_by,
            context=context,
            auto_confirmed=auto_confirmed,
        )

    async def add_task(
        self,
        request: AddProjectTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.add_task(
            project_id=request.project_id,
            title=request.title,
            specification=request.specification,
            assigned_to=request.assigned_to,
            delegated_to_team=request.delegated_to_team,
            dependencies=request.dependencies,
            context=context,
        )

    async def close(
        self,
        request: CloseProjectInput,
        *,
        context: MutationContext,
    ) -> ProjectReceipt:
        return await self._service.close(
            project_id=request.project_id,
            force=request.force,
            context=context,
        )


class GitDelegationTools:
    """Typed Git facade with parsing separated from confirmed execution."""

    def __init__(self, service: GitDelegationService) -> None:
        self._service = service

    @staticmethod
    def inspect(request: GitDelegationInput) -> GitRequest:
        return GitRequestParser.parse(request.message)

    async def execute(
        self,
        request: GitDelegationInput,
        *,
        context: MutationContext,
        confirmed: bool = False,
    ) -> GitDelegationReceipt:
        return await self._service.execute(
            GitRequestParser.parse(request.message),
            context=context,
            confirmed=confirmed,
        )


class TaskToolkit:
    """Closed-schema tools filtered by the immutable Matrix room policy."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        tasks: TaskReader,
        projects: ProjectReader,
        task_service: TaskService | Any,
        project_service: ProjectService | Any,
        file_sync: FileSyncPort | Any,
        git: GitDelegationService | Any,
        context_provider: ContextProvider | None = None,
        yolo: bool = False,
    ) -> None:
        self._policy = policy
        self._tasks = tasks
        self._projects = projects
        self._task_service = task_service
        self._project_service = project_service
        self._file_sync = file_sync
        self._git = git
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        specs: tuple[
            tuple[
                str,
                str,
                type[BaseModel],
                Callable[[BaseModel], Awaitable[object]],
                bool,
            ],
            ...,
        ] = (
            (
                "list_tasks",
                "List durable tasks visible in this room.",
                _EmptyInput,
                self._list_tasks,
                True,
            ),
            (
                "get_task",
                "Get one durable task visible in this room.",
                _TaskIdInput,
                self._get_task,
                True,
            ),
            (
                "create_task",
                "Prepare and dispatch one finite task.",
                CreateFiniteTaskInput,
                self._create_task,
                False,
            ),
            (
                "update_task",
                "Complete, record, or cancel one task transition.",
                UpdateTaskInput,
                self._update_task,
                False,
            ),
            (
                "delete_task",
                "Cancel one finite or recurring task.",
                CancelTaskInput,
                self._delete_task,
                False,
            ),
            (
                "delegate_task",
                "Delegate one finite task to a Worker Room.",
                CreateFiniteTaskInput,
                self._create_task,
                False,
            ),
            (
                "delegate_team_task",
                "Delegate one finite task to a Team Leader Room.",
                DelegateTeamTaskInput,
                self._delegate_team_task,
                False,
            ),
            (
                "inspect_task_result",
                "Pull and validate a submitted result without accepting it.",
                _TaskIdInput,
                self._inspect_task_result,
                True,
            ),
            (
                "complete_task",
                "Apply a result decision using its inspected digest.",
                CompleteTaskInput,
                self._complete_task,
                False,
            ),
            (
                "schedule_task",
                "Create one five-field recurring task schedule.",
                CreateRecurringTaskInput,
                self._schedule_task,
                False,
            ),
            (
                "create_project",
                "Prepare one project plan and private Matrix room.",
                CreateProjectInput,
                self._create_project,
                False,
            ),
            (
                "confirm_project_plan",
                "Confirm a prepared project plan and activate execution.",
                ConfirmProjectPlanInput,
                self._confirm_project_plan,
                False,
            ),
            (
                "list_projects",
                "List durable projects visible in this room.",
                _EmptyInput,
                self._list_projects,
                True,
            ),
            (
                "get_project",
                "Get one durable project visible in this room.",
                _ProjectIdInput,
                self._get_project,
                True,
            ),
            (
                "update_project",
                "Add or complete one project task.",
                UpdateProjectInput,
                self._update_project,
                False,
            ),
            (
                "request_project_revision",
                "Create a linked revision task and hold downstream work.",
                RequestProjectRevisionInput,
                self._request_project_revision,
                False,
            ),
            (
                "reassign_project_task",
                "Revoke one task assignment and dispatch it to another participant.",
                ReassignProjectTaskInput,
                self._reassign_project_task,
                False,
            ),
            (
                "report_project_blocked",
                "Report an assigned project task blocker with actor validation.",
                ReportProjectBlockedInput,
                self._report_project_blocked,
                False,
            ),
            (
                "revise_project_plan",
                "Apply and version one minor project plan change.",
                ReviseProjectPlanInput,
                self._revise_project_plan,
                False,
            ),
            (
                "revise_project_plan_major",
                "Apply a confirmed major project plan change.",
                ReviseProjectPlanInput,
                self._revise_project_plan_major,
                False,
            ),
            (
                "update_project_participants",
                "Add or remove project participants after administrator confirmation.",
                UpdateProjectParticipantsInput,
                self._update_project_participants,
                False,
            ),
            (
                "delete_project",
                "Close one project after confirmation.",
                CloseProjectInput,
                self._delete_project,
                False,
            ),
            (
                "sync_files",
                "Pull or push one task cache through verified MinIO I/O.",
                SyncFilesInput,
                self._sync_files,
                False,
            ),
            (
                "read_task_file",
                "Read one bounded UTF-8 file from a pulled task cache.",
                ReadTaskFileInput,
                self._read_task_file,
                True,
            ),
            (
                "inspect_git_request",
                "Parse a Git request and report its risk without execution.",
                GitDelegationInput,
                self._inspect_git,
                True,
            ),
            (
                "git_delegate",
                "Execute a low or medium-risk constrained Git request.",
                GitDelegationInput,
                self._git_delegate,
                False,
            ),
            (
                "git_delegate_high_risk",
                "Execute a confirmed high-risk constrained Git request.",
                GitDelegationInput,
                self._git_delegate_high_risk,
                False,
            ),
        )
        return tuple(
            self._tool(
                name=name,
                description=description,
                request_model=request_model,
                handler=handler,
                read_only=read_only,
            )
            for (
                name,
                description,
                request_model,
                handler,
                read_only,
            ) in specs
            if name in self._policy.allowed_tools
        )

    def _tool(
        self,
        *,
        name: str,
        description: str,
        request_model: type[BaseModel],
        handler: Callable[[BaseModel], Awaitable[object]],
        read_only: bool,
    ) -> ManagerTool:
        async def invoke(**raw: Any) -> object:
            self._require_tool(name)
            return await handler(request_model.model_validate(raw))

        return ManagerTool(
            name=name,
            description=description,
            input_schema=request_model.model_json_schema(),
            policy=self._policy,
            handler=invoke,
            is_read_only=read_only,
            is_concurrency_safe=read_only,
            yolo=self._yolo,
        )

    def _require_tool(self, name: str) -> None:
        if name not in self._policy.allowed_tools:
            raise PermissionDeniedError(
                f"{name} is not allowed in {self._policy.kind.value}",
            )

    async def _context(self) -> MutationContext:
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned an invalid context")
        return value

    async def _list_tasks(self, request: BaseModel) -> object:
        del request
        tasks = tuple(
            task
            for task in await self._tasks.list_all()
            if self._task_visible(task)
        )
        return TaskCollectionReceipt(
            tool="list_tasks",
            items=tuple(
                task.model_dump(mode="json") for task in tasks
            ),
            total=len(tasks),
        )

    async def _get_task(self, request: BaseModel) -> object:
        item = _TaskIdInput.model_validate(request)
        task = await self._tasks.get(item.task_id)
        if task is not None and not self._task_visible(task):
            raise PermissionDeniedError(
                f"task/{item.task_id} is outside this room's scope",
            )
        return LookupReceipt(
            tool="get_task",
            status="found" if task is not None else "not_found",
            item=(
                task.model_dump(mode="json")
                if task is not None
                else None
            ),
        )

    async def _create_task(self, request: BaseModel) -> object:
        item = CreateFiniteTaskInput.model_validate(request)
        self._require_assignment_scope(
            worker=item.assigned_to,
            team=item.delegated_to_team,
        )
        if item.project_id is not None:
            await self._require_visible_project(item.project_id)
            return await self._project_service.add_task(
                project_id=item.project_id,
                title=item.title,
                specification=item.specification,
                assigned_to=item.assigned_to,
                delegated_to_team=item.delegated_to_team,
                dependencies=(),
                context=await self._context(),
            )
        return await TaskTools(self._task_service).create_finite(
            item,
            context=await self._context(),
        )

    async def _update_task(self, request: BaseModel) -> object:
        item = UpdateTaskInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        context = await self._context()
        if item.action == "cancel":
            return await self._task_service.cancel(
                task_id=item.task_id,
                context=context,
            )
        if item.action == "record_execution":
            return await self._task_service.record_execution(
                task_id=item.task_id,
                worker_event_id=context.event_id,
            )
        return await self._task_service.record_completion(
            task_id=item.task_id,
            worker_event_id=context.event_id,
            structured_result=item.result,
            accepted=item.accepted,
            result_digest=item.result_digest,
        )

    async def _inspect_task_result(self, request: BaseModel) -> object:
        item = _TaskIdInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        return await self._task_service.inspect_result(
            task_id=item.task_id,
        )

    async def _delete_task(self, request: BaseModel) -> object:
        item = CancelTaskInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        return await self._task_service.cancel(
            task_id=item.task_id,
            context=await self._context(),
        )

    async def _delegate_team_task(self, request: BaseModel) -> object:
        item = DelegateTeamTaskInput.model_validate(request)
        self._require_assignment_scope(
            worker=item.leader,
            team=item.team_name,
        )
        if item.project_id is not None:
            await self._require_visible_project(item.project_id)
            return await self._project_service.add_task(
                project_id=item.project_id,
                title=item.title,
                specification=item.specification,
                assigned_to=item.leader,
                delegated_to_team=item.team_name,
                dependencies=(),
                context=await self._context(),
            )
        return await self._task_service.create_finite(
            title=item.title,
            spec=item.specification,
            assigned_to=item.leader,
            delegated_to_team=item.team_name,
            project_id=item.project_id,
            project_room_id=item.project_room_id,
            context=await self._context(),
        )

    async def _complete_task(self, request: BaseModel) -> object:
        item = CompleteTaskInput.model_validate(request)
        task = await self._tasks.get(item.task_id)
        if task is None:
            raise NotFoundError(f"task/{item.task_id} does not exist")
        if not self._task_visible(task):
            raise PermissionDeniedError(
                f"task/{item.task_id} is outside this room's scope",
            )
        context = await self._context()
        if task.project_id:
            return await self._project_service.complete_task(
                project_id=task.project_id,
                task_id=task.task_id,
                worker_event_id=context.event_id,
                sender_id=_policy_sender(self._policy),
                structured_result=item.result,
                accepted=item.accepted,
                result_digest=item.result_digest,
            )
        if task.task_type in {"infinite", "recurring"}:
            return await self._task_service.record_execution(
                task_id=item.task_id,
                worker_event_id=context.event_id,
            )
        return await self._task_service.record_completion(
            task_id=item.task_id,
            worker_event_id=context.event_id,
            structured_result=item.result,
            accepted=item.accepted,
            result_digest=item.result_digest,
        )

    async def _schedule_task(self, request: BaseModel) -> object:
        item = CreateRecurringTaskInput.model_validate(request)
        return await TaskTools(self._task_service).create_recurring(
            item,
            context=await self._context(),
        )

    async def _create_project(self, request: BaseModel) -> object:
        item = CreateProjectInput.model_validate(request)
        context = await self._context()
        receipt = await ProjectTools(self._project_service).create(
            item,
            context=context,
        )
        if (
            receipt.status == "planning"
            and self._yolo
        ):
            return await self._project_service.confirm_plan(
                project_id=receipt.project_id,
                confirmed_by=_policy_sender(self._policy),
                context=MutationContext(
                    room_id=context.room_id,
                    event_id=context.event_id,
                    tool_call_id=(
                        f"{context.tool_call_id}:auto-confirm-plan"
                    ),
                ),
                auto_confirmed=True,
            )
        return receipt

    async def _confirm_project_plan(self, request: BaseModel) -> object:
        item = ConfirmProjectPlanInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.confirm_plan(
            project_id=item.project_id,
            confirmed_by=_policy_sender(self._policy),
            context=await self._context(),
        )

    async def _list_projects(self, request: BaseModel) -> object:
        del request
        projects = tuple(
            project
            for project in await self._projects.list_all()
            if self._project_visible(project)
        )
        return TaskCollectionReceipt(
            tool="list_projects",
            items=tuple(
                project.model_dump(mode="json")
                for project in projects
            ),
            total=len(projects),
        )

    async def _get_project(self, request: BaseModel) -> object:
        item = _ProjectIdInput.model_validate(request)
        project = await self._projects.get(item.project_id)
        if project is not None and not self._project_visible(project):
            raise PermissionDeniedError(
                f"project/{item.project_id} is outside this room's scope",
            )
        return LookupReceipt(
            tool="get_project",
            status="found" if project is not None else "not_found",
            item=(
                project.model_dump(mode="json")
                if project is not None
                else None
            ),
        )

    async def _update_project(self, request: BaseModel) -> object:
        item = UpdateProjectInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        context = await self._context()
        if item.action == "complete_task":
            assert item.task_id is not None
            return await self._project_service.complete_task(
                project_id=item.project_id,
                task_id=item.task_id,
                worker_event_id=context.event_id,
                sender_id=_policy_sender(self._policy),
                structured_result=item.result,
                accepted=item.accepted,
                result_digest=item.result_digest,
            )
        assert item.title is not None
        assert item.specification is not None
        assert item.assigned_to is not None
        return await self._project_service.add_task(
            project_id=item.project_id,
            title=item.title,
            specification=item.specification,
            assigned_to=item.assigned_to,
            delegated_to_team=item.delegated_to_team,
            dependencies=item.dependencies,
            context=context,
        )

    async def _delete_project(self, request: BaseModel) -> object:
        item = CloseProjectInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.close(
            project_id=item.project_id,
            force=item.force,
            context=await self._context(),
        )

    async def _request_project_revision(
        self,
        request: BaseModel,
    ) -> object:
        item = RequestProjectRevisionInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.request_revision(
            project_id=item.project_id,
            task_id=item.task_id,
            feedback=item.feedback,
            assigned_to=item.assigned_to,
            triggered_by_task_id=item.triggered_by_task_id,
            context=await self._context(),
        )

    async def _reassign_project_task(
        self,
        request: BaseModel,
    ) -> object:
        item = ReassignProjectTaskInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.reassign_task(
            project_id=item.project_id,
            task_id=item.task_id,
            assigned_to=item.assigned_to,
            reason=item.reason,
            context=await self._context(),
        )

    async def _report_project_blocked(
        self,
        request: BaseModel,
    ) -> object:
        item = ReportProjectBlockedInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.report_blocked(
            project_id=item.project_id,
            task_id=item.task_id,
            sender_id=_policy_sender(self._policy),
            reason=item.reason,
        )

    async def _revise_project_plan(self, request: BaseModel) -> object:
        item = ReviseProjectPlanInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.revise_plan(
            project_id=item.project_id,
            plan=item.plan,
            change_kind="minor",
            reason=item.reason,
            context=await self._context(),
        )

    async def _revise_project_plan_major(
        self,
        request: BaseModel,
    ) -> object:
        item = ReviseProjectPlanInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.revise_plan(
            project_id=item.project_id,
            plan=item.plan,
            change_kind="major",
            reason=item.reason,
            context=await self._context(),
        )

    async def _update_project_participants(
        self,
        request: BaseModel,
    ) -> object:
        item = UpdateProjectParticipantsInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.update_participants(
            project_id=item.project_id,
            add=item.add,
            remove=item.remove,
            reason=item.reason,
            context=await self._context(),
        )

    async def _sync_files(self, request: BaseModel) -> object:
        item = SyncFilesInput.model_validate(request)
        if item.root == "task_artifacts":
            assert item.task_id is not None
            await self._require_visible_task(item.task_id)
            synced = await self._file_sync.sync_task(
                item.task_id,
                direction=item.direction,
                processor=self._policy.resource_name or "manager",
                context=await self._context(),
            )
            result = (
                synced.model_dump(mode="json")
                if isinstance(synced, BaseModel)
                else {"path": str(synced)}
            )
        else:
            if (
                item.root == "worker_workspace"
                and not self._policy.resource_scope_all
                and item.worker_name != self._policy.resource_name
                and item.worker_name
                not in self._policy.allowed_worker_names
            ):
                raise PermissionDeniedError(
                    f"worker/{item.worker_name} is outside room scope",
                )
            root = cast(
                Literal["worker_workspace", "shared_knowledge"],
                item.root,
            )
            if item.direction == "pull":
                receipt = await self._file_sync.pull_root(
                    root,
                    worker_name=item.worker_name,
                )
            else:
                receipt = await self._file_sync.push_root(
                    root,
                    processor=self._policy.resource_name or "manager",
                    worker_name=item.worker_name,
                )
            result = (
                receipt.model_dump(mode="json")
                if isinstance(receipt, BaseModel)
                else {"path": str(receipt)}
            )
        return SyncReceipt(
            direction=item.direction,
            root=item.root,
            task_id=item.task_id,
            worker_name=item.worker_name,
            result=result,
        )

    async def _read_task_file(self, request: BaseModel) -> object:
        item = ReadTaskFileInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        return await self._file_sync.read_task_file(
            item.task_id,
            item.path,
            max_bytes=item.max_bytes,
        )

    async def _inspect_git(self, request: BaseModel) -> object:
        item = GitDelegationInput.model_validate(request)
        return GitRequestParser.parse(item.message)

    async def _git_delegate(self, request: BaseModel) -> object:
        item = GitDelegationInput.model_validate(request)
        return await self._git.execute(
            GitRequestParser.parse(item.message),
            context=await self._context(),
            confirmed=False,
        )

    async def _git_delegate_high_risk(
        self,
        request: BaseModel,
    ) -> object:
        item = GitDelegationInput.model_validate(request)
        parsed = GitRequestParser.parse(item.message)
        if not parsed.requires_confirmation:
            raise ValueError(
                "use git_delegate for low or medium-risk requests",
            )
        return await self._git.execute(
            parsed,
            context=await self._context(),
            confirmed=True,
        )

    def _task_visible(self, task: TaskRecord) -> bool:
        if self._policy.resource_scope_all:
            return True
        if self._policy.kind is RoomKind.WORKER_ROOM:
            return task.assigned_to == self._policy.resource_name
        if self._policy.kind is RoomKind.LEADER_ROOM:
            return (
                task.delegated_to_team == self._policy.team_name
                or task.assigned_to
                in self._policy.allowed_worker_names
            )
        if self._policy.kind is RoomKind.PROJECT_ROOM:
            return task.project_id == self._policy.project_id
        if task.assigned_to in self._policy.allowed_worker_names:
            return True
        return bool(
            task.delegated_to_team
            and task.delegated_to_team
            in self._policy.allowed_team_names
        )

    def _project_visible(self, project: ProjectRecord) -> bool:
        if self._policy.resource_scope_all:
            return True
        if self._policy.kind is RoomKind.PROJECT_ROOM:
            return project.project_id == self._policy.project_id
        participants = frozenset(
            str(item)
            for item in project.metadata.get("participants", ())
        )
        return bool(participants & self._policy.allowed_worker_names)

    async def _require_visible_task(self, task_id: str) -> TaskRecord:
        task = await self._tasks.get(task_id)
        if task is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        if not self._task_visible(task):
            raise PermissionDeniedError(
                f"task/{task_id} is outside this room's scope",
            )
        return task

    async def _require_visible_project(
        self,
        project_id: str,
    ) -> ProjectRecord:
        project = await self._projects.get(project_id)
        if project is None:
            raise NotFoundError(
                f"project/{project_id} does not exist",
            )
        if not self._project_visible(project):
            raise PermissionDeniedError(
                f"project/{project_id} is outside this room's scope",
            )
        return project

    def _require_assignment_scope(
        self,
        *,
        worker: str,
        team: str | None,
    ) -> None:
        if self._policy.resource_scope_all:
            return
        if self._policy.kind is RoomKind.WORKER_ROOM:
            if worker == self._policy.resource_name and team is None:
                return
        elif self._policy.kind is RoomKind.LEADER_ROOM:
            if team == self._policy.team_name:
                return
        elif team is not None:
            if team in self._policy.allowed_team_names:
                return
        elif worker in self._policy.allowed_worker_names:
            return
        raise PermissionDeniedError(
            "task assignment target is outside this room's scope",
        )


class TaskToolkitFactory:
    """Create a fresh task tool set for each immutable room policy."""

    def __init__(
        self,
        *,
        tasks: TaskReader,
        projects: ProjectReader,
        task_service: TaskService,
        project_service: ProjectService,
        file_sync: FileSyncPort,
        git: GitDelegationService,
        yolo: bool = False,
    ) -> None:
        self._tasks = tasks
        self._projects = projects
        self._task_service = task_service
        self._project_service = project_service
        self._file_sync = file_sync
        self._git = git
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        return TaskToolkit(
            policy=policy,
            tasks=self._tasks,
            projects=self._projects,
            task_service=self._task_service,
            project_service=self._project_service,
            file_sync=self._file_sync,
            git=self._git,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )


def _policy_sender(policy: RoomPolicy) -> str:
    if len(policy.allowed_senders) != 1:
        raise PermissionDeniedError(
            "project mutation requires one resolved Matrix sender",
        )
    return next(iter(policy.allowed_senders))
