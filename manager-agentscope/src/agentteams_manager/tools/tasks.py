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
        # 逻辑说明：接受结果必须携带 inspect 得到的 digest，把“看过产物”变成可验证前置条件；拒绝结果则允许没有 digest。
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
        # 逻辑说明：按 add_task/complete_task 动作校验互相关联的字段，并要求接受结果时携带已检查 digest，阻止未验收内容直接进入项目状态机。
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
        # 逻辑说明：要求至少一项增删且禁止同一 Worker 同时出现两侧，消除 workflow 无法确定的参与者最终状态。
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
        # 逻辑说明：按 task_artifacts/worker_workspace/shared_knowledge 强制目标字段互斥，防止一个请求解析到错误本地根或对象前缀。
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
        # 逻辑说明：保存强类型 Task workflow 门面；构造时不创建或迁移任务，后续每个方法仍要求调用方显式传入 MutationContext。
        self._service = service

    async def create_finite(
        self,
        request: CreateFiniteTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        # 逻辑说明：把已验证输入拆成 TaskService 的显式字段并传递幂等上下文；创建、房间派发和回执由 workflow 原子/可恢复地协调。
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
        # 逻辑说明：使用当前 Matrix event 作为 Worker 完成证据，连同结构化结果、接受决定和 digest 交给服务验证后迁移状态。
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
        # 逻辑说明：把 schedule、timezone 与指派信息显式传入 recurring workflow，并保留当前 mutation context 以实现重复调用幂等。
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
        # 逻辑说明：以当前 event ID 记录 recurring/infinite 一次执行，服务负责防止相同周期重复推进 next_scheduled_at。
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
        # 逻辑说明：把 task ID 与当前上下文交给取消 workflow；服务校验生命周期并记录终态，失败不伪报删除。
        return await self._service.cancel(
            task_id=request.task_id,
            context=context,
        )


class ProjectTools:
    """Typed project facade retained for direct workflow composition."""

    def __init__(self, service: ProjectService) -> None:
        # 逻辑说明：保存 Project workflow 门面供 create/confirm/add/close 转发；生命周期状态机和幂等性仍由 service 与显式 context 负责。
        self._service = service

    async def create(
        self,
        request: CreateProjectInput,
        *,
        context: MutationContext,
    ) -> ProjectReceipt:
        # 逻辑说明：传递计划正文、参与者和幂等上下文创建 planning 项目及房间；是否确认由单独阶段或明确 auto-confirm 规则决定。
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
        # 逻辑说明：携带确认人、上下文和 auto-confirm 标志调用计划确认状态机；服务验证项目仍在 planning，重复确认按幂等结果处理。
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
        # 逻辑说明：把项目任务规格、指派与依赖边一起交给服务，服务在验证参与者/图后创建和派发，失败不留下孤立边。
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
        # 逻辑说明：把项目 ID、force 决策与上下文交给关闭 workflow；非 force 时服务检查未完成任务，成功后才返回终态。
        return await self._service.close(
            project_id=request.project_id,
            force=request.force,
            context=context,
        )


class GitDelegationTools:
    """Typed Git facade with parsing separated from confirmed execution."""

    def __init__(self, service: GitDelegationService) -> None:
        # 逻辑说明：保存受约束的 Git 委托服务；此时不解析自然语言也不执行仓库命令，inspect/execute 分离预检与已确认副作用。
        self._service = service

    @staticmethod
    def inspect(request: GitDelegationInput) -> GitRequest:
        # 逻辑说明：仅通过固定 GitRequestParser 把消息转换为允许的结构化 Git 请求，解析失败直接抛错，函数不访问仓库。
        return GitRequestParser.parse(request.message)

    async def execute(
        self,
        request: GitDelegationInput,
        *,
        context: MutationContext,
        confirmed: bool = False,
    ) -> GitDelegationReceipt:
        # 逻辑说明：先用约束 parser 将自然语言变成固定 GitRequest，再传递确认标志与幂等上下文执行；不把原文直接交给 shell。
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
        # 逻辑说明：组合 Task/Project 服务、文件同步、Git、房间 policy 与 mutation context，并建立可见任务工具；构造不派发任务。
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
        # 逻辑说明：声明任务、项目、文件和 Git 工具的闭合 schema/handler/只读性，再仅注册 room policy 允许的能力，模型看不到越权入口。
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
        # 逻辑说明：把任务、项目或 Git handler 与闭合输入 schema 封装成 ManagerTool；执行时复核房间白名单，读工具才标记并发安全。
        async def invoke(**raw: Any) -> object:
            # 逻辑说明：执行时重新核对 allowed_tools，验证闭合输入并调用绑定 handler；越权/schema 错误在任务或外部副作用前终止。
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
        # 逻辑说明：在任务读写进入 workflow 前执行房间工具白名单检查；拒绝路径不创建 Operation、Task 或确认请求。
        if name not in self._policy.allowed_tools:
            raise PermissionDeniedError(
                f"{name} is not allowed in {self._policy.kind.value}",
            )

    async def _context(self) -> MutationContext:
        # 逻辑说明：解析同步或异步 context provider，并只接受当前 Matrix turn 的 MutationContext；无效身份不能进入 mutation workflow。
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned an invalid context")
        return value

    async def _list_tasks(self, request: BaseModel) -> object:
        # 逻辑说明：读取全部持久任务后逐项应用 room policy 可见性，再序列化有界集合；不可见任务不会出现在总数中。
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
        # 逻辑说明：验证 task ID、读取记录并在返回前检查可见性；越权明确拒绝，不以 not_found 隐藏权限审计。
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
        # 逻辑说明：验证有限任务与指派 scope；带 project 时先验证项目并走 ProjectService 加任务，否则走普通 TaskService 创建派发。
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
        # 逻辑说明：验证任务可见性和当前上下文，再按 cancel/record_execution/complete 分派固定状态机方法；完成证据使用真实 Matrix event ID。
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
        # 逻辑说明：验证 task 可见性后拉取并检查提交结果，返回 digest 供后续接受；只读验收不直接迁移完成状态。
        item = _TaskIdInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        return await self._task_service.inspect_result(
            task_id=item.task_id,
        )

    async def _delete_task(self, request: BaseModel) -> object:
        # 逻辑说明：验证可见性并携带幂等上下文调用 cancel；工具名 delete 在领域上是可恢复取消，不物理擦除审计记录。
        item = CancelTaskInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        return await self._task_service.cancel(
            task_id=item.task_id,
            context=await self._context(),
        )

    async def _delegate_team_task(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Leader/Team 指派 scope；项目任务走项目图服务，独立任务走普通创建，二者都携带稳定 mutation context。
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
        # 逻辑说明：读取任务并验证可见性，按 project、recurring/infinite、finite 三类选择正确完成路径；项目完成还绑定唯一 policy sender 与验收 digest。
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
        # 逻辑说明：验证五字段 recurring 规格并通过 typed facade 创建日程任务，传递当前幂等上下文。
        item = CreateRecurringTaskInput.model_validate(request)
        return await TaskTools(self._task_service).create_recurring(
            item,
            context=await self._context(),
        )

    async def _create_project(self, request: BaseModel) -> object:
        # 逻辑说明：先创建 planning 项目；仅 YOLO 且回执仍为 planning 时，用派生 tool-call ID 显式调用 auto-confirm，保持创建和确认各自可幂等恢复。
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
        # 逻辑说明：验证项目可见性、解析唯一确认 sender，并携带当前上下文调用计划确认；服务检查状态与计划版本。
        item = ConfirmProjectPlanInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.confirm_plan(
            project_id=item.project_id,
            confirmed_by=_policy_sender(self._policy),
            context=await self._context(),
        )

    async def _list_projects(self, request: BaseModel) -> object:
        # 逻辑说明：读取全部项目并按 room policy 过滤可见集合，再序列化和计算总数，防止跨项目 metadata 泄漏。
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
        # 逻辑说明：验证 project ID、查询并检查可见性，返回 found/not_found；存在但越权时明确拒绝。
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
        # 逻辑说明：先验证项目可见性与上下文；complete_task 绑定 sender/事件/验收 digest，add_task 则传入规格、指派和依赖图。
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
        # 逻辑说明：验证项目 scope 后调用 close workflow；force 语义由服务检查，只有完成对账才返回关闭回执。
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
        # 逻辑说明：验证项目可见性，传递目标任务、反馈、可选新负责人/触发任务和上下文，由服务创建关联返修并阻塞下游。
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
        # 逻辑说明：验证项目可见性后，把新负责人、原因和幂等上下文交给项目服务原子撤销旧指派并重新派发。
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
        # 逻辑说明：验证项目可见性并用唯一 policy sender 报告阻塞；服务核对 sender 是否为当前指派者后记录状态与原因。
        item = ReportProjectBlockedInput.model_validate(request)
        await self._require_visible_project(item.project_id)
        return await self._project_service.report_blocked(
            project_id=item.project_id,
            task_id=item.task_id,
            sender_id=_policy_sender(self._policy),
            reason=item.reason,
        )

    async def _revise_project_plan(self, request: BaseModel) -> object:
        # 逻辑说明：验证项目后将计划变更固定标记为 minor，携带理由和上下文版本化保存并更新当前计划指针。
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
        # 逻辑说明：验证项目后以 major 类型调用计划版本 workflow；工具权限/确认层已在进入此 handler 前处理高风险批准。
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
        # 逻辑说明：验证项目和无交集增删集合，再携带原因与上下文更新参与者；服务负责活跃任务约束及 Matrix 房间成员对账。
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
        # 逻辑说明：验证 root/target 组合；task artifacts 先校验任务可见性并走可恢复 sync_task，其他根再检查 Worker scope 后选择 pull/push，统一返回路径或结构化回执。
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
        # 逻辑说明：验证 task ID、可见性、相对路径和上限，再调用 FileSync 的安全缓存读取；不允许借此读取任意主机路径。
        item = ReadTaskFileInput.model_validate(request)
        await self._require_visible_task(item.task_id)
        return await self._file_sync.read_task_file(
            item.task_id,
            item.path,
            max_bytes=item.max_bytes,
        )

    async def _inspect_git(self, request: BaseModel) -> object:
        # 逻辑说明：验证消息并用约束 parser 返回结构化 GitRequest 与风险等级；只解析，不触发仓库副作用。
        item = GitDelegationInput.model_validate(request)
        return GitRequestParser.parse(item.message)

    async def _git_delegate(self, request: BaseModel) -> object:
        # 逻辑说明：验证并解析 Git 请求，携带当前上下文以未确认模式执行；服务仅允许低/中风险操作，高风险会拒绝。
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
        # 逻辑说明：验证并解析请求，要求 parser 明确认定需要确认后才以 confirmed=True 执行；低/中风险引导使用普通入口，避免批准语义混淆。
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
        # 逻辑说明：按 Manager、Worker、Leader、Project 房间范围判断单个任务可见性；默认仅允许显式 Worker/Team 关联，防止跨房间泄露任务。
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
        # 逻辑说明：全局 scope 允许全部；Project Room 只允许同 ID；其他受限房间仅在项目参与者与 allowed workers 有交集时可见。
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
        # 逻辑说明：读取任务，区分不存在与越权，并仅返回通过 _task_visible 的记录；所有 mutation handler 复用此边界。
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
        # 逻辑说明：读取项目并应用当前 room policy 的可见性判定；不存在或越权分别报错，防止 mutation 跨项目执行。
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
        # 逻辑说明：按 room kind 校验 Worker/Team 指派：Worker 只能派自己，Leader 只能本 Team，其他 Human scope 只能白名单目标；不匹配即拒绝。
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
        # 逻辑说明：保存共享任务依赖和默认确认策略，后续依据房间 policy 创建隔离 toolkit；Factory 本身不改变 Task 或 Project 状态。
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
        # 逻辑说明：为当前 policy 创建独立 TaskToolkit，注入共享读库和 workflow 后返回已按任务/项目资源范围过滤的工具，不跨房间缓存授权。
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
    # 逻辑说明：从统一工具上下文复制 room/event/call ID，生成 Task/Project workflow 的稳定 operation 身份；脱离 Matrix turn 调用立即失败。
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )


def _policy_sender(policy: RoomPolicy) -> str:
    # 逻辑说明：项目状态变更要求 policy 已解析为恰好一个 Matrix sender；数量不为一时拒绝，避免把共享房间权限误作具体执行者身份。
    if len(policy.allowed_senders) != 1:
        raise PermissionDeniedError(
            "project mutation requires one resolved Matrix sender",
        )
    return next(iter(policy.allowed_senders))
