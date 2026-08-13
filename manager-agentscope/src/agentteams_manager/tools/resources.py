"""Resource authorization and deterministic Matrix channel selection.

提供 Worker、Team、Human 与 Matrix 资源的查询和变更工具。

读取工具通过 Controller typed client 返回权威资源；mutation 会从当前 Matrix event 派生
稳定 operation ID，进入 ResourceService 完成“记录意图—执行—核验—回执”。房间类型
还会约束目标资源范围，例如 Team Leader Room 不能借此管理全局 Worker。
"""

from __future__ import annotations

import inspect
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    model_validator,
)

from agentteams_manager.clients.agt import (
    HumanCreateRequest,
    HumanUpdateRequest,
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import (
    HumanResource,
    MediaReference,
    RoomKind,
    RoomPolicy,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.matrix.policy import (
    policy_for_human,
)
from agentteams_manager.tools.base import (
    ManagerTool,
    current_tool_invocation,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceService,
    TeamSpec,
    WorkerDiscovery,
)
from agentteams_manager.workflows.matrix_resources import (
    ChannelResolver as ChannelResolver,
    ChannelStore,
    MatrixMutationWorkflow,
)

RESOURCE_TOOL_NAMES = frozenset(
    {
        "find_worker",
        "import_worker",
        "list_workers",
        "get_worker",
        "create_worker",
        "update_worker",
        "sleep_worker",
        "wake_worker",
        "reset_worker",
        "delete_worker",
        "list_teams",
        "get_team",
        "create_team",
        "update_team",
        "delete_team",
        "list_humans",
        "get_human",
        "create_human",
        "update_human",
        "delete_human",
        "list_channels",
        "create_channel",
        "update_channel",
        "delete_channel",
        "send_notification",
        "list_matrix_rooms",
        "list_matrix_members",
        "lookup_matrix_user",
        "register_matrix_user",
        "get_matrix_room_state",
        "upload_matrix_media",
        "download_matrix_media",
        "invite_matrix_user",
        "kick_matrix_user",
        "ban_matrix_user",
        "unban_matrix_user",
    },
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _EmptyInput(_Input):
    pass


class _NameInput(_Input):
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")


class _FindWorkerInput(_Input):
    query: str = Field(min_length=1, max_length=300)


class _ImportWorkerInput(_Input):
    discovery: WorkerDiscovery | None = None
    candidate_name: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    package_uri: str | None = Field(default=None, pattern=r"^nacos://")
    worker_name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")

    @model_validator(mode="after")
    def choose_source(self) -> _ImportWorkerInput:
        # 逻辑说明：强制 Worker 导入只能二选一：使用 discovery+候选名，或直接给 package URI；混用/缺失会在工具执行外部导入前失败。
        searched = (
            self.discovery is not None
            and self.candidate_name is not None
            and self.package_uri is None
        )
        direct = (
            self.discovery is None
            and self.candidate_name is None
            and self.package_uri is not None
        )
        if not (searched or direct):
            raise ValueError(
                "provide discovery with candidate_name, or package_uri",
            )
        return self


class _RoomInput(_Input):
    room_id: str = Field(min_length=1)


class _UserInput(_Input):
    user_id: str = Field(pattern=r"^@[^:\s]+:.+$")


class _RegisterMatrixUserInput(_Input):
    username: str = Field(pattern=r"^[a-z0-9._=-]{1,255}$")
    password_env: str = Field(pattern=r"^[A-Z][A-Z0-9_]{1,127}$")
    admin: bool = False


class _RoomUserInput(_Input):
    room_id: str = Field(min_length=1)
    user_id: str = Field(pattern=r"^@[^:\s]+:.+$")


class _RoomUserReasonInput(_RoomUserInput):
    reason: str = Field(default="", max_length=500)


class _CreateChannelInput(_Input):
    name: str = Field(min_length=1, max_length=200)
    topic: str = Field(default="", max_length=1000)
    invite: tuple[str, ...] = ()


class _UpdateChannelInput(_Input):
    action: Literal["set_primary", "clear_primary", "trust"]
    user_id: str = Field(pattern=r"^@[^:\s]+:.+$")
    room_id: str | None = None
    peer_user_id: str | None = None

    @model_validator(mode="after")
    def validate_action(self) -> _UpdateChannelInput:
        # 逻辑说明：按 set_primary/trust 动作校验必须的 room 与 peer 字段，防止含糊请求进入 Matrix workflow 后产生不完整关系。
        if self.action == "set_primary" and not self.room_id:
            raise ValueError("set_primary requires room_id")
        if self.action == "trust" and (
            not self.room_id or not self.peer_user_id
        ):
            raise ValueError("trust requires room_id and peer_user_id")
        return self


class _DeleteChannelInput(_Input):
    action: Literal["clear_primary", "remove_trusted"]
    user_id: str = Field(pattern=r"^@[^:\s]+:.+$")
    peer_user_id: str | None = None

    @model_validator(mode="after")
    def validate_action(self) -> _DeleteChannelInput:
        # 逻辑说明：移除信任关系时必须给出对端用户，其他 channel 删除动作不强加该字段，避免 workflow 收到含糊目标。
        if self.action == "remove_trusted" and not self.peer_user_id:
            raise ValueError("remove_trusted requires peer_user_id")
        return self


class _SendNotificationInput(_Input):
    recipient: str = Field(pattern=r"^@[^:\s]+:.+$")
    text: str = Field(min_length=1, max_length=20_000)


class _UploadMediaInput(_Input):
    path: str = Field(min_length=1)


class _DownloadMediaInput(_Input):
    mxc_uri: str = Field(pattern=r"^mxc://")
    media_type: str = Field(min_length=1)
    filename: str | None = None
    size: int | None = Field(default=None, ge=0)
    encryption_key: str | None = None
    encryption_hash: str | None = None
    encryption_iv: str | None = None


class ToolReceipt(BaseModel):
    """Secret-free result returned to the Agent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tool: str
    status: Literal["accepted", "succeeded", "not_found"] = "succeeded"
    resource_type: str | None = None
    name: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)


class ToolCollectionReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tool: str
    items: tuple[dict[str, Any], ...]
    total: int = Field(ge=0)


class ResourceMatrix(Protocol):
    async def joined_rooms(self) -> tuple[str, ...]: ...

    async def members(self, room_id: str) -> tuple[str, ...]: ...

    async def lookup_user(
        self,
        user_id: str,
    ) -> dict[str, str | None]: ...

    async def register_user(
        self,
        *,
        username: str,
        password: SecretStr,
        admin: bool = False,
    ) -> dict[str, str | bool]: ...

    async def create_private_room(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        creation_marker: dict[str, str | int],
    ) -> str: ...

    async def invite_user(self, room_id: str, user_id: str) -> None: ...

    async def kick_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None: ...

    async def ban_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None: ...

    async def unban_user(self, room_id: str, user_id: str) -> None: ...

    async def room_state(
        self,
        room_id: str,
    ) -> tuple[dict[str, Any], ...]: ...

    async def upload_media(self, path: Path) -> str: ...

    async def download_media(
        self,
        reference: MediaReference,
    ) -> tuple[Any, ...]: ...

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str: ...


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]

def human_room_policy(
    human: HumanResource,
    *,
    room_id: str,
    revision: int,
) -> RoomPolicy:
    """Map one Controller Human permission tier to immutable authority."""
    return policy_for_human(
        human,
        room_id=room_id,
        revision=revision,
    )


def authorize_resource_target(
    policy: RoomPolicy,
    *,
    resource_type: str,
    name: str,
) -> None:
    """Reject a target outside the Human's Controller-declared scope."""
    # 逻辑说明：全局 scope 直接允许；否则按 team/worker 选择冻结白名单并校验名称，未知资源类型或越界目标在任何 Controller 副作用前拒绝。
    if policy.resource_scope_all:
        return
    if resource_type == "team":
        allowed = policy.allowed_team_names
    elif resource_type == "worker":
        allowed = policy.allowed_worker_names
    else:
        raise PermissionDeniedError(
            f"resource type {resource_type!r} is not scopeable",
        )
    if name not in allowed:
        raise PermissionDeniedError(
            f"{resource_type}/{name} is outside this room's scope",
        )


class ResourceToolkit:
    """Policy-bound typed tools for resource and Matrix administration."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        resources: ResourceService | Any,
        matrix: ResourceMatrix,
        matrix_workflows: MatrixMutationWorkflow,
        channels: ChannelStore,
        manager_admin_room: str,
        context_provider: ContextProvider | None = None,
        yolo: bool = False,
    ) -> None:
        # 逻辑说明：组合资源、Matrix、渠道 workflow、房间 policy 与 mutation context，并构建权限过滤后的资源工具；构造不创建任何 CR 或房间。
        self._policy = policy
        self._resources = resources
        self._matrix = matrix
        self._matrix_workflows = matrix_workflows
        self._channels = channels
        self._manager_admin_room = manager_admin_room
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        # 逻辑说明：声明所有资源、频道和 Matrix typed tools 的 schema/handler/只读属性，再按 room policy 白名单过滤；用户注册额外限制为 Manager Admin 房间。
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
                "find_worker",
                "Search typed Nacos Worker templates without importing.",
                _FindWorkerInput,
                self._find_worker,
                True,
            ),
            (
                "import_worker",
                "Import exactly one confirmed Nacos Worker candidate.",
                _ImportWorkerInput,
                self._import_worker,
                False,
            ),
            (
                "list_workers",
                "List Controller Workers visible in this room.",
                _EmptyInput,
                self._list_workers,
                True,
            ),
            (
                "get_worker",
                "Get one Controller Worker visible in this room.",
                _NameInput,
                self._get_worker,
                True,
            ),
            (
                "create_worker",
                "Submit one Worker for background provisioning; the returned "
                "resource may be Pending and the admin is notified when ready.",
                WorkerCreateRequest,
                self._create_worker,
                False,
            ),
            (
                "update_worker",
                "Update one Worker, including CoPaw/QwenPaw console state.",
                WorkerUpdateRequest,
                self._update_worker,
                False,
            ),
            (
                "sleep_worker",
                "Sleep one idle Worker through the Controller.",
                _NameInput,
                self._sleep_worker,
                False,
            ),
            (
                "wake_worker",
                "Wake one Worker through the Controller.",
                _NameInput,
                self._wake_worker,
                False,
            ),
            (
                "reset_worker",
                "Recreate one Worker from its saved desired configuration.",
                _NameInput,
                self._reset_worker,
                False,
            ),
            (
                "delete_worker",
                "Delete one Worker after explicit confirmation.",
                _NameInput,
                self._delete_worker,
                False,
            ),
            (
                "list_teams",
                "List Controller Teams visible in this room.",
                _EmptyInput,
                self._list_teams,
                True,
            ),
            (
                "get_team",
                "Get one Controller Team visible in this room.",
                _NameInput,
                self._get_team,
                True,
            ),
            (
                "create_team",
                "Create one typed Team and wait for its Leader Room.",
                TeamSpec,
                self._create_team,
                False,
            ),
            (
                "update_team",
                "Apply a complete typed Team desired state.",
                TeamSpec,
                self._update_team,
                False,
            ),
            (
                "delete_team",
                "Delete one Controller Team.",
                _NameInput,
                self._delete_team,
                False,
            ),
            (
                "list_humans",
                "List Controller Human access resources.",
                _EmptyInput,
                self._list_humans,
                True,
            ),
            (
                "get_human",
                "Get one Controller Human access resource.",
                _NameInput,
                self._get_human,
                True,
            ),
            (
                "create_human",
                "Create one Human identity and permission scope.",
                HumanCreateRequest,
                self._create_human,
                False,
            ),
            (
                "update_human",
                "Update one Human permission scope.",
                HumanUpdateRequest,
                self._update_human,
                False,
            ),
            (
                "delete_human",
                "Remove one Human access resource.",
                _NameInput,
                self._delete_human,
                False,
            ),
            (
                "list_channels",
                "List explicit primary and trusted Matrix channels.",
                _UserInput,
                self._list_channels,
                True,
            ),
            (
                "create_channel",
                "Create a private Matrix coordination room.",
                _CreateChannelInput,
                self._create_channel,
                False,
            ),
            (
                "update_channel",
                "Set a primary or trusted Matrix relationship.",
                _UpdateChannelInput,
                self._update_channel,
                False,
            ),
            (
                "delete_channel",
                "Remove a primary or trusted Matrix relationship.",
                _DeleteChannelInput,
                self._delete_channel,
                False,
            ),
            (
                "send_notification",
                "Send one idempotent Matrix notification.",
                _SendNotificationInput,
                self._send_notification,
                False,
            ),
            (
                "list_matrix_rooms",
                "List rooms joined by the Manager Matrix account.",
                _EmptyInput,
                self._list_matrix_rooms,
                True,
            ),
            (
                "list_matrix_members",
                "List current members of one Matrix room.",
                _RoomInput,
                self._list_matrix_members,
                True,
            ),
            (
                "lookup_matrix_user",
                "Look up one Matrix profile.",
                _UserInput,
                self._lookup_matrix_user,
                True,
            ),
            (
                "register_matrix_user",
                (
                    "Register a local Matrix user from a password "
                    "environment variable; Admin DM only."
                ),
                _RegisterMatrixUserInput,
                self._register_matrix_user,
                False,
            ),
            (
                "get_matrix_room_state",
                "Read current Matrix room state events.",
                _RoomInput,
                self._get_matrix_room_state,
                True,
            ),
            (
                "upload_matrix_media",
                "Upload one local file to Matrix media.",
                _UploadMediaInput,
                self._upload_matrix_media,
                False,
            ),
            (
                "download_matrix_media",
                "Download one Matrix media reference.",
                _DownloadMediaInput,
                self._download_matrix_media,
                True,
            ),
            (
                "invite_matrix_user",
                "Invite one user to a Matrix room.",
                _RoomUserInput,
                self._invite_matrix_user,
                False,
            ),
            (
                "kick_matrix_user",
                "Kick one user from a Matrix room.",
                _RoomUserReasonInput,
                self._kick_matrix_user,
                False,
            ),
            (
                "ban_matrix_user",
                "Ban one user from a Matrix room.",
                _RoomUserReasonInput,
                self._ban_matrix_user,
                False,
            ),
            (
                "unban_matrix_user",
                "Unban one user in a Matrix room.",
                _RoomUserInput,
                self._unban_matrix_user,
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
            and (
                name != "register_matrix_user"
                or (
                    self._policy.kind is RoomKind.ADMIN_DM
                    and self._policy.room_id == self._manager_admin_room
                )
            )
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
        # 逻辑说明：把一个资源操作封装成带闭合 schema 和并发安全标志的 ManagerTool；invoke 再查白名单并验证请求，只有只读工具才声明可并发。
        async def invoke(**raw: Any) -> object:
            # 逻辑说明：执行时复核工具白名单、用 Pydantic 验证闭合输入，再调用固定 handler；权限或校验失败不会到达资源/Matrix 服务。
            self._require_tool(name)
            request = request_model.model_validate(raw)
            return await handler(request)

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
        # 逻辑说明：在调用任何资源 workflow 前核对当前房间的工具白名单；未授权名称直接拒绝且不产生 Controller/Matrix 副作用。
        if name not in self._policy.allowed_tools:
            raise PermissionDeniedError(
                f"{name} is not allowed in {self._policy.kind.value}",
            )

    async def _context(self) -> MutationContext:
        # 逻辑说明：解析同步或异步 context provider 并强制 MutationContext 类型，把外部副作用绑定到稳定 Matrix event/tool-call 身份。
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned an invalid context")
        return value

    def _target(self, resource_type: str, name: str) -> None:
        authorize_resource_target(
            self._policy,
            resource_type=resource_type,
            name=name,
        )

    async def _find_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证查询文本并调用只读发现服务，返回候选而不导入或创建 Worker；远端搜索失败原样传播。
        item = _FindWorkerInput.model_validate(request)
        return await self._resources.find_worker(item.query)

    async def _import_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Worker 目标 scope；直接 URI 先走异步确认，发现候选走本地确认，再携带幂等上下文执行导入并返回脱敏资源回执。
        item = _ImportWorkerInput.model_validate(request)
        self._target("worker", item.worker_name)
        if item.package_uri is not None:
            confirmation = await self._resources.confirm_direct_import(
                package_uri=item.package_uri,
                worker_name=item.worker_name,
            )
        else:
            assert item.discovery is not None
            assert item.candidate_name is not None
            confirmation = self._resources.confirm_import(
                item.discovery,
                candidate_name=item.candidate_name,
                worker_name=item.worker_name,
            )
        worker = await self._resources.import_worker(
            confirmation,
            context=await self._context(),
        )
        return _resource_receipt("import_worker", "worker", worker)

    async def _list_workers(self, request: BaseModel) -> object:
        # 逻辑说明：读取所有 Worker 后按 policy 的资源 scope 投影可见集合，再统一编码回执；不会通过列表泄漏越权资源。
        del request
        workers = await self._resources.list_workers()
        if not self._policy.resource_scope_all:
            workers = tuple(
                worker
                for worker in workers
                if worker.name in self._policy.allowed_worker_names
            )
        return _collection("list_workers", workers)

    async def _get_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证名称和目标 scope 后读取 Worker，存在或 not_found 都包装为稳定回执，不扩大权限。
        item = _NameInput.model_validate(request)
        self._target("worker", item.name)
        worker = await self._resources.get_worker(item.name)
        return _optional_resource("get_worker", "worker", item.name, worker)

    async def _create_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Worker 规格与 scope，携带当前上下文调用异步创建 workflow；返回 accepted 和后台 provisioning 标记，不把 desired 写入冒充 ready。
        item = WorkerCreateRequest.model_validate(request)
        self._target("worker", item.name)
        worker = await self._resources.create_worker(
            item,
            context=await self._context(),
        )
        return ToolReceipt(
            tool="create_worker",
            status="accepted",
            resource_type="worker",
            name=worker.name,
            result={
                **worker.model_dump(mode="json"),
                "background_provisioning": True,
            },
        )

    async def _update_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证更新输入与 Worker scope，调用可恢复更新 workflow 并返回最新资源回执；失败不返回部分 desired state。
        item = WorkerUpdateRequest.model_validate(request)
        self._target("worker", item.name)
        worker = await self._resources.update_worker(
            item,
            context=await self._context(),
        )
        return _resource_receipt("update_worker", "worker", worker)

    async def _sleep_worker(self, request: BaseModel) -> object:
        # 逻辑说明：委托统一 Worker 生命周期路径执行 sleep，复用相同 scope、上下文与回执规则。
        return await self._worker_lifecycle("sleep", request)

    async def _wake_worker(self, request: BaseModel) -> object:
        # 逻辑说明：委托统一 Worker 生命周期路径执行 wake，异常和 Controller 对账结果由服务层传播。
        return await self._worker_lifecycle("wake", request)

    async def _reset_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证名称与 scope，取得幂等上下文后调用 reset workflow，并把 Controller 返回资源转成无 Secret 回执。
        item = _NameInput.model_validate(request)
        self._target("worker", item.name)
        worker = await self._resources.reset_worker(
            item.name,
            context=await self._context(),
        )
        return _resource_receipt(
            "reset_worker",
            "worker",
            worker,
        )

    async def _worker_lifecycle(
        self,
        action: str,
        request: BaseModel,
    ) -> object:
        # 逻辑说明：统一验证 Worker 名称/scope，按受限 action 选择固定服务方法并传递上下文；完成后返回同格式资源回执。
        item = _NameInput.model_validate(request)
        self._target("worker", item.name)
        method = getattr(self._resources, f"{action}_worker")
        worker = await method(
            item.name,
            context=await self._context(),
        )
        return _resource_receipt(
            f"{action}_worker",
            "worker",
            worker,
        )

    async def _delete_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Worker 目标，携带当前 operation 上下文执行删除；服务成功后才返回 deleted，失败或对账中不会伪报终态。
        item = _NameInput.model_validate(request)
        self._target("worker", item.name)
        await self._resources.delete_worker(
            item.name,
            context=await self._context(),
        )
        return _deleted("delete_worker", "worker", item.name)

    async def _list_teams(self, request: BaseModel) -> object:
        # 逻辑说明：读取 Team 后按 allowed_team_names 过滤 Human scope，再返回有界结构化集合。
        del request
        teams = await self._resources.list_teams()
        if not self._policy.resource_scope_all:
            teams = tuple(
                team
                for team in teams
                if team.name in self._policy.allowed_team_names
            )
        return _collection("list_teams", teams)

    async def _get_team(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Team 名称与 scope，读取并返回资源或 not_found 回执。
        item = _NameInput.model_validate(request)
        self._target("team", item.name)
        team = await self._resources.get_team(item.name)
        return _optional_resource("get_team", "team", item.name, team)

    async def _create_team(self, request: BaseModel) -> object:
        # 逻辑说明：验证完整 TeamSpec 与目标 scope，携带幂等上下文创建 Controller desired state并返回资源回执。
        item = TeamSpec.model_validate(request)
        self._target("team", item.name)
        team = await self._resources.create_team(
            item,
            context=await self._context(),
        )
        return _resource_receipt("create_team", "team", team)

    async def _update_team(self, request: BaseModel) -> object:
        # 逻辑说明：验证 TeamSpec 和 scope，调用 apply workflow 对账期望与实际 Team，并只返回提交后的资源表示。
        item = TeamSpec.model_validate(request)
        self._target("team", item.name)
        team = await self._resources.apply_team(
            item,
            context=await self._context(),
        )
        return _resource_receipt("update_team", "team", team)

    async def _delete_team(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Team scope 后执行可恢复删除，并在成功回执中明确保留的 Worker，避免用户误以为成员也被删除。
        item = _NameInput.model_validate(request)
        self._target("team", item.name)
        preserved_workers = await self._resources.delete_team(
            item.name,
            context=await self._context(),
        )
        return ToolReceipt(
            tool="delete_team",
            resource_type="team",
            name=item.name,
            result={
                "deleted": True,
                "preservedWorkers": list(preserved_workers),
            },
        )

    async def _list_humans(self, request: BaseModel) -> object:
        # 逻辑说明：调用只读 Human 列表服务并统一序列化集合；工具注册策略已限定能看到该能力的房间。
        del request
        return _collection(
            "list_humans",
            await self._resources.list_humans(),
        )

    async def _get_human(self, request: BaseModel) -> object:
        # 逻辑说明：验证名称后读取 Human，统一返回资源或 not_found 回执，不暴露内部客户端对象。
        item = _NameInput.model_validate(request)
        human = await self._resources.get_human(item.name)
        return _optional_resource("get_human", "human", item.name, human)

    async def _create_human(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Human 权限规格并携带当前幂等上下文创建 Controller 资源，成功后返回脱敏回执。
        item = HumanCreateRequest.model_validate(request)
        human = await self._resources.create_human(
            item,
            context=await self._context(),
        )
        return _resource_receipt("create_human", "human", human)

    async def _update_human(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Human 更新及权限字段，调用可恢复 workflow；失败不会回传尚未生效的权限。
        item = HumanUpdateRequest.model_validate(request)
        human = await self._resources.update_human(
            item,
            context=await self._context(),
        )
        return _resource_receipt("update_human", "human", human)

    async def _delete_human(self, request: BaseModel) -> object:
        # 逻辑说明：验证目标并带上下文执行删除，只有服务确认后返回 deleted；Matrix 账号处理边界仍由 workflow 决定。
        item = _NameInput.model_validate(request)
        await self._resources.delete_human(
            item.name,
            context=await self._context(),
        )
        return _deleted("delete_human", "human", item.name)

    async def _list_channels(self, request: BaseModel) -> object:
        # 逻辑说明：并行语义上分别读取指定用户的 primary 与 trusted 关系并合成回执；任一数据库读取失败则整次调用失败，不返回不完整拓扑。
        item = _UserInput.model_validate(request)
        return ToolReceipt(
            tool="list_channels",
            result={
                "user_id": item.user_id,
                "primary_room_id": (
                    await self._channels.primary_channel(item.user_id)
                ),
                "trusted_room_ids": list(
                    await self._channels.trusted_channels(item.user_id),
                ),
            },
        )

    async def _create_channel(self, request: BaseModel) -> object:
        # 逻辑说明：验证房间名称、topic 与邀请列表，附上 policy revision 和幂等上下文调用 Matrix workflow；成功后返回新 room ID。
        item = _CreateChannelInput.model_validate(request)
        room_id = await self._matrix_workflows.create_channel(
            name=item.name,
            topic=item.topic,
            invite=item.invite,
            revision=self._policy.revision,
            context=await self._context(),
        )
        return ToolReceipt(
            tool="create_channel",
            resource_type="channel",
            name=room_id,
            result={"room_id": room_id},
        )

    async def _update_channel(self, request: BaseModel) -> object:
        # 逻辑说明：验证 action 对应字段后，携带当前上下文更新 primary/trusted 关系并返回 workflow 的对账结果。
        item = _UpdateChannelInput.model_validate(request)
        result = await self._matrix_workflows.update_channel(
            action=item.action,
            user_id=item.user_id,
            room_id=item.room_id,
            peer_user_id=item.peer_user_id,
            context=await self._context(),
        )
        return ToolReceipt(
            tool="update_channel",
            resource_type="channel",
            result=result,
        )

    async def _delete_channel(self, request: BaseModel) -> object:
        # 逻辑说明：验证 clear/remove 动作及用户对，调用可恢复关系删除 workflow；只删除关系，不假设物理 Matrix 房间被删除。
        item = _DeleteChannelInput.model_validate(request)
        result = await self._matrix_workflows.delete_channel(
            action=item.action,
            user_id=item.user_id,
            peer_user_id=item.peer_user_id,
            context=await self._context(),
        )
        return ToolReceipt(
            tool="delete_channel",
            resource_type="channel",
            result=result,
        )

    async def _send_notification(self, request: BaseModel) -> object:
        # 逻辑说明：验证收件人与正文，携带幂等上下文执行 exactly-once 通知 workflow，再用实际 room/event 结果生成回执。
        item = _SendNotificationInput.model_validate(request)
        result = await self._matrix_workflows.send_notification(
            recipient=item.recipient,
            text=item.text,
            context=await self._context(),
        )
        room_id = str(result["room_id"])
        return ToolReceipt(
            tool="send_notification",
            resource_type="channel",
            name=room_id,
            result=result,
        )

    async def _list_matrix_rooms(self, request: BaseModel) -> object:
        # 逻辑说明：读取当前 Matrix bot 已加入房间并转换为带总数的结构化集合；不把未加入或不可见房间推测进结果。
        del request
        rooms = await self._matrix.joined_rooms()
        return ToolCollectionReceipt(
            tool="list_matrix_rooms",
            items=tuple({"room_id": room_id} for room_id in rooms),
            total=len(rooms),
        )

    async def _list_matrix_members(self, request: BaseModel) -> object:
        # 逻辑说明：验证 room ID、读取 homeserver 当前成员并返回有序结构化集合；远端权限或网络失败直接传播。
        item = _RoomInput.model_validate(request)
        members = await self._matrix.members(item.room_id)
        return ToolCollectionReceipt(
            tool="list_matrix_members",
            items=tuple({"user_id": user_id} for user_id in members),
            total=len(members),
        )

    async def _lookup_matrix_user(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Matrix user ID 后调用只读用户查询，并把结果放入不含凭据的 ToolReceipt。
        item = _UserInput.model_validate(request)
        return ToolReceipt(
            tool="lookup_matrix_user",
            resource_type="matrix_user",
            name=item.user_id,
            result=await self._matrix.lookup_user(item.user_id),
        )

    async def _register_matrix_user(self, request: BaseModel) -> object:
        # 逻辑说明：验证用户名与受限环境变量名，从进程环境读取密码并包装 SecretStr 后调用注册 API；缺 Secret 立即失败，回执不包含密码。
        item = _RegisterMatrixUserInput.model_validate(request)
        raw_password = os.environ.get(item.password_env)
        if not raw_password:
            raise RuntimeError(
                f"password environment variable {item.password_env} "
                "is not configured",
            )
        result = await self._matrix.register_user(
            username=item.username,
            password=SecretStr(raw_password),
            admin=item.admin,
        )
        return ToolReceipt(
            tool="register_matrix_user",
            resource_type="matrix_user",
            name=str(result["user_id"]),
            result=dict(result),
        )

    async def _get_matrix_room_state(self, request: BaseModel) -> object:
        # 逻辑说明：验证 room ID，读取当前 state events 并附总数返回；不修改房间也不缓存为授权事实。
        item = _RoomInput.model_validate(request)
        events = await self._matrix.room_state(item.room_id)
        return ToolCollectionReceipt(
            tool="get_matrix_room_state",
            items=events,
            total=len(events),
        )

    async def _upload_matrix_media(self, request: BaseModel) -> object:
        # 逻辑说明：验证本地路径字符串，绑定当前幂等上下文后由 Matrix workflow 上传；成功只返回 MXC URI，不回显文件内容。
        item = _UploadMediaInput.model_validate(request)
        uri = await self._matrix_workflows.upload_media(
            path=Path(item.path),
            context=await self._context(),
        )
        return ToolReceipt(
            tool="upload_matrix_media",
            resource_type="matrix_media",
            name=uri,
            result={"mxc_uri": uri},
        )

    async def _download_matrix_media(self, request: BaseModel) -> object:
        # 逻辑说明：验证 MXC 与可选加密元数据，调用 Matrix 客户端下载/解密，再把消息块规范为 JSON；任一步失败不返回部分块。
        item = _DownloadMediaInput.model_validate(request)
        blocks = await self._matrix.download_media(
            MediaReference.model_validate(item.model_dump()),
        )
        serialized = tuple(_block_value(block) for block in blocks)
        return ToolCollectionReceipt(
            tool="download_matrix_media",
            items=serialized,
            total=len(serialized),
        )

    async def _invite_matrix_user(self, request: BaseModel) -> object:
        # 逻辑说明：验证 room/user 并以 invite 动作调用统一成员 workflow，带幂等上下文完成远端效果和回执对账。
        item = _RoomUserInput.model_validate(request)
        result = await self._matrix_workflows.change_membership(
            action="invite",
            room_id=item.room_id,
            user_id=item.user_id,
            reason="",
            context=await self._context(),
        )
        return _membership_receipt(
            "invite_matrix_user",
            item,
            result=result,
        )

    async def _kick_matrix_user(self, request: BaseModel) -> object:
        # 逻辑说明：验证 room/user/reason，调用统一 membership workflow 执行 kick；成功后返回实际对账结果。
        item = _RoomUserReasonInput.model_validate(request)
        result = await self._matrix_workflows.change_membership(
            action="kick",
            room_id=item.room_id,
            user_id=item.user_id,
            reason=item.reason,
            context=await self._context(),
        )
        return _membership_receipt(
            "kick_matrix_user",
            item,
            result=result,
        )

    async def _ban_matrix_user(self, request: BaseModel) -> object:
        # 逻辑说明：验证目标和原因，携带当前操作上下文执行 ban；远端失败或结果不明确由 workflow 进入恢复，不伪报成功。
        item = _RoomUserReasonInput.model_validate(request)
        result = await self._matrix_workflows.change_membership(
            action="ban",
            room_id=item.room_id,
            user_id=item.user_id,
            reason=item.reason,
            context=await self._context(),
        )
        return _membership_receipt(
            "ban_matrix_user",
            item,
            result=result,
        )

    async def _unban_matrix_user(self, request: BaseModel) -> object:
        # 逻辑说明：验证 room/user 后以空 reason 执行 unban workflow，并返回标准 membership receipt。
        item = _RoomUserInput.model_validate(request)
        result = await self._matrix_workflows.change_membership(
            action="unban",
            room_id=item.room_id,
            user_id=item.user_id,
            reason="",
            context=await self._context(),
        )
        return _membership_receipt(
            "unban_matrix_user",
            item,
            result=result,
        )


class ResourceToolkitFactory:
    """Create a fresh policy-bound resource tool set per room session."""

    def __init__(
        self,
        *,
        resources: ResourceService,
        matrix: ResourceMatrix,
        matrix_workflows: MatrixMutationWorkflow,
        channels: ChannelStore,
        manager_admin_room: str,
        yolo: bool = False,
    ) -> None:
        # 逻辑说明：保存跨房间共享的资源与 Matrix 服务，Factory 只负责按 policy 装配 toolkit，不在此执行 Worker、Team 或 Project 变更。
        self._resources = resources
        self._matrix = matrix
        self._matrix_workflows = matrix_workflows
        self._channels = channels
        self._manager_admin_room = manager_admin_room
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        # 逻辑说明：使用共享资源、Matrix workflow 和渠道存储为当前 policy 创建隔离 toolkit，并返回经过 room kind、scope 与白名单过滤的工具集合。
        return ResourceToolkit(
            policy=policy,
            resources=self._resources,
            matrix=self._matrix,
            matrix_workflows=self._matrix_workflows,
            channels=self._channels,
            manager_admin_room=self._manager_admin_room,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    # 逻辑说明：把当前 Matrix turn 的 room、event 和 tool-call ID 映射为资源 workflow 的稳定幂等上下文；未绑定调用被统一边界拒绝。
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )


def _resource_receipt(
    tool: str,
    resource_type: str,
    resource: WorkerResource | TeamResource | HumanResource,
) -> ToolReceipt:
    # 逻辑说明：把强类型 Worker/Team/Human 资源转成 JSON-safe、无 Secret 的统一成功回执，保留工具名、资源类型和名称。
    return ToolReceipt(
        tool=tool,
        resource_type=resource_type,
        name=resource.name,
        result=resource.model_dump(mode="json"),
    )


def _optional_resource(
    tool: str,
    resource_type: str,
    name: str,
    resource: WorkerResource | TeamResource | HumanResource | None,
) -> ToolReceipt:
    # 逻辑说明：将 None 映射为显式 not_found，否则复用资源回执；避免调用方把空结果误认为成功创建或删除。
    if resource is None:
        return ToolReceipt(
            tool=tool,
            status="not_found",
            resource_type=resource_type,
            name=name,
        )
    return _resource_receipt(tool, resource_type, resource)


def _collection(
    tool: str,
    resources: tuple[
        WorkerResource | TeamResource | HumanResource,
        ...,
    ],
) -> ToolCollectionReceipt:
    # 逻辑说明：逐个序列化同类资源并计算总数，返回不可变集合回执，隐藏服务实现对象。
    items = tuple(
        resource.model_dump(mode="json")
        for resource in resources
    )
    return ToolCollectionReceipt(
        tool=tool,
        items=items,
        total=len(items),
    )


def _deleted(tool: str, resource_type: str, name: str) -> ToolReceipt:
    # 逻辑说明：只有对应删除 workflow 已成功返回后才构造标准 deleted 回执，保留工具、资源类型和名称供审计。
    return ToolReceipt(
        tool=tool,
        resource_type=resource_type,
        name=name,
        result={"deleted": True},
    )


def _membership_receipt(
    tool: str,
    request: _RoomUserInput,
    *,
    result: dict[str, object] | None = None,
) -> ToolReceipt:
    return ToolReceipt(
        tool=tool,
        resource_type="matrix_membership",
        name=request.user_id,
        result=result
        or {
            "room_id": request.room_id,
            "user_id": request.user_id,
        },
    )


def _block_value(block: Any) -> dict[str, Any]:
    # 逻辑说明：Pydantic 消息块按 JSON 模式序列化；未知块降级为类型名与字符串，确保工具返回可编码而不改变原块。
    if isinstance(block, BaseModel):
        return block.model_dump(mode="json")
    return {"type": type(block).__name__, "value": str(block)}
