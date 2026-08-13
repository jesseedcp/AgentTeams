"""Admin-only AgentScope tools for MCP and service integrations.

提供模型、MCP 与服务发布集成的 Admin-only typed tools。

tool 负责 schema 校验和调用上下文，integration workflow 负责预检、写入 Higress、更新
Controller runtime document、验证最终状态与恢复。模型不能直接编辑 MCP 配置文件或
构造任意代理请求；删除、替换、发布等高风险操作仍遵守确认策略。
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SecretStr,
    model_validator,
)

from agentteams_manager.clients.higress import (
    ProxyHeader,
    ProxyMCPRequest,
    RestMCPRequest,
    ServiceSource,
)
from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import (
    ManagerTool,
    current_tool_invocation,
)
from agentteams_manager.workflows.integrations import (
    IntegrationService,
    MCPConfiguration,
)
from agentteams_manager.workflows.resources import MutationContext

INTEGRATION_TOOL_NAMES = frozenset(
    {
        "list_mcp_servers",
        "configure_mcp",
        "remove_mcp",
        "publish_service",
    },
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _ListMCPInput(_Input):
    pass


SecretReference = Annotated[
    str,
    Field(pattern=r"^env:[A-Z][A-Z0-9_]{2,127}$"),
]


class _RestServerInput(_Input):
    kind: Literal["rest"]
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    description: str = Field(default="", max_length=2_000)
    yaml_template: str = Field(min_length=1)
    credential_ref: SecretReference
    service: ServiceSource


class _ProxyHeaderInput(_Input):
    name: str = Field(min_length=1, max_length=256)
    credential_ref: SecretReference
    scheme: Literal["raw", "bearer", "basic"] = "raw"

    @model_validator(mode="after")
    def authorization_scheme_matches_header(self) -> Self:
        # 逻辑说明：当请求自动拼 bearer/basic 前缀时强制目标头为 Authorization；不匹配立即拒绝，防止把凭据以错误语义发给任意 Header。
        if (
            self.scheme != "raw"
            and self.name.casefold() != "authorization"
        ):
            raise ValueError(
                "bearer/basic schemes require Authorization header",
            )
        return self


class _ProxyServerInput(_Input):
    kind: Literal["proxy"]
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    description: str = Field(default="", max_length=2_000)
    backend_url: str = Field(min_length=1, max_length=2_048)
    transport: Literal["http", "sse"]
    headers: tuple[_ProxyHeaderInput, ...] = ()


ServerInput = Annotated[
    _RestServerInput | _ProxyServerInput,
    Field(discriminator="kind"),
]


class _ConfigureMCPInput(_Input):
    action: Literal["upsert", "grant", "revoke"]
    server: ServerInput | None = None
    name: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    workers: tuple[str, ...] = ()
    verification_tool: str | None = None
    verification_arguments: dict[str, JsonValue] = Field(
        default_factory=dict,
        repr=False,
    )

    @model_validator(mode="after")
    def validate_action_shape(self) -> Self:
        # 逻辑说明：按 upsert 与 grant/revoke 两种形状校验互斥字段；确保服务端不会面对“既创建又授权”或缺少验证工具的含糊请求。
        if self.action == "upsert":
            if (
                self.server is None
                or self.name is not None
                or not self.verification_tool
            ):
                raise ValueError(
                    "upsert requires server and verification_tool only",
                )
        elif (
            self.server is not None
            or self.name is None
            or self.verification_tool is not None
            or self.verification_arguments
        ):
            raise ValueError(
                "grant/revoke require name and workers only",
            )
        return self


class _RemoveMCPInput(_Input):
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )


ServicePort = Annotated[int, Field(ge=1, le=65535)]


class _PublishServiceInput(_Input):
    action: Literal["publish", "unpublish"]
    worker: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    ports: tuple[ServicePort, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def require_unique_ports(self) -> Self:
        # 逻辑说明：拒绝重复端口，避免一次发布请求生成重复路由或让恢复回执无法与输入一一对应。
        if len(self.ports) != len(set(self.ports)):
            raise ValueError("service ports must be unique")
        return self


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]
SecretResolver = Callable[
    [str],
    SecretStr | Awaitable[SecretStr],
]


class IntegrationToolkit:
    """Expose integration tools only when immutable room policy permits."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        service: IntegrationService,
        context_provider: ContextProvider | None = None,
        secret_resolver: SecretResolver | None = None,
        yolo: bool = False,
    ) -> None:
        # 逻辑说明：组合集成 workflow、房间 policy、Secret resolver 与 mutation context，并生成允许的集成工具；不会在构造时连接外部系统。
        self._policy = policy
        self._service = service
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._secret_resolver = secret_resolver
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        # 逻辑说明：仅在管理员私聊构造 MCP 与服务发布工具，并按 room policy 白名单过滤；公开路由变更额外附带风险确认说明。
        if self._policy.kind is not RoomKind.ADMIN_DM:
            return ()
        tools = (
            self._tool(
                name="list_mcp_servers",
                description="List secret-free Higress MCP server state.",
                request_model=_ListMCPInput,
                handler=self._list_mcp_servers,
                is_read_only=True,
            ),
            self._tool(
                name="configure_mcp",
                description=(
                    "Upsert, grant, or revoke MCP access after confirmation."
                ),
                request_model=_ConfigureMCPInput,
                handler=self._configure_mcp,
            ),
            self._tool(
                name="remove_mcp",
                description=(
                    "Delete one MCP route and its Controller descriptors "
                    "after confirmation."
                ),
                request_model=_RemoveMCPInput,
                handler=self._remove_mcp,
            ),
            self._tool(
                name="publish_service",
                description=(
                    "Publish or unpublish Worker ports through Controller "
                    "reconciliation."
                ),
                request_model=_PublishServiceInput,
                handler=self._publish_service,
                confirmation_message=(
                    "Confirm changing public, unauthenticated Worker routes. "
                    "Anyone who can reach the generated domain can access "
                    "the service."
                ),
            ),
        )
        return tuple(
            tool
            for tool in tools
            if tool.name in self._policy.allowed_tools
        )

    def _tool(
        self,
        *,
        name: str,
        description: str,
        request_model: type[BaseModel],
        handler: Callable[[BaseModel], Awaitable[object]],
        is_read_only: bool = False,
        confirmation_message: str | None = None,
    ) -> ManagerTool:
        # 逻辑说明：将 MCP/服务发布工具的闭合输入模型、确认属性和固定 handler 封装成 ManagerTool；调用时重新核验管理员权限，避免旧 toolkit 越权执行。
        async def invoke(**raw: Any) -> object:
            # 逻辑说明：执行时复核管理员房间与 allowed_tools，验证闭合请求后才进入固定 workflow；权限或 schema 错误不会接触 Higress/Controller。
            if (
                self._policy.kind is not RoomKind.ADMIN_DM
                or name not in self._policy.allowed_tools
            ):
                raise PermissionDeniedError(
                    f"{name} is not allowed in this room",
                )
            return await handler(request_model.model_validate(raw))

        return ManagerTool(
            name=name,
            description=description,
            input_schema=request_model.model_json_schema(),
            policy=self._policy,
            handler=invoke,
            is_read_only=is_read_only,
            yolo=self._yolo,
            confirmation_message=confirmation_message,
        )

    async def _context(self) -> MutationContext:
        # 逻辑说明：解析同步或异步上下文提供器并强制 MutationContext 类型，使所有集成副作用都绑定稳定 Matrix event 与 tool-call ID。
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned invalid context")
        return value

    async def _list_mcp_servers(self, request: BaseModel) -> object:
        # 逻辑说明：验证空输入、读取服务层的已脱敏 MCP 状态并转成 JSON 列表；不解析或返回任何 Secret。
        _ListMCPInput.model_validate(request)
        servers = await self._service.list_mcp_servers()
        return {
            "servers": [
                server.model_dump(mode="json")
                for server in servers
            ],
        }

    async def _configure_mcp(self, request: BaseModel) -> object:
        # 逻辑说明：验证 action 并先取得幂等上下文；upsert 解析服务与 Secret 后配置，grant/revoke 则按名称修改 Worker 授权，非法分支在外部副作用前终止。
        item = _ConfigureMCPInput.model_validate(request)
        context = await self._context()
        if item.action == "upsert":
            if item.server is None or item.verification_tool is None:
                raise ValueError("invalid upsert request")
            server = await self._server_request(item.server)
            return await self._service.configure_mcp(
                MCPConfiguration(
                    server=server,
                    workers=item.workers,
                    verification_tool=item.verification_tool,
                    verification_arguments=item.verification_arguments,
                ),
                context=context,
            )
        if item.name is None:
            raise ValueError("MCP name is required")
        if item.action == "grant":
            return await self._service.grant_mcp(
                item.name,
                workers=item.workers,
                context=context,
            )
        return await self._service.revoke_mcp(
            item.name,
            workers=item.workers,
            context=context,
        )

    async def _server_request(
        self,
        server: _RestServerInput | _ProxyServerInput,
    ) -> RestMCPRequest | ProxyMCPRequest:
        # 逻辑说明：REST 分支解析单个 credential，Proxy 分支逐头解析并按 scheme 拼装 SecretStr；任一引用失败就不返回半成品请求。
        if isinstance(server, _RestServerInput):
            credential = await self._resolve_secret(
                server.credential_ref,
            )
            return RestMCPRequest(
                name=server.name,
                description=server.description,
                yaml_template=server.yaml_template,
                credential=credential,
                service=server.service,
            )
        headers: list[ProxyHeader] = []
        for header in server.headers:
            secret = await self._resolve_secret(header.credential_ref)
            value = secret.get_secret_value()
            if header.scheme != "raw":
                value = f"{header.scheme.title()} {value}"
            headers.append(
                ProxyHeader(
                    name=header.name,
                    value=SecretStr(value),
                ),
            )
        return ProxyMCPRequest(
            name=server.name,
            description=server.description,
            backend_url=server.backend_url,
            transport=server.transport,
            headers=tuple(headers),
        )

    async def _resolve_secret(self, reference: str) -> SecretStr:
        # 逻辑说明：要求已配置 resolver，兼容同步或异步结果并验证 SecretStr 非空；明文只在构造远端请求时短暂使用，不进入工具输出。
        if self._secret_resolver is None:
            raise RuntimeError(
                "MCP secret references are not configured",
            )
        value = self._secret_resolver(reference)
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, SecretStr):
            raise TypeError("secret_resolver returned invalid secret")
        if not value.get_secret_value():
            raise ValueError(f"secret reference {reference!r} is empty")
        return value

    async def _remove_mcp(self, request: BaseModel) -> object:
        # 逻辑说明：验证 MCP 名称并携带当前幂等上下文调用删除 workflow；Higress 与 Controller 描述符的对账/恢复由服务层处理。
        item = _RemoveMCPInput.model_validate(request)
        return await self._service.delete_mcp(
            item.name,
            context=await self._context(),
        )

    async def _publish_service(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Worker 与唯一端口集合并取得操作上下文，再按 action 选择发布或取消发布；服务层负责 Controller reconcile 和最终状态验证。
        item = _PublishServiceInput.model_validate(request)
        context = await self._context()
        if item.action == "publish":
            return await self._service.publish_service(
                worker=item.worker,
                ports=item.ports,
                context=context,
            )
        return await self._service.unpublish_service(
            worker=item.worker,
            ports=item.ports,
            context=context,
        )


class IntegrationToolkitFactory:
    def __init__(
        self,
        *,
        service: IntegrationService,
        secret_resolver: SecretResolver | None = None,
        yolo: bool = False,
    ) -> None:
        # 逻辑说明：保存共享集成服务和 Secret resolver，供各房间创建权限隔离 toolkit；Factory 本身不执行 Git、MCP 或渠道变更。
        self._service = service
        self._secret_resolver = secret_resolver
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        # 逻辑说明：针对每个 RoomPolicy 新建 IntegrationToolkit，并注入共享服务与 Secret resolver；只返回该房间当下允许的集成工具，不缓存权限结果。
        return IntegrationToolkit(
            policy=policy,
            service=self._service,
            secret_resolver=self._secret_resolver,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    # 逻辑说明：把当前 Matrix 工具调用身份映射为 Integration workflow 的 MutationContext；脱离已绑定 turn 的调用会被统一边界拒绝。
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )
