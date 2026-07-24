"""Admin-only AgentScope tools for MCP and service integrations."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from agentteams_manager.clients.higress import (
    ProxyMCPRequest,
    RestMCPRequest,
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


class _RestServerInput(RestMCPRequest):
    kind: Literal["rest"]

    def request(self) -> RestMCPRequest:
        return RestMCPRequest.model_validate(
            self.model_dump(exclude={"kind"}),
        )


class _ProxyServerInput(ProxyMCPRequest):
    kind: Literal["proxy"]

    def request(self) -> ProxyMCPRequest:
        return ProxyMCPRequest.model_validate(
            self.model_dump(exclude={"kind"}),
        )


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
    verification_arguments: dict[str, object] = Field(
        default_factory=dict,
        repr=False,
    )

    @model_validator(mode="after")
    def validate_action_shape(self) -> Self:
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
        if len(self.ports) != len(set(self.ports)):
            raise ValueError("service ports must be unique")
        return self


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]


class IntegrationToolkit:
    """Expose integration tools only when immutable room policy permits."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        service: IntegrationService,
        context_provider: ContextProvider | None = None,
        yolo: bool = False,
    ) -> None:
        self._policy = policy
        self._service = service
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
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
        async def invoke(**raw: Any) -> object:
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
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned invalid context")
        return value

    async def _list_mcp_servers(self, request: BaseModel) -> object:
        _ListMCPInput.model_validate(request)
        servers = await self._service.list_mcp_servers()
        return {
            "servers": [
                server.model_dump(mode="json")
                for server in servers
            ],
        }

    async def _configure_mcp(self, request: BaseModel) -> object:
        item = _ConfigureMCPInput.model_validate(request)
        context = await self._context()
        if item.action == "upsert":
            if item.server is None or item.verification_tool is None:
                raise ValueError("invalid upsert request")
            server = item.server.request()
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

    async def _remove_mcp(self, request: BaseModel) -> object:
        item = _RemoveMCPInput.model_validate(request)
        return await self._service.delete_mcp(
            item.name,
            context=await self._context(),
        )

    async def _publish_service(self, request: BaseModel) -> object:
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
        yolo: bool = False,
    ) -> None:
        self._service = service
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        return IntegrationToolkit(
            policy=policy,
            service=self._service,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )
