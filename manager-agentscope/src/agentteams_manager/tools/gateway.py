"""Admin-only AgentScope tools for typed Higress gateway resources.

提供 Higress Provider、Route、Credential 与 Consumer 的 typed tools。

输入模型限制字段与资源类型，读取操作可直接查询，写操作进入可恢复 Gateway workflow。
Secret 只以引用或受保护输入存在，返回给 Agent 的回执会脱敏。Admin full 可免去工具级
确认，但不会允许任意 URL、任意凭据读取或越过 room policy。
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from agentteams_manager.clients.higress import (
    AIRouteRequest,
    AIRouteUpstream,
    ConsumerRequest,
    KeyAuthCredential,
    LLMProviderRequest,
    ProviderType,
    RouteAuthConfig,
    RoutePredicate,
    TokenFailoverConfig,
)
from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import (
    ManagerTool,
    current_tool_invocation,
)
from agentteams_manager.workflows.gateway import (
    GatewayRequest,
    GatewayResourceKind,
    GatewayService,
)
from agentteams_manager.workflows.resources import MutationContext

GATEWAY_TOOL_NAMES = frozenset(
    {
        "list_gateway_resources",
        "get_gateway_resource",
        "upsert_gateway_resource",
        "delete_gateway_resource",
    },
)
SecretReference = Annotated[
    str,
    Field(pattern=r"^env:[A-Z][A-Z0-9_]{2,127}$"),
]


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _ListInput(_Input):
    resource_kind: GatewayResourceKind


class _GetInput(_Input):
    resource_kind: GatewayResourceKind
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )


class _ProviderInput(_Input):
    kind: Literal["provider"]
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    provider_type: ProviderType
    protocol: Literal["openai/v1", "original"] = "openai/v1"
    token_refs: tuple[SecretReference, ...] = Field(min_length=1)
    proxy_name: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    model_mapping: dict[str, str] = Field(default_factory=dict)
    token_failover: TokenFailoverConfig = Field(
        default_factory=TokenFailoverConfig,
    )
    raw_configs: dict[str, object] = Field(default_factory=dict)


class _RouteInput(_Input):
    kind: Literal["route"]
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    domains: tuple[str, ...] = Field(min_length=1)
    path_predicate: RoutePredicate = Field(
        default_factory=lambda: RoutePredicate(
            matchType="PRE",
            matchValue="/",
        ),
    )
    upstreams: tuple[AIRouteUpstream, ...] = Field(min_length=1)
    model_predicates: tuple[RoutePredicate, ...] = ()
    auth: RouteAuthConfig = Field(default_factory=RouteAuthConfig)


class _CredentialInput(_Input):
    source: Literal["BEARER", "HEADER", "QUERY"]
    key: str | None = Field(default=None, min_length=1, max_length=256)
    value_refs: tuple[SecretReference, ...] = Field(min_length=1)


class _ConsumerInput(_Input):
    kind: Literal["consumer"]
    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    credentials: tuple[_CredentialInput, ...] = Field(min_length=1)


ResourceInput = Annotated[
    _ProviderInput | _RouteInput | _ConsumerInput,
    Field(discriminator="kind"),
]


class _UpsertInput(_Input):
    resource: ResourceInput


class _DeleteInput(_GetInput):
    pass


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]
SecretResolver = Callable[
    [str],
    SecretStr | Awaitable[SecretStr],
]


class GatewayToolkit:
    """Expose no generic HTTP path: only three supported resource kinds."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        service: GatewayService,
        context_provider: ContextProvider | None = None,
        secret_resolver: SecretResolver | None = None,
        yolo: bool = False,
    ) -> None:
        self._policy = policy
        self._service = service
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._secret_resolver = secret_resolver
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        if self._policy.kind is not RoomKind.ADMIN_DM:
            return ()
        candidates = (
            self._tool(
                name="list_gateway_resources",
                description=(
                    "List secret-free Higress providers, AI routes, or "
                    "consumers."
                ),
                request_model=_ListInput,
                handler=self._list,
                read_only=True,
            ),
            self._tool(
                name="get_gateway_resource",
                description=(
                    "Get one secret-free Higress provider, AI route, or "
                    "consumer."
                ),
                request_model=_GetInput,
                handler=self._get,
                read_only=True,
            ),
            self._tool(
                name="upsert_gateway_resource",
                description=(
                    "Create or replace one typed Higress gateway resource."
                ),
                request_model=_UpsertInput,
                handler=self._upsert,
                confirmation_message=(
                    "Confirm creating or replacing this Higress gateway "
                    "configuration."
                ),
            ),
            self._tool(
                name="delete_gateway_resource",
                description="Delete one typed Higress gateway resource.",
                request_model=_DeleteInput,
                handler=self._delete,
                confirmation_message=(
                    "Confirm deleting this Higress gateway resource."
                ),
            ),
        )
        return tuple(
            tool
            for tool in candidates
            if tool.name in self._policy.allowed_tools
        )

    def _tool(
        self,
        *,
        name: str,
        description: str,
        request_model: type[BaseModel],
        handler: Callable[[BaseModel], Awaitable[object]],
        read_only: bool = False,
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
            is_read_only=read_only,
            yolo=self._yolo,
            confirmation_message=confirmation_message,
        )

    async def _list(self, request: BaseModel) -> object:
        item = _ListInput.model_validate(request)
        resources = await self._service.list(item.resource_kind)
        return {
            "resource_kind": item.resource_kind,
            "items": [
                resource.model_dump(mode="json")
                for resource in resources
            ],
        }

    async def _get(self, request: BaseModel) -> object:
        item = _GetInput.model_validate(request)
        resource = await self._service.get(
            item.resource_kind,
            item.name,
        )
        return resource.model_dump(mode="json")

    async def _upsert(self, request: BaseModel) -> object:
        item = _UpsertInput.model_validate(request)
        resource = await self._resource_request(item.resource)
        return await self._service.upsert(
            resource,
            context=await self._context(),
        )

    async def _delete(self, request: BaseModel) -> object:
        item = _DeleteInput.model_validate(request)
        return await self._service.delete(
            item.resource_kind,
            item.name,
            context=await self._context(),
        )

    async def _resource_request(
        self,
        item: ResourceInput,
    ) -> GatewayRequest:
        if isinstance(item, _ProviderInput):
            return LLMProviderRequest(
                name=item.name,
                type=item.provider_type,
                protocol=item.protocol,
                tokens=tuple(
                    [
                        await self._resolve_secret(reference)
                        for reference in item.token_refs
                    ],
                ),
                proxyName=item.proxy_name,
                modelMapping=item.model_mapping,
                tokenFailoverConfig=item.token_failover,
                rawConfigs=item.raw_configs,
            )
        if isinstance(item, _RouteInput):
            return AIRouteRequest(
                name=item.name,
                domains=item.domains,
                pathPredicate=item.path_predicate,
                upstreams=item.upstreams,
                modelPredicates=item.model_predicates,
                authConfig=item.auth,
            )
        credentials: list[KeyAuthCredential] = []
        for credential in item.credentials:
            credentials.append(
                KeyAuthCredential(
                    source=credential.source,
                    key=credential.key,
                    values=tuple(
                        [
                            await self._resolve_secret(reference)
                            for reference in credential.value_refs
                        ],
                    ),
                ),
            )
        return ConsumerRequest(
            name=item.name,
            credentials=tuple(credentials),
        )

    async def _resolve_secret(self, reference: str) -> SecretStr:
        if self._secret_resolver is None:
            raise RuntimeError(
                "gateway secret references are not configured",
            )
        value = self._secret_resolver(reference)
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, SecretStr):
            raise TypeError("secret_resolver returned invalid secret")
        if not value.get_secret_value():
            raise ValueError(f"secret reference {reference!r} is empty")
        return value

    async def _context(self) -> MutationContext:
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned invalid context")
        return value


class GatewayToolkitFactory:
    def __init__(
        self,
        *,
        service: GatewayService,
        secret_resolver: SecretResolver | None = None,
        yolo: bool = False,
    ) -> None:
        self._service = service
        self._secret_resolver = secret_resolver
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        return GatewayToolkit(
            policy=policy,
            service=self._service,
            secret_resolver=self._secret_resolver,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )
