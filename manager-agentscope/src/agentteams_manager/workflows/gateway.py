"""Durable, typed administration of Higress gateway resources.

以可恢复 Operation 管理 Higress gateway 资源。

每次 Provider、Route、Credential 或 Consumer 变更先持久化期望请求，再执行 client 写入，
最后读取实际资源比较。若 HTTP 超时，Operation 进入 reconciling：恢复时先查当前状态，
匹配即补记成功，不匹配才决定重试或报告，避免重复创建和覆盖未知变更。
"""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.clients.higress import (
    AIRouteRequest,
    AIRouteState,
    ConsumerRequest,
    ConsumerState,
    HigressError,
    HigressNotFoundError,
    HigressTransportError,
    LLMProviderRequest,
    LLMProviderState,
)
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    RecoveryError,
)
from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceSupervisor,
)

GatewayResourceKind = Literal["provider", "route", "consumer"]
GatewayRequest = LLMProviderRequest | AIRouteRequest | ConsumerRequest
GatewayState = LLMProviderState | AIRouteState | ConsumerState


class GatewayClient(Protocol):
    async def list_providers(self) -> tuple[LLMProviderState, ...]: ...

    async def get_provider(self, name: str) -> LLMProviderState: ...

    async def upsert_provider(
        self,
        request: LLMProviderRequest,
    ) -> LLMProviderState: ...

    async def delete_provider(self, name: str) -> None: ...

    async def list_routes(self) -> tuple[AIRouteState, ...]: ...

    async def get_route(self, name: str) -> AIRouteState: ...

    async def upsert_route(
        self,
        request: AIRouteRequest,
    ) -> AIRouteState: ...

    async def delete_route(self, name: str) -> None: ...

    async def list_consumers(self) -> tuple[ConsumerState, ...]: ...

    async def get_consumer(self, name: str) -> ConsumerState: ...

    async def upsert_consumer(
        self,
        request: ConsumerRequest,
    ) -> ConsumerState: ...

    async def delete_consumer(self, name: str) -> None: ...


class GatewayReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=32, max_length=32)
    action: Literal["create", "update", "delete"]
    resource_kind: GatewayResourceKind
    name: str
    state: dict[str, object] | None = None


class GatewayService:
    """Journal gateway mutations without persisting credential values."""

    def __init__(
        self,
        *,
        gateway: GatewayClient | None,
        supervisor: ResourceSupervisor,
    ) -> None:
        # 逻辑说明：`__init__` 接收 `gateway`、`supervisor`，初始化 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，返回 `None`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        self._gateway = gateway
        self._supervisor = supervisor

    @property
    def available(self) -> bool:
        # 逻辑说明：`available` 接收 当前服务依赖，判断可用性 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，返回 `bool`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
        return self._gateway is not None

    async def list(
        self,
        resource_kind: GatewayResourceKind,
    ) -> tuple[GatewayState, ...]:
        # 逻辑说明：`list` 接收 `resource_kind`，列出 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，依次复用 `_require_gateway`、`list_providers`、`list_routes`，返回 `tuple[GatewayState, ...]`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        gateway = self._require_gateway()
        if resource_kind == "provider":
            return await gateway.list_providers()
        if resource_kind == "route":
            return await gateway.list_routes()
        return await gateway.list_consumers()

    async def get(
        self,
        resource_kind: GatewayResourceKind,
        name: str,
    ) -> GatewayState:
        # 逻辑说明：`get` 接收 `resource_kind`、`name`，读取 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，依次复用 `_require_gateway`、`get_provider`、`get_route`，返回 `GatewayState`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        gateway = self._require_gateway()
        if resource_kind == "provider":
            return await gateway.get_provider(name)
        if resource_kind == "route":
            return await gateway.get_route(name)
        return await gateway.get_consumer(name)

    async def upsert(
        self,
        request: GatewayRequest,
        *,
        context: MutationContext,
    ) -> GatewayReceipt:
        # 逻辑说明：`upsert` 接收 `request`、`context`，创建或更新 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，依次复用 `_request_kind`、`_safe_request`、`begin`，返回 `GatewayReceipt`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        resource_kind = _request_kind(request)
        durable_request: dict[str, object] = {
            "action": "upsert",
            "resource_kind": resource_kind,
            "name": request.name,
            "spec": _safe_request(request),
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CONFIGURE_GATEWAY,
            target_key=f"gateway/{resource_kind}/{request.name}",
            request=durable_request,
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return GatewayReceipt.model_validate(operation.result)
        if operation.status is not OperationStatus.PLANNED:
            return await self.resume_operation(operation)

        existing = await self._get_optional(resource_kind, request.name)
        action: Literal["create", "update"] = (
            "create" if existing is None else "update"
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "operation": f"{action}_{resource_kind}",
                "name": request.name,
                "spec": _safe_request(request),
            },
        )
        try:
            state = await self._upsert(request)
        except HigressTransportError as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                type(exc).__name__,
            )
            raise
        except HigressError as exc:
            await self._supervisor.effect_failed(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                type(exc).__name__,
            )
            raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "resource_kind": resource_kind,
                "name": request.name,
                "observed": _state_summary(state),
            },
        )
        receipt = GatewayReceipt(
            operation_id=operation.operation_id,
            action=action,
            resource_kind=resource_kind,
            name=request.name,
            state=state.model_dump(mode="json"),
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def delete(
        self,
        resource_kind: GatewayResourceKind,
        name: str,
        *,
        context: MutationContext,
    ) -> GatewayReceipt:
        # 逻辑说明：`delete` 接收 `resource_kind`、`name`、`context`，删除 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，依次复用 `begin`、`model_validate`、`resume_operation`，返回 `GatewayReceipt`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CONFIGURE_GATEWAY,
            target_key=f"gateway/{resource_kind}/{name}",
            request={
                "action": "delete",
                "resource_kind": resource_kind,
                "name": name,
            },
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return GatewayReceipt.model_validate(operation.result)
        if operation.status is not OperationStatus.PLANNED:
            return await self.resume_operation(operation)
        return await self._delete_from_operation(operation)

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> GatewayReceipt:
        # 逻辑说明：`resume_operation` 接收 `operation`，根据 journal 恢复 `operation`，依次复用 `RecoveryError`、`model_validate`、`_operation_field`，返回 `GatewayReceipt`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        if operation.kind is not OperationKind.CONFIGURE_GATEWAY:
            raise RecoveryError("operation is not a gateway mutation")
        if operation.status is OperationStatus.SUCCEEDED:
            return GatewayReceipt.model_validate(operation.result)
        if operation.status in {
            OperationStatus.FAILED,
            OperationStatus.NEEDS_ATTENTION,
        }:
            raise RecoveryError(
                f"gateway operation {operation.operation_id} is terminal",
            )
        action = _operation_field(operation, "action")
        resource_kind = _resource_kind(
            _operation_field(operation, "resource_kind"),
        )
        name = _operation_field(operation, "name")
        if action == "delete":
            return await self._delete_from_operation(operation)
        if action != "upsert":
            raise RecoveryError("gateway operation action is invalid")
        if resource_kind in {"provider", "consumer"}:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                "credential value cannot be proven after restart",
            )
            raise RecoveryError(
                "gateway secret mutation cannot be recovered without "
                "resubmitting the secret",
            )
        raw_spec = operation.request.get("spec")
        if not isinstance(raw_spec, dict):
            raise RecoveryError("gateway route recovery spec is missing")
        route = AIRouteRequest.model_validate(raw_spec)
        observed = await self._get_optional("route", name)
        if observed is None or not _route_matches(observed, route):
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                {
                    "operation": "reconcile_route",
                    "name": name,
                    "spec": raw_spec,
                },
            )
            try:
                observed = await self._require_gateway().upsert_route(route)
            except HigressTransportError as exc:
                await self._supervisor.effect_ambiguous(
                    operation.operation_id,
                    ExternalEffect.HIGRESS,
                    type(exc).__name__,
                )
                raise
            except HigressError as exc:
                await self._supervisor.effect_failed(
                    operation.operation_id,
                    ExternalEffect.HIGRESS,
                    type(exc).__name__,
                )
                raise
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "resource_kind": "route",
                "name": name,
                "observed": _state_summary(observed),
            },
        )
        receipt = GatewayReceipt(
            operation_id=operation.operation_id,
            action="update",
            resource_kind="route",
            name=name,
            state=observed.model_dump(mode="json"),
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _delete_from_operation(
        self,
        operation: OperationRecord,
    ) -> GatewayReceipt:
        # 逻辑说明：`_delete_from_operation` 接收 `operation`，删除 `from operation`，依次复用 `_resource_kind`、`_operation_field`、`before_effect`，返回 `GatewayReceipt`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        resource_kind = _resource_kind(
            _operation_field(operation, "resource_kind"),
        )
        name = _operation_field(operation, "name")
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "operation": f"delete_{resource_kind}",
                "name": name,
            },
        )
        try:
            await self._delete(resource_kind, name)
        except HigressNotFoundError:
            pass
        except HigressTransportError as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                type(exc).__name__,
            )
            raise
        except HigressError as exc:
            await self._supervisor.effect_failed(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                type(exc).__name__,
            )
            raise
        try:
            remaining = await self._get_optional(resource_kind, name)
        except HigressTransportError as exc:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                type(exc).__name__,
            )
            raise
        if remaining is not None:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.HIGRESS,
                "resource still exists after delete",
            )
            raise AmbiguousEffectError(
                f"Higress {resource_kind}/{name} still exists after delete",
            )
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            {
                "resource_kind": resource_kind,
                "name": name,
                "absent": True,
            },
        )
        receipt = GatewayReceipt(
            operation_id=operation.operation_id,
            action="delete",
            resource_kind=resource_kind,
            name=name,
        )
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.HIGRESS,
            receipt.model_dump(mode="json"),
        )
        return receipt

    async def _upsert(self, request: GatewayRequest) -> GatewayState:
        # 逻辑说明：`_upsert` 接收 `request`，创建或更新 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，依次复用 `_require_gateway`、`upsert_provider`、`upsert_route`，返回 `GatewayState`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        gateway = self._require_gateway()
        if isinstance(request, LLMProviderRequest):
            return await gateway.upsert_provider(request)
        if isinstance(request, AIRouteRequest):
            return await gateway.upsert_route(request)
        return await gateway.upsert_consumer(request)

    async def _delete(
        self,
        resource_kind: GatewayResourceKind,
        name: str,
    ) -> None:
        # 逻辑说明：`_delete` 接收 `resource_kind`、`name`，删除 `Higress Gateway 的 Provider、Route 与 Consumer 资源`，依次复用 `_require_gateway`、`delete_provider`、`delete_route`，返回 `None`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        gateway = self._require_gateway()
        if resource_kind == "provider":
            await gateway.delete_provider(name)
        elif resource_kind == "route":
            await gateway.delete_route(name)
        else:
            await gateway.delete_consumer(name)

    async def _get_optional(
        self,
        resource_kind: GatewayResourceKind,
        name: str,
    ) -> GatewayState | None:
        # 逻辑说明：`_get_optional` 接收 `resource_kind`、`name`，读取 `optional`，依次复用 `get`，返回 `GatewayState | None`。 它会推进 Higress Gateway 的 Provider、Route 与 Consumer 资源 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        try:
            return await self.get(resource_kind, name)
        except HigressNotFoundError:
            return None

    def _require_gateway(self) -> GatewayClient:
        # 逻辑说明：`_require_gateway` 接收 当前服务依赖，校验并取得 `gateway`，依次复用 `RuntimeError`，返回 `GatewayClient`。 它只计算、校验或读取数据，不直接产生外部副作用；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        if self._gateway is None:
            raise RuntimeError(
                "Higress gateway administration is unavailable",
            )
        return self._gateway


def _request_kind(request: GatewayRequest) -> GatewayResourceKind:
    # 逻辑说明：`_request_kind` 接收 `request`，识别请求 `kind`，返回 `GatewayResourceKind`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
    if isinstance(request, LLMProviderRequest):
        return "provider"
    if isinstance(request, AIRouteRequest):
        return "route"
    return "consumer"


def _safe_request(request: GatewayRequest) -> dict[str, object]:
    # 逻辑说明：`_safe_request` 接收 `request`，生成可持久化的安全副本 `request`，依次复用 `model_dump`，返回 `dict[str, object]`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
    if isinstance(request, LLMProviderRequest):
        return {
            "name": request.name,
            "type": request.provider_type,
            "protocol": request.protocol,
            "token_count": len(request.tokens),
            "proxyName": request.proxy_name,
            "modelMapping": dict(request.model_mapping),
            "tokenFailoverConfig": request.token_failover.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "rawConfigs": request.raw_configs,
        }
    if isinstance(request, AIRouteRequest):
        return request.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    return {
        "name": request.name,
        "credentials": [
            {
                "type": item.credential_type,
                "source": item.source,
                "key": item.key,
                "value_count": len(item.values),
            }
            for item in request.credentials
        ],
    }


def _state_summary(state: GatewayState) -> dict[str, object]:
    # 逻辑说明：`_state_summary` 接收 `state`，提取状态 `summary`，依次复用 `model_dump`，返回 `dict[str, object]`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
    return state.model_dump(mode="json")


def _route_matches(
    state: GatewayState,
    request: AIRouteRequest,
) -> bool:
    # 逻辑说明：`_route_matches` 接收 `state`、`request`，判断路由 `matches`，返回 `bool`。 它只计算、校验或读取数据，不直接产生外部副作用；下游失败沿用现有错误语义，不会伪造成功回执。
    if not isinstance(state, AIRouteState):
        return False
    return (
        state.name == request.name
        and state.domains == request.domains
        and state.path_predicate == request.path_predicate
        and state.upstreams == request.upstreams
        and state.model_predicates == request.model_predicates
        and state.auth == request.auth
    )


def _resource_kind(value: str) -> GatewayResourceKind:
    # 逻辑说明：`_resource_kind` 接收 `value`，解析资源 `kind`，依次复用 `RecoveryError`，返回 `GatewayResourceKind`。 它只计算、校验或读取数据，不直接产生外部副作用；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
    if value not in {"provider", "route", "consumer"}:
        raise RecoveryError("gateway resource kind is invalid")
    return value  # type: ignore[return-value]


def _operation_field(operation: OperationRecord, name: str) -> str:
    # 逻辑说明：`_operation_field` 接收 `operation`、`name`，读取 operation `field`，依次复用 `get`、`RecoveryError`，返回 `str`。 它只计算、校验或读取数据，不直接产生外部副作用；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
    value = operation.request.get(name)
    if not isinstance(value, str) or not value:
        raise RecoveryError(f"gateway operation {name} is missing")
    return value
