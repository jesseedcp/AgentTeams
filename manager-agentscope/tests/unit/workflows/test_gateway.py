from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from agentteams_manager.clients.higress import (
    AIRouteRequest,
    AIRouteState,
    AIRouteUpstream,
    ConsumerRequest,
    ConsumerState,
    CredentialSummary,
    HigressConflictError,
    HigressTransportError,
    KeyAuthCredential,
    LLMProviderRequest,
    LLMProviderState,
    RouteAuthConfig,
)
from agentteams_manager.domain.errors import RecoveryError
from agentteams_manager.domain.models import OperationKind, OperationStatus
from agentteams_manager.workflows.gateway import GatewayService
from agentteams_manager.workflows.resources import MutationContext
from pydantic import SecretStr


class Supervisor:
    def __init__(self) -> None:
        self.operations: dict[str, SimpleNamespace] = {}
        self.events: list[tuple[str, tuple[object, ...]]] = []

    async def begin(self, **kwargs: object) -> SimpleNamespace:
        operation_id = str(kwargs["operation_id"])
        operation = self.operations.get(operation_id)
        if operation is None:
            operation = SimpleNamespace(
                operation_id=operation_id,
                kind=kwargs["kind"],
                target_key=kwargs["target_key"],
                request=kwargs["request"],
                status=OperationStatus.PLANNED,
                result={},
            )
            self.operations[operation_id] = operation
        return operation

    async def before_effect(self, *args: object) -> object:
        self.events.append(("before", args))
        self.operations[str(args[0])].status = OperationStatus.DISPATCHED
        return object()

    async def effect_acknowledged(self, *args: object) -> object:
        self.events.append(("acknowledged", args))
        self.operations[str(args[0])].status = OperationStatus.RUNNING
        return object()

    async def effect_succeeded(self, *args: object) -> object:
        self.events.append(("succeeded", args))
        operation = self.operations[str(args[0])]
        operation.status = OperationStatus.SUCCEEDED
        operation.result = args[2]
        return operation

    async def effect_ambiguous(self, *args: object) -> object:
        self.events.append(("ambiguous", args))
        self.operations[str(args[0])].status = OperationStatus.RECONCILING
        return object()

    async def effect_failed(self, *args: object) -> object:
        self.events.append(("failed", args))
        self.operations[str(args[0])].status = OperationStatus.FAILED
        return object()


class Gateway:
    def __init__(self) -> None:
        self.providers: dict[str, LLMProviderState] = {}
        self.routes: dict[str, AIRouteState] = {}
        self.consumers: dict[str, ConsumerState] = {}
        self.calls: list[tuple[str, str]] = []
        self.failure: Exception | None = None

    async def list_providers(self):
        return tuple(self.providers.values())

    async def get_provider(self, name):
        from agentteams_manager.clients.higress import HigressNotFoundError

        if name not in self.providers:
            raise HigressNotFoundError("missing")
        return self.providers[name]

    async def upsert_provider(self, request):
        self.calls.append(("upsert_provider", request.name))
        if self.failure:
            raise self.failure
        item = LLMProviderState(
            name=request.name,
            provider_type=request.provider_type,
            protocol=request.protocol,
            token_count=len(request.tokens),
            model_mapping=request.model_mapping,
            token_failover=request.token_failover,
            raw_configs=request.raw_configs,
        )
        self.providers[item.name] = item
        return item

    async def delete_provider(self, name):
        self.calls.append(("delete_provider", name))
        if self.failure:
            raise self.failure
        self.providers.pop(name, None)

    async def list_routes(self):
        return tuple(self.routes.values())

    async def get_route(self, name):
        from agentteams_manager.clients.higress import HigressNotFoundError

        if name not in self.routes:
            raise HigressNotFoundError("missing")
        return self.routes[name]

    async def upsert_route(self, request):
        self.calls.append(("upsert_route", request.name))
        if self.failure:
            raise self.failure
        item = AIRouteState.model_validate(
            request.model_dump(mode="json", by_alias=True),
        )
        self.routes[item.name] = item
        return item

    async def delete_route(self, name):
        self.calls.append(("delete_route", name))
        if self.failure:
            raise self.failure
        self.routes.pop(name, None)

    async def list_consumers(self):
        return tuple(self.consumers.values())

    async def get_consumer(self, name):
        from agentteams_manager.clients.higress import HigressNotFoundError

        if name not in self.consumers:
            raise HigressNotFoundError("missing")
        return self.consumers[name]

    async def upsert_consumer(self, request):
        self.calls.append(("upsert_consumer", request.name))
        if self.failure:
            raise self.failure
        item = ConsumerState(
            name=request.name,
            credentials=tuple(
                CredentialSummary(
                    credential_type=credential.credential_type,
                    source=credential.source,
                    key=credential.key,
                    value_count=len(credential.values),
                )
                for credential in request.credentials
            ),
        )
        self.consumers[item.name] = item
        return item

    async def delete_consumer(self, name):
        self.calls.append(("delete_consumer", name))
        if self.failure:
            raise self.failure
        self.consumers.pop(name, None)


def context(call: str = "gateway-call") -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id=call,
    )


def provider() -> LLMProviderRequest:
    return LLMProviderRequest(
        name="deepseek",
        provider_type="deepseek",
        protocol="openai/v1",
        tokens=(SecretStr("provider-secret"),),
    )


def route() -> AIRouteRequest:
    return AIRouteRequest(
        name="deepseek-route",
        domains=("aigw-local.agentteams.io",),
        upstreams=(AIRouteUpstream(provider="deepseek"),),
        auth=RouteAuthConfig(allowed_consumers=("manager",)),
    )


@pytest.mark.asyncio
async def test_gateway_upsert_is_durable_idempotent_and_secret_safe() -> None:
    gateway = Gateway()
    supervisor = Supervisor()
    service = GatewayService(gateway=gateway, supervisor=supervisor)

    first = await service.upsert(provider(), context=context())
    second = await service.upsert(provider(), context=context())

    assert first == second
    assert first.action == "create"
    assert gateway.calls == [("upsert_provider", "deepseek")]
    operation = next(iter(supervisor.operations.values()))
    assert operation.kind is OperationKind.CONFIGURE_GATEWAY
    assert "provider-secret" not in json.dumps(operation.request)
    assert operation.request["spec"]["token_count"] == 1


@pytest.mark.asyncio
async def test_gateway_lists_gets_and_deletes_each_resource_kind() -> None:
    gateway = Gateway()
    supervisor = Supervisor()
    service = GatewayService(gateway=gateway, supervisor=supervisor)
    await service.upsert(provider(), context=context("provider"))
    await service.upsert(route(), context=context("route"))
    await service.upsert(
        ConsumerRequest(
            name="manager",
            credentials=(
                KeyAuthCredential(
                    source="BEARER",
                    values=(SecretStr("consumer-secret"),),
                ),
            ),
        ),
        context=context("consumer"),
    )

    assert len(await service.list("provider")) == 1
    assert (await service.get("route", "deepseek-route")).name == (
        "deepseek-route"
    )
    deleted = await service.delete(
        "consumer",
        "manager",
        context=context("delete-consumer"),
    )
    assert deleted.action == "delete"
    assert await service.list("consumer") == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "event"),
    [
        (HigressTransportError("timeout"), "ambiguous"),
        (HigressConflictError("conflict"), "failed"),
    ],
)
async def test_gateway_classifies_ambiguous_and_definite_failures(
    failure: Exception,
    event: str,
) -> None:
    gateway = Gateway()
    gateway.failure = failure
    supervisor = Supervisor()
    service = GatewayService(gateway=gateway, supervisor=supervisor)

    with pytest.raises(type(failure)):
        await service.upsert(provider(), context=context())
    assert supervisor.events[-1][0] == event


@pytest.mark.asyncio
async def test_recovery_proves_routes_but_never_guesses_secret_values() -> None:
    gateway = Gateway()
    supervisor = Supervisor()
    service = GatewayService(gateway=gateway, supervisor=supervisor)

    route_receipt = await service.upsert(route(), context=context("route"))
    route_operation = supervisor.operations[route_receipt.operation_id]
    route_operation.status = OperationStatus.DISPATCHED
    route_operation.result = {}
    recovered = await service.resume_operation(route_operation)
    assert recovered.name == "deepseek-route"

    provider_receipt = await service.upsert(
        provider(),
        context=context("provider"),
    )
    provider_operation = supervisor.operations[provider_receipt.operation_id]
    provider_operation.status = OperationStatus.DISPATCHED
    provider_operation.result = {}
    with pytest.raises(RecoveryError, match="secret"):
        await service.resume_operation(provider_operation)
