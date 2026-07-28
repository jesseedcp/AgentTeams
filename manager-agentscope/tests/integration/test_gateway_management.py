from __future__ import annotations

import json

import pytest
from agentscope.permission import PermissionBehavior, PermissionContext
from agentteams_manager.clients.higress import LLMProviderState
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.gateway import GatewayToolkit
from agentteams_manager.workflows.resources import MutationContext
from pydantic import SecretStr


class Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, object]] = []

    async def list(self, resource_kind):
        self.calls.append(("list", resource_kind, None))
        return (
            LLMProviderState(
                name="deepseek",
                provider_type="deepseek",
                token_count=1,
            ),
        )

    async def get(self, resource_kind, name):
        self.calls.append(("get", resource_kind, name))
        return LLMProviderState(
            name=name,
            provider_type="deepseek",
            token_count=1,
        )

    async def upsert(self, request, *, context):
        assert request.tokens[0].get_secret_value() == "provider-secret"
        self.calls.append(("upsert", request, context))
        return {"operation_id": context.operation_id, "name": request.name}

    async def delete(self, resource_kind, name, *, context):
        self.calls.append(("delete", (resource_kind, name), context))
        return {"operation_id": context.operation_id, "name": name}


def policy(*, admin: bool = True) -> RoomPolicy:
    names = {
        "list_gateway_resources",
        "get_gateway_resource",
        "upsert_gateway_resource",
        "delete_gateway_resource",
    }
    return RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM if admin else RoomKind.WORKER_ROOM,
        revision=1,
        allowed_tools=frozenset(names if admin else ()),
        confirm_tools=frozenset(
            {
                "upsert_gateway_resource",
                "delete_gateway_resource",
            }
            if admin
            else (),
        ),
    )


def context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$gateway",
        tool_call_id="gateway-tool",
    )


@pytest.mark.asyncio
async def test_gateway_tools_are_admin_only_confirmed_and_secret_typed() -> None:
    service = Service()
    toolkit = GatewayToolkit(
        policy=policy(),
        service=service,
        context_provider=context,
        secret_resolver=lambda reference: SecretStr(
            {"env:DEEPSEEK_API_TOKEN": "provider-secret"}[reference],
        ),
    )
    tools = {tool.name: tool for tool in toolkit.tools}

    assert set(tools) == {
        "list_gateway_resources",
        "get_gateway_resource",
        "upsert_gateway_resource",
        "delete_gateway_resource",
    }
    assert all(
        tool.input_schema["additionalProperties"] is False
        for tool in tools.values()
    )
    decision = await tools["upsert_gateway_resource"].check_permissions(
        {},
        PermissionContext(),
    )
    assert decision.behavior is PermissionBehavior.ASK

    listed = await tools["list_gateway_resources"].call(
        resource_kind="provider",
    )
    assert json.loads(listed.content[0].text)["items"][0]["token_count"] == 1

    changed = await tools["upsert_gateway_resource"].call(
        resource={
            "kind": "provider",
            "name": "deepseek",
            "provider_type": "deepseek",
            "token_refs": ["env:DEEPSEEK_API_TOKEN"],
        },
    )
    assert json.loads(changed.content[0].text)["name"] == "deepseek"
    assert service.calls[-1][2] == context()
    schema = json.dumps(tools["upsert_gateway_resource"].input_schema)
    assert "provider-secret" not in schema
    assert '"tokens"' not in schema


def test_non_admin_receives_no_gateway_administration_tools() -> None:
    toolkit = GatewayToolkit(
        policy=policy(admin=False),
        service=Service(),
        context_provider=context,
    )
    assert toolkit.tools == ()
