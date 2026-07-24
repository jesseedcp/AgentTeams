from __future__ import annotations

import json

import pytest
from agentscope.permission import PermissionBehavior, PermissionContext

from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.integrations import IntegrationToolkit
from agentteams_manager.workflows.resources import MutationContext


class Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, MutationContext | None]] = []

    async def list_mcp_servers(self):
        self.calls.append(("list", None, None))
        return ()

    async def configure_mcp(self, request, *, context):
        assert (
            request.server.credential.get_secret_value()
            == "github-secret"
        )
        self.calls.append(("configure", request, context))
        return {"configured": request.server.name}

    async def grant_mcp(self, name, *, workers, context):
        self.calls.append(("grant", (name, workers), context))
        return {"granted": name}

    async def revoke_mcp(self, name, *, workers, context):
        self.calls.append(("revoke", (name, workers), context))
        return {"revoked": name}

    async def delete_mcp(self, name, *, context):
        self.calls.append(("delete", name, context))
        return {"deleted": name}

    async def publish_service(self, *, worker, ports, context):
        self.calls.append(("publish", (worker, ports), context))
        return {"published": worker, "ports": list(ports)}

    async def unpublish_service(self, *, worker, ports, context):
        self.calls.append(("unpublish", (worker, ports), context))
        return {"unpublished": worker, "ports": list(ports)}


def _context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$mcp",
        tool_call_id="tool-mcp",
    )


def _policy(*, allowed: bool = True) -> RoomPolicy:
    names = {
        "list_mcp_servers",
        "configure_mcp",
        "remove_mcp",
    }
    return RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM if allowed else RoomKind.WORKER_ROOM,
        revision=1,
        allowed_tools=frozenset(names if allowed else ()),
        confirm_tools=frozenset(
            {"configure_mcp", "remove_mcp"} if allowed else (),
        ),
    )


@pytest.mark.asyncio
async def test_admin_mcp_tool_is_closed_confirmed_and_secret_typed() -> None:
    service = Service()
    toolkit = IntegrationToolkit(
        policy=_policy(),
        service=service,
        context_provider=_context,
    )
    tools = {tool.name: tool for tool in toolkit.tools}

    assert set(tools) == {
        "list_mcp_servers",
        "configure_mcp",
        "remove_mcp",
    }
    assert all(
        tool.input_schema["additionalProperties"] is False
        for tool in tools.values()
    )
    decision = await tools["configure_mcp"].check_permissions(
        {},
        PermissionContext(),
    )
    assert decision.behavior is PermissionBehavior.ASK

    chunk = await tools["configure_mcp"].call(
        action="upsert",
        server={
            "kind": "rest",
            "name": "github",
            "description": "GitHub",
            "yaml_template": (
                "server:\n"
                "  name: github-mcp-server\n"
                "  config:\n"
                '    accessToken: ""\n'
            ),
            "credential": "github-secret",
            "service": {
                "name": "github-api",
                "domain": "api.github.com",
                "port": 443,
                "protocol": "https",
            },
        },
        workers=["alice"],
        verification_tool="mcp__github__search_issues",
        verification_arguments={"query": "is:open"},
    )

    assert json.loads(chunk.content[0].text) == {
        "configured": "github",
    }
    assert service.calls[0][0] == "configure"
    assert service.calls[0][2] == _context()


def test_non_admin_receives_no_mcp_administration_tools() -> None:
    toolkit = IntegrationToolkit(
        policy=_policy(allowed=False),
        service=Service(),
        context_provider=_context,
    )

    assert toolkit.tools == ()


@pytest.mark.asyncio
async def test_service_tool_warns_route_is_public_and_unauthenticated() -> None:
    service = Service()
    policy = RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_tools=frozenset({"publish_service"}),
        confirm_tools=frozenset({"publish_service"}),
    )
    toolkit = IntegrationToolkit(
        policy=policy,
        service=service,
        context_provider=_context,
    )

    assert [tool.name for tool in toolkit.tools] == ["publish_service"]
    tool = toolkit.tools[0]
    decision = await tool.check_permissions(
        {"action": "publish", "worker": "alice", "ports": [8080]},
        PermissionContext(),
    )
    assert decision.behavior is PermissionBehavior.ASK
    assert "public" in decision.message.casefold()
    assert "unauthenticated" in decision.message.casefold()

    chunk = await tool.call(
        action="publish",
        worker="alice",
        ports=[8080],
    )

    assert json.loads(chunk.content[0].text) == {
        "ports": [8080],
        "published": "alice",
    }
    assert service.calls[-1] == (
        "publish",
        ("alice", (8080,)),
        _context(),
    )
