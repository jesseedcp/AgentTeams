from __future__ import annotations

from dataclasses import dataclass

import pytest
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.state import AgentState
from agentscope.tool import ToolBase
from pydantic import SecretStr, ValidationError

from agentteams_manager.config import (
    MCPServerDocument,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.runtime.mcp import (
    MCPPreparationError,
    MCPRegistry,
)


class FakeTool(ToolBase):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name
        self.description = "Search issues."
        self.input_schema = {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        }
        self.is_read_only = True
        self.is_concurrency_safe = False

    async def call(self, **kwargs):
        raise AssertionError(f"unexpected call: {kwargs}")

    async def check_permissions(self, *args, **kwargs):
        del args, kwargs
        return PermissionDecision(
            behavior=PermissionBehavior.ALLOW,
            message="read only",
        )


@dataclass
class FakeClient:
    name: str
    is_stateful: bool
    mcp_config: object
    tools: list[ToolBase]
    close_calls: int = 0

    async def list_tools(self) -> list[ToolBase]:
        return self.tools

    async def close(self) -> None:
        self.close_calls += 1


class ClientFactory:
    def __init__(self, tool_names: dict[str, tuple[str, ...]]) -> None:
        self.tool_names = tool_names
        self.created: list[FakeClient] = []

    def __call__(self, **kwargs) -> FakeClient:
        name = kwargs["name"]
        client = FakeClient(
            name=name,
            is_stateful=kwargs["is_stateful"],
            mcp_config=kwargs["mcp_config"],
            tools=[
                FakeTool(tool_name)
                for tool_name in self.tool_names[name]
            ],
        )
        self.created.append(client)
        return client


def _runtime(
    *servers: MCPServerDocument,
    revision: int = 1,
) -> RuntimeDocument:
    return RuntimeDocument(
        revision=revision,
        manager_name="manager",
        model="qwen",
        mcp_servers=servers,
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


def _policy(
    kind: RoomKind,
    *,
    allowed_mcp_names: frozenset[str] = frozenset(),
) -> RoomPolicy:
    return RoomPolicy(
        room_id="!room:example",
        kind=kind,
        revision=1,
        allowed_mcp_names=allowed_mcp_names,
    )


@pytest.mark.asyncio
async def test_registry_uses_runtime_descriptor_and_secret_header() -> None:
    factory = ClientFactory(
        {"github": ("mcp__github__search_issues",)},
    )
    registry = MCPRegistry(
        gateway_key=SecretStr("gateway-secret"),
        client_factory=factory,
    )
    runtime = _runtime(
        MCPServerDocument(
            name="github",
            url="http://higress:8080/mcp-servers/mcp-github/mcp",
        ),
    )

    generation = await registry.prepare(runtime)

    client = factory.created[0]
    assert generation.revision == 1
    assert client.is_stateful is False
    assert client.mcp_config.url.endswith("/mcp-github/mcp")
    assert client.mcp_config.headers == {
        "Authorization": "Bearer gateway-secret",
    }
    assert "gateway-secret" not in runtime.model_dump_json()
    assert registry.clients_for(
        _policy(RoomKind.ADMIN_DM),
        revision=1,
    ) == (client,)


@pytest.mark.asyncio
async def test_non_admin_receives_only_explicitly_granted_mcp() -> None:
    factory = ClientFactory(
        {
            "github": ("mcp__github__search_issues",),
            "jira": ("mcp__jira__search",),
        },
    )
    registry = MCPRegistry(
        gateway_key=SecretStr("secret"),
        client_factory=factory,
    )
    await registry.prepare(
        _runtime(
            MCPServerDocument(
                name="github",
                url="https://gateway/mcp/github",
            ),
            MCPServerDocument(
                name="jira",
                url="https://gateway/mcp/jira",
            ),
        ),
    )

    clients = registry.clients_for(
        _policy(
            RoomKind.WORKER_ROOM,
            allowed_mcp_names=frozenset({"jira"}),
        ),
        revision=1,
    )

    assert tuple(client.name for client in clients) == ("jira",)
    assert (
        registry.clients_for(
            _policy(RoomKind.HUMAN_OR_CHANNEL_ROOM),
            revision=1,
        )
        == ()
    )


@pytest.mark.asyncio
async def test_prepare_rejects_tool_name_collision() -> None:
    factory = ClientFactory(
        {
            "github": (
                "mcp__github__search",
                "mcp__github__search",
            ),
        },
    )
    registry = MCPRegistry(
        gateway_key=SecretStr("secret"),
        client_factory=factory,
    )

    with pytest.raises(MCPPreparationError, match="duplicate"):
        await registry.prepare(
            _runtime(
                MCPServerDocument(
                    name="github",
                    url="https://gateway/mcp/github",
                ),
            ),
        )

    assert factory.created[0].close_calls == 1


def test_runtime_rejects_duplicate_mcp_names() -> None:
    server = MCPServerDocument(
        name="github",
        url="https://gateway/mcp/github",
    )

    with pytest.raises(ValidationError, match="unique"):
        _runtime(server, server)


@pytest.mark.asyncio
async def test_generation_closes_only_after_last_agent_releases() -> None:
    factory = ClientFactory(
        {"github": ("mcp__github__search",)},
    )
    registry = MCPRegistry(
        gateway_key=SecretStr("secret"),
        client_factory=factory,
    )
    await registry.prepare(
        _runtime(
            MCPServerDocument(
                name="github",
                url="https://gateway/mcp/github",
            ),
        ),
    )
    registry.retain(1)
    registry.retain(1)

    await registry.release(1, active_revision=2)
    assert factory.created[0].close_calls == 0

    await registry.release(1, active_revision=2)
    assert factory.created[0].close_calls == 1
    with pytest.raises(KeyError):
        registry.clients_for(
            _policy(RoomKind.ADMIN_DM),
            revision=1,
        )
