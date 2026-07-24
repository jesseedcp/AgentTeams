from __future__ import annotations

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk
from pydantic import SecretStr

from agentteams_manager.config import (
    MCPServerDocument,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.runtime.mcp import MCPRegistry


class VerificationTool(ToolBase):
    def __init__(self) -> None:
        super().__init__()
        self.name = "mcp__github__search_issues"
        self.description = "Perform a safe verification search."
        self.input_schema = {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        }
        self.is_read_only = True
        self.is_concurrency_safe = False
        self.calls: list[dict[str, str]] = []

    async def call(self, **kwargs) -> ToolChunk:
        self.calls.append(kwargs)
        return ToolChunk(
            content=[TextBlock(text="issue-42")],
            state=ToolResultState.SUCCESS,
            is_last=True,
        )

    async def check_permissions(self, *args, **kwargs):
        del args, kwargs
        return PermissionDecision(
            behavior=PermissionBehavior.ALLOW,
            message="read-only verification",
        )


class Client:
    name = "github"
    is_stateful = False

    def __init__(self, *, mcp_config, **kwargs) -> None:
        del kwargs
        self.mcp_config = mcp_config
        self.tool = VerificationTool()

    async def list_tools(self):
        return [self.tool]

    async def close(self) -> None:
        return None


async def test_registry_is_the_native_list_and_call_verification_boundary() -> None:
    runtime = RuntimeDocument(
        revision=2,
        manager_name="manager",
        model="qwen",
        mcp_servers=(
            MCPServerDocument(
                name="github",
                url=(
                    "http://aigw-local.agentteams.io:8080/"
                    "mcp-servers/mcp-github/mcp"
                ),
            ),
        ),
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )
    registry = MCPRegistry(
        gateway_key=SecretStr("manager-gateway-secret"),
        client_factory=Client,
    )
    await registry.prepare(runtime)

    tools = await registry.list_server_tools("github", revision=2)
    result = await registry.call_server_tool(
        "github",
        "mcp__github__search_issues",
        {"query": "repo:agentscope-ai/AgentTeams"},
        revision=2,
    )

    assert [tool.name for tool in tools] == [
        "mcp__github__search_issues",
    ]
    assert tools[0].calls == [
        {"query": "repo:agentscope-ai/AgentTeams"},
    ]
    assert result.state is ToolResultState.SUCCESS
