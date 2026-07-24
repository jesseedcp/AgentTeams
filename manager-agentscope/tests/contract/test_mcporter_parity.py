from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentscope.message import TextBlock, ToolCallBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.state import AgentState
from agentscope.tool import ToolBase, ToolChunk
from pydantic import SecretStr

from agentteams_manager.config import (
    MCPServerDocument,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.runtime.mcp import MCPRegistry
from agentteams_manager.tools.base import ManagerToolkit


class SearchIssuesTool(ToolBase):
    def __init__(self) -> None:
        super().__init__()
        self.name = "mcp__github__search_issues"
        self.description = "Search GitHub issues."
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
            message="read only",
        )


class Client:
    name = "github"
    is_stateful = False

    def __init__(self, *, mcp_config, **kwargs) -> None:
        del kwargs
        self.mcp_config = mcp_config
        self.tool = SearchIssuesTool()

    async def list_tools(self):
        return [self.tool]

    async def close(self) -> None:
        return None


def _runtime() -> RuntimeDocument:
    return RuntimeDocument(
        revision=1,
        manager_name="manager",
        model="qwen",
        mcp_servers=(
            MCPServerDocument(
                name="github",
                url="https://gateway/mcp/github",
            ),
        ),
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


@pytest.mark.asyncio
async def test_mcporter_list_schema_and_structured_call_use_toolkit() -> None:
    registry = MCPRegistry(
        gateway_key=SecretStr("secret"),
        client_factory=Client,
    )
    await registry.prepare(_runtime())
    clients = registry.clients_for(
        RoomPolicy(
            room_id="!admin:example",
            kind=RoomKind.ADMIN_DM,
            revision=1,
        ),
        revision=1,
    )
    toolkit = ManagerToolkit(mcps=list(clients))

    schemas = await toolkit.get_tool_schemas()
    names = {
        schema["function"]["name"]
        for schema in schemas
    }
    assert names == {"mcp__github__search_issues"}

    call = ToolCallBlock(
        id="mcp-call",
        name="mcp__github__search_issues",
        input=json.dumps({"query": "is:open label:bug"}),
    )
    async for _ in toolkit.call_tool(call, AgentState()):
        pass

    assert clients[0].tool.calls == [
        {"query": "is:open label:bug"},
    ]


def test_native_mcp_runtime_never_starts_mcporter_subprocess() -> None:
    source = Path(
        "manager-agentscope/src/agentteams_manager/runtime/mcp.py",
    ).read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert "Popen" not in source
    assert "StdioMCPConfig" not in source
