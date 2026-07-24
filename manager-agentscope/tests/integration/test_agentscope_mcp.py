from __future__ import annotations

import asyncio
import json
import socket
from contextlib import asynccontextmanager

import pytest
import uvicorn
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from mcp.server.fastmcp import FastMCP
from pydantic import SecretStr

from agentteams_manager.config import (
    MCPServerDocument,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.runtime.mcp import MCPRegistry
from agentteams_manager.tools.base import ManagerToolkit


@asynccontextmanager
async def _mcp_server():
    mcp = FastMCP(
        "AgentTeams test MCP",
        stateless_http=True,
        json_response=True,
    )

    @mcp.tool(name="search_issues")
    async def search_issues(query: str) -> dict[str, object]:
        """Search issues."""
        return {"query": query, "issues": [42]}

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            mcp.streamable_http_app(),
            log_level="error",
            lifespan="on",
        ),
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.01)
        else:
            raise RuntimeError("test MCP server did not start")
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)
        sock.close()


@pytest.mark.asyncio
async def test_agentscope_discovers_and_calls_real_mcp_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AgentScope 2.0.4 constructs its own HTTPX client with trust_env=True.
    # Keep the in-process test server out of developer machine proxies.
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    async with _mcp_server() as url:
        runtime = RuntimeDocument(
            revision=1,
            manager_name="manager",
            model="qwen",
            mcp_servers=(
                MCPServerDocument(name="github", url=url),
            ),
            prompt_sources=PromptSources(
                soul="SOUL.md",
                agents="AGENTS.md",
                tools="TOOLS.md",
                heartbeat="HEARTBEAT.md",
            ),
        )
        registry = MCPRegistry(
            gateway_key=SecretStr("test-gateway-key"),
        )
        await registry.prepare(runtime)
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
        assert {
            item["function"]["name"]
            for item in schemas
        } == {"mcp__github__search_issues"}

        chunks = [
            item
            async for item in toolkit.call_tool(
                ToolCallBlock(
                    id="call-search",
                    name="mcp__github__search_issues",
                    input=json.dumps({"query": "is:open"}),
                ),
                AgentState(),
            )
        ]

        assert chunks
        assert "42" in str(chunks[-1].model_dump(mode="json"))
