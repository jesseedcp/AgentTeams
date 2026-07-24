from __future__ import annotations

import json

import pytest

from agentteams_manager.clients.agt import AgtClient
from agentteams_manager.clients.process import ProcessTimeout
from agentteams_manager.config import MCPServerDocument
from tests.fixtures.fake_agt import FakeProcess


def _server(name: str = "github") -> MCPServerDocument:
    return MCPServerDocument(
        name=name,
        url=f"https://gateway/mcp/{name}",
        transport="http",
    )


def _manager_payload(*servers: MCPServerDocument) -> dict:
    return {
        "name": "default",
        "phase": "Running",
        "model": "qwen",
        "runtime": "agentscope",
        "mcpServers": [
            server.model_dump(mode="json")
            for server in servers
        ],
    }


def _worker_payload(*servers: MCPServerDocument) -> dict:
    return {
        "name": "alice",
        "phase": "Running",
        "model": "qwen",
        "runtime": "qwenpaw",
        "mcpServers": [
            server.model_dump(mode="json")
            for server in servers
        ],
    }


@pytest.mark.asyncio
async def test_replace_manager_mcp_uses_json_stdin() -> None:
    process = FakeProcess()
    process.queue_error("", returncode=0)
    process.queue_json(_manager_payload(_server()))

    manager = await AgtClient(process).replace_manager_mcp_servers(
        "default",
        (_server(),),
    )

    assert process.calls[0][0] == (
        "agt",
        "update",
        "manager",
        "--name",
        "default",
        "--mcp-servers-file",
        "-",
    )
    assert json.loads(process.calls[0][1]) == [
        {
            "name": "github",
            "url": "https://gateway/mcp/github",
            "transport": "http",
        },
    ]
    assert manager.mcp_servers == (_server(),)


@pytest.mark.asyncio
async def test_replace_worker_mcp_can_clear_complete_set() -> None:
    process = FakeProcess()
    process.queue_error("", returncode=0)
    process.queue_json(_worker_payload())

    worker = await AgtClient(process).replace_worker_mcp_servers(
        "alice",
        (),
    )

    assert process.calls[0][0][-2:] == ("--mcp-servers-file", "-")
    assert process.calls[0][1] == b"[]"
    assert worker.spec["mcpServers"] == []


@pytest.mark.asyncio
async def test_ambiguous_timeout_reconciles_observed_mcp_set() -> None:
    process = FakeProcess()
    process.results.append(ProcessTimeout("timed out"))
    process.queue_json(_worker_payload(_server("jira")))

    worker = await AgtClient(process).replace_worker_mcp_servers(
        "alice",
        (_server("jira"),),
    )

    assert worker.spec["mcpServers"] == [
        _server("jira").model_dump(mode="json"),
    ]
