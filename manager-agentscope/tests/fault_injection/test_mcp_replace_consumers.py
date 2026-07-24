from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from agentteams_manager.clients.agt import ManagerResource
from agentteams_manager.clients.model_gateway import ModelGatewayClient
from agentteams_manager.config import MCPServerDocument
from agentteams_manager.domain.models import WorkerResource
from agentteams_manager.workflows.integrations import IntegrationService
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 24, 9, tzinfo=UTC)


class Higress:
    def __init__(self) -> None:
        self.consumers = {"manager", "worker-alice"}
        self.replacements: list[frozenset[str]] = []
        self.timeout_once = True

    async def get_consumers(self, name: str):
        assert name == "github"
        return frozenset(self.consumers)

    async def replace_consumers(self, name: str, consumers):
        assert name == "github"
        complete = frozenset({"manager", *consumers})
        self.replacements.append(complete)
        self.consumers = set(complete)
        if self.timeout_once:
            self.timeout_once = False
            raise TimeoutError("Console accepted the replacement")
        return complete

    def descriptor(self, name: str):
        return MCPServerDocument(
            name=name,
            url="http://gateway:8080/mcp-servers/mcp-github/mcp",
        )


class Agt:
    def __init__(self) -> None:
        self.manager = ManagerResource(
            name="manager",
            phase="Running",
            model="qwen",
            runtime="agentscope",
        )
        self.worker = WorkerResource(
            name="bob",
            runtime="qwenpaw",
            phase="Running",
            spec={"mcpServers": []},
        )

    async def get_manager(self, name: str):
        assert name == "manager"
        return self.manager

    async def replace_manager_mcp_servers(self, name: str, servers):
        self.manager = self.manager.model_copy(
            update={"mcp_servers": tuple(servers)},
        )
        return self.manager

    async def get_worker(self, name: str):
        assert name == "bob"
        return self.worker

    async def replace_worker_mcp_servers(self, name: str, servers):
        assert name == "bob"
        self.worker = self.worker.model_copy(
            update={
                "spec": {
                    "mcpServers": [
                        item.model_dump(mode="json")
                        for item in servers
                    ],
                },
            },
        )
        return self.worker


@pytest.mark.asyncio
async def test_ambiguous_replace_reconciles_before_replay() -> None:
    higress = Higress()
    service = IntegrationService(
        agt=Agt(),
        gateway=ModelGatewayClient(
            base_url="http://unused",
            api_key=SecretStr("unused"),
        ),
        supervisor=TaskSupervisor(Clock()),
        clock=Clock(),
        manager_name="manager",
        registry=type("Registry", (), {"revision": 1})(),
        watcher=type(
            "Watcher",
            (),
            {"poll_once": lambda self: None},
        )(),
        sleep=lambda _: None,
        higress=higress,
    )
    context = MutationContext(
        room_id="!admin:example",
        event_id="$grant",
        tool_call_id="grant-github",
    )

    with pytest.raises(TimeoutError):
        await service.grant_mcp(
            "github",
            workers=("bob",),
            context=context,
        )
    receipt = await service.grant_mcp(
        "github",
        workers=("bob",),
        context=context,
    )

    assert receipt.consumers == frozenset(
        {"manager", "worker-alice", "worker-bob"},
    )
    assert higress.replacements == [
        frozenset({"manager", "worker-alice", "worker-bob"}),
    ]
