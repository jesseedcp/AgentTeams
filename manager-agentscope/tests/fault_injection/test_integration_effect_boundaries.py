from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from agentteams_manager.clients.agt import ManagerResource
from agentteams_manager.clients.higress import (
    MCPServerState,
    RestMCPRequest,
    ServiceSource,
)
from agentteams_manager.config import (
    MCPServerDocument,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.domain.models import (
    OperationStatus,
    WorkerResource,
)
from agentteams_manager.workflows.integrations import (
    IntegrationService,
    MCPConfiguration,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 24, 13, tzinfo=UTC)


def _document(
    revision: int,
    *servers: MCPServerDocument,
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


class CrashOnce:
    def __init__(self, boundary: str) -> None:
        self.boundary = boundary
        self.enabled = True

    def after(self, boundary: str) -> None:
        if self.enabled and self.boundary == boundary:
            self.enabled = False
            raise TimeoutError(f"crash at {boundary}")


class Higress:
    def __init__(
        self,
        crash: CrashOnce,
        descriptor: MCPServerDocument,
    ) -> None:
        self.crash = crash
        self.descriptor_value = descriptor
        self.server_exists = False
        self.consumers = {"manager"}

    async def upsert_rest_server(self, request):
        assert request.credential.get_secret_value() == "upstream-secret"
        self.server_exists = True
        self.crash.after("after_higress_upsert")
        return self.descriptor_value

    async def upsert_proxy(self, request):
        del request
        raise AssertionError("unexpected proxy")

    async def list_mcp_servers(self):
        if not self.server_exists:
            return ()
        return (
            MCPServerState(
                name="github",
                consumers=frozenset(self.consumers),
            ),
        )

    async def get_consumers(self, name: str):
        assert name == "github"
        return frozenset(self.consumers)

    async def replace_consumers(self, name: str, consumers):
        assert name == "github"
        self.consumers = {"manager", *consumers}
        self.crash.after("after_consumer_replace")
        return frozenset(self.consumers)

    async def delete_server(self, name: str):
        assert name == "github"
        self.server_exists = False

    def descriptor(self, name: str):
        assert name == "github"
        return self.descriptor_value


class Agt:
    def __init__(
        self,
        crash: CrashOnce,
    ) -> None:
        self.crash = crash
        self.manager = ManagerResource(
            name="manager",
            phase="Running",
            model="qwen",
            runtime="agentscope",
        )
        self.workers = {
            "alice": WorkerResource(
                name="alice",
                runtime="qwenpaw",
                phase="Running",
                spec={"mcpServers": [], "expose": []},
                status={"exposedPorts": []},
            ),
        }

    async def get_manager(self, name: str):
        assert name == "manager"
        return self.manager

    async def replace_manager_mcp_servers(self, name: str, servers):
        assert name == "manager"
        self.manager = self.manager.model_copy(
            update={"mcp_servers": tuple(servers)},
        )
        self.crash.after("after_manager_descriptor_update")
        return self.manager

    async def get_worker(self, name: str):
        return self.workers.get(name)

    async def list_workers(self):
        return tuple(self.workers.values())

    async def replace_worker_mcp_servers(self, name: str, servers):
        worker = self.workers[name]
        self.workers[name] = worker.model_copy(
            update={
                "spec": {
                    **worker.spec,
                    "mcpServers": [
                        server.model_dump(mode="json")
                        for server in servers
                    ],
                },
            },
        )
        self.crash.after("after_worker_descriptor_update")
        return self.workers[name]

    async def update_worker_expose(self, name: str, ports):
        worker = self.workers[name]
        routes = [
            {
                "port": port,
                "domain": f"controller-{port}.example",
            }
            for port in ports
        ]
        self.workers[name] = worker.model_copy(
            update={
                "spec": {**worker.spec, "expose": list(ports)},
                "status": {**worker.status, "exposedPorts": routes},
            },
        )
        self.crash.after("after_service_expose_update")
        return self.workers[name]


class Registry:
    def __init__(self) -> None:
        self.revision = 1
        self.current = SimpleNamespace(document=_document(1))


class Watcher:
    def __init__(self, registry: Registry, agt: Agt) -> None:
        self.registry = registry
        self.agt = agt

    async def poll_once(self):
        if self.agt.manager.mcp_servers:
            self.registry.revision = 2
            self.registry.current = SimpleNamespace(
                document=_document(
                    2,
                    *self.agt.manager.mcp_servers,
                ),
            )


class Verifier:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def list_server_tools(self, server_name: str, *, revision: int):
        assert server_name == "github"
        assert revision == 2
        return (
            SimpleNamespace(
                name="mcp__github__search_issues",
                is_read_only=True,
            ),
        )

    async def call_server_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: dict[str, object],
        *,
        revision: int,
    ):
        assert server_name == "github"
        assert tool_name == "mcp__github__search_issues"
        assert revision == 2
        self.calls.append(arguments)
        return {"ok": True}


class Notifications:
    def __init__(self) -> None:
        self.sources: set[str] = set()
        self.attempts = 0

    async def notify_worker(
        self,
        worker: str,
        text: str,
        *,
        source_operation_id: str,
    ) -> None:
        assert worker == "alice"
        assert "github" in text
        self.attempts += 1
        self.sources.add(source_operation_id)


class Gateway:
    async def preflight(self, request):
        raise AssertionError(f"unexpected model preflight: {request}")


def _service(boundary: str):
    crash = CrashOnce(boundary)
    descriptor = MCPServerDocument(
        name="github",
        url="http://gateway:8080/mcp-servers/mcp-github/mcp",
    )
    agt = Agt(crash)
    higress = Higress(crash, descriptor)
    registry = Registry()
    verifier = Verifier()
    notifications = Notifications()
    supervisor = TaskSupervisor(Clock())
    service = IntegrationService(
        agt=agt,
        gateway=Gateway(),
        supervisor=supervisor,
        clock=Clock(),
        manager_name="manager",
        registry=registry,
        watcher=Watcher(registry, agt),
        sleep=lambda _: None,
        higress=higress,
        mcp_verifier=verifier,
        worker_notifications=notifications,
    )
    return (
        service,
        crash,
        agt,
        higress,
        verifier,
        notifications,
        supervisor,
    )


def _context(boundary: str) -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id=f"${boundary}",
        tool_call_id="integration-effect",
    )


def _configuration() -> MCPConfiguration:
    return MCPConfiguration(
        server=RestMCPRequest(
            name="github",
            description="GitHub",
            yaml_template=(
                "server:\n"
                "  name: github-mcp-server\n"
                "  config:\n"
                '    accessToken: ""\n'
            ),
            credential=SecretStr("upstream-secret"),
            service=ServiceSource(
                name="github-api",
                domain="api.github.com",
                port=443,
                protocol="https",
            ),
        ),
        workers=("alice",),
        verification_tool="mcp__github__search_issues",
        verification_arguments={"query": "is:open"},
    )


@pytest.mark.parametrize(
    "boundary",
    (
        "after_higress_upsert",
        "after_consumer_replace",
        "after_manager_descriptor_update",
        "after_worker_descriptor_update",
    ),
)
@pytest.mark.asyncio
async def test_restart_reconciles_mcp_effect_boundary(
    boundary: str,
) -> None:
    (
        service,
        crash,
        agt,
        higress,
        verifier,
        notifications,
        supervisor,
    ) = _service(boundary)
    context = _context(boundary)

    with pytest.raises(TimeoutError):
        await service.configure_mcp(
            _configuration(),
            context=context,
        )
    operation = supervisor.operations[context.operation_id]
    assert operation.status is OperationStatus.RECONCILING
    crash.enabled = False

    await service.resume_operation(operation)

    assert supervisor.operations[context.operation_id].status is (
        OperationStatus.SUCCEEDED
    )
    assert higress.consumers == {"manager", "worker-alice"}
    assert agt.manager.mcp_servers[0].name == "github"
    assert agt.workers["alice"].spec["mcpServers"][0]["name"] == "github"
    assert verifier.calls == [{"query": "is:open"}]
    assert len(notifications.sources) == 1


@pytest.mark.asyncio
async def test_restart_reconciles_service_expose_boundary() -> None:
    service, crash, agt, *_, supervisor = _service(
        "after_service_expose_update",
    )
    context = _context("after_service_expose_update")

    with pytest.raises(TimeoutError):
        await service.publish_service(
            worker="alice",
            ports=(8080,),
            context=context,
        )
    operation = supervisor.operations[context.operation_id]
    crash.enabled = False

    receipt = await service.resume_operation(operation)

    assert receipt.domains == ("controller-8080.example",)
    assert supervisor.operations[context.operation_id].status is (
        OperationStatus.SUCCEEDED
    )
    assert agt.workers["alice"].spec["expose"] == [8080]
