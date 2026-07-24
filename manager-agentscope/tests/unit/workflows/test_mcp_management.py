from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from agentteams_manager.clients.agt import ManagerResource
from agentteams_manager.clients.higress import (
    ProxyHeader,
    ProxyMCPRequest,
    RestMCPRequest,
    ServiceSource,
)
from agentteams_manager.clients.model_gateway import ModelGatewayClient
from agentteams_manager.config import (
    MCPServerDocument,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.domain.models import WorkerResource
from agentteams_manager.workflows.integrations import (
    CloudMCPManagementUnsupported,
    IntegrationService,
    MCPConfiguration,
    MCPVerificationError,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 24, 8, tzinfo=UTC)


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


class Registry:
    def __init__(self) -> None:
        self.current = SimpleNamespace(document=_document(1))
        self.revision = 1


class Agt:
    def __init__(
        self,
        order: list[str],
        descriptor: MCPServerDocument,
    ) -> None:
        self.order = order
        self.descriptor = descriptor
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
                room_id="!alice:example",
                spec={"mcpServers": []},
            ),
            "bob": WorkerResource(
                name="bob",
                runtime="qwenpaw",
                phase="Running",
                room_id="!bob:example",
                spec={"mcpServers": []},
            ),
        }

    async def get_manager(self, name: str):
        assert name == "manager"
        return self.manager

    async def replace_manager_mcp_servers(self, name: str, servers):
        assert name == "manager"
        self.order.append("agt.manager_descriptor")
        self.manager = self.manager.model_copy(
            update={"mcp_servers": tuple(servers)},
        )
        return self.manager

    async def get_worker(self, name: str):
        return self.workers.get(name)

    async def list_workers(self):
        return tuple(self.workers.values())

    async def replace_worker_mcp_servers(self, name: str, servers):
        self.order.append(f"agt.worker_descriptor.{name}")
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
        return self.workers[name]


class Higress:
    def __init__(
        self,
        order: list[str],
        descriptor: MCPServerDocument,
    ) -> None:
        self.order = order
        self.descriptor_value = descriptor
        self.calls: list[str] = []
        self.consumers = {"manager", "worker-alice"}
        self.replacements: list[frozenset[str]] = []

    async def upsert_rest_server(self, request):
        assert request.credential.get_secret_value() == "github-secret"
        self.calls.append("upsert")
        self.order.append("higress.upsert")
        return self.descriptor_value

    async def upsert_proxy(self, request):
        assert (
            request.headers[0].value.get_secret_value()
            == "header-secret"
        )
        self.calls.append("upsert")
        self.order.append("higress.upsert")
        return self.descriptor_value

    async def get_consumers(self, name: str):
        self.calls.append("get_consumers")
        self.order.append("higress.get_consumers")
        assert name == "github"
        return frozenset(self.consumers)

    async def replace_consumers(self, name: str, consumers):
        self.calls.append("replace_consumers")
        self.order.append("higress.replace_consumers")
        assert name == "github"
        replacement = frozenset({"manager", *consumers})
        self.consumers = set(replacement)
        self.replacements.append(replacement)
        return replacement

    async def list_mcp_servers(self):
        self.calls.append("list")
        return ()

    async def delete_server(self, name: str):
        self.calls.append(f"delete:{name}")

    def descriptor(self, name: str):
        assert name == "github"
        return self.descriptor_value


class Watcher:
    def __init__(
        self,
        order: list[str],
        registry: Registry,
        agt: Agt,
    ) -> None:
        self.order = order
        self.registry = registry
        self.agt = agt

    async def poll_once(self):
        self.order.append("runtime.poll")
        if self.agt.manager.mcp_servers:
            self.registry.revision = 2
            self.registry.current = SimpleNamespace(
                document=_document(
                    2,
                    *self.agt.manager.mcp_servers,
                ),
            )


class Verifier:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    async def list_server_tools(self, server_name: str, *, revision: int):
        self.order.append("mcp.list_tools")
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
        self.order.append("mcp.tool_call.verify")
        self.calls.append((server_name, tool_name, arguments))
        assert revision == 2
        return {"issues": [42]}


class Notifications:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.workers: list[str] = []

    async def notify_worker(
        self,
        worker: str,
        text: str,
        *,
        source_operation_id: str,
    ) -> None:
        assert "github" in text
        assert len(source_operation_id) == 32
        self.order.append("matrix.worker_notification")
        self.workers.append(worker)


def _context(suffix: str = "configure") -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id=f"${suffix}",
        tool_call_id=f"{suffix}-mcp",
    )


def _configuration(*workers: str) -> MCPConfiguration:
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
            credential=SecretStr("github-secret"),
            service=ServiceSource(
                name="github-api",
                domain="api.github.com",
                port=443,
                protocol="https",
            ),
        ),
        workers=workers,
        verification_tool="mcp__github__search_issues",
        verification_arguments={"query": "repo:agentscope-ai/AgentTeams"},
    )


def _proxy_configuration() -> MCPConfiguration:
    return MCPConfiguration(
        server=ProxyMCPRequest(
            name="github",
            description="Proxied MCP",
            backend_url=(
                "https://mcp.example/tenant/path-secret/mcp"
                "?api_key=query-secret"
            ),
            transport="http",
            headers=(
                ProxyHeader(
                    name="Authorization",
                    value=SecretStr("header-secret"),
                ),
            ),
        ),
        verification_tool="mcp__github__search_issues",
        verification_arguments={"query": "safe"},
    )


def _service(
    *,
    runtime_mode: str = "local",
    verifier_override: object | None = None,
    mcp_propagation_timeout: float | None = None,
) -> tuple[
    IntegrationService,
    Higress,
    Agt,
    Verifier,
    Notifications,
    list[str],
    TaskSupervisor,
]:
    order: list[str] = []
    descriptor = MCPServerDocument(
        name="github",
        url=(
            "http://aigw-local.agentteams.io:8080/"
            "mcp-servers/mcp-github/mcp"
        ),
    )
    registry = Registry()
    agt = Agt(order, descriptor)
    higress = Higress(order, descriptor)
    verifier = verifier_override or Verifier(order)
    notifications = Notifications(order)
    supervisor = TaskSupervisor(Clock())
    options = {}
    if mcp_propagation_timeout is not None:
        options["mcp_propagation_timeout"] = mcp_propagation_timeout
    service = IntegrationService(
        agt=agt,
        gateway=ModelGatewayClient(
            base_url="http://unused",
            api_key=SecretStr("unused"),
        ),
        supervisor=supervisor,
        clock=Clock(),
        manager_name="manager",
        registry=registry,
        watcher=Watcher(order, registry, agt),
        sleep=lambda _: None,
        higress=higress,
        mcp_verifier=verifier,
        worker_notifications=notifications,
        runtime_mode=runtime_mode,
        **options,
    )
    return (
        service,
        higress,
        agt,
        verifier,
        notifications,
        order,
        supervisor,
    )


@pytest.mark.asyncio
async def test_cloud_mode_refuses_before_higress_mutation() -> None:
    service, higress, *_ = _service(runtime_mode="aliyun")

    with pytest.raises(CloudMCPManagementUnsupported):
        await service.configure_mcp(
            _configuration("alice"),
            context=_context(),
        )

    assert higress.calls == []


@pytest.mark.asyncio
async def test_consumer_update_sends_complete_set() -> None:
    service, higress, *_ = _service()

    receipt = await service.grant_mcp(
        "github",
        workers=("bob",),
        context=_context(),
    )

    assert higress.replacements[-1] == frozenset(
        {"manager", "worker-alice", "worker-bob"},
    )
    assert receipt.consumers == frozenset(
        {"manager", "worker-alice", "worker-bob"},
    )


@pytest.mark.asyncio
async def test_worker_notification_occurs_after_real_tool_call() -> None:
    (
        service,
        _,
        agt,
        verifier,
        notifications,
        order,
        supervisor,
    ) = _service()

    receipt = await service.configure_mcp(
        _configuration("alice"),
        context=_context(),
    )

    assert order[-2:] == [
        "mcp.tool_call.verify",
        "matrix.worker_notification",
    ]
    assert verifier.calls == [
        (
            "github",
            "mcp__github__search_issues",
            {"query": "repo:agentscope-ai/AgentTeams"},
        ),
    ]
    assert notifications.workers == ["alice"]
    assert receipt.verified is True
    assert receipt.runtime_revision == 2
    assert agt.manager.mcp_servers == (receipt.descriptor,)
    assert agt.workers["alice"].spec["mcpServers"] == [
        receipt.descriptor.model_dump(mode="json"),
    ]
    operation = next(iter(supervisor.operations.values()))
    assert operation.request["verification_arguments"] == {
        "query": "repo:agentscope-ai/AgentTeams",
    }
    serialized = repr(
        [
            operation.request
            for operation in supervisor.operations.values()
        ]
        + supervisor.events,
    )
    assert "github-secret" not in serialized
    assert "accessToken" not in serialized


def test_verification_arguments_reject_credential_like_fields() -> None:
    configured = _configuration()

    with pytest.raises(ValueError, match="credential-like"):
        MCPConfiguration(
            server=configured.server,
            verification_tool=configured.verification_tool,
            verification_arguments={
                "filters": {
                    "Authorization": "Bearer do-not-persist",
                },
            },
        )


class HangingVerifier:
    async def list_server_tools(self, server_name: str, *, revision: int):
        del server_name, revision
        await asyncio.Event().wait()

    async def call_server_tool(self, *args, **kwargs):
        del args, kwargs
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_mcp_auth_propagation_wait_has_a_hard_deadline() -> None:
    service, *_ = _service(
        verifier_override=HangingVerifier(),
        mcp_propagation_timeout=0.01,
    )

    with pytest.raises(MCPVerificationError, match="bounded"):
        await asyncio.wait_for(
            service.configure_mcp(
                _configuration(),
                context=_context("bounded"),
            ),
            timeout=0.5,
        )


@pytest.mark.asyncio
async def test_revoke_and_delete_remove_controller_descriptors() -> None:
    service, higress, agt, *_ = _service()
    await service.configure_mcp(
        _configuration("alice"),
        context=_context("configure-cleanup"),
    )

    revoked = await service.revoke_mcp(
        "github",
        workers=("alice",),
        context=_context("revoke"),
    )

    assert revoked.consumers == frozenset({"manager"})
    # Alice was already present before this test's grant. Revocation removes
    # that exact consumer and its Controller descriptor.
    assert "worker-alice" not in higress.consumers
    assert agt.workers["alice"].spec["mcpServers"] == []

    deleted = await service.delete_mcp(
        "github",
        context=_context("delete"),
    )

    assert deleted.action == "delete"
    assert agt.manager.mcp_servers == ()
    assert "delete:github" in higress.calls


@pytest.mark.asyncio
async def test_proxy_credentials_and_opaque_url_parts_never_enter_journal() -> None:
    service, *_, supervisor = _service()

    await service.configure_mcp(
        _proxy_configuration(),
        context=_context("proxy-secret"),
    )

    serialized = repr(
        [
            operation.request
            for operation in supervisor.operations.values()
        ]
        + supervisor.events,
    )
    assert "header-secret" not in serialized
    assert "query-secret" not in serialized
    assert "path-secret" not in serialized
