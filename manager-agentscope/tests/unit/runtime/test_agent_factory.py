from pathlib import Path

import pytest
from agentscope.tool import Toolkit
from pydantic import SecretStr

from agentteams_manager.config import (
    ManagerConfig,
    PromptSources,
    RuntimeDocument,
)
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.runtime.agent_factory import AgentFactory
from agentteams_manager.runtime.prompts import PromptBuilder
from agentteams_manager.tools.base import ManagerTool


class EmptyToolkitFactory:
    async def for_policy(self, policy: RoomPolicy) -> Toolkit:
        del policy
        return Toolkit()


class RegisteredToolkitFactory:
    async def for_policy(self, policy: RoomPolicy) -> Toolkit:
        return Toolkit(
            tools=[
                ManagerTool(
                    name="actual_tool",
                    description="Actual registered tool.",
                    input_schema={
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                    policy=policy,
                    handler=lambda: {},
                ),
            ],
        )


class MCP:
    name = "github"
    is_stateful = False

    async def list_tools(self):
        return []


class RecordingMCPRegistry:
    def __init__(self) -> None:
        self.clients = {1: MCP(), 2: MCP()}
        self.retained: list[int] = []
        self.released: list[tuple[int, int]] = []

    def clients_for(self, policy, *, revision):
        del policy
        return (self.clients[revision],)

    def retain(self, revision):
        self.retained.append(revision)

    async def release(self, revision, *, active_revision):
        self.released.append((revision, active_revision))


def manager_config(tmp_path: Path) -> ManagerConfig:
    return ManagerConfig(
        manager_name="manager",
        manager_user_id="@manager:example",
        matrix_url="http://matrix",
        matrix_domain="example",
        matrix_access_token=SecretStr("matrix-secret"),
        controller_url="http://controller",
        controller_auth_token=None,
        ai_gateway_url="http://higress",
        gateway_key=SecretStr("gateway-secret"),
        fs_endpoint="http://minio",
        fs_bucket="agentteams",
        fs_access_key="access",
        fs_secret_key=SecretStr("storage-secret"),
        storage_prefix="agentteams",
        default_model="qwen3.6-plus",
        workspace=tmp_path,
        runtime_document_path=tmp_path / "runtime.json",
        runtime_document_key="manager/runtime.json",
        session_database=tmp_path / "state.db",
    )


def runtime_document() -> RuntimeDocument:
    return RuntimeDocument(
        revision=1,
        manager_name="manager",
        model="qwen3.6-plus",
        context_window=150_000,
        max_tokens=4_096,
        reasoning=True,
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


@pytest.mark.asyncio
async def test_factory_creates_direct_agentscope_agent(
    tmp_path: Path,
) -> None:
    factory = AgentFactory(
        config=manager_config(tmp_path),
        runtime=runtime_document(),
        prompt_builder=PromptBuilder(Path("manager/agent")),
        toolkit_factory=EmptyToolkitFactory(),
    )
    policy = RoomPolicy(
        room_id="!room:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
    )

    agent = await factory.create("!room:example", policy)

    assert agent.name == "manager"
    assert agent.state.session_id == "matrix:!room:example"
    assert agent.model.model == "qwen3.6-plus"
    assert agent.model.context_size == 150_000


@pytest.mark.asyncio
async def test_factory_prompt_uses_actual_toolkit_names(
    tmp_path: Path,
) -> None:
    factory = AgentFactory(
        config=manager_config(tmp_path),
        runtime=runtime_document(),
        prompt_builder=PromptBuilder(Path("manager/agent")),
        toolkit_factory=RegisteredToolkitFactory(),
    )
    policy = RoomPolicy(
        room_id="!room:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_tools=frozenset({"stale_policy_name"}),
    )

    agent = await factory.create("!room:example", policy)

    assert "Registered tools: actual_tool" in agent._system_prompt
    assert "Registered tools: stale_policy_name" not in agent._system_prompt


@pytest.mark.asyncio
async def test_factory_leases_mcp_until_old_agent_is_retired(
    tmp_path: Path,
) -> None:
    registry = RecordingMCPRegistry()
    factory = AgentFactory(
        config=manager_config(tmp_path),
        runtime=runtime_document(),
        prompt_builder=PromptBuilder(Path("manager/agent")),
        toolkit_factory=EmptyToolkitFactory(),
        mcp_registry=registry,
    )
    policy = RoomPolicy(
        room_id="!room:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
    )

    old_agent = await factory.create("!room:example", policy)
    factory.replace_runtime(
        runtime_document().model_copy(update={"revision": 2}),
    )
    new_agent = await factory.create("!room:example", policy)
    await factory.retire(old_agent)

    assert old_agent.toolkit.tool_groups[0].mcps == [
        registry.clients[1],
    ]
    assert new_agent.toolkit.tool_groups[0].mcps == [
        registry.clients[2],
    ]
    assert registry.retained == [1, 2]
    assert registry.released == [(1, 2)]
