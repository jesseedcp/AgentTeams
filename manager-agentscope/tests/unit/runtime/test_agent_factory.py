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


class EmptyToolkitFactory:
    async def for_policy(self, policy: RoomPolicy) -> Toolkit:
        del policy
        return Toolkit()


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
