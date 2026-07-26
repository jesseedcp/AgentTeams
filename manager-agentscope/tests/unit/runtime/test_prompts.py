from pathlib import Path

from agentteams_manager.config import PromptSources, RuntimeDocument
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.runtime.prompts import PromptBuilder


def runtime_document() -> RuntimeDocument:
    return RuntimeDocument(
        revision=1,
        manager_name="manager",
        model="qwen3.6-plus",
        skills=("worker-management",),
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


def test_prompt_uses_typed_tools_not_legacy_manager_commands() -> None:
    builder = PromptBuilder(Path("manager/agent"))
    policy = RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM,
        revision=3,
        allowed_tools=frozenset({"list_workers", "create_worker"}),
        confirm_tools=frozenset({"create_worker"}),
    )

    prompt = builder.build(policy, runtime_document())

    assert "typed AgentScope tools" in prompt
    assert "Room policy: admin_dm" in prompt
    assert "openclaw gateway" not in prompt.casefold()
    assert "copaw channels send" not in prompt.casefold()
    assert "/opt/agentteams/agent/skills/" not in prompt
    assert "state.json" not in prompt
    assert "workers-registry.json" not in prompt


def test_prompt_renders_tools_from_the_registered_toolkit() -> None:
    builder = PromptBuilder(Path("manager/agent"))
    policy = RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM,
        revision=3,
        allowed_tools=frozenset({"stale_policy_name"}),
    )

    prompt = builder.build(
        policy,
        runtime_document(),
        registered_tools=("actual_tool", "skill_viewer"),
    )

    assert "Registered tools: actual_tool, skill_viewer" in prompt
    assert "Registered tools: stale_policy_name" not in prompt


def test_prompt_source_cannot_escape_prompt_root(tmp_path: Path) -> None:
    builder = PromptBuilder(tmp_path)
    runtime = runtime_document().model_copy(
        update={
            "prompt_sources": PromptSources(
                soul="../secret",
                agents="AGENTS.md",
                tools="TOOLS.md",
                heartbeat="HEARTBEAT.md",
            ),
        },
    )
    policy = RoomPolicy(
        room_id="!admin:example",
        kind=RoomKind.ADMIN_DM,
        revision=1,
    )

    try:
        builder.build(policy, runtime)
    except ValueError as error:
        assert "escapes prompt root" in str(error)
    else:
        raise AssertionError("path traversal was accepted")
