from __future__ import annotations

from pathlib import Path

from agentteams_manager.matrix.policy import ALL_MANAGER_TOOLS

ROOT = Path("manager/agent/skills")

SKILL_TOOLS = {
    "channel-management": {
        "list_channels",
        "create_channel",
        "update_channel",
        "delete_channel",
        "send_notification",
    },
    "mcp-server-management": {
        "list_mcp_servers",
        "configure_mcp",
        "remove_mcp",
    },
    "model-switch": {"switch_model"},
    "service-publishing": {"publish_service"},
    "worker-model-switch": {"switch_worker_model"},
}

PROHIBITED_EXECUTION_PATHS = (
    "setup-mcp-server.sh",
    "setup-mcp-proxy.sh",
    "update-manager-model.sh",
    "mcporter list",
    "mcporter call",
    "openclaw gateway restart",
    "config/mcporter.json",
    "curl -x put",
    "curl -x delete",
    "bash /opt/agentteams",
)


def test_integration_skills_name_only_registered_typed_tools() -> None:
    for family, required_tools in SKILL_TOOLS.items():
        skill = (ROOT / family / "SKILL.md").read_text(encoding="utf-8")
        for tool in required_tools:
            assert f"`{tool}`" in skill, (family, tool)
            assert tool in ALL_MANAGER_TOOLS


def test_mcporter_compatibility_skill_uses_agentscope_toolkit() -> None:
    skill = (ROOT / "mcporter" / "SKILL.md").read_text(
        encoding="utf-8",
    )

    assert "AgentScope" in skill
    assert "Toolkit" in skill
    assert "mcp__" in skill
    assert "executable" in skill


def test_integration_docs_reject_legacy_execution_paths() -> None:
    families = (
        *SKILL_TOOLS,
        "mcporter",
    )
    text = "\n".join(
        path.read_text(encoding="utf-8").casefold()
        for family in families
        for path in (ROOT / family).rglob("*.md")
    )

    for prohibited in PROHIBITED_EXECUTION_PATHS:
        assert prohibited.casefold() not in text, prohibited


def test_github_template_keeps_one_empty_credential_slot() -> None:
    template = (
        ROOT
        / "mcp-server-management"
        / "references"
        / "mcp-github.yaml"
    ).read_text(encoding="utf-8")

    assert template.count('accessToken: ""') == 1
    assert "{{.config.accessToken}}" in template


def test_replaced_integration_shell_scripts_are_not_shipped() -> None:
    scripts = tuple(
        path
        for family in (
            "mcp-server-management",
            "model-switch",
            "service-publishing",
            "worker-model-switch",
        )
        for path in (ROOT / family).rglob("*.sh")
    )

    assert scripts == ()
