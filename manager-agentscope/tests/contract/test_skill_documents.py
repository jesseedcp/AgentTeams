from __future__ import annotations

import json
import re
from pathlib import Path

from agentteams_manager.matrix.policy import ALL_MANAGER_TOOLS
from agentteams_manager.runtime.skills import EXPECTED_MANAGER_SKILLS
from agentteams_manager.runtime.tool_docs import (
    documented_tool_names,
    render_tool_catalog,
    replace_tool_catalog,
)

ROOT = Path(".")
SKILL_ROOT = ROOT / "manager" / "agent" / "skills"
MANIFEST = ROOT / "tests" / "manager-skill-parity.json"
LEGACY_MANAGER_PATHS = (
    "/root/manager-workspace",
    "openclaw gateway",
    "copaw channels send",
    "copaw app",
    "redis-server",
    "state.json",
    "workers-registry.json",
    "pending-workers.json",
    "worker-openclaw.json.tmpl",
    "yolo-mode",
)
LEGACY_MANAGER_SCRIPT_PATH = re.compile(
    r"(?:/opt/agentteams/agent/skills|manager/agent/skills)/[^\s`]+/scripts",
    re.IGNORECASE,
)
MCPORTER_COMMAND = re.compile(
    r"(?im)^\s*(?:`{0,3})mcporter\s+(?:list|call)\b"
)


def _manifest() -> dict[str, object]:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_skill_parity_manifest_is_complete_and_pinned() -> None:
    manifest = _manifest()
    skills = manifest["skills"]

    assert manifest["schemaVersion"] == 1
    assert manifest["managerRuntime"] == "agentscope"
    assert manifest["agentScopeVersion"] == "2.0.4.post1"
    assert len(skills) == 19
    assert {item["name"] for item in skills} == EXPECTED_MANAGER_SKILLS
    assert {
        path.name
        for path in SKILL_ROOT.iterdir()
        if path.is_dir()
    } == EXPECTED_MANAGER_SKILLS


def test_every_retained_skill_has_a_valid_document_and_evidence() -> None:
    for item in _manifest()["skills"]:
        name = item["name"]
        skill_file = Path(item["skillFile"])
        assert skill_file == SKILL_ROOT / name / "SKILL.md"
        assert skill_file.is_file(), name

        text = skill_file.read_text(encoding="utf-8")
        match = re.search(r"(?m)^name:\s*(\S+)\s*$", text)
        assert match is not None and match.group(1) == name

        evidence = item["evidence"]
        assert len(evidence) >= 2, name
        for evidence_path in map(Path, evidence):
            assert evidence_path.is_file(), (name, evidence_path)
            assert evidence_path.parts[:2] == (
                "manager-agentscope",
                "tests",
            )


def test_manifest_tools_are_typed_registered_and_documented() -> None:
    covered: set[str] = set()
    for item in _manifest()["skills"]:
        name = item["name"]
        tools = item["tools"]
        assert len(tools) == len(set(tools)), name
        docs = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (SKILL_ROOT / name).rglob("*.md")
        )
        for tool in tools:
            assert tool in ALL_MANAGER_TOOLS, (name, tool)
            assert f"`{tool}`" in docs, (name, tool)
            covered.add(tool)

    assert covered == ALL_MANAGER_TOOLS


def test_generated_tool_guide_matches_canonical_registry() -> None:
    guide = (ROOT / "manager" / "agent" / "TOOLS.md").read_text(
        encoding="utf-8",
    )

    assert documented_tool_names(guide) == ALL_MANAGER_TOOLS
    assert replace_tool_catalog(guide, ALL_MANAGER_TOOLS) == guide
    assert render_tool_catalog(ALL_MANAGER_TOOLS) in guide


def test_only_mcporter_declares_namespaced_dynamic_mcp_tools() -> None:
    dynamic = {
        item["name"]: item.get("dynamicTools", [])
        for item in _manifest()["skills"]
        if item.get("dynamicTools")
    }
    assert dynamic == {"mcporter": ["mcp__<server>__<tool>"]}


def test_skill_payload_has_no_legacy_manager_execution_surface() -> None:
    assert tuple(SKILL_ROOT.rglob("*.sh")) == ()
    text = "\n".join(
        path.read_text(encoding="utf-8").casefold()
        for path in SKILL_ROOT.rglob("*.md")
    )
    for legacy in LEGACY_MANAGER_PATHS:
        assert legacy.casefold() not in text, legacy
    assert LEGACY_MANAGER_SCRIPT_PATH.search(text) is None
    assert MCPORTER_COMMAND.search(text) is None
