from pathlib import Path

import pytest

from agentteams_manager.runtime.skills import SkillRegistry


@pytest.mark.asyncio
async def test_all_upstream_manager_skills_load() -> None:
    registry = SkillRegistry(Path("manager/agent/skills"))

    skills = await registry.load()

    assert {skill.name for skill in skills} == {
            "agentteams-find-worker",
            "channel-management",
            "coding-cli-management",
            "file-sync-management",
            "git-delegation-management",
            "higress-gateway-management",
            "human-management",
        "matrix-server-management",
        "mcp-server-management",
        "mcporter",
        "model-switch",
        "project-management",
        "service-publishing",
        "task-coordination",
        "task-management",
        "team-management",
        "worker-management",
        "worker-model-switch",
    }


@pytest.mark.asyncio
async def test_additional_valid_skill_does_not_break_required_catalog(
    tmp_path: Path,
) -> None:
    source = Path(
        "manager/agent/skills/worker-management/SKILL.md",
    )
    for required in Path("manager/agent/skills").iterdir():
        if not required.is_dir():
            continue
        target = tmp_path / required.name
        target.mkdir()
        (target / "SKILL.md").write_text(
            (required / "SKILL.md").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    extra = tmp_path / "local-extra"
    extra.mkdir()
    (extra / "SKILL.md").write_text(
        source.read_text(encoding="utf-8").replace(
            "name: worker-management",
            "name: local-extra",
            1,
        ),
        encoding="utf-8",
    )

    skills = await SkillRegistry(tmp_path).load()

    assert "local-extra" in {skill.name for skill in skills}
