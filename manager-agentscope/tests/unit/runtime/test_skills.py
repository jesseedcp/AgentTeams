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
        "file-sync-management",
        "git-delegation-management",
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

