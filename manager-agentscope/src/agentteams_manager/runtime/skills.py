"""Load the retained Manager skill catalog through AgentScope."""

from __future__ import annotations

from pathlib import Path

from agentscope.skill import LocalSkillLoader, Skill
from agentscope.tool import Toolkit

from agentteams_manager.domain.models import RoomPolicy

EXPECTED_MANAGER_SKILLS = frozenset(
    {
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
    },
)


class SkillRegistry:
    """Fail closed when the shipped skill catalog is incomplete."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.loader = LocalSkillLoader(
            directory=str(self.directory),
            scan_subdir=True,
        )

    async def load(self) -> tuple[Skill, ...]:
        skills = tuple(await self.loader.list_skills())
        names = [skill.name for skill in skills]
        if len(names) != len(set(names)):
            raise RuntimeError("duplicate AgentTeams Manager skill name")
        actual = frozenset(names)
        if actual != EXPECTED_MANAGER_SKILLS:
            missing = sorted(EXPECTED_MANAGER_SKILLS - actual)
            extra = sorted(actual - EXPECTED_MANAGER_SKILLS)
            raise RuntimeError(
                f"invalid Manager skill catalog; missing={missing}, "
                f"extra={extra}",
            )
        return tuple(sorted(skills, key=lambda skill: skill.name))


class SkillToolkitFactory:
    """Base Toolkit factory; later plans add policy-bound typed tools."""

    def __init__(self, registry: SkillRegistry) -> None:
        self._registry = registry

    async def for_policy(self, policy: RoomPolicy) -> Toolkit:
        del policy
        await self._registry.load()
        return Toolkit(skills_or_loaders=[self._registry.loader])

