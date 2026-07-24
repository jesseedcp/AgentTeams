"""Load the retained Manager skill catalog through AgentScope."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from agentscope.skill import LocalSkillLoader, Skill
from agentscope.tool import ToolBase, Toolkit

from agentteams_manager.domain.models import RoomPolicy
from agentteams_manager.tools.base import ManagerToolkit

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


class ToolProvider(Protocol):
    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ToolBase, ...]: ...


class CompositeToolProvider:
    """Combine independently owned tool families without name collisions."""

    def __init__(self, *providers: ToolProvider) -> None:
        self._providers = providers

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ToolBase, ...]:
        tools = tuple(
            tool
            for provider in self._providers
            for tool in provider.tools_for_policy(policy)
        )
        names = [tool.name for tool in tools]
        if len(names) != len(set(names)):
            raise RuntimeError("duplicate Manager tool registration")
        return tools


class SkillToolkitFactory:
    """Combine retained skills with policy-bound typed tools."""

    def __init__(
        self,
        registry: SkillRegistry,
        *,
        tools: ToolProvider | None = None,
        metrics: object | None = None,
    ) -> None:
        self._registry = registry
        self._tools = tools
        self._metrics = metrics

    async def for_policy(self, policy: RoomPolicy) -> Toolkit:
        await self._registry.load()
        registered = (
            self._tools.tools_for_policy(policy)
            if self._tools is not None
            else ()
        )
        return ManagerToolkit(
            tools=list(registered),
            skills_or_loaders=[self._registry.loader],
            metrics=self._metrics,
        )
