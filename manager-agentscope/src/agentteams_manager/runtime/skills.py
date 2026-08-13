"""Load the retained Manager skill catalog through AgentScope.

加载并校验 Manager 的 skill guidance，再组装 AgentScope toolkit。

Skill 文档告诉模型“何时、如何使用能力”，但自身不执行任何操作。真正 capability 必须
有注册的 typed tool、room policy 和 deterministic workflow。本模块还验证预期 skill
目录完整，避免镜像漏打包后 Manager 表面启动成功却失去关键操作说明。
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from agentscope.skill import LocalSkillLoader, Skill
from agentscope.tool import ToolBase, Toolkit

from agentteams_manager.domain.models import RoomPolicy
from agentteams_manager.tools.base import ManagerToolkit

REQUIRED_MANAGER_SKILLS = frozenset(
    {
        "agentteams-find-worker",
        "channel-management",
        "coding-cli-management",
        "file-sync-management",
        "git-delegation-management",
        "human-management",
        "higress-gateway-management",
        "matrix-server-management",
        "mcp-server-management",
        "mcporter",
        "memory-management",
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
EXPECTED_MANAGER_SKILLS = REQUIRED_MANAGER_SKILLS


class SkillRegistry:
    """Fail closed when the shipped skill catalog is incomplete."""

    def __init__(self, directory: Path) -> None:
        # 逻辑说明：解析 skill 根目录并创建递归 LocalSkillLoader，供后续 load/toolkit 复用；构造时仅配置 loader，不扫描或执行 skill 内容。
        self.directory = directory.resolve()
        self.loader = LocalSkillLoader(
            directory=str(self.directory),
            scan_subdir=True,
        )

    async def load(self) -> tuple[Skill, ...]:
        # 逻辑说明：异步列出本地 skills，拒绝重名并核对 REQUIRED_MANAGER_SKILLS 全部存在，最后按名称排序返回不可变元组；目录不完整时抛错而不发布残缺 catalog。
        skills = tuple(await self.loader.list_skills())
        names = [skill.name for skill in skills]
        if len(names) != len(set(names)):
            raise RuntimeError("duplicate AgentTeams Manager skill name")
        actual = frozenset(names)
        if not REQUIRED_MANAGER_SKILLS.issubset(actual):
            missing = sorted(REQUIRED_MANAGER_SKILLS - actual)
            raise RuntimeError(
                f"invalid Manager skill catalog; missing={missing}",
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
        # 逻辑说明：按传入顺序保存独立工具提供器，真正的 policy 过滤和同名冲突检查延迟到 tools_for_policy，构造时不实例化或执行工具。
        self._providers = providers

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ToolBase, ...]:
        # 逻辑说明：按 provider 声明顺序汇总其针对当前 RoomPolicy 返回的 typed tools，并拒绝跨 provider 的同名注册；成功时保持原顺序返回，不修改 policy 或工具对象。
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
        # 逻辑说明：保存 SkillRegistry、可选 policy 工具提供者和 metrics 依赖，供每次房间 toolkit 装配使用；构造阶段不加载 skill、计算权限或注册工具。
        self._registry = registry
        self._tools = tools
        self._metrics = metrics

    async def for_policy(self, policy: RoomPolicy) -> Toolkit:
        # 逻辑说明：先调用 registry.load 验证已发布 skill catalog，再按 RoomPolicy 获取可用 typed tools，并以共享 loader 和 metrics 构造 ManagerToolkit；skill 或工具校验失败时不返回部分 toolkit。
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
