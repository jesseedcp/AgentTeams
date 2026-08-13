"""Direct AgentScope runtime assembly."""

from typing import Any

__all__ = [
    "AgentFactory",
    "MCPRegistry",
    "PromptBuilder",
    "RoomSessionManager",
    "SkillRegistry",
]


def __getattr__(name: str) -> Any:
    """Keep public imports lazy so runtime modules do not form cycles."""
    # 逻辑说明：按请求的公开名称延迟导入并返回对应 runtime 类，避免模块初始化形成循环；名称不在导出表时抛出 AttributeError，且不缓存或改写模块状态。
    if name == "AgentFactory":
        from .agent_factory import AgentFactory

        return AgentFactory
    if name == "MCPRegistry":
        from .mcp import MCPRegistry

        return MCPRegistry
    if name == "PromptBuilder":
        from .prompts import PromptBuilder

        return PromptBuilder
    if name == "RoomSessionManager":
        from .session_manager import RoomSessionManager

        return RoomSessionManager
    if name == "SkillRegistry":
        from .skills import SkillRegistry

        return SkillRegistry
    raise AttributeError(name)
