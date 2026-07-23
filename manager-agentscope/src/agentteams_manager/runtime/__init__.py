"""Direct AgentScope runtime assembly."""

from typing import Any

__all__ = [
    "AgentFactory",
    "PromptBuilder",
    "RoomSessionManager",
    "SkillRegistry",
]


def __getattr__(name: str) -> Any:
    """Keep public imports lazy so runtime modules do not form cycles."""
    if name == "AgentFactory":
        from .agent_factory import AgentFactory

        return AgentFactory
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
