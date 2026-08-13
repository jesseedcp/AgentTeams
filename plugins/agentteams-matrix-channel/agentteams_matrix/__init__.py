"""QwenPaw Worker 使用的 AgentTeams Matrix Channel 插件包。

Matrix 是房间、成员与对话的权威来源；本包只负责把它们转换成 QwenPaw channel
事件，并在边界执行 Worker 身份/allow-list/提及策略。

AgentTeams Matrix Channel plugin package.
"""

from .channel import AgentTeamsMatrixChannel

__all__ = ["AgentTeamsMatrixChannel"]
