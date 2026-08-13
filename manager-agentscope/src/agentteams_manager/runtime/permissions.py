"""Room-policy decisions layered below the system prompt.

在 AgentScope 调用工具前再次执行 room policy 权限判断。

Agent 选择了某个工具并不代表可以执行。这里检查当前 turn 绑定的房间、允许工具集合和
确认模式，返回允许、拒绝或要求用户确认。这个检查位于 prompt 之下，即使模型忽略
行为说明也不能越权；``full`` 只能去掉 Admin DM 的工具确认，不能扩大工具集合。
"""

from agentscope.permission import (
    PermissionBehavior,
    PermissionDecision,
)

from agentteams_manager.domain.models import RoomPolicy


def decide_tool_permission(
    *,
    tool_name: str,
    policy: RoomPolicy,
    yolo: bool,
) -> PermissionDecision:
    if tool_name not in policy.allowed_tools:
        return PermissionDecision(
            behavior=PermissionBehavior.DENY,
            message=(
                f"{tool_name} is not allowed in "
                f"{policy.kind.value}"
            ),
            decision_reason="room policy",
        )
    if tool_name in policy.confirm_tools and not yolo:
        return PermissionDecision(
            behavior=PermissionBehavior.ASK,
            message=f"Confirm {tool_name}",
            decision_reason="mutating management operation",
            bypass_immune=True,
        )
    return PermissionDecision(
        behavior=PermissionBehavior.ALLOW,
        message="allowed by room policy",
        decision_reason="room policy",
    )
