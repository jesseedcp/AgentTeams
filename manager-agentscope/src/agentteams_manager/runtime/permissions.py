"""Room-policy decisions layered below the system prompt."""

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

