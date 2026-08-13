"""Stable identifiers used across retry and recovery boundaries.

生成跨重试与重启保持稳定的业务标识符。

同一 Matrix 事件里的同一次 tool call 必须得到相同 operation ID；否则超时重试会被
系统误认成新操作，可能创建重复 Worker 或重复发消息。这里集中定义确定性 ID 和供
新资源使用的时间戳 ID，使 workflow 不必各自发明不兼容的命名规则。
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from secrets import token_hex


def operation_id_for(
    room_id: str,
    event_id: str,
    tool_call_id: str,
) -> str:
    """Derive the same operation ID for the same Matrix tool invocation."""
    raw = "\0".join((room_id, event_id, tool_call_id)).encode("utf-8")
    return sha256(raw).hexdigest()[:32]


def matrix_transaction_id(operation_id: str, effect_sequence: int) -> str:
    """Return the Matrix transaction ID for one journaled effect."""
    if effect_sequence < 0:
        raise ValueError("effect_sequence must not be negative")
    return f"agentteams:{operation_id}:{effect_sequence}"


def _timestamped_id(prefix: str, now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    return f"{prefix}-{timestamp:%Y%m%d-%H%M%S}-{token_hex(3)}"


def new_task_id(now: datetime | None = None) -> str:
    """Create a human-readable, collision-resistant task identifier."""
    return _timestamped_id("task", now)


def new_project_id(now: datetime | None = None) -> str:
    """Create a human-readable, collision-resistant project identifier."""
    return _timestamped_id("project", now)
