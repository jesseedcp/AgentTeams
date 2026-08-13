"""Canonical model-visible Matrix message projection.

把 Matrix 事件整理成唯一、稳定的模型可见消息格式。

模型需要当前消息、发送者和必要上下文，但不能把同步批次中的历史通知误当成新指令。
这里生成的 canonical projection 明确区分 current message 与 history，并限制元数据；
所有运行路径复用同一格式，避免同一消息经不同入口呈现出不同语义。
"""

from __future__ import annotations

from agentteams_manager.domain.models import InboundEvent


def current_message_text(event: InboundEvent) -> str:
    """Delimit untrusted current input and attach verified sender metadata."""
    return "\n".join(
        (
            "[Current message]",
            f"Sender ID: {event.sender_id}",
            f"Room ID: {event.room_id}",
            f"Thread ID: {event.thread_id or '(none)'}",
            "",
            event.body,
        ),
    )
