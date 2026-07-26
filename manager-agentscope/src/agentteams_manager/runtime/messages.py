"""Canonical model-visible Matrix message projection."""

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
