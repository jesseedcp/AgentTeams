"""Matrix events shaped like the normalized Manager domain boundary."""

from __future__ import annotations

from datetime import UTC, datetime

from agentteams_manager.domain.models import InboundEvent


def matrix_text_event(
    *,
    room_id: str,
    event_id: str,
    sender: str,
    body: str,
) -> InboundEvent:
    return InboundEvent(
        room_id=room_id,
        event_id=event_id,
        sender=sender,
        body=body,
        timestamp=datetime.now(UTC),
    )
