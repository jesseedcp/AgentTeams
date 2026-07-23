"""Matrix thread relations and bounded room history."""

from __future__ import annotations

from collections import defaultdict, deque

from agentteams_manager.domain.models import InboundEvent


class ThreadProjector:
    """Build Matrix relation payloads without transport side effects."""

    @staticmethod
    def relation(thread_id: str) -> dict[str, object]:
        return {
            "rel_type": "m.thread",
            "event_id": thread_id,
            "is_falling_back": True,
            "m.in_reply_to": {"event_id": thread_id},
        }

    @staticmethod
    def replacement(
        event_id: str,
    ) -> dict[str, str]:
        return {
            "rel_type": "m.replace",
            "event_id": event_id,
        }


class RoomHistory:
    """Keep a strictly bounded normalized history per room."""

    def __init__(self, *, limit: int) -> None:
        if limit < 0:
            raise ValueError("history limit cannot be negative")
        self.limit = limit
        self._events: dict[str, deque[InboundEvent]] = defaultdict(
            lambda: deque(maxlen=self.limit),
        )

    def append(self, event: InboundEvent) -> None:
        if self.limit:
            self._events[event.room_id].append(event)

    def entries(self, room_id: str) -> tuple[InboundEvent, ...]:
        return tuple(self._events.get(room_id, ()))

    def prefix(
        self,
        room_id: str,
        *,
        exclude_event_id: str | None = None,
    ) -> str:
        lines = [
            f"{event.sender_id}: {event.body}"
            for event in self.entries(room_id)
            if event.event_id != exclude_event_id
        ]
        if not lines:
            return ""
        return "[Recent room history]\n" + "\n".join(lines) + "\n\n"
