from __future__ import annotations

from typing import Any


class ProjectMatrix:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.rooms: dict[str, dict[str, Any]] = {}
        self.messages: list[dict[str, Any]] = []
        self.timeout_after_create = False
        self.hide_joined_once = False
        self.hide_after_timeout = False

    async def create_private_room(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        creation_marker: dict[str, str | int],
    ) -> str:
        self.order.append("matrix.project_room")
        room_id = f"!project-{len(self.rooms) + 1}:example"
        self.rooms[room_id] = {
            "name": name,
            "topic": topic,
            "members": set(invite) | {"@manager:example"},
            "marker": dict(creation_marker),
        }
        if self.timeout_after_create:
            self.timeout_after_create = False
            if self.hide_after_timeout:
                self.hide_joined_once = True
            raise TimeoutError("Matrix created room but response was lost")
        return room_id

    async def joined_rooms(self) -> tuple[str, ...]:
        if self.hide_joined_once:
            self.hide_joined_once = False
            return ()
        return tuple(self.rooms)

    async def room_state(
        self,
        room_id: str,
    ) -> tuple[dict[str, object], ...]:
        return (
            {
                "type": "io.agentteams.creation",
                "state_key": "",
                "content": dict(self.rooms[room_id]["marker"]),
            },
        )

    async def members(self, room_id: str) -> tuple[str, ...]:
        return tuple(sorted(self.rooms[room_id]["members"]))

    async def invite_user(self, room_id: str, user_id: str) -> None:
        self.order.append("matrix.invite")
        self.rooms[room_id]["members"].add(user_id)

    async def kick_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None:
        self.order.append("matrix.kick")
        self.rooms[room_id]["members"].discard(user_id)

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        self.messages.append(
            {
                "room_id": room_id,
                "text": text,
                "txn_id": txn_id,
                "thread_id": thread_id,
                "mentions": mentions,
            },
        )
        return f"$message-{len(self.messages)}"
