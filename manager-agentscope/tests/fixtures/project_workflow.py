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
        self.fail_once_for_text_prefix: str | None = None
        self.joined_members_only = False
        self.reject_duplicate_invites = False
        self.fail_invite_after_effect_once: str | None = None
        self.create_invites: list[tuple[str, ...]] = []
        self.invite_attempts: list[tuple[str, str]] = []

    async def create_private_room(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        creation_marker: dict[str, str | int],
    ) -> str:
        self.order.append("matrix.project_room")
        self.create_invites.append(invite)
        room_id = f"!project-{len(self.rooms) + 1}:example"
        self.rooms[room_id] = {
            "name": name,
            "topic": topic,
            "members": set(invite) | {"@manager:example"},
            "invited": set(invite),
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
        membership_events = tuple(
            {
                "type": "m.room.member",
                "state_key": user_id,
                "content": {"membership": "invite"},
            }
            for user_id in sorted(self.rooms[room_id]["invited"])
        )
        return (
            {
                "type": "io.agentteams.creation",
                "state_key": "",
                "content": dict(self.rooms[room_id]["marker"]),
            },
            *membership_events,
        )

    async def members(self, room_id: str) -> tuple[str, ...]:
        if self.joined_members_only:
            return ("@manager:example",)
        return tuple(sorted(self.rooms[room_id]["members"]))

    async def invite_user(self, room_id: str, user_id: str) -> None:
        self.order.append("matrix.invite")
        self.invite_attempts.append((room_id, user_id))
        if (
            self.reject_duplicate_invites
            and user_id in self.rooms[room_id]["invited"]
        ):
            raise RuntimeError("Matrix invite room member failed: already invited")
        self.rooms[room_id]["invited"].add(user_id)
        self.rooms[room_id]["members"].add(user_id)
        if self.fail_invite_after_effect_once == user_id:
            self.fail_invite_after_effect_once = None
            raise TimeoutError("Matrix invite response was lost")

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
        if (
            self.fail_once_for_text_prefix
            and text.startswith(self.fail_once_for_text_prefix)
        ):
            self.fail_once_for_text_prefix = None
            raise TimeoutError("Matrix acknowledgement was lost")
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
