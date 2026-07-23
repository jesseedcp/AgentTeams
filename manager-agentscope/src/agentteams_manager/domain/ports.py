"""I/O ports that keep workflows independent from infrastructure clients."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol

from .models import (
    HumanResource,
    MirrorReceipt,
    ObjectReceipt,
    TeamResource,
    WorkerResource,
)


class MatrixPort(Protocol):
    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str: ...


class MatrixAdministrationPort(Protocol):
    async def create_private_room(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        creation_marker: dict[str, str | int],
    ) -> str: ...

    async def joined_rooms(self) -> tuple[str, ...]: ...

    async def members(self, room_id: str) -> tuple[str, ...]: ...

    async def invite_user(self, room_id: str, user_id: str) -> None: ...

    async def kick_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None: ...

    async def ban_user(
        self,
        room_id: str,
        user_id: str,
        *,
        reason: str,
    ) -> None: ...

    async def unban_user(self, room_id: str, user_id: str) -> None: ...


class ArtifactPort(Protocol):
    async def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
    ) -> ObjectReceipt: ...

    async def get_bytes(self, key: str) -> bytes: ...

    async def mirror_down(
        self,
        prefix: str,
        destination: Path,
    ) -> MirrorReceipt: ...

    async def mirror_up(
        self,
        source: Path,
        prefix: str,
    ) -> MirrorReceipt: ...


class ControllerPort(Protocol):
    async def get_worker(self, name: str) -> WorkerResource | None: ...

    async def list_workers(self) -> tuple[WorkerResource, ...]: ...

    async def get_team(self, name: str) -> TeamResource | None: ...

    async def list_teams(self) -> tuple[TeamResource, ...]: ...

    async def get_human(self, name: str) -> HumanResource | None: ...

    async def list_humans(self) -> tuple[HumanResource, ...]: ...


class Clock(Protocol):
    def now(self) -> datetime: ...
