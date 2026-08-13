"""I/O ports that keep workflows independent from infrastructure clients.

声明 workflow 所需的外部能力接口，而不是绑定具体 SDK。

例如任务 workflow 只需要“上传 artifact”这一能力，不需要知道 MinIO 客户端如何认证。
这种 Protocol 边界让生产环境接入真实 Matrix/Controller/MinIO，测试则接入可控 fake；
workflow 因而可以专注顺序、幂等和恢复规则，不被传输细节污染。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from .models import (
    HumanResource,
    MediaReference,
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

    async def lookup_user(
        self,
        user_id: str,
    ) -> dict[str, str | None]: ...

    async def room_state(
        self,
        room_id: str,
    ) -> tuple[dict[str, object], ...]: ...

    async def upload_media(self, path: Path) -> str: ...

    async def download_media(
        self,
        reference: MediaReference,
    ) -> tuple[object, ...]: ...

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

    async def get_json(self, key: str) -> Any: ...

    async def head(self, key: str) -> ObjectReceipt | None: ...

    async def list_prefix(
        self,
        prefix: str,
    ) -> tuple[ObjectReceipt, ...]: ...

    async def put_bytes_if_version(
        self,
        key: str,
        data: bytes,
        *,
        expected_etag: str | None,
        content_type: str,
    ) -> ObjectReceipt: ...

    async def put_json_if_version(
        self,
        key: str,
        value: Any,
        *,
        expected_etag: str | None,
    ) -> ObjectReceipt: ...

    async def delete_if_version(
        self,
        key: str,
        *,
        expected_etag: str,
    ) -> None: ...

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
