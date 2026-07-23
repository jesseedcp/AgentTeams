from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from agentteams_manager.domain.models import MediaReference
from agentteams_manager.matrix.client import (
    MatrixClient,
    MatrixClientConfig,
)


class State:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get_value(self, key: str) -> str | None:
        return self.values.get(key)

    async def set_value(self, key: str, value: str) -> None:
        self.values[key] = value

    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool:
        del room_id, event_id
        return True


class Nio:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def joined_rooms(self) -> object:
        return SimpleNamespace(rooms=["!admin:example", "!room:example"])

    async def joined_members(self, room_id: str) -> object:
        self.calls.append(("members", (room_id,), {}))
        return SimpleNamespace(
            members=[
                SimpleNamespace(user_id="@manager:example"),
                SimpleNamespace(user_id="@alice:example"),
            ],
        )

    async def get_profile(self, user_id: str) -> object:
        self.calls.append(("profile", (user_id,), {}))
        return SimpleNamespace(
            displayname="Alice",
            avatar_url="mxc://example/avatar",
        )

    async def room_create(self, **kwargs: Any) -> object:
        self.calls.append(("create", (), kwargs))
        return SimpleNamespace(room_id="!created:example")

    async def room_invite(self, room_id: str, user_id: str) -> object:
        self.calls.append(("invite", (room_id, user_id), {}))
        return SimpleNamespace()

    async def room_kick(
        self,
        room_id: str,
        user_id: str,
        reason: str,
    ) -> object:
        self.calls.append(("kick", (room_id, user_id, reason), {}))
        return SimpleNamespace()

    async def room_ban(
        self,
        room_id: str,
        user_id: str,
        reason: str,
    ) -> object:
        self.calls.append(("ban", (room_id, user_id, reason), {}))
        return SimpleNamespace()

    async def room_unban(self, room_id: str, user_id: str) -> object:
        self.calls.append(("unban", (room_id, user_id), {}))
        return SimpleNamespace()

    async def room_get_state(self, room_id: str) -> object:
        self.calls.append(("state", (room_id,), {}))
        return SimpleNamespace(
            events=[
                {
                    "type": "m.room.name",
                    "state_key": "",
                    "content": {"name": "Project"},
                },
            ],
        )

    async def upload(self, **kwargs: Any) -> tuple[object, None]:
        self.calls.append(("upload", (), kwargs))
        return SimpleNamespace(content_uri="mxc://example/file"), None

    async def download(self, *, mxc: str) -> object:
        self.calls.append(("download", (mxc,), {}))
        return SimpleNamespace(body=b"hello", filename="hello.txt")


def config(tmp_path: Path) -> MatrixClientConfig:
    return MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:example",
        access_token=SecretStr("token"),
        device_name="agentteams-manager",
        crypto_store=tmp_path / "crypto",
        media_dir=tmp_path / "media",
    )


@pytest.mark.asyncio
async def test_matrix_admin_and_media_use_owned_adapter(
    tmp_path: Path,
) -> None:
    nio = Nio()
    client = MatrixClient(
        config(tmp_path),
        State(),
        nio_client=nio,
    )
    upload = tmp_path / "hello.txt"
    upload.write_text("hello", encoding="utf-8")

    assert await client.joined_rooms() == (
        "!admin:example",
        "!room:example",
    )
    assert await client.members("!room:example") == (
        "@alice:example",
        "@manager:example",
    )
    profile = await client.lookup_user("@alice:example")
    assert profile["display_name"] == "Alice"
    room_id = await client.create_private_room(
        name="Project",
        topic="Private project",
        invite=("@alice:example",),
        creation_marker={"kind": "project", "revision": 1},
    )
    assert room_id == "!created:example"
    await client.invite_user(room_id, "@bob:example")
    await client.kick_user(room_id, "@bob:example", reason="scope removed")
    await client.ban_user(room_id, "@bad:example", reason="abuse")
    await client.unban_user(room_id, "@bad:example")
    assert (await client.room_state(room_id))[0]["type"] == "m.room.name"
    assert await client.upload_media(upload) == "mxc://example/file"
    blocks = await client.download_media(
        MediaReference(
            mxc_uri="mxc://example/file",
            media_type="text/plain",
            filename="hello.txt",
        ),
    )
    assert len(blocks) == 1
    assert [call[0] for call in nio.calls] == [
        "members",
        "profile",
        "create",
        "invite",
        "kick",
        "ban",
        "unban",
        "state",
        "upload",
        "download",
    ]
