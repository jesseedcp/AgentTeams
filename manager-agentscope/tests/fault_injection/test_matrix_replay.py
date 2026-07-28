from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from agentteams_manager.domain.models import (
    InboundEvent,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.client import MatrixClient, MatrixClientConfig
from agentteams_manager.matrix.router import EventRouter
from agentteams_manager.state.database import Database
from agentteams_manager.state.operations import OperationRepository


class CrashBeforeCursor:
    def __init__(self, repository: OperationRepository) -> None:
        self._repository = repository

    async def get_value(self, key: str) -> str | None:
        return await self._repository.get_value(key)

    async def set_value(self, key: str, value: str) -> None:
        del key, value
        raise RuntimeError("process exited before cursor commit")

    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool:
        return await self._repository.claim_matrix_event(room_id, event_id)


class Nio:
    olm = None
    rooms: dict[str, object] = {}

    def __init__(self) -> None:
        event = SimpleNamespace(
            event_id="$replayed",
            sender="@admin:local",
            body="run once",
            server_timestamp=1_700_000_000_000,
            source={"content": {"body": "run once"}},
        )
        self.response = SimpleNamespace(
            next_batch="next",
            rooms=SimpleNamespace(
                invite={},
                join={
                    "!room:local": SimpleNamespace(
                        timeline=SimpleNamespace(events=[event]),
                    ),
                },
            ),
        )

    async def sync(self, **kwargs: Any) -> object:
        del kwargs
        return self.response

    async def send_to_device_messages(self) -> None:
        return None


class Resolver:
    async def resolve(self, event: InboundEvent) -> RoomPolicy:
        return RoomPolicy(
            room_id=event.room_id,
            kind=RoomKind.ADMIN_DM,
            revision=1,
        )


def _config(tmp_path: Path) -> MatrixClientConfig:
    return MatrixClientConfig(
        homeserver="http://matrix:6167",
        user_id="@manager:local",
        access_token=SecretStr("token"),
        device_name="agentteams-manager",
        crypto_store=tmp_path / "matrix-e2ee",
        media_dir=tmp_path / "media",
        encryption=False,
    )


@pytest.mark.asyncio
async def test_replayed_event_is_rejected_after_cursor_crash(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)
    handled: list[str] = []

    async def handler(
        event: InboundEvent,
        policy: RoomPolicy,
    ) -> None:
        del policy
        handled.append(event.event_id)

    first_router = EventRouter(
        claims=repository,
        resolver=Resolver(),
        handler=handler,
    )
    await first_router.start()
    first_client = MatrixClient(
        _config(tmp_path),
        CrashBeforeCursor(repository),
        nio_client=Nio(),
    )
    first_client.bind_handler(first_router.submit)

    with pytest.raises(RuntimeError, match="cursor commit"):
        await first_client.sync_once()
    await first_router.stop()

    second_router = EventRouter(
        claims=repository,
        resolver=Resolver(),
        handler=handler,
    )
    await second_router.start()
    second_client = MatrixClient(
        _config(tmp_path),
        repository,
        nio_client=Nio(),
    )
    second_client.bind_handler(second_router.submit)
    await second_client.sync_once()
    await second_router.stop()

    assert handled == ["$replayed"]
