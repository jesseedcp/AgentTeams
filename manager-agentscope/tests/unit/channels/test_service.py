from __future__ import annotations

import json
from pathlib import Path

import pytest

from agentteams_manager.channels.base import (
    ChannelMessage,
    ChannelWebhookRequest,
    ChannelWebhookResponse,
)
from agentteams_manager.channels.service import (
    ChannelService,
    ExternalContactRepository,
)
from agentteams_manager.state.database import Database


class Adapter:
    provider = "discord"

    def handle_webhook(self, request):
        assert isinstance(request, ChannelWebhookRequest)
        return ChannelWebhookResponse(
            message=ChannelMessage(
                provider="discord",
                external_user_id="u1",
                display_name="Alice",
                destination_id="room1",
                message_id="m1",
                text="hello",
            ),
        )

    async def send(self, destination_id, text):
        assert destination_id == "room1"
        assert text
        return "out-1"

    async def close(self):
        return None


class Escalation:
    def __init__(self):
        self.first = 0
        self.trusted = 0

    async def first_contact(self, message):
        del message
        self.first += 1

    async def trusted_message(self, message):
        del message
        self.trusted += 1


@pytest.mark.asyncio
async def test_first_contact_is_pending_until_admin_approval(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "state.db")
    await database.open()
    escalation = Escalation()
    service = ChannelService(
        contacts=ExternalContactRepository(database),
        adapters=(Adapter(),),
        escalation=escalation,
    )

    first = await service.ingest("discord", {}, b"{}")
    assert first.status == "pending_approval"
    assert escalation.first == 1
    with pytest.raises(PermissionError):
        await service.send("discord", "u1", "reply")

    contact = await service.approve("discord", "u1")
    assert contact.status == "trusted"
    accepted = await service.ingest("discord", {}, b"{}")
    assert accepted.status == "accepted"
    assert escalation.trusted == 1
    assert (await service.set_primary("discord", "u1")).is_primary
    assert (await service.send("discord", "u1", "reply"))[
        "message_id"
    ] == "out-1"


@pytest.mark.asyncio
async def test_duplicate_provider_event_is_acknowledged_only_once(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "state.db")
    await database.open()
    escalation = Escalation()
    service = ChannelService(
        contacts=ExternalContactRepository(database),
        adapters=(Adapter(),),
        escalation=escalation,
    )

    first = await service.handle_webhook(
        "discord",
        ChannelWebhookRequest(method="POST", headers={}, query={}, body=b"{}"),
    )
    duplicate = await service.handle_webhook(
        "discord",
        ChannelWebhookRequest(method="POST", headers={}, query={}, body=b"{}"),
    )

    assert first.status_code == 200
    assert json.loads(first.body)["status"] == "pending_approval"
    assert json.loads(duplicate.body)["status"] == "duplicate"
    assert escalation.first == 1
