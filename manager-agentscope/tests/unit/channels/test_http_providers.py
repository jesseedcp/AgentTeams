from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from agentteams_manager.channels.http_providers import HttpChannelAdapter


def _signature(secret: str, body: bytes) -> str:
    return hmac.new(
        secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()


def test_telegram_webhook_requires_signature_and_normalizes_identity() -> None:
    body = json.dumps(
        {
            "message": {
                "message_id": 42,
                "text": "hello",
                "from": {"id": 7, "username": "alice"},
                "chat": {"id": 99},
            },
        },
    ).encode()
    client = httpx.AsyncClient()
    adapter = HttpChannelAdapter(
        provider="telegram",
        outbound_url="https://example.test/send",
        token="token",
        webhook_secret="secret",
        client=client,
    )
    try:
        with pytest.raises(PermissionError):
            adapter.verify_and_parse({}, body)
        message = adapter.verify_and_parse(
            {"x-agentteams-signature": _signature("secret", body)},
            body,
        )
        assert message.external_user_id == "7"
        assert message.destination_id == "99"
        assert message.text == "hello"
    finally:
        import asyncio

        asyncio.run(client.aclose())


@pytest.mark.asyncio
async def test_slack_outbound_uses_bearer_without_exposing_token() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["authorization"]
        captured["payload"] = json.loads(request.content)
        return httpx.Response(200, json={"ts": "123.4"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    )
    adapter = HttpChannelAdapter(
        provider="slack",
        outbound_url="https://example.test/send",
        token="secret-token",
        webhook_secret="hook-secret",
        client=client,
    )
    try:
        assert await adapter.send("C123", "hello") == "123.4"
        assert captured == {
            "authorization": "Bearer secret-token",
            "payload": {"channel": "C123", "text": "hello"},
        }
    finally:
        await client.aclose()
