from __future__ import annotations

import json

import httpx
import pytest

from agentteams_manager.channels.base import (
    ChannelWebhookResponse,
)
from agentteams_manager.health import HealthServer, ReadinessState
from agentteams_manager.observability.metrics import MetricsRegistry


@pytest.mark.asyncio
async def test_health_server_forwards_get_query_and_provider_response() -> None:
    calls: list[tuple[str, str, dict[str, str], bytes]] = []

    async def webhook(provider, request):
        calls.append(
            (
                provider,
                request.method,
                dict(request.query),
                request.body,
            ),
        )
        return ChannelWebhookResponse(
            status_code=200,
            body=b"challenge-value",
            content_type="text/plain; charset=utf-8",
        )

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        webhook_handler=webhook,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.get(
                "/manager-admin/hooks/whatsapp",
                params={
                    "hub.mode": "subscribe",
                    "hub.verify_token": "verify-me",
                    "hub.challenge": "challenge-value",
                },
            )
        assert response.status_code == 200
        assert response.text == "challenge-value"
        assert calls == [
            (
                "whatsapp",
                "GET",
                {
                    "hub.mode": "subscribe",
                    "hub.verify_token": "verify-me",
                    "hub.challenge": "challenge-value",
                },
                b"",
            ),
        ]
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_health_server_preserves_native_json_challenge() -> None:
    async def webhook(provider, request):
        assert provider == "slack"
        assert request.method == "POST"
        return ChannelWebhookResponse(
            status_code=200,
            body=json.dumps({"challenge": "abc"}).encode(),
            content_type="application/json",
        )

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        webhook_handler=webhook,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.post(
                "/manager-admin/hooks/slack",
                json={
                    "type": "url_verification",
                    "challenge": "abc",
                },
            )
        assert response.status_code == 200
        assert response.json() == {"challenge": "abc"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_health_server_rejects_declared_non_json_webhook_body() -> None:
    called = False

    async def webhook(provider, request):
        nonlocal called
        del provider, request
        called = True
        return ChannelWebhookResponse()

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        webhook_handler=webhook,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.post(
                "/manager-admin/hooks/slack",
                content="not-json",
                headers={"Content-Type": "text/plain"},
            )
        assert response.status_code == 415
        assert called is False
    finally:
        await server.stop()
