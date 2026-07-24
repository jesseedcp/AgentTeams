from __future__ import annotations

import json

import httpx
import pytest
from pydantic import SecretStr

from agentteams_manager.clients.model_gateway import (
    ModelGatewayClient,
    ModelNotReachable,
    ModelSpec,
)


@pytest.mark.asyncio
async def test_preflight_uses_gateway_route_and_effective_defaults() -> None:
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["authorization"] = request.headers.get("authorization")
        seen["trace"] = request.headers.get("x-agentteams-trace-id")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {"message": {"role": "assistant", "content": "OK"}},
                ],
            },
        )

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://higress",
    )
    client = ModelGatewayClient(
        base_url="http://higress",
        api_key=SecretStr("gateway-secret"),
        client=http,
    )

    capabilities = await client.preflight(
        ModelSpec(model="agentteams-gateway/custom-model"),
    )

    assert seen["path"] == "/v1/chat/completions"
    assert seen["authorization"] == "Bearer gateway-secret"
    assert seen["trace"]
    assert seen["body"] == {
        "model": "custom-model",
        "messages": [{"role": "user", "content": "Reply with OK."}],
        "max_tokens": 8,
        "stream": False,
    }
    assert capabilities.context_window == 150_000
    assert capabilities.max_tokens == 128_000
    assert capabilities.reasoning is True
    await http.aclose()


@pytest.mark.asyncio
async def test_non_success_is_redacted_and_actionable() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            404,
            text="api_key=should-never-appear",
        )

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    )
    client = ModelGatewayClient(
        base_url="http://higress",
        api_key=SecretStr("gateway-secret"),
        client=http,
    )

    with pytest.raises(ModelNotReachable) as caught:
        await client.preflight(ModelSpec(model="missing"))

    message = str(caught.value)
    assert "404" in message
    assert "provider" in message
    assert "should-never-appear" not in message
    assert "gateway-secret" not in message
    await http.aclose()
