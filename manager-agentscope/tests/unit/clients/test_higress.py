from __future__ import annotations

import json

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from agentteams_manager.clients.higress import (
    HigressClient,
    HigressProtocolError,
    ProxyHeader,
    ProxyMCPRequest,
    RestMCPRequest,
    ServiceSource,
)


def _client(
    handler: httpx.AsyncBaseTransport,
) -> tuple[HigressClient, httpx.AsyncClient]:
    http = httpx.AsyncClient(transport=handler)
    return (
        HigressClient(
            console_url="http://console:8001",
            gateway_domain="aigw-local.agentteams.io",
            session_cookie=SecretStr("SESSION=console-secret"),
            client=http,
        ),
        http,
    )


@pytest.mark.asyncio
async def test_rest_upsert_uses_one_credential_slot_and_typed_payload() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"success": True})

    client, http = _client(httpx.MockTransport(handler))
    descriptor = await client.upsert_rest_server(
        RestMCPRequest(
            name="github",
            description="GitHub API",
            yaml_template=(
                "server:\n"
                "  name: github-mcp-server\n"
                "  config:\n"
                '    accessToken: ""\n'
            ),
            credential=SecretStr('ghp_a"b\nc'),
            service=ServiceSource(
                name="github-api",
                domain="api.github.com",
                port=443,
                protocol="https",
            ),
        ),
    )

    assert [request.url.path for request in requests] == [
        "/v1/service-sources",
        "/v1/mcpServer",
    ]
    assert all(
        request.headers["cookie"] == "SESSION=console-secret"
        for request in requests
    )
    assert json.loads(requests[0].content) == {
        "type": "dns",
        "name": "github-api",
        "domain": "api.github.com",
        "port": 443,
        "protocol": "https",
    }
    body = json.loads(requests[1].content)
    assert body["name"] == "mcp-github"
    assert body["mcpServerName"] == "mcp-github"
    assert body["services"] == [
        {"name": "github-api.dns", "port": 443, "weight": 100},
    ]
    assert body["consumerAuthInfo"]["allowedConsumers"] == ["manager"]
    assert 'accessToken: "ghp_a\\"b\\nc"' in body["rawConfigurations"]
    assert body["rawConfigurations"].count("accessToken:") == 1
    assert "ghp_" not in descriptor.model_dump_json()
    assert descriptor.name == "github"
    assert descriptor.url == (
        "http://aigw-local.agentteams.io:8080/"
        "mcp-servers/mcp-github/mcp"
    )
    await http.aclose()


def test_rest_template_requires_exactly_one_empty_credential_slot() -> None:
    common = {
        "name": "github",
        "credential": SecretStr("secret"),
        "service": ServiceSource(
            name="github-api",
            domain="api.github.com",
            port=443,
            protocol="https",
        ),
    }

    with pytest.raises(ValidationError, match="exactly one"):
        RestMCPRequest(yaml_template="server: {}", **common)
    with pytest.raises(ValidationError, match="exactly one"):
        RestMCPRequest(
            yaml_template='accessToken: ""\naccessToken: ""\n',
            **common,
        )


@pytest.mark.asyncio
async def test_proxy_upsert_renders_structured_security_as_json_yaml() -> None:
    bodies: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"success": True})

    client, http = _client(httpx.MockTransport(handler))
    await client.upsert_proxy(
        ProxyMCPRequest(
            name="notion",
            description="Notion upstream",
            backend_url="https://mcp.notion.example/mcp",
            transport="http",
            headers=(
                ProxyHeader(
                    name="Authorization",
                    value=SecretStr('Bearer token"\nsecuritySchemes: []'),
                ),
                ProxyHeader(
                    name="X-API-Key",
                    value=SecretStr("key"),
                ),
            ),
        ),
    )

    raw = bodies[0]["rawConfigurations"]
    parsed = json.loads(str(raw))
    server = parsed["server"]
    assert server["mcpServerURL"] == "https://mcp.notion.example/mcp"
    assert server["transport"] == "http"
    assert server["securitySchemes"][0] == {
        "id": "UpstreamAuth0",
        "type": "http",
        "scheme": "bearer",
        "defaultCredential": 'token"\nsecuritySchemes: []',
    }
    assert server["securitySchemes"][1] == {
        "id": "UpstreamAuth1",
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
        "defaultCredential": "key",
    }
    assert server["defaultUpstreamSecurity"] == {
        "id": "UpstreamAuth0",
    }
    await http.aclose()


@pytest.mark.asyncio
async def test_lists_and_replaces_complete_consumer_set() -> None:
    seen: list[tuple[str, str, dict[str, object] | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, body))
        if request.url.path.endswith("/consumers") and request.method == "GET":
            return httpx.Response(
                200,
                json={"data": {"consumers": ["manager", "worker-alice"]}},
            )
        if request.url.path == "/v1/mcpServer" and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "name": "mcp-github",
                            "consumerAuthInfo": {
                                "allowedConsumers": [
                                    "manager",
                                    "worker-alice",
                                ],
                            },
                        },
                    ],
                },
            )
        return httpx.Response(200, json={"success": True})

    client, http = _client(httpx.MockTransport(handler))

    servers = await client.list_mcp_servers()
    consumers = await client.get_consumers("github")
    replacement = await client.replace_consumers(
        "github",
        {"worker-bob", *consumers},
    )

    assert servers[0].name == "github"
    assert servers[0].consumers == frozenset(
        {"manager", "worker-alice"},
    )
    assert replacement == frozenset(
        {"manager", "worker-alice", "worker-bob"},
    )
    assert seen[-1] == (
        "PUT",
        "/v1/mcpServer/consumers",
        {
            "mcpServerName": "mcp-github",
            "consumers": ["manager", "worker-alice", "worker-bob"],
        },
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_success_false_and_response_body_are_not_treated_as_success() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={
                "success": False,
                "message": "credential console-secret was rejected",
            },
        )

    client, http = _client(httpx.MockTransport(handler))

    with pytest.raises(HigressProtocolError) as caught:
        await client.delete_server("github")

    message = str(caught.value)
    assert "success=false" in message
    assert "console-secret" not in message
    await http.aclose()


@pytest.mark.parametrize(
    ("url", "transport"),
    (
        ("ftp://mcp.example/mcp", "http"),
        ("https://user:password@mcp.example/mcp", "http"),
        ("https://mcp.example/mcp", "stdio"),
    ),
)
def test_proxy_rejects_unsafe_backends(url: str, transport: str) -> None:
    with pytest.raises(ValidationError):
        ProxyMCPRequest(
            name="unsafe",
            backend_url=url,
            transport=transport,
        )
