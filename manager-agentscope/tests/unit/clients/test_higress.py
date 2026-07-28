from __future__ import annotations

import json

import httpx
import pytest
from agentteams_manager.clients.higress import (
    AIRouteRequest,
    AIRouteUpstream,
    ConsumerRequest,
    HigressClient,
    HigressConflictError,
    HigressForbiddenError,
    HigressNotFoundError,
    HigressProtocolError,
    HigressTransportError,
    HigressUnauthorizedError,
    KeyAuthCredential,
    LLMProviderRequest,
    ProxyHeader,
    ProxyMCPRequest,
    RestMCPRequest,
    RouteAuthConfig,
    RoutePredicate,
    ServiceSource,
)
from pydantic import SecretStr, ValidationError


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


@pytest.mark.asyncio
async def test_admin_credentials_create_and_refresh_console_session() -> None:
    login_count = 0
    request_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal login_count, request_count
        if request.url.path == "/session/login":
            login_count += 1
            return httpx.Response(
                200,
                headers={"Set-Cookie": f"SESSION=session-{login_count}; Path=/"},
                json={"success": True},
            )
        request_count += 1
        if request_count == 1:
            return httpx.Response(401, json={"success": False})
        assert request.headers["cookie"] == "SESSION=session-2"
        return httpx.Response(200, json={"success": True})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = HigressClient(
        console_url="http://console:8001",
        gateway_domain="aigw-local.agentteams.io",
        admin_user="admin",
        admin_password=SecretStr("secret"),
        client=http,
    )

    await client.delete_server("github")

    assert login_count == 2
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


@pytest.mark.asyncio
async def test_provider_crud_uses_typed_console_contract_without_leaking_tokens(
) -> None:
    requests: list[httpx.Request] = []
    provider = {
        "name": "deepseek",
        "type": "deepseek",
        "protocol": "openai/v1",
        "tokens": ["console-must-redact"],
        "version": "7",
        "rawConfigs": {},
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith(
            "/deepseek",
        ):
            return httpx.Response(200, json={"success": True, "data": provider})
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"success": True, "data": [provider]},
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json={"success": True, "data": provider})

    client, http = _client(httpx.MockTransport(handler))
    update = LLMProviderRequest(
        name="deepseek",
        provider_type="deepseek",
        protocol="openai/v1",
        tokens=(SecretStr("new-provider-token"),),
    )

    listed = await client.list_providers()
    fetched = await client.get_provider("deepseek")
    changed = await client.upsert_provider(update)
    await client.delete_provider("deepseek")

    assert listed == (fetched,)
    assert changed.token_count == 1
    assert "console-must-redact" not in fetched.model_dump_json()
    assert "new-provider-token" not in repr(update)
    put = next(request for request in requests if request.method == "PUT")
    assert put.url.path == "/v1/ai/providers/deepseek"
    put_body = json.loads(put.content)
    assert put_body["tokens"] == ["new-provider-token"]
    assert put_body["version"] == "7"
    assert requests[-1].method == "DELETE"
    await http.aclose()


@pytest.mark.asyncio
async def test_route_crud_preserves_console_version_and_supported_shape() -> None:
    requests: list[httpx.Request] = []
    existing = {
        "name": "deepseek-route",
        "version": "12",
        "domains": ["aigw-local.agentteams.io"],
        "pathPredicate": {
            "matchType": "PRE",
            "matchValue": "/",
            "caseSensitive": False,
        },
        "upstreams": [
            {"provider": "deepseek", "weight": 100, "modelMapping": {}},
        ],
        "modelPredicates": [
            {
                "matchType": "PRE",
                "matchValue": "deepseek",
                "caseSensitive": False,
            },
        ],
        "authConfig": {
            "enabled": True,
            "allowedCredentialTypes": ["key-auth"],
            "allowedConsumers": ["manager"],
        },
        "customLabels": {"preserved": "yes"},
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith(
            "/deepseek-route",
        ):
            return httpx.Response(
                200,
                json={"success": True, "data": existing},
            )
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"success": True, "data": [existing]},
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json={"success": True, "data": existing})

    client, http = _client(httpx.MockTransport(handler))
    request = AIRouteRequest(
        name="deepseek-route",
        domains=("aigw-local.agentteams.io",),
        path_predicate=RoutePredicate(match_type="PRE", match_value="/"),
        upstreams=(
            AIRouteUpstream(provider="deepseek", weight=100),
        ),
        model_predicates=(
            RoutePredicate(match_type="PRE", match_value="deepseek"),
        ),
        auth=RouteAuthConfig(allowed_consumers=("manager",)),
    )

    assert (await client.list_routes())[0].name == "deepseek-route"
    assert (await client.get_route("deepseek-route")).version == "12"
    await client.upsert_route(request)
    await client.delete_route("deepseek-route")

    put = next(request for request in requests if request.method == "PUT")
    body = json.loads(put.content)
    assert put.url.path == "/v1/ai/routes/deepseek-route"
    assert body["version"] == "12"
    assert body["customLabels"] == {"preserved": "yes"}
    assert body["modelPredicates"][0]["matchType"] == "PRE"
    await http.aclose()


@pytest.mark.asyncio
async def test_consumer_crud_returns_only_credential_summaries() -> None:
    requests: list[httpx.Request] = []
    existing = {
        "name": "worker-alice",
        "credentials": [
            {
                "type": "key-auth",
                "source": "BEARER",
                "values": ["existing-secret"],
            },
        ],
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith(
            "/worker-alice",
        ):
            return httpx.Response(
                200,
                json={"success": True, "data": existing},
            )
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"success": True, "data": [existing]},
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json={"success": True, "data": existing})

    client, http = _client(httpx.MockTransport(handler))
    request = ConsumerRequest(
        name="worker-alice",
        credentials=(
            KeyAuthCredential(
                source="BEARER",
                values=(SecretStr("new-consumer-secret"),),
            ),
        ),
    )

    listed = await client.list_consumers()
    fetched = await client.get_consumer("worker-alice")
    await client.upsert_consumer(request)
    await client.delete_consumer("worker-alice")

    assert listed == (fetched,)
    assert fetched.credentials[0].value_count == 1
    assert "existing-secret" not in fetched.model_dump_json()
    assert "new-consumer-secret" not in repr(request)
    put = next(request for request in requests if request.method == "PUT")
    assert json.loads(put.content)["credentials"][0]["values"] == [
        "new-consumer-secret",
    ]
    await http.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, HigressUnauthorizedError),
        (403, HigressForbiddenError),
        (404, HigressNotFoundError),
        (409, HigressConflictError),
    ],
)
async def test_gateway_statuses_have_typed_secret_safe_errors(
    status: int,
    error_type: type[Exception],
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            status,
            json={"message": "do not expose console-secret"},
        )

    client, http = _client(httpx.MockTransport(handler))
    with pytest.raises(error_type) as caught:
        await client.get_provider("deepseek")
    assert "console-secret" not in str(caught.value)
    await http.aclose()


@pytest.mark.asyncio
async def test_only_safe_reads_retry_transient_console_failures() -> None:
    get_calls = 0
    post_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal get_calls, post_calls
        if request.method == "GET":
            get_calls += 1
            if request.url.path.endswith("/deepseek"):
                return httpx.Response(404)
            if get_calls == 1:
                return httpx.Response(503)
            return httpx.Response(200, json={"success": True, "data": []})
        post_calls += 1
        return httpx.Response(503)

    client, http = _client(httpx.MockTransport(handler))
    assert await client.list_providers() == ()
    with pytest.raises(HigressTransportError):
        await client.upsert_provider(
            LLMProviderRequest(
                name="deepseek",
                provider_type="deepseek",
                protocol="openai/v1",
                tokens=(SecretStr("secret"),),
            ),
        )
    assert get_calls == 3
    assert post_calls == 1
    await http.aclose()
