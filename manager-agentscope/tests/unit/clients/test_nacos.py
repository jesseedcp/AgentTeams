from __future__ import annotations

import json

import httpx
import pytest

from agentteams_manager.clients.nacos import (
    NacosClient,
    NacosProtocolError,
)


def _list_response(*items: dict[str, object]) -> dict[str, object]:
    return {
        "code": 0,
        "message": "success",
        "data": {
            "totalCount": len(items),
            "pageItems": list(items),
        },
    }


def _detail_response(
    name: str,
    *,
    runtime: str = "hermes",
    display_name: str = "Remote Coder",
) -> dict[str, object]:
    return {
        "code": 0,
        "message": "success",
        "data": {
            "namespaceId": "public",
            "name": name,
            "description": "Writes and reviews production code",
            "content": json.dumps(
                {
                    "displayName": display_name,
                    "runtime": runtime,
                },
            ),
            "resource": {},
        },
    }


@pytest.mark.asyncio
async def test_search_uses_typed_v3_api_and_returns_pinned_candidates() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/agentspecs/list"):
            return httpx.Response(
                200,
                json=_list_response(
                    {
                        "namespaceId": "public",
                        "name": "remote-coder",
                        "description": "Production coder",
                        "enable": True,
                        "onlineCnt": 1,
                        "labels": {"latest": "1.4.0"},
                    },
                    {
                        "namespaceId": "public",
                        "name": "writer",
                        "description": "Documentation writer",
                        "enable": True,
                        "onlineCnt": 1,
                        "labels": {"latest": "2.0.0"},
                    },
                ),
            )
        return httpx.Response(
            200,
            json=_detail_response("remote-coder"),
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = NacosClient(
            registry_uri="nacos://registry.example:8848/public",
            http_client=http,
            max_results=3,
        )
        workers = await client.search_workers("production coder")

    assert len(workers) == 1
    worker = workers[0]
    assert worker.name == "remote-coder"
    assert worker.display_name == "Remote Coder"
    assert worker.runtime == "hermes"
    assert worker.version == "1.4.0"
    assert worker.package_uri == (
        "nacos://registry.example:8848/public/remote-coder/1.4.0"
    )
    assert worker.digest.startswith("sha256:")
    assert len(worker.digest) == 71
    assert requests[0].url.path == "/nacos/v3/admin/ai/agentspecs/list"
    assert requests[0].url.params["namespaceId"] == "public"
    assert requests[0].url.params["pageSize"] == "100"
    assert requests[1].url.params["version"] == "1.4.0"


@pytest.mark.asyncio
async def test_search_caps_detail_fetches_and_drops_offline_specs() -> None:
    detail_names: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/agentspecs/list"):
            return httpx.Response(
                200,
                json=_list_response(
                    *[
                        {
                            "namespaceId": "public",
                            "name": f"coder-{index}",
                            "description": "coder",
                            "enable": index != 3,
                            "onlineCnt": 0 if index == 4 else 1,
                            "labels": {"latest": f"1.0.{index}"},
                        }
                        for index in range(5)
                    ],
                ),
            )
        name = request.url.params["name"]
        detail_names.append(name)
        return httpx.Response(200, json=_detail_response(name))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        workers = await NacosClient(
            registry_uri="nacos://registry.example/public",
            http_client=http,
            max_results=2,
        ).search_workers("coder")

    assert len(workers) == 2
    assert detail_names == ["coder-0", "coder-1"]


@pytest.mark.asyncio
async def test_username_password_login_is_not_exposed_in_results() -> None:
    authorization: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth/user/login"):
            assert request.content == b"username=alice&password=swordfish"
            return httpx.Response(
                200,
                json={"accessToken": "registry-token", "tokenTtl": 3600},
            )
        authorization.append(request.headers.get("Authorization", ""))
        if request.url.path.endswith("/agentspecs/list"):
            return httpx.Response(
                200,
                json=_list_response(
                    {
                        "namespaceId": "public",
                        "name": "coder",
                        "description": "coder",
                        "enable": True,
                        "onlineCnt": 1,
                        "labels": {"latest": "1"},
                    },
                ),
            )
        return httpx.Response(200, json=_detail_response("coder"))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        workers = await NacosClient(
            registry_uri=(
                "nacos://alice:swordfish@registry.example:8848/public"
            ),
            http_client=http,
        ).search_workers("coder")

    assert authorization == ["Bearer registry-token"] * 2
    assert "alice" not in workers[0].package_uri
    assert "swordfish" not in workers[0].package_uri


@pytest.mark.asyncio
async def test_malformed_list_response_fails_closed_without_credentials() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            content=b'{"code":0,"data":{"pageItems":"not-a-list"}}',
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = NacosClient(
            registry_uri=(
                "nacos://alice:swordfish@registry.example:8848/public"
            ),
            http_client=http,
            access_token="already-authenticated",
        )
        with pytest.raises(NacosProtocolError) as caught:
            await client.search_workers("coder")

    message = str(caught.value)
    assert "swordfish" not in message
    assert "registry response" in message
