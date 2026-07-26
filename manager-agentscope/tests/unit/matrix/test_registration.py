from __future__ import annotations

import hashlib
import hmac
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from agentteams_manager.matrix.client import MatrixClient, MatrixClientConfig


class _State:
    async def get_value(self, key: str) -> str | None:
        del key
        return None

    async def set_value(self, key: str, value: str) -> None:
        del key, value

    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool:
        del room_id, event_id
        return True


class _Response:
    def __init__(
        self,
        payload: dict[str, object],
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class _RegistrationHttp:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict[str, object]]] = []

    async def get(self, path: str) -> _Response:
        assert path == "/_synapse/admin/v1/register"
        return _Response({"nonce": "nonce-1"})

    async def post(
        self,
        path: str,
        *,
        json: dict[str, object],
    ) -> _Response:
        self.posts.append((path, json))
        if path == "/_matrix/client/v3/register":
            return _Response(
                {
                    "user_id": "@alice:matrix.local",
                    "access_token": "must-not-escape",
                },
            )
        return _Response(
            {
                "user_id": "@alice:matrix.local",
                "access_token": "must-not-escape",
            },
        )


class _ExclusiveRegistrationHttp(_RegistrationHttp):
    async def post(
        self,
        path: str,
        *,
        json: dict[str, object],
    ) -> _Response:
        if path == "/_matrix/client/v3/register":
            self.posts.append((path, json))
            return _Response(
                {"errcode": "M_EXCLUSIVE"},
                status_code=400,
            )
        return await super().post(path, json=json)


def _client(http: _RegistrationHttp) -> MatrixClient:
    return MatrixClient(
        MatrixClientConfig(
            homeserver="http://matrix.local",
            user_id="@manager:matrix.local",
            access_token=SecretStr("token"),
            device_name="manager",
            crypto_store=Path("crypto"),
            media_dir=Path("media"),
            registration_token=SecretStr("shared-secret"),
        ),
        _State(),
        registration_http=http,
        nio_client=SimpleNamespace(),
    )


@pytest.mark.asyncio
async def test_register_user_uses_tuwunel_registration_token_without_returning_token() -> None:
    http = _RegistrationHttp()

    result = await _client(http).register_user(
        username="alice",
        password=SecretStr("user-password"),
        admin=False,
    )

    assert result == {"user_id": "@alice:matrix.local", "admin": False}
    assert http.posts == [
        (
            "/_matrix/client/v3/register",
            {
                "username": "alice",
                "password": "user-password",
                "auth": {
                    "type": "m.login.registration_token",
                    "token": "shared-secret",
                },
            },
        ),
    ]


@pytest.mark.asyncio
async def test_register_admin_uses_synapse_nonce_fallback() -> None:
    http = _RegistrationHttp()

    result = await _client(http).register_user(
        username="alice",
        password=SecretStr("user-password"),
        admin=True,
    )

    expected_mac = hmac.new(
        b"shared-secret",
        b"nonce-1\x00alice\x00user-password\x00admin",
        hashlib.sha1,
    ).hexdigest()
    assert result == {"user_id": "@alice:matrix.local", "admin": True}
    assert http.posts == [
        (
            "/_synapse/admin/v1/register",
            {
                "nonce": "nonce-1",
                "username": "alice",
                "password": "user-password",
                "admin": True,
                "mac": expected_mac,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_appservice_exclusive_namespace_falls_back_to_shared_secret() -> None:
    http = _ExclusiveRegistrationHttp()

    result = await _client(http).register_user(
        username="alice",
        password=SecretStr("user-password"),
    )

    assert result == {"user_id": "@alice:matrix.local", "admin": False}
    assert [path for path, _ in http.posts] == [
        "/_matrix/client/v3/register",
        "/_synapse/admin/v1/register",
    ]


@pytest.mark.asyncio
async def test_register_user_requires_registration_token() -> None:
    config = MatrixClientConfig(
        homeserver="http://matrix.local",
        user_id="@manager:matrix.local",
        access_token=SecretStr("token"),
        device_name="manager",
        crypto_store=Path("crypto"),
        media_dir=Path("media"),
    )
    client = MatrixClient(
        config,
        _State(),
        registration_http=_RegistrationHttp(),
        nio_client=SimpleNamespace(),
    )

    with pytest.raises(RuntimeError, match="registration token"):
        await client.register_user(
            username="alice",
            password=SecretStr("password"),
        )
