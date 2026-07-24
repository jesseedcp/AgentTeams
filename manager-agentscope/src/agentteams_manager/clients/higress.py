"""Typed, secret-safe administration of local Higress MCP routes."""

from __future__ import annotations

import json
import re
from collections.abc import Collection
from http.cookies import SimpleCookie
from typing import Any, Literal, Self
from urllib.parse import urlsplit

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from agentteams_manager.config import MCPServerDocument

_NAME_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
_CONSUMER_PATTERN = re.compile(
    r"^(?:manager|worker-[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)$",
)
_CREDENTIAL_SLOT = re.compile(
    r'(?m)^(?P<indent>[ \t]*)accessToken:[ \t]*""[ \t]*(?:#.*)?$',
)
_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


class HigressError(RuntimeError):
    """Base error for the Higress Console boundary."""


class HigressTransportError(HigressError):
    """The Console request did not produce an unambiguous response."""


class HigressProtocolError(HigressError):
    """The Console response violated its success contract."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ServiceSource(_StrictModel):
    """One DNS service source used by a generated MCP server."""

    name: str = Field(pattern=_NAME_PATTERN)
    domain: str = Field(min_length=1, max_length=253)
    port: int = Field(ge=1, le=65535)
    protocol: Literal["http", "https"]

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, value: str) -> str:
        parsed = urlsplit(f"//{value}")
        if (
            parsed.hostname != value
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
        ):
            raise ValueError("service source domain must be a bare hostname")
        return value


class RestMCPRequest(_StrictModel):
    """REST-to-MCP input whose credential exists only in memory."""

    name: str = Field(pattern=_NAME_PATTERN)
    description: str = Field(default="", max_length=2_000)
    yaml_template: str = Field(min_length=1, repr=False)
    credential: SecretStr = Field(repr=False)
    service: ServiceSource

    @model_validator(mode="after")
    def require_one_credential_slot(self) -> Self:
        if len(_CREDENTIAL_SLOT.findall(self.yaml_template)) != 1:
            raise ValueError(
                'REST MCP template must contain exactly one accessToken: "" '
                "credential slot",
            )
        if not self.credential.get_secret_value():
            raise ValueError("REST MCP credential must not be empty")
        return self


class ProxyHeader(_StrictModel):
    """One upstream header; its value must never leave Higress."""

    name: str = Field(min_length=1, max_length=256)
    value: SecretStr = Field(repr=False)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if _HEADER_NAME.fullmatch(value) is None:
            raise ValueError("invalid HTTP header name")
        return value

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value():
            raise ValueError("proxy header value must not be empty")
        return value


class ProxyMCPRequest(_StrictModel):
    """An existing HTTP/SSE MCP endpoint to expose through Higress."""

    name: str = Field(pattern=_NAME_PATTERN)
    description: str = Field(default="", max_length=2_000)
    backend_url: str = Field(min_length=1, max_length=2_048)
    transport: Literal["http", "sse"]
    headers: tuple[ProxyHeader, ...] = ()

    @model_validator(mode="after")
    def validate_backend(self) -> Self:
        parsed = _parse_backend_url(self.backend_url)
        if self.transport == "sse":
            path = parsed.path.rstrip("/")
            if not (path.endswith("/sse") or path.endswith("/messages")):
                raise ValueError(
                    "SSE proxy URLs must end in /sse or /messages",
                )
        lowered = [header.name.casefold() for header in self.headers]
        if len(lowered) != len(set(lowered)):
            raise ValueError("proxy header names must be unique")
        return self


class MCPServerState(_StrictModel):
    """Secret-free observed MCP state returned by Higress."""

    name: str = Field(pattern=_NAME_PATTERN)
    consumers: frozenset[str] = frozenset()


class HigressClient:
    """Strict client for the local Higress Console MCP contract."""

    def __init__(
        self,
        *,
        console_url: str,
        gateway_domain: str,
        session_cookie: SecretStr | None = None,
        admin_user: str | None = None,
        admin_password: SecretStr | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 15,
    ) -> None:
        if timeout <= 0:
            raise ValueError("Higress timeout must be positive")
        console = urlsplit(console_url)
        if (
            console.scheme not in {"http", "https"}
            or not console.hostname
            or console.username is not None
            or console.password is not None
            or console.query
            or console.fragment
        ):
            raise ValueError("invalid Higress Console URL")
        gateway = urlsplit(f"//{gateway_domain}")
        if (
            gateway.hostname != gateway_domain
            or gateway.username is not None
            or gateway.password is not None
            or gateway.port is not None
        ):
            raise ValueError("gateway domain must be a bare hostname")
        has_cookie = bool(
            session_cookie and session_cookie.get_secret_value(),
        )
        has_credentials = bool(
            admin_user
            and admin_password
            and admin_password.get_secret_value(),
        )
        if not has_cookie and not has_credentials:
            raise ValueError(
                "Higress session cookie or admin credentials are required",
            )
        self._console_url = console_url.rstrip("/")
        self._gateway_domain = gateway_domain
        self._session_cookie = session_cookie
        self._admin_user = admin_user
        self._admin_password = admin_password
        self._client = client or httpx.AsyncClient()
        self._owns_client = client is None
        self._timeout = timeout

    async def list_mcp_servers(self) -> tuple[MCPServerState, ...]:
        payload = await self._request("GET", "/v1/mcpServer")
        items = _response_items(payload)
        states: list[MCPServerState] = []
        for item in items:
            name = item.get("name") or item.get("mcpServerName")
            if not isinstance(name, str):
                raise HigressProtocolError(
                    "Higress MCP list item has no valid name",
                )
            consumers = _allowed_consumers(item)
            states.append(
                MCPServerState(
                    name=_logical_name(name),
                    consumers=frozenset(consumers),
                ),
            )
        return tuple(states)

    async def get_consumers(self, name: str) -> frozenset[str]:
        logical_name = _validate_logical_name(name)
        payload = await self._request(
            "GET",
            "/v1/mcpServer/consumers",
            params={"mcpServerName": _api_name(logical_name)},
        )
        consumers = _consumers_response(payload)
        if consumers is not None:
            return frozenset(consumers)
        for server in await self.list_mcp_servers():
            if server.name == logical_name:
                return server.consumers
        return frozenset()

    async def upsert_rest_server(
        self,
        request: RestMCPRequest,
    ) -> MCPServerDocument:
        await self._upsert_service_source(request.service)
        raw_configuration = _CREDENTIAL_SLOT.sub(
            lambda match: (
                f"{match.group('indent')}accessToken: "
                + json.dumps(
                    request.credential.get_secret_value(),
                    ensure_ascii=False,
                )
            ),
            request.yaml_template,
        )
        await self._upsert_mcp_server(
            name=request.name,
            description=request.description,
            raw_configuration=raw_configuration,
            service=request.service,
        )
        return self.descriptor(request.name)

    async def upsert_proxy(
        self,
        request: ProxyMCPRequest,
    ) -> MCPServerDocument:
        parsed = _parse_backend_url(request.backend_url)
        service = ServiceSource(
            name=f"{request.name}-proxy",
            domain=str(parsed.hostname),
            port=parsed.port or (443 if parsed.scheme == "https" else 80),
            protocol=parsed.scheme,
        )
        await self._upsert_service_source(service)
        raw_configuration = json.dumps(
            _proxy_configuration(request),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        await self._upsert_mcp_server(
            name=request.name,
            description=request.description,
            raw_configuration=raw_configuration,
            service=service,
        )
        return self.descriptor(request.name)

    async def replace_consumers(
        self,
        name: str,
        consumers: Collection[str],
    ) -> frozenset[str]:
        logical_name = _validate_logical_name(name)
        complete = {"manager", *consumers}
        invalid = sorted(
            consumer
            for consumer in complete
            if _CONSUMER_PATTERN.fullmatch(consumer) is None
        )
        if invalid:
            raise ValueError("invalid Higress consumer name")
        ordered = sorted(
            complete,
            key=lambda item: (item != "manager", item),
        )
        await self._request(
            "PUT",
            "/v1/mcpServer/consumers",
            json_body={
                "mcpServerName": _api_name(logical_name),
                "consumers": ordered,
            },
        )
        return frozenset(complete)

    async def delete_server(self, name: str) -> None:
        logical_name = _validate_logical_name(name)
        await self._request(
            "DELETE",
            "/v1/mcpServer",
            params={"name": _api_name(logical_name)},
        )

    def descriptor(self, name: str) -> MCPServerDocument:
        logical_name = _validate_logical_name(name)
        return MCPServerDocument(
            name=logical_name,
            url=(
                f"http://{self._gateway_domain}:8080/"
                f"mcp-servers/{_api_name(logical_name)}/mcp"
            ),
            transport="http",
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _upsert_service_source(
        self,
        service: ServiceSource,
    ) -> None:
        await self._request(
            "POST",
            "/v1/service-sources",
            json_body={
                "type": "dns",
                "name": service.name,
                "domain": service.domain,
                "port": service.port,
                "protocol": service.protocol,
            },
            accepted_statuses={409},
        )

    async def _upsert_mcp_server(
        self,
        *,
        name: str,
        description: str,
        raw_configuration: str,
        service: ServiceSource,
    ) -> None:
        api_name = _api_name(name)
        await self._request(
            "PUT",
            "/v1/mcpServer",
            json_body={
                "name": api_name,
                "description": description,
                "type": "OPEN_API",
                "rawConfigurations": raw_configuration,
                "mcpServerName": api_name,
                "domains": [self._gateway_domain],
                "services": [
                    {
                        "name": f"{service.name}.dns",
                        "port": service.port,
                        "weight": 100,
                    },
                ],
                "consumerAuthInfo": {
                    "type": "key-auth",
                    "enable": True,
                    "allowedConsumers": ["manager"],
                },
            },
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, object] | None = None,
        accepted_statuses: set[int] | None = None,
    ) -> object:
        await self._ensure_session()
        response = await self._send(
            method,
            path,
            params=params,
            json_body=json_body,
        )
        if response.status_code == 401 and self._admin_password is not None:
            self._session_cookie = None
            await self._ensure_session()
            response = await self._send(
                method,
                path,
                params=params,
                json_body=json_body,
            )
        accepted = accepted_statuses or set()
        if not 200 <= response.status_code < 300:
            if response.status_code in accepted:
                return {}
            raise HigressProtocolError(
                f"Higress {method} {path} returned HTTP "
                f"{response.status_code}",
            )
        if not response.content:
            return {}
        try:
            payload: object = response.json()
        except ValueError:
            raise HigressProtocolError(
                f"Higress {method} {path} returned invalid JSON",
            ) from None
        if isinstance(payload, dict) and payload.get("success") is False:
            raise HigressProtocolError(
                f"Higress {method} {path} returned success=false",
            )
        return payload

    async def _ensure_session(self) -> None:
        if self._session_cookie is not None:
            return
        if self._admin_user is None or self._admin_password is None:
            raise HigressTransportError(
                "Higress Console session is unavailable",
            )
        try:
            response = await self._client.post(
                f"{self._console_url}/session/login",
                json={
                    "username": self._admin_user,
                    "password": self._admin_password.get_secret_value(),
                },
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            raise HigressTransportError(
                "Higress Console login transport failed "
                f"({type(exc).__name__})",
            ) from None
        if not 200 <= response.status_code < 300:
            raise HigressProtocolError(
                "Higress Console login failed with HTTP "
                f"{response.status_code}",
            )
        cookie = SimpleCookie()
        for value in response.headers.get_list("set-cookie"):
            cookie.load(value)
        serialized = "; ".join(
            f"{name}={morsel.value}"
            for name, morsel in cookie.items()
        )
        if not serialized:
            raise HigressProtocolError(
                "Higress Console login returned no session cookie",
            )
        self._session_cookie = SecretStr(serialized)

    async def _send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None,
        json_body: dict[str, object] | None,
    ) -> httpx.Response:
        if self._session_cookie is None:
            raise HigressTransportError(
                "Higress Console session is unavailable",
            )
        try:
            return await self._client.request(
                method,
                f"{self._console_url}{path}",
                params=params,
                json=json_body,
                headers={
                    "Cookie": self._session_cookie.get_secret_value(),
                    "Accept": "application/json",
                },
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            raise HigressTransportError(
                f"Higress {method} {path} transport failed "
                f"({type(exc).__name__})",
            ) from None


def _parse_backend_url(value: str):
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("invalid MCP backend URL") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError(
            "MCP backend URL must be credential-free HTTP(S)",
        )
    del port
    return parsed


def _proxy_configuration(request: ProxyMCPRequest) -> dict[str, object]:
    server: dict[str, object] = {
        "name": f"{request.name}-mcp-server",
        "type": "mcp-proxy",
        "transport": request.transport,
        "mcpServerURL": request.backend_url,
        "timeout": 10_000 if request.transport == "sse" else 5_000,
    }
    schemes: list[dict[str, object]] = []
    for index, header in enumerate(request.headers):
        scheme_id = f"UpstreamAuth{index}"
        value = header.value.get_secret_value()
        if header.name.casefold() == "authorization":
            kind, separator, credential = value.partition(" ")
            if separator and kind.casefold() in {"bearer", "basic"}:
                schemes.append(
                    {
                        "id": scheme_id,
                        "type": "http",
                        "scheme": kind.casefold(),
                        "defaultCredential": credential,
                    },
                )
                continue
        schemes.append(
            {
                "id": scheme_id,
                "type": "apiKey",
                "in": "header",
                "name": header.name,
                "defaultCredential": value,
            },
        )
    if schemes:
        server["securitySchemes"] = schemes
        server["defaultUpstreamSecurity"] = {"id": "UpstreamAuth0"}
    return {"server": server}


def _response_items(payload: object) -> list[dict[str, Any]]:
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        for key in ("items", "mcpServers", "servers"):
            if key in data:
                data = data[key]
                break
    if not isinstance(data, list):
        raise HigressProtocolError("Higress MCP list is not an array")
    if not all(isinstance(item, dict) for item in data):
        raise HigressProtocolError("Higress MCP list contains invalid items")
    return data


def _allowed_consumers(item: dict[str, Any]) -> set[str]:
    auth = item.get("consumerAuthInfo", {})
    raw = auth.get("allowedConsumers", []) if isinstance(auth, dict) else []
    return _validated_consumers(raw)


def _consumers_response(payload: object) -> set[str] | None:
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        for key in ("consumers", "allowedConsumers"):
            if key in data:
                return _validated_consumers(data[key])
        auth = data.get("consumerAuthInfo")
        if isinstance(auth, dict) and "allowedConsumers" in auth:
            return _validated_consumers(auth["allowedConsumers"])
    if isinstance(data, list):
        raw: list[str] = []
        for item in data:
            if isinstance(item, str):
                raw.append(item)
            elif isinstance(item, dict):
                candidate = item.get("consumerName") or item.get("name")
                if isinstance(candidate, str):
                    raw.append(candidate)
                else:
                    return None
            else:
                return None
        return _validated_consumers(raw)
    return None


def _validated_consumers(value: object) -> set[str]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        raise HigressProtocolError("Higress consumers are not an array")
    consumers: set[str] = set()
    for item in value:
        if (
            not isinstance(item, str)
            or _CONSUMER_PATTERN.fullmatch(item) is None
        ):
            raise HigressProtocolError(
                "Higress returned an invalid consumer name",
            )
        consumers.add(item)
    return consumers


def _validate_logical_name(name: str) -> str:
    logical = _logical_name(name)
    if re.fullmatch(_NAME_PATTERN, logical) is None:
        raise ValueError("invalid MCP server name")
    return logical


def _api_name(name: str) -> str:
    logical = _validate_logical_name(name)
    return f"mcp-{logical}"


def _logical_name(name: str) -> str:
    return name.removeprefix("mcp-")
