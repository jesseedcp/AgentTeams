"""Typed, secret-safe administration of local Higress MCP routes."""

from __future__ import annotations

import json
import math
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


class HigressUnauthorizedError(HigressProtocolError):
    """The Console rejected the configured session or credentials."""


class HigressForbiddenError(HigressProtocolError):
    """The authenticated Console identity cannot perform the operation."""


class HigressNotFoundError(HigressProtocolError):
    """The requested Console resource does not exist."""


class HigressConflictError(HigressProtocolError):
    """The requested Console mutation conflicts with current state."""


class HigressRequestError(HigressProtocolError):
    """The Console rejected a typed request as invalid."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )


class _ObservedModel(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        populate_by_name=True,
    )


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
            if not path.endswith(("/sse", "/messages")):
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


ProviderType = Literal[
    "qwen",
    "openai",
    "moonshot",
    "azure",
    "ai360",
    "github",
    "groq",
    "baichuan",
    "yi",
    "deepseek",
    "zhipuai",
    "ollama",
    "claude",
    "baidu",
    "hunyuan",
    "stepfun",
    "minimax",
    "cloudflare",
    "spark",
    "gemini",
    "deepl",
    "mistral",
    "cohere",
    "doubao",
    "coze",
    "together-ai",
    "openrouter",
    "grok",
]
PredicateType = Literal["EXACT", "PRE", "REGEX"]


class TokenFailoverConfig(_StrictModel):
    enabled: bool = False
    failure_threshold: int | None = Field(
        default=None,
        alias="failureThreshold",
        ge=1,
    )
    success_threshold: int | None = Field(
        default=None,
        alias="successThreshold",
        ge=1,
    )
    health_check_interval: int | None = Field(
        default=None,
        alias="healthCheckInterval",
        ge=1,
    )
    health_check_timeout: int | None = Field(
        default=None,
        alias="healthCheckTimeout",
        ge=1,
    )
    health_check_model: str | None = Field(
        default=None,
        alias="healthCheckModel",
        min_length=1,
        max_length=256,
    )


class LLMProviderRequest(_StrictModel):
    """Supported provider fields; tokens exist only for the live request."""

    name: str = Field(pattern=_NAME_PATTERN)
    provider_type: ProviderType = Field(alias="type")
    protocol: Literal["openai/v1", "original"] = "openai/v1"
    tokens: tuple[SecretStr, ...] = Field(min_length=1, repr=False)
    proxy_name: str | None = Field(
        default=None,
        alias="proxyName",
        pattern=_NAME_PATTERN,
    )
    model_mapping: dict[str, str] = Field(
        default_factory=dict,
        alias="modelMapping",
    )
    token_failover: TokenFailoverConfig = Field(
        default_factory=TokenFailoverConfig,
        alias="tokenFailoverConfig",
    )
    raw_configs: dict[str, object] = Field(
        default_factory=dict,
        alias="rawConfigs",
    )

    @field_validator("tokens")
    @classmethod
    def require_nonempty_tokens(
        cls,
        value: tuple[SecretStr, ...],
    ) -> tuple[SecretStr, ...]:
        if any(not item.get_secret_value() for item in value):
            raise ValueError("provider tokens must not be empty")
        return value

    @field_validator("raw_configs")
    @classmethod
    def require_public_json_config(
        cls,
        value: dict[str, object],
    ) -> dict[str, object]:
        _validate_public_json(value)
        return value


class LLMProviderState(_ObservedModel):
    """Observed provider metadata with all token values removed."""

    name: str = Field(pattern=_NAME_PATTERN)
    provider_type: ProviderType = Field(alias="type")
    protocol: Literal["openai/v1", "original"] = "openai/v1"
    proxy_name: str | None = Field(default=None, alias="proxyName")
    version: str | None = None
    token_count: int = Field(ge=0)
    model_mapping: dict[str, str] = Field(
        default_factory=dict,
        alias="modelMapping",
    )
    token_failover: TokenFailoverConfig = Field(
        default_factory=TokenFailoverConfig,
        alias="tokenFailoverConfig",
    )
    raw_configs: dict[str, object] = Field(
        default_factory=dict,
        alias="rawConfigs",
    )


class RoutePredicate(_StrictModel):
    match_type: PredicateType = Field(alias="matchType")
    match_value: str = Field(alias="matchValue", min_length=1, max_length=512)
    case_sensitive: bool = Field(default=False, alias="caseSensitive")


class AIRouteUpstream(_StrictModel):
    provider: str = Field(pattern=_NAME_PATTERN)
    weight: int = Field(default=100, ge=1, le=100)
    model_mapping: dict[str, str] = Field(
        default_factory=dict,
        alias="modelMapping",
    )


class RouteAuthConfig(_StrictModel):
    enabled: bool = True
    allowed_credential_types: tuple[Literal["key-auth"], ...] = Field(
        default=("key-auth",),
        alias="allowedCredentialTypes",
    )
    allowed_consumers: tuple[str, ...] = Field(
        default=("manager",),
        alias="allowedConsumers",
    )

    @model_validator(mode="after")
    def validate_auth(self) -> Self:
        if len(self.allowed_consumers) != len(set(self.allowed_consumers)):
            raise ValueError("route consumers must be unique")
        if any(
            re.fullmatch(_NAME_PATTERN, item) is None
            for item in self.allowed_consumers
        ):
            raise ValueError("route contains an invalid consumer name")
        if self.enabled and (
            not self.allowed_consumers
            or not self.allowed_credential_types
        ):
            raise ValueError(
                "enabled route auth requires consumers and credential types",
            )
        return self


class AIRouteRequest(_StrictModel):
    name: str = Field(pattern=_NAME_PATTERN)
    domains: tuple[str, ...] = Field(min_length=1)
    path_predicate: RoutePredicate = Field(
        default_factory=lambda: RoutePredicate(
            matchType="PRE",
            matchValue="/",
        ),
        alias="pathPredicate",
    )
    upstreams: tuple[AIRouteUpstream, ...] = Field(min_length=1)
    model_predicates: tuple[RoutePredicate, ...] = Field(
        default=(),
        alias="modelPredicates",
    )
    auth: RouteAuthConfig = Field(
        default_factory=RouteAuthConfig,
        alias="authConfig",
    )

    @field_validator("domains")
    @classmethod
    def validate_domains(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("route domains must be unique")
        for domain in value:
            _validate_route_domain(domain)
        return value

    @model_validator(mode="after")
    def require_valid_upstream_weights(self) -> Self:
        providers = [item.provider for item in self.upstreams]
        if len(providers) != len(set(providers)):
            raise ValueError("route upstream providers must be unique")
        if sum(item.weight for item in self.upstreams) != 100:
            raise ValueError("route upstream weights must total 100")
        return self


class AIRouteState(_ObservedModel):
    name: str = Field(pattern=_NAME_PATTERN)
    version: str | None = None
    domains: tuple[str, ...] = ()
    path_predicate: RoutePredicate | None = Field(
        default=None,
        alias="pathPredicate",
    )
    upstreams: tuple[AIRouteUpstream, ...] = ()
    model_predicates: tuple[RoutePredicate, ...] = Field(
        default=(),
        alias="modelPredicates",
    )
    auth: RouteAuthConfig | None = Field(default=None, alias="authConfig")


class KeyAuthCredential(_StrictModel):
    credential_type: Literal["key-auth"] = Field(
        default="key-auth",
        alias="type",
    )
    source: Literal["BEARER", "HEADER", "QUERY"]
    key: str | None = Field(default=None, min_length=1, max_length=256)
    values: tuple[SecretStr, ...] = Field(min_length=1, repr=False)

    @model_validator(mode="after")
    def validate_key_and_values(self) -> Self:
        if self.source in {"HEADER", "QUERY"} and not self.key:
            raise ValueError("HEADER/QUERY credentials require key")
        if self.source == "BEARER" and self.key is not None:
            raise ValueError("BEARER credentials cannot set key")
        if any(not item.get_secret_value() for item in self.values):
            raise ValueError("consumer credential values must not be empty")
        return self


class ConsumerRequest(_StrictModel):
    name: str = Field(pattern=_NAME_PATTERN)
    credentials: tuple[KeyAuthCredential, ...] = Field(
        min_length=1,
        repr=False,
    )

    @model_validator(mode="after")
    def require_unique_credentials(self) -> Self:
        identities = [
            (item.credential_type, item.source, item.key)
            for item in self.credentials
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("consumer credentials must be unique")
        return self


class CredentialSummary(_StrictModel):
    credential_type: Literal["key-auth"] = Field(alias="type")
    source: Literal["BEARER", "HEADER", "QUERY"]
    key: str | None = None
    value_count: int = Field(ge=0)


class ConsumerState(_ObservedModel):
    name: str = Field(pattern=_NAME_PATTERN)
    credentials: tuple[CredentialSummary, ...] = ()


class HigressClient:
    """Strict client for supported local Higress Console contracts."""

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

    async def list_providers(self) -> tuple[LLMProviderState, ...]:
        payload = await self._request("GET", "/v1/ai/providers")
        return tuple(
            _provider_state(item)
            for item in _gateway_items(payload, "provider")
        )

    async def get_provider(self, name: str) -> LLMProviderState:
        raw = await self._get_gateway_resource(
            "/v1/ai/providers",
            name,
        )
        return _provider_state(raw)

    async def upsert_provider(
        self,
        request: LLMProviderRequest,
    ) -> LLMProviderState:
        current = await self._get_optional_gateway_resource(
            "/v1/ai/providers",
            request.name,
        )
        body = _provider_body(request)
        method = "POST" if current is None else "PUT"
        path = (
            "/v1/ai/providers"
            if current is None
            else f"/v1/ai/providers/{request.name}"
        )
        if current is not None and current.get("version") is not None:
            body["version"] = str(current["version"])
        payload = await self._request(method, path, json_body=body)
        result = _gateway_data(payload)
        if result is None:
            return await self.get_provider(request.name)
        return _provider_state(result)

    async def delete_provider(self, name: str) -> None:
        safe_name = _validate_gateway_name(name)
        await self._request(
            "DELETE",
            f"/v1/ai/providers/{safe_name}",
        )

    async def list_routes(self) -> tuple[AIRouteState, ...]:
        payload = await self._request("GET", "/v1/ai/routes")
        return tuple(
            _route_state(item)
            for item in _gateway_items(payload, "AI route")
        )

    async def get_route(self, name: str) -> AIRouteState:
        raw = await self._get_gateway_resource(
            "/v1/ai/routes",
            name,
        )
        return _route_state(raw)

    async def upsert_route(
        self,
        request: AIRouteRequest,
    ) -> AIRouteState:
        current = await self._get_optional_gateway_resource(
            "/v1/ai/routes",
            request.name,
        )
        body = dict(current or {})
        body.update(_route_body(request))
        method = "POST" if current is None else "PUT"
        path = (
            "/v1/ai/routes"
            if current is None
            else f"/v1/ai/routes/{request.name}"
        )
        payload = await self._request(method, path, json_body=body)
        result = _gateway_data(payload)
        if result is None:
            return await self.get_route(request.name)
        return _route_state(result)

    async def delete_route(self, name: str) -> None:
        safe_name = _validate_gateway_name(name)
        await self._request(
            "DELETE",
            f"/v1/ai/routes/{safe_name}",
        )

    async def list_consumers(self) -> tuple[ConsumerState, ...]:
        payload = await self._request("GET", "/v1/consumers")
        return tuple(
            _consumer_state(item)
            for item in _gateway_items(payload, "consumer")
        )

    async def get_consumer(self, name: str) -> ConsumerState:
        raw = await self._get_gateway_resource(
            "/v1/consumers",
            name,
        )
        return _consumer_state(raw)

    async def upsert_consumer(
        self,
        request: ConsumerRequest,
    ) -> ConsumerState:
        current = await self._get_optional_gateway_resource(
            "/v1/consumers",
            request.name,
        )
        body = _consumer_body(request)
        method = "POST" if current is None else "PUT"
        path = (
            "/v1/consumers"
            if current is None
            else f"/v1/consumers/{request.name}"
        )
        payload = await self._request(method, path, json_body=body)
        result = _gateway_data(payload)
        if result is None:
            return await self.get_consumer(request.name)
        return _consumer_state(result)

    async def delete_consumer(self, name: str) -> None:
        safe_name = _validate_gateway_name(name)
        await self._request(
            "DELETE",
            f"/v1/consumers/{safe_name}",
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

    async def _get_gateway_resource(
        self,
        collection: str,
        name: str,
    ) -> dict[str, Any]:
        safe_name = _validate_gateway_name(name)
        payload = await self._request("GET", f"{collection}/{safe_name}")
        data = _gateway_data(payload)
        if data is None:
            raise HigressProtocolError(
                f"Higress GET {collection}/{safe_name} returned no resource",
            )
        return data

    async def _get_optional_gateway_resource(
        self,
        collection: str,
        name: str,
    ) -> dict[str, Any] | None:
        try:
            return await self._get_gateway_resource(collection, name)
        except HigressNotFoundError:
            return None

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
        read_retries = 1 if method == "GET" else 0
        refreshed_session = False
        while True:
            try:
                response = await self._send(
                    method,
                    path,
                    params=params,
                    json_body=json_body,
                )
            except HigressTransportError:
                if read_retries:
                    read_retries -= 1
                    continue
                raise
            if (
                response.status_code == 401
                and self._admin_password is not None
                and not refreshed_session
            ):
                refreshed_session = True
                self._session_cookie = None
                await self._ensure_session()
                continue
            if (
                response.status_code in {502, 503, 504}
                and read_retries
            ):
                read_retries -= 1
                continue
            break

        accepted = accepted_statuses or set()
        if response.status_code in accepted:
            return {}
        if response.status_code == 401:
            raise HigressUnauthorizedError(
                f"Higress {method} {path} returned HTTP 401",
            )
        if response.status_code == 403:
            raise HigressForbiddenError(
                f"Higress {method} {path} returned HTTP 403",
            )
        if response.status_code == 404:
            raise HigressNotFoundError(
                f"Higress {method} {path} returned HTTP 404",
            )
        if response.status_code == 409:
            raise HigressConflictError(
                f"Higress {method} {path} returned HTTP 409",
            )
        if response.status_code in {400, 422}:
            raise HigressRequestError(
                f"Higress {method} {path} returned HTTP "
                f"{response.status_code}",
            )
        if response.status_code in {408, 425, 429, 500, 502, 503, 504}:
            raise HigressTransportError(
                f"Higress {method} {path} returned retryable HTTP "
                f"{response.status_code}",
            )
        if not 200 <= response.status_code < 300:
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
            error_type = (
                HigressUnauthorizedError
                if response.status_code in {401, 403}
                else HigressProtocolError
            )
            raise error_type(
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


def _validate_gateway_name(name: str) -> str:
    if re.fullmatch(_NAME_PATTERN, name) is None:
        raise ValueError("invalid Higress gateway resource name")
    return name


def _validate_route_domain(value: str) -> str:
    candidate = value.removeprefix("*.")
    parsed = urlsplit(f"//{candidate}")
    if (
        not candidate
        or parsed.hostname != candidate
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
    ):
        raise ValueError("route domain must be a bare hostname or wildcard")
    return value


def _validate_public_json(value: object, *, key: str = "") -> None:
    lowered = key.casefold()
    if any(
        marker in lowered
        for marker in ("token", "secret", "password", "api_key", "apikey")
    ):
        raise ValueError(
            "provider raw_configs cannot contain credential-like keys",
        )
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("provider raw_configs must contain finite JSON")
        return
    if isinstance(value, dict):
        for child_key, child in value.items():
            if not isinstance(child_key, str) or not child_key:
                raise ValueError(
                    "provider raw_configs keys must be nonempty strings",
                )
            _validate_public_json(child, key=child_key)
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            _validate_public_json(child)
        return
    raise ValueError("provider raw_configs must contain JSON values only")


def _redact_public_json(value: object, *, key: str = "") -> object:
    lowered = key.casefold()
    if any(
        marker in lowered
        for marker in ("token", "secret", "password", "api_key", "apikey")
    ):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            str(child_key): _redact_public_json(
                child,
                key=str(child_key),
            )
            for child_key, child in value.items()
        }
    if isinstance(value, list):
        return [_redact_public_json(child) for child in value]
    return value


def _provider_body(request: LLMProviderRequest) -> dict[str, object]:
    body: dict[str, object] = {
        "name": request.name,
        "type": request.provider_type,
        "protocol": request.protocol,
        "tokens": [
            item.get_secret_value()
            for item in request.tokens
        ],
        "modelMapping": dict(request.model_mapping),
        "tokenFailoverConfig": request.token_failover.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "rawConfigs": request.raw_configs,
    }
    if request.proxy_name is not None:
        body["proxyName"] = request.proxy_name
    return body


def _route_body(request: AIRouteRequest) -> dict[str, object]:
    return request.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )


def _consumer_body(request: ConsumerRequest) -> dict[str, object]:
    return {
        "name": request.name,
        "credentials": [
            {
                "type": item.credential_type,
                "source": item.source,
                **({"key": item.key} if item.key is not None else {}),
                "values": [
                    value.get_secret_value()
                    for value in item.values
                ],
            }
            for item in request.credentials
        ],
    }


def _gateway_items(
    payload: object,
    label: str,
) -> list[dict[str, Any]]:
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        for key in ("items", "providers", "routes", "consumers"):
            if key in data:
                data = data[key]
                break
    if not isinstance(data, list):
        raise HigressProtocolError(
            f"Higress {label} list is not an array",
        )
    if not all(isinstance(item, dict) for item in data):
        raise HigressProtocolError(
            f"Higress {label} list contains invalid items",
        )
    return data


def _gateway_data(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    if "success" not in payload:
        return payload
    return None


def _provider_state(item: dict[str, Any]) -> LLMProviderState:
    tokens = item.get("tokens", ())
    if not isinstance(tokens, (list, tuple)):
        raise HigressProtocolError("Higress provider tokens are not an array")
    raw_configs = item.get("rawConfigs", {})
    if not isinstance(raw_configs, dict):
        raise HigressProtocolError(
            "Higress provider rawConfigs is not an object",
        )
    public = dict(item)
    public.pop("tokens", None)
    public["token_count"] = len(tokens)
    public["rawConfigs"] = _redact_public_json(raw_configs)
    try:
        return LLMProviderState.model_validate(public)
    except ValueError as exc:
        raise HigressProtocolError(
            "Higress returned an invalid provider",
        ) from exc


def _route_state(item: dict[str, Any]) -> AIRouteState:
    try:
        return AIRouteState.model_validate(item)
    except ValueError as exc:
        raise HigressProtocolError(
            "Higress returned an invalid AI route",
        ) from exc


def _consumer_state(item: dict[str, Any]) -> ConsumerState:
    raw_credentials = item.get("credentials", ())
    if not isinstance(raw_credentials, (list, tuple)):
        raise HigressProtocolError(
            "Higress consumer credentials are not an array",
        )
    summaries: list[CredentialSummary] = []
    try:
        for raw in raw_credentials:
            if not isinstance(raw, dict):
                raise TypeError("credential is not an object")
            values = raw.get("values", ())
            if not isinstance(values, (list, tuple)):
                raise TypeError("credential values are not an array")
            summaries.append(
                CredentialSummary.model_validate(
                    {
                        "type": raw.get("type"),
                        "source": raw.get("source"),
                        "key": raw.get("key"),
                        "value_count": len(values),
                    },
                ),
            )
        return ConsumerState.model_validate(
            {
                "name": item.get("name"),
                "credentials": tuple(summaries),
            },
        )
    except (TypeError, ValueError) as exc:
        raise HigressProtocolError(
            "Higress returned an invalid consumer",
        ) from exc


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
