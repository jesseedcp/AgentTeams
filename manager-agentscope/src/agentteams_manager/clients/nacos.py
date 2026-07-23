"""Typed, read-only Nacos AgentSpec discovery."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Literal
from urllib.parse import quote, unquote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

NacosRuntime = Literal[
    "openclaw",
    "copaw",
    "hermes",
    "qwenpaw",
    "openhuman",
]


class NacosError(RuntimeError):
    """Base failure for registry discovery."""


class NacosProtocolError(NacosError):
    """The registry returned data outside the Nacos AgentSpec contract."""


class NacosIntegrityError(NacosError):
    """A confirmed AgentSpec no longer matches its discovery digest."""


class NacosWorker(BaseModel):
    """A safe, immutable Worker candidate exposed to AgentScope."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    display_name: str = Field(min_length=1)
    description: str
    runtime: NacosRuntime
    package_uri: str = Field(pattern=r"^nacos://")
    version: str = Field(min_length=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class _Summary(BaseModel):
    model_config = ConfigDict(extra="ignore")

    namespace_id: str = Field(alias="namespaceId")
    name: str = Field(min_length=1)
    description: str = ""
    enable: bool
    online_count: int = Field(alias="onlineCnt", ge=0)
    labels: dict[str, str] = Field(default_factory=dict)


class _ListData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    total_count: int = Field(alias="totalCount", ge=0)
    page_items: tuple[_Summary, ...] = Field(alias="pageItems")


class _AgentSpecResource(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    type: str = ""
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    def digest_value(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "content": self.content,
        }
        if self.metadata:
            value["metadata"] = self.metadata
        return value


class _AgentSpec(BaseModel):
    model_config = ConfigDict(extra="ignore")

    namespace_id: str = Field(alias="namespaceId")
    name: str
    description: str = ""
    biz_tags: str = Field(default="", alias="bizTags")
    content: str
    resource: dict[str, _AgentSpecResource] = Field(default_factory=dict)

    def digest_value(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "namespaceId": self.namespace_id,
            "name": self.name,
            "description": self.description,
            "content": self.content,
        }
        if self.biz_tags:
            value["bizTags"] = self.biz_tags
        if self.resource:
            value["resource"] = {
                name: resource.digest_value()
                for name, resource in self.resource.items()
            }
        return value


class _V3Envelope(BaseModel):
    model_config = ConfigDict(extra="ignore")

    code: int
    message: str = ""
    data: Any


class _Registry:
    def __init__(self, raw_uri: str) -> None:
        parsed = urlsplit(raw_uri)
        if parsed.scheme != "nacos" or not parsed.hostname:
            raise ValueError(
                "Nacos registry must use "
                "nacos://[user:pass@]host[:port]/namespace",
            )
        path_parts = [unquote(item) for item in parsed.path.split("/") if item]
        if len(path_parts) != 1:
            raise ValueError(
                "Nacos registry URI must contain exactly one namespace",
            )
        self.namespace = path_parts[0]
        self.username = (
            unquote(parsed.username) if parsed.username is not None else ""
        )
        self.password = (
            unquote(parsed.password) if parsed.password is not None else ""
        )
        host = parsed.hostname
        if ":" in host:
            host = f"[{host}]"
        self.authority = (
            f"{host}:{parsed.port}" if parsed.port is not None else host
        )
        self.http_base = f"http://{self.authority}"
        self.package_base = (
            f"nacos://{self.authority}/{quote(self.namespace, safe='')}"
        )


class NacosClient:
    """Search AgentSpecs without exposing registry credentials to tools."""

    def __init__(
        self,
        *,
        registry_uri: str,
        http_client: httpx.AsyncClient | None = None,
        username: str | None = None,
        password: str | None = None,
        access_token: str | None = None,
        max_results: int = 3,
        page_size: int = 100,
    ) -> None:
        if not 1 <= max_results <= 20:
            raise ValueError("max_results must be between 1 and 20")
        if not 1 <= page_size <= 100:
            raise ValueError("page_size must be between 1 and 100")
        self._registry = _Registry(registry_uri)
        self._username = (
            username
            if username is not None
            else self._registry.username
            or os.getenv("AGENTTEAMS_NACOS_USERNAME", "")
        )
        self._password = (
            password
            if password is not None
            else self._registry.password
            or os.getenv("AGENTTEAMS_NACOS_PASSWORD", "")
        )
        if bool(self._username) != bool(self._password):
            raise ValueError(
                "both Nacos username and password must be configured",
            )
        self._access_token = (
            access_token
            if access_token is not None
            else os.getenv("AGENTTEAMS_NACOS_ACCESS_TOKEN", "")
        )
        self._http = http_client or httpx.AsyncClient(timeout=30)
        self._owns_http = http_client is None
        self._max_results = max_results
        self._page_size = page_size

    @classmethod
    def from_environment(
        cls,
        *,
        http_client: httpx.AsyncClient | None = None,
        max_results: int = 3,
    ) -> NacosClient:
        return cls(
            registry_uri=os.getenv(
                "AGENTTEAMS_NACOS_REGISTRY_URI",
                "nacos://market.agentteams.io:80/public",
            ),
            http_client=http_client,
            max_results=max_results,
        )

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def search_workers(
        self,
        query: str,
    ) -> tuple[NacosWorker, ...]:
        normalized_query = " ".join(query.split()).casefold()
        if not normalized_query:
            raise ValueError("Worker search query cannot be empty")
        if len(normalized_query) > 300:
            raise ValueError("Worker search query is too long")

        raw = await self._request_json(
            "GET",
            "/nacos/v3/admin/ai/agentspecs/list",
            params={
                "namespaceId": self._registry.namespace,
                "pageNo": "1",
                "pageSize": str(self._page_size),
            },
        )
        listed = self._parse_envelope(_ListData, raw)
        ranked = sorted(
            (
                (_score(summary, normalized_query), summary)
                for summary in listed.page_items
                if summary.enable and summary.online_count > 0
            ),
            key=lambda item: (-item[0], item[1].name),
        )
        selected = [
            summary
            for score, summary in ranked
            if score > 0
        ][: self._max_results]

        candidates: list[NacosWorker] = []
        for summary in selected:
            version = summary.labels.get("latest", "").strip()
            spec = await self._fetch_spec(
                summary.name,
                version=version or None,
            )
            candidates.append(
                _candidate(
                    self._registry,
                    summary,
                    spec,
                    version=version or _manifest_version(spec.content),
                ),
            )
        return tuple(candidates)

    async def verify_worker(self, candidate: NacosWorker) -> None:
        expected_base = (
            f"{self._registry.package_base}/"
            f"{quote(candidate.name, safe='')}"
        )
        if not candidate.package_uri.startswith(expected_base):
            raise NacosIntegrityError(
                "confirmed package URI is outside the configured registry",
            )
        version = None if candidate.version == "latest" else candidate.version
        current = await self._fetch_spec(candidate.name, version=version)
        digest = _digest(current.digest_value())
        if digest != candidate.digest:
            raise NacosIntegrityError(
                f"AgentSpec {candidate.name!r} changed after confirmation",
            )

    async def _fetch_spec(
        self,
        name: str,
        *,
        version: str | None,
    ) -> _AgentSpec:
        params = {
            "namespaceId": self._registry.namespace,
            "name": name,
        }
        if version:
            params["version"] = version
        raw = await self._request_json(
            "GET",
            "/nacos/v3/client/ai/agentspecs",
            params=params,
        )
        return self._parse_envelope(_AgentSpec, raw)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
    ) -> object:
        if (
            not self._access_token
            and self._username
            and self._password
        ):
            await self._login()
        headers = (
            {"Authorization": f"Bearer {self._access_token}"}
            if self._access_token
            else {}
        )
        try:
            response = await self._http.request(
                method,
                self._registry.http_base + path,
                params=params,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise NacosError(
                _redact(f"Nacos request failed: {exc}", self._secrets),
            ) from exc
        if response.status_code < 200 or response.status_code >= 300:
            detail = response.text.strip()[:500]
            raise NacosError(
                _redact(
                    f"Nacos request failed (HTTP {response.status_code}): "
                    f"{detail or 'no diagnostic output'}",
                    self._secrets,
                ),
            )
        try:
            return response.json()
        except ValueError as exc:
            raise NacosProtocolError(
                "Nacos registry response is not valid JSON",
            ) from exc

    async def _login(self) -> None:
        form = {
            "username": self._username,
            "password": self._password,
        }
        for path in (
            "/nacos/v3/auth/user/login",
            "/nacos/v1/auth/login",
        ):
            try:
                response = await self._http.post(
                    self._registry.http_base + path,
                    data=form,
                )
            except httpx.HTTPError as exc:
                raise NacosError(
                    _redact(
                        f"Nacos login failed: {exc}",
                        self._secrets,
                    ),
                ) from exc
            if response.status_code != 200:
                continue
            try:
                payload = response.json()
            except ValueError:
                continue
            data = payload.get("data", payload)
            if isinstance(data, dict):
                token = data.get("accessToken")
                if isinstance(token, str) and token:
                    self._access_token = token
                    return
        raise NacosError("Nacos login failed")

    @property
    def _secrets(self) -> tuple[str, ...]:
        return tuple(
            secret
            for secret in (
                self._username,
                self._password,
                self._access_token,
            )
            if secret
        )

    @staticmethod
    def _parse_envelope(
        model: type[BaseModel],
        raw: object,
    ) -> Any:
        try:
            envelope = _V3Envelope.model_validate(raw)
        except ValidationError as exc:
            raise NacosProtocolError(
                "Nacos registry response does not match the v3 envelope",
            ) from exc
        if envelope.code != 0:
            raise NacosProtocolError(
                f"Nacos registry response failed with code {envelope.code}",
            )
        try:
            return model.model_validate(envelope.data)
        except ValidationError as exc:
            raise NacosProtocolError(
                "Nacos registry response data has an invalid shape",
            ) from exc


def _score(summary: _Summary, query: str) -> int:
    name = summary.name.casefold()
    description = summary.description.casefold()
    if name == query:
        return 1000
    score = 0
    if query in name:
        score += 700
    elif query in description:
        score += 500
    tokens = tuple(
        token
        for token in re.split(r"\s+", query)
        if token
    )
    matches = 0
    for token in tokens:
        if token in name:
            score += 100
            matches += 1
        elif token in description:
            score += 50
            matches += 1
    if len(tokens) > 1 and matches == len(tokens):
        score += 120
    return score


def _candidate(
    registry: _Registry,
    summary: _Summary,
    spec: _AgentSpec,
    *,
    version: str,
) -> NacosWorker:
    try:
        manifest = json.loads(spec.content)
    except json.JSONDecodeError as exc:
        raise NacosProtocolError(
            f"AgentSpec {summary.name!r} has an invalid manifest",
        ) from exc
    if not isinstance(manifest, dict):
        raise NacosProtocolError(
            f"AgentSpec {summary.name!r} manifest must be an object",
        )
    worker = manifest.get("worker", {})
    if not isinstance(worker, dict):
        worker = {}
    runtime = worker.get("runtime", manifest.get("runtime", "openclaw"))
    display_name = worker.get(
        "displayName",
        manifest.get("displayName", summary.name),
    )
    description = worker.get(
        "description",
        manifest.get("description", summary.description),
    )
    package_uri = (
        f"{registry.package_base}/{quote(summary.name, safe='')}"
    )
    resolved_version = version or "latest"
    if resolved_version != "latest":
        package_uri += f"/{quote(resolved_version, safe='')}"
    try:
        return NacosWorker(
            name=summary.name,
            display_name=str(display_name),
            description=str(description),
            runtime=runtime,
            package_uri=package_uri,
            version=resolved_version,
            digest=_digest(spec.digest_value()),
        )
    except ValidationError as exc:
        raise NacosProtocolError(
            f"AgentSpec {summary.name!r} is not a valid Worker template",
        ) from exc


def _manifest_version(content: str) -> str:
    try:
        manifest = json.loads(content)
    except json.JSONDecodeError:
        return "latest"
    if not isinstance(manifest, dict):
        return "latest"
    value = manifest.get("version", "latest")
    return str(value).strip() or "latest"


def _digest(value: dict[str, Any]) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise NacosProtocolError(
            "AgentSpec contains non-canonical metadata",
        ) from exc
    # Match Go encoding/json's HTML-safe escaping used by the Controller.
    encoded = (
        encoded.replace(b"&", b"\\u0026")
        .replace(b"<", b"\\u003c")
        .replace(b">", b"\\u003e")
        .replace("\u2028".encode(), b"\\u2028")
        .replace("\u2029".encode(), b"\\u2029")
    )
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _redact(message: str, secrets: tuple[str, ...]) -> str:
    safe = re.sub(
        r"(?i)(nacos://)[^/@\s]+:[^/@\s]+@",
        r"\1[REDACTED]@",
        message,
    )
    for secret in secrets:
        safe = safe.replace(secret, "[REDACTED]")
    return safe[:1000]
