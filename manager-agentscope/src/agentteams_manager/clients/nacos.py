"""Typed, read-only Nacos AgentSpec discovery.

从 Nacos 只读发现可导入的 AgentSpec，并做严格结构校验。

发现结果只是候选 Worker 定义，不会自动创建资源。本模块从不同 Nacos 响应形态中提取
并排序候选，拒绝无效或含糊的数据，再把类型化结果交给资源导入 workflow。真正导入
仍需要权限、确认和 Controller 写操作，避免外部注册中心直接控制集群。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Literal, cast
from urllib.parse import quote, unquote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

NacosRuntime = Literal[
    "openclaw",
    "copaw",
    "hermes",
    "qwenpaw",
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
        # 逻辑说明：只把协议定义字段和非空 metadata 纳入稳定摘要输入，忽略服务端额外字段。
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
        # 逻辑说明：按 Controller 的字段名组装规范摘要对象，并递归规范化每个资源条目。
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
        # 逻辑说明：解析并严格限定 nacos URI/单一 namespace，分离凭据后构造不含凭据的 HTTP 与包基址。
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
        # 逻辑说明：验证结果/分页上限，按显式参数→URI→环境读取凭据，并记录 HTTP client 所有权。
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
        # 逻辑说明：从环境读取注册中心 URI（无配置时用公开市场），其余校验和凭据解析仍走正式构造器。
        return cls(
            registry_uri=os.getenv(
                "AGENTTEAMS_NACOS_REGISTRY_URI",
                "nacos://market.agentteams.io:80/public",
            ),
            http_client=http_client,
            max_results=max_results,
        )

    async def close(self) -> None:
        # 逻辑说明：仅关闭本实例创建的 HTTP client，调用方或测试注入的共享 client 不在此释放。
        if self._owns_http:
            await self._http.aclose()

    async def search_workers(
        self,
        query: str,
    ) -> tuple[NacosWorker, ...]:
        # 逻辑说明：规范化查询、拉取在线摘要、确定性评分截取，再逐项获取完整 spec 并生成摘要绑定候选。
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
        # 逻辑说明：确认候选 URI 仍属于配置 registry，按原 selector 重取 spec，并比较确认时的 digest。
        expected_base = (
            f"{self._registry.package_base}/"
            f"{quote(candidate.name, safe='')}"
        )
        if not candidate.package_uri.startswith(expected_base):
            raise NacosIntegrityError(
                "confirmed package URI is outside the configured registry",
            )
        selector = candidate.version
        label = (
            selector.removeprefix("label:")
            if selector.startswith("label:")
            else None
        )
        version = (
            None
            if selector == "latest" or label is not None
            else selector
        )
        current = await self._fetch_spec(
            candidate.name,
            version=version,
            label=label,
        )
        digest = _digest(current.digest_value())
        if digest != candidate.digest:
            raise NacosIntegrityError(
                f"AgentSpec {candidate.name!r} changed after confirmation",
            )

    async def inspect_worker_uri(self, package_uri: str) -> NacosWorker:
        """Resolve one explicit URI without performing a market search."""
        # 逻辑说明：严格检查 authority/namespace/路径，按版本或 label 获取 spec，再生成同搜索路径一致的候选。
        parsed = urlsplit(package_uri)
        if parsed.scheme != "nacos" or not parsed.hostname:
            raise ValueError("Worker package must use a valid nacos:// URI")
        host = parsed.hostname
        if ":" in host:
            host = f"[{host}]"
        authority = (
            f"{host}:{parsed.port}" if parsed.port is not None else host
        )
        if authority != self._registry.authority:
            raise NacosIntegrityError(
                "direct package URI is outside the configured registry",
            )
        parts = [unquote(item) for item in parsed.path.split("/") if item]
        if len(parts) not in {2, 3}:
            raise ValueError(
                "Nacos package URI must contain namespace, name, "
                "and optional version",
            )
        namespace, name = parts[:2]
        if namespace != self._registry.namespace:
            raise NacosIntegrityError(
                "direct package URI is outside the configured namespace",
            )
        selector = parts[2] if len(parts) == 3 else ""
        label = (
            selector.removeprefix("label:")
            if selector.startswith("label:")
            else None
        )
        version = None if label else selector or None
        spec = await self._fetch_spec(
            name,
            version=version,
            label=label,
        )
        summary = _Summary(
            namespaceId=namespace,
            name=name,
            description=spec.description,
            enable=True,
            onlineCnt=1,
            labels={},
        )
        return _candidate(
            self._registry,
            summary,
            spec,
            version=selector or _manifest_version(spec.content),
        )

    async def _fetch_spec(
        self,
        name: str,
        *,
        version: str | None,
        label: str | None = None,
    ) -> _AgentSpec:
        # 逻辑说明：只为实际提供的 version/label 追加参数，调用统一认证请求并校验 v3 envelope 与 AgentSpec。
        params = {
            "namespaceId": self._registry.namespace,
            "name": name,
        }
        if version:
            params["version"] = version
        if label:
            params["label"] = label
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
        # 逻辑说明：需要时先登录，再带 bearer token 请求；HTTP/状态/JSON 错误分类且所有诊断先脱敏。
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
        # 逻辑说明：依次尝试 Nacos v3/v1 登录接口，只有解析到非空 token 才更新会话；错误文本不含凭据。
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
        # 逻辑说明：先校验统一 v3 envelope 和成功 code，再用目标 Pydantic 模型验证 data 的具体形状。
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
    # 逻辑说明：按精确名、子串和全部 token 匹配累积分数，供稳定排序；零分候选不会进入网络详情查询。
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
    # 逻辑说明：解析 manifest、验证 runtime、合并展示字段并构造版本 URI，最终绑定整个 spec 的规范摘要。
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
    runtime = str(
        worker.get("runtime", manifest.get("runtime", "openclaw")),
    )
    if runtime not in {"openclaw", "copaw", "hermes", "qwenpaw"}:
        raise NacosProtocolError(
            f"AgentSpec {summary.name!r} has unsupported runtime {runtime!r}",
        )
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
            runtime=cast(NacosRuntime, runtime),
            package_uri=package_uri,
            version=resolved_version,
            digest=_digest(spec.digest_value()),
        )
    except ValidationError as exc:
        raise NacosProtocolError(
            f"AgentSpec {summary.name!r} is not a valid Worker template",
        ) from exc


def _manifest_version(content: str) -> str:
    # 逻辑说明：从可解析对象中读取非空 version；任何旧版/畸形内容安全回退 latest，不在此执行导入。
    try:
        manifest = json.loads(content)
    except json.JSONDecodeError:
        return "latest"
    if not isinstance(manifest, dict):
        return "latest"
    value = manifest.get("version", "latest")
    return str(value).strip() or "latest"


def _digest(value: dict[str, Any]) -> str:
    # 逻辑说明：按确定键序列化并复刻 Go JSON 的 HTML 转义，再计算 SHA-256，确保 Python/Controller 摘要一致。
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
    # 逻辑说明：先遮蔽 Nacos URI userinfo，再替换所有已知凭据并截断，网络异常可诊断但不可泄密。
    safe = re.sub(
        r"(?i)(nacos://)[^/@\s]+:[^/@\s]+@",
        r"\1[REDACTED]@",
        message,
    )
    for secret in secrets:
        safe = safe.replace(secret, "[REDACTED]")
    return safe[:1000]
