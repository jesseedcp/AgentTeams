"""Secret-safe OpenAI-compatible model route preflight.

在切换模型前探测 OpenAI-compatible gateway 是否真的可用。

配置中出现一个模型名称并不代表它能完成对话或 tool call。本模块使用受控的小请求验证
路由、认证和声明能力，并只返回模型能力摘要；API key 保持为 Secret，不进入日志或
回执。探测通过后，integration workflow 才发布新的 runtime revision。
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr


class ModelNotReachable(RuntimeError):
    """The requested route did not pass a live gateway preflight."""


class ModelSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    model: str = Field(min_length=1, max_length=500)
    context_window: int | None = Field(default=None, gt=0)
    max_tokens: int | None = Field(default=None, gt=0)
    reasoning: bool | None = None
    input_modalities: tuple[str, ...] | None = None


class ModelCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    model: str
    context_window: int = Field(gt=0)
    max_tokens: int = Field(gt=0)
    reasoning: bool
    input_modalities: tuple[str, ...] = ("text",)


class ModelGatewayClient:
    """Probe one model without exposing gateway credentials or body text."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: SecretStr,
        client: httpx.AsyncClient | None = None,
        timeout: float = 15,
        known_models: Mapping[str, ModelCapabilities] | None = None,
    ) -> None:
        # 逻辑说明：校验探测超时并保存网关凭据；记录 client 所有权，关闭时不误释放调用方注入资源。
        if timeout <= 0:
            raise ValueError("model preflight timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._client = client or httpx.AsyncClient()
        self._owns_client = client is None
        self._timeout = timeout
        self._known_models = dict(known_models or {})

    async def preflight(self, spec: ModelSpec) -> ModelCapabilities:
        # 逻辑说明：用最小 completion 验证路由/认证，再合并显式与已知能力；不返回响应正文。
        routed_model = spec.model.removeprefix("agentteams-gateway/")
        if not routed_model:
            raise ValueError("model route must not be empty")
        try:
            response = await self._client.post(
                f"{self._base_url}/v1/chat/completions",
                headers={
                    "Authorization": (
                        "Bearer " + self._api_key.get_secret_value()
                    ),
                    "X-AgentTeams-Trace-ID": uuid.uuid4().hex,
                },
                json={
                    "model": routed_model,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Reply with OK.",
                        },
                    ],
                    "max_tokens": 8,
                    "stream": False,
                },
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            raise ModelNotReachable(
                "model preflight transport failed; verify the provider "
                "and its AgentTeams gateway prefix route "
                f"({type(exc).__name__})",
            ) from exc
        if not 200 <= response.status_code < 300:
            raise ModelNotReachable(
                f"model preflight returned HTTP {response.status_code}; "
                "create or repair the provider and its AgentTeams gateway "
                "prefix route instead of editing the managed default",
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ModelNotReachable(
                "model preflight returned invalid JSON",
            ) from exc
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("choices"), list)
            or not payload["choices"]
        ):
            raise ModelNotReachable(
                "model preflight returned no completion choice",
            )
        known = self._known_models.get(
            spec.model,
            self._known_models.get(routed_model),
        )
        return ModelCapabilities(
            model=spec.model,
            context_window=(
                spec.context_window
                or (known.context_window if known else 150_000)
            ),
            max_tokens=(
                spec.max_tokens
                or (known.max_tokens if known else 128_000)
            ),
            reasoning=(
                spec.reasoning
                if spec.reasoning is not None
                else known.reasoning if known else True
            ),
            input_modalities=(
                spec.input_modalities
                or (
                    known.input_modalities
                    if known is not None
                    else ("text",)
                )
            ),
        )

    async def close(self) -> None:
        # 逻辑说明：只关闭本实例创建的连接池，调用方注入 client 的生命周期仍归调用方。
        if self._owns_client:
            await self._client.aclose()
