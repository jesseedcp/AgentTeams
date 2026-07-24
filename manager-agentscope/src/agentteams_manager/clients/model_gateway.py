"""Secret-safe OpenAI-compatible model route preflight."""

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
        if timeout <= 0:
            raise ValueError("model preflight timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._client = client or httpx.AsyncClient()
        self._owns_client = client is None
        self._timeout = timeout
        self._known_models = dict(known_models or {})

    async def preflight(self, spec: ModelSpec) -> ModelCapabilities:
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
        if self._owns_client:
            await self._client.aclose()
