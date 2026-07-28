"""Provider-neutral external channel contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

Provider = Literal[
    "discord",
    "telegram",
    "slack",
    "feishu",
    "whatsapp",
    "signal",
    "dingtalk",
]
PROVIDERS: tuple[Provider, ...] = (
    "discord",
    "telegram",
    "slack",
    "feishu",
    "whatsapp",
    "signal",
    "dingtalk",
)


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ChannelMessage(_Frozen):
    provider: Provider
    external_user_id: str
    display_name: str
    destination_id: str
    message_id: str
    text: str


class ChannelContact(_Frozen):
    provider: Provider
    external_user_id: str
    display_name: str
    destination_id: str
    status: Literal["pending", "trusted", "blocked"]
    is_primary: bool = False


class ChannelReceipt(_Frozen):
    provider: Provider
    external_user_id: str
    status: Literal[
        "pending_approval",
        "accepted",
        "blocked",
        "duplicate",
    ]


@dataclass(frozen=True, slots=True)
class ChannelWebhookRequest:
    """Raw HTTP request data needed by provider-native verification."""

    method: str
    headers: Mapping[str, str]
    query: Mapping[str, str]
    body: bytes


@dataclass(frozen=True, slots=True)
class ChannelWebhookResponse:
    """Provider-specific HTTP acknowledgement and optional message."""

    status_code: int = 200
    body: bytes = b""
    content_type: str = "application/json"
    message: ChannelMessage | None = None
    response_headers: Mapping[str, str] = field(default_factory=dict)


class ChannelAdapter(Protocol):
    provider: Provider

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse: ...

    async def send(self, destination_id: str, text: str) -> str: ...

    async def close(self) -> None: ...
