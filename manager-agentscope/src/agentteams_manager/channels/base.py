"""Provider-neutral external channel contracts.

定义 Telegram、Slack 等外部聊天渠道共同遵守的数据边界。

每个平台的 webhook 格式不同，本模块先把它们统一成 ``ChannelMessage``、联系人和
发送回执等中立模型。后续信任判断与 Matrix 升级流程只依赖这些模型，因此不需要在
每条业务链路中分别理解六种平台协议；平台签名校验仍由各自 adapter 在进入此边界前
完成。
"""

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
