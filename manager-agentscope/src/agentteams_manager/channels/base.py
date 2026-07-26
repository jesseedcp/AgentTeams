"""Provider-neutral external channel contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

Provider = Literal[
    "discord",
    "telegram",
    "slack",
    "feishu",
    "whatsapp",
    "signal",
]
PROVIDERS: tuple[Provider, ...] = (
    "discord",
    "telegram",
    "slack",
    "feishu",
    "whatsapp",
    "signal",
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
    status: Literal["pending_approval", "accepted", "blocked"]


class ChannelAdapter(Protocol):
    provider: Provider

    def verify_and_parse(
        self,
        headers: Mapping[str, str],
        body: bytes,
    ) -> ChannelMessage: ...

    async def send(self, destination_id: str, text: str) -> str: ...

    async def close(self) -> None: ...
