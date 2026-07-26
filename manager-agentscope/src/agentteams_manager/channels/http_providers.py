"""Signed JSON webhook adapters using the existing httpx dependency."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from typing import Any

import httpx

from .base import ChannelMessage, Provider


class HttpChannelAdapter:
    """Normalize supported providers without retaining credentials."""

    def __init__(
        self,
        *,
        provider: Provider,
        outbound_url: str,
        token: str,
        webhook_secret: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not token or not webhook_secret:
            raise ValueError(f"{provider} credentials must not be empty")
        self.provider = provider
        self._outbound_url = outbound_url
        self._token = token
        self._secret = webhook_secret.encode()
        self._client = client or httpx.AsyncClient(timeout=15)
        self._owns_client = client is None

    def verify_and_parse(
        self,
        headers: Mapping[str, str],
        body: bytes,
    ) -> ChannelMessage:
        supplied = headers.get("x-agentteams-signature", "")
        supplied = supplied.removeprefix("sha256=")
        expected = hmac.new(
            self._secret,
            body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise PermissionError("invalid webhook signature")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid webhook JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("webhook payload must be an object")
        values = _normalize(self.provider, payload)
        if not all(values):
            raise ValueError("webhook payload is missing identity or text")
        return ChannelMessage(
            provider=self.provider,
            external_user_id=str(values[0]),
            display_name=str(values[1]),
            destination_id=str(values[2]),
            message_id=str(values[3]),
            text=str(values[4]),
        )

    async def send(self, destination_id: str, text: str) -> str:
        headers = {"Authorization": f"Bearer {self._token}"}
        payload = _outbound_payload(self.provider, destination_id, text)
        response = await self._client.post(
            self._outbound_url,
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        try:
            result = response.json()
        except ValueError:
            result = {}
        if isinstance(result, dict):
            return str(
                result.get("id")
                or result.get("message_id")
                or result.get("ts")
                or "sent"
            )
        return "sent"

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def _normalize(
    provider: Provider,
    payload: dict[str, Any],
) -> tuple[object, object, object, object, object]:
    if provider == "discord":
        author = _dict(payload.get("author"))
        return (
            author.get("id"),
            author.get("username") or author.get("global_name"),
            payload.get("channel_id"),
            payload.get("id"),
            payload.get("content"),
        )
    if provider == "telegram":
        message = _dict(payload.get("message"))
        sender = _dict(message.get("from"))
        chat = _dict(message.get("chat"))
        return (
            sender.get("id"),
            sender.get("username") or sender.get("first_name"),
            chat.get("id"),
            message.get("message_id"),
            message.get("text"),
        )
    if provider == "slack":
        event = _dict(payload.get("event"))
        return (
            event.get("user"),
            event.get("user") or "Slack user",
            event.get("channel"),
            event.get("client_msg_id") or event.get("ts"),
            event.get("text"),
        )
    if provider == "feishu":
        event = _dict(payload.get("event"))
        sender = _dict(_dict(event.get("sender")).get("sender_id"))
        message = _dict(event.get("message"))
        content = message.get("content")
        try:
            text = _dict(json.loads(content)).get("text")
        except (TypeError, json.JSONDecodeError):
            text = content
        return (
            sender.get("open_id") or sender.get("user_id"),
            sender.get("open_id") or "Feishu user",
            message.get("chat_id"),
            message.get("message_id"),
            text,
        )
    if provider == "whatsapp":
        value = _dict(
            _first(_dict(_first(payload.get("entry"))).get("changes")).get(
                "value",
            ),
        )
        message = _dict(_first(value.get("messages")))
        contact = _dict(_first(value.get("contacts")))
        return (
            message.get("from"),
            _dict(contact.get("profile")).get("name") or message.get("from"),
            message.get("from"),
            message.get("id"),
            _dict(message.get("text")).get("body"),
        )
    envelope = _dict(payload.get("envelope"))
    data = _dict(envelope.get("dataMessage"))
    return (
        envelope.get("sourceUuid") or envelope.get("sourceNumber"),
        envelope.get("sourceName") or envelope.get("sourceNumber"),
        envelope.get("sourceNumber") or envelope.get("sourceUuid"),
        envelope.get("timestamp"),
        data.get("message"),
    )


def _outbound_payload(
    provider: Provider,
    destination_id: str,
    text: str,
) -> dict[str, object]:
    if provider == "telegram":
        return {"chat_id": destination_id, "text": text}
    if provider == "slack":
        return {"channel": destination_id, "text": text}
    if provider == "feishu":
        return {
            "receive_id": destination_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
    if provider == "whatsapp":
        return {
            "to": destination_id,
            "type": "text",
            "text": {"body": text},
        }
    if provider == "signal":
        return {"recipient": [destination_id], "message": text}
    return {"content": text}


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first(value: object) -> object:
    return value[0] if isinstance(value, list) and value else {}
