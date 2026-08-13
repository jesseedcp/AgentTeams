"""Provider-native webhook and outbound HTTP adapters.

解析外部聊天平台的原生 webhook，并按平台协议发送回复。

以 Telegram 消息为例，adapter 先验证其请求、提取稳定的联系人和消息 ID，再生成
provider-neutral ``ChannelMessage`` 交给 ChannelService；出站时再把统一回复翻译成
Telegram 所需的 HTTP 请求。平台返回超时不等于发送失败，因此上层仍需依赖稳定 ID
与持久化状态处理可能已经发生的外部效果。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import (
    parse_qsl,
    quote,
    urlencode,
    urlsplit,
    urlunsplit,
)

import httpx
from Crypto.Cipher import AES
from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa
from Crypto.Util.Padding import unpad

from .base import (
    ChannelAdapter,
    ChannelMessage,
    ChannelWebhookRequest,
    ChannelWebhookResponse,
    Provider,
)

ChannelMode = Literal["native", "relay"]


class _HttpChannelAdapter:
    provider: Provider

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not outbound_url:
            raise ValueError("channel outbound URL must not be empty")
        if not token:
            raise ValueError("channel token must not be empty")
        self._outbound_url = outbound_url
        self._token = token
        self._client = client
        self._owns_client = client is None

    def _replace_client_for_test(self, client: httpx.AsyncClient) -> None:
        """Inject a mock transport before the lazily-created client exists."""
        if self._client is not None and self._owns_client:
            raise RuntimeError("owned HTTP client is already initialized")
        self._client = client
        self._owns_client = False

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15)
        return self._client

    async def _post(
        self,
        *,
        url: str,
        headers: Mapping[str, str] | None,
        payload: Mapping[str, object],
    ) -> str:
        response = await self._http_client().post(
            url,
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        return _outbound_message_id(response)

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
        self._client = None


class RelayChannelAdapter(_HttpChannelAdapter):
    """Backward-compatible AgentTeams HMAC relay.

    This is intentionally separate from every native provider so a custom
    ``x-agentteams-signature`` can never be mistaken for platform security.
    """

    def __init__(
        self,
        *,
        provider: Provider,
        outbound_url: str,
        token: str,
        webhook_secret: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        if not webhook_secret:
            raise ValueError("relay webhook secret must not be empty")
        self.provider = provider
        self._webhook_secret = webhook_secret.encode()

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        if request.method.upper() != "POST":
            raise ValueError("relay webhook requires POST")
        supplied = _header(
            request.headers,
            "x-agentteams-signature",
        ).removeprefix("sha256=")
        expected = hmac.new(
            self._webhook_secret,
            request.body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise PermissionError("invalid relay webhook signature")
        payload = _json_object(request.body)
        values = _normalize_relay(self.provider, payload)
        return ChannelWebhookResponse(
            message=_channel_message(self.provider, values),
        )

    def verify_and_parse(
        self,
        headers: Mapping[str, str],
        body: bytes,
    ) -> ChannelMessage:
        """Compatibility shim for pre-v2 in-process relay integrations."""
        response = self.handle_webhook(
            ChannelWebhookRequest(
                method="POST",
                headers=headers,
                query={},
                body=body,
            ),
        )
        if response.message is None:
            raise ValueError("relay payload did not contain a message")
        return response.message

    async def send(self, destination_id: str, text: str) -> str:
        return await self._post(
            url=self._outbound_url,
            headers={"Authorization": f"Bearer {self._token}"},
            payload=_relay_outbound_payload(
                self.provider,
                destination_id,
                text,
            ),
        )


class TelegramChannelAdapter(_HttpChannelAdapter):
    provider: Provider = "telegram"

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        webhook_secret: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        if not webhook_secret:
            raise ValueError("Telegram webhook secret must not be empty")
        self._webhook_secret = webhook_secret

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        _require_post(request, self.provider)
        supplied = _header(
            request.headers,
            "x-telegram-bot-api-secret-token",
        )
        if not hmac.compare_digest(supplied, self._webhook_secret):
            raise PermissionError("invalid Telegram webhook secret")
        payload = _json_object(request.body)
        message = _dict(payload.get("message"))
        if not message:
            return ChannelWebhookResponse()
        sender = _dict(message.get("from"))
        chat = _dict(message.get("chat"))
        update_id = payload.get("update_id")
        message_id = (
            update_id
            if update_id is not None
            else f"{chat.get('id')}:{message.get('message_id')}"
        )
        return ChannelWebhookResponse(
            message=_channel_message(
                self.provider,
                (
                    sender.get("id"),
                    sender.get("username")
                    or sender.get("first_name")
                    or sender.get("id"),
                    chat.get("id"),
                    message_id,
                    message.get("text"),
                ),
            ),
        )

    async def send(self, destination_id: str, text: str) -> str:
        url = self._outbound_url.replace(
            "{token}",
            quote(self._token, safe=""),
        )
        return await self._post(
            url=url,
            headers=None,
            payload={"chat_id": destination_id, "text": text},
        )


class SlackChannelAdapter(_HttpChannelAdapter):
    provider: Provider = "slack"

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        signing_secret: str,
        client: httpx.AsyncClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        if not signing_secret:
            raise ValueError("Slack signing secret must not be empty")
        self._signing_secret = signing_secret.encode()
        self._now = now or (lambda: datetime.now(UTC))

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        _require_post(request, self.provider)
        timestamp = _header(
            request.headers,
            "x-slack-request-timestamp",
        )
        try:
            timestamp_value = int(timestamp)
        except ValueError as exc:
            raise PermissionError("invalid Slack timestamp") from exc
        if abs(int(self._now().timestamp()) - timestamp_value) > 300:
            raise PermissionError("stale Slack timestamp")
        base = b"v0:" + timestamp.encode() + b":" + request.body
        expected = "v0=" + hmac.new(
            self._signing_secret,
            base,
            hashlib.sha256,
        ).hexdigest()
        supplied = _header(request.headers, "x-slack-signature")
        if not hmac.compare_digest(supplied, expected):
            raise PermissionError("invalid Slack signature")

        payload = _json_object(request.body)
        if payload.get("type") == "url_verification":
            challenge = _required_text(payload.get("challenge"), "challenge")
            return _json_response({"challenge": challenge})
        event = _dict(payload.get("event"))
        if (
            payload.get("type") != "event_callback"
            or event.get("type") != "message"
            or event.get("bot_id")
            or event.get("subtype") == "bot_message"
        ):
            return ChannelWebhookResponse()
        return ChannelWebhookResponse(
            message=_channel_message(
                self.provider,
                (
                    event.get("user"),
                    event.get("user") or "Slack user",
                    event.get("channel"),
                    payload.get("event_id")
                    or event.get("client_msg_id")
                    or event.get("ts"),
                    event.get("text"),
                ),
            ),
        )

    async def send(self, destination_id: str, text: str) -> str:
        return await self._post(
            url=self._outbound_url,
            headers={"Authorization": f"Bearer {self._token}"},
            payload={"channel": destination_id, "text": text},
        )


class WhatsAppChannelAdapter(_HttpChannelAdapter):
    provider: Provider = "whatsapp"

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        app_secret: str,
        verify_token: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        if not app_secret or not verify_token:
            raise ValueError("WhatsApp app and verify secrets must not be empty")
        self._app_secret = app_secret.encode()
        self._verify_token = verify_token

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        if request.method.upper() == "GET":
            mode = request.query.get("hub.mode", "")
            token = request.query.get("hub.verify_token", "")
            if (
                mode != "subscribe"
                or not hmac.compare_digest(token, self._verify_token)
            ):
                raise PermissionError("invalid WhatsApp verification token")
            challenge = _required_text(
                request.query.get("hub.challenge"),
                "hub.challenge",
            )
            return ChannelWebhookResponse(
                body=challenge.encode(),
                content_type="text/plain; charset=utf-8",
            )
        _require_post(request, self.provider)
        supplied = _header(
            request.headers,
            "x-hub-signature-256",
        )
        expected = "sha256=" + hmac.new(
            self._app_secret,
            request.body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise PermissionError("invalid WhatsApp signature")
        payload = _json_object(request.body)
        entry = _dict(_first(payload.get("entry")))
        change = _dict(_first(entry.get("changes")))
        value = _dict(change.get("value"))
        message = _dict(_first(value.get("messages")))
        if not message:
            return ChannelWebhookResponse(
                body=b"EVENT_RECEIVED",
                content_type="text/plain; charset=utf-8",
            )
        contact = _dict(_first(value.get("contacts")))
        user_id = message.get("from")
        return ChannelWebhookResponse(
            message=_channel_message(
                self.provider,
                (
                    user_id,
                    _dict(contact.get("profile")).get("name") or user_id,
                    user_id,
                    message.get("id"),
                    _dict(message.get("text")).get("body"),
                ),
            ),
        )

    async def send(self, destination_id: str, text: str) -> str:
        return await self._post(
            url=self._outbound_url,
            headers={"Authorization": f"Bearer {self._token}"},
            payload={
                "messaging_product": "whatsapp",
                "to": destination_id,
                "type": "text",
                "text": {"body": text},
            },
        )


class FeishuChannelAdapter(_HttpChannelAdapter):
    provider: Provider = "feishu"

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        verification_token: str,
        encrypt_key: str = "",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        if not verification_token:
            raise ValueError("Feishu verification token must not be empty")
        self._verification_token = verification_token
        self._encrypt_key = encrypt_key

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        _require_post(request, self.provider)
        if self._encrypt_key:
            self._verify_signature(request)
        payload = _json_object(request.body)
        if "encrypt" in payload:
            if not self._encrypt_key:
                raise PermissionError(
                    "Feishu encrypted event requires encrypt key",
                )
            payload = _json_object(
                _decrypt_feishu(
                    _required_text(payload.get("encrypt"), "encrypt"),
                    self._encrypt_key,
                ),
            )
        token = _dict(payload.get("header")).get("token") or payload.get(
            "token",
        )
        if not isinstance(token, str) or not hmac.compare_digest(
            token,
            self._verification_token,
        ):
            raise PermissionError("invalid Feishu verification token")
        if payload.get("type") == "url_verification":
            challenge = _required_text(payload.get("challenge"), "challenge")
            return _json_response({"challenge": challenge})

        header = _dict(payload.get("header"))
        event = _dict(payload.get("event"))
        message = _dict(event.get("message"))
        if (
            header.get("event_type") != "im.message.receive_v1"
            or not message
        ):
            return ChannelWebhookResponse()
        sender = _dict(_dict(event.get("sender")).get("sender_id"))
        content = message.get("content")
        text = _text_content(content)
        user_id = sender.get("open_id") or sender.get("user_id")
        return ChannelWebhookResponse(
            message=_channel_message(
                self.provider,
                (
                    user_id,
                    user_id or "Feishu user",
                    message.get("chat_id"),
                    header.get("event_id") or message.get("message_id"),
                    text,
                ),
            ),
        )

    def _verify_signature(self, request: ChannelWebhookRequest) -> None:
        timestamp = _header(
            request.headers,
            "x-lark-request-timestamp",
        )
        nonce = _header(request.headers, "x-lark-request-nonce")
        supplied = _header(request.headers, "x-lark-signature")
        expected = hashlib.sha256(
            timestamp.encode()
            + nonce.encode()
            + self._encrypt_key.encode()
            + request.body,
        ).hexdigest()
        if (
            not timestamp
            or not nonce
            or not hmac.compare_digest(supplied, expected)
        ):
            raise PermissionError("invalid Feishu signature")

    async def send(self, destination_id: str, text: str) -> str:
        return await self._post(
            url=self._outbound_url,
            headers={"Authorization": f"Bearer {self._token}"},
            payload={
                "receive_id": destination_id,
                "msg_type": "text",
                "content": json.dumps(
                    {"text": text},
                    ensure_ascii=False,
                ),
            },
        )


class DingTalkChannelAdapter(_HttpChannelAdapter):
    provider: Provider = "dingtalk"

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        webhook_secret: str,
        client: httpx.AsyncClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        if not webhook_secret:
            raise ValueError("DingTalk webhook secret must not be empty")
        self._webhook_secret = webhook_secret
        self._now = now or (lambda: datetime.now(UTC))

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        _require_post(request, self.provider)
        timestamp = _header(request.headers, "timestamp")
        supplied = _header(request.headers, "sign")
        try:
            timestamp_value = int(timestamp)
        except ValueError as exc:
            raise PermissionError("invalid DingTalk timestamp") from exc
        now_ms = int(self._now().timestamp() * 1000)
        if abs(now_ms - timestamp_value) > 3_600_000:
            raise PermissionError("stale DingTalk timestamp")
        expected = base64.b64encode(
            hmac.new(
                self._webhook_secret.encode(),
                f"{timestamp}\n{self._webhook_secret}".encode(),
                hashlib.sha256,
            ).digest(),
        ).decode()
        if not hmac.compare_digest(supplied, expected):
            raise PermissionError("invalid DingTalk signature")

        payload = _json_object(request.body)
        if payload.get("msgtype") != "text":
            return ChannelWebhookResponse()
        user_id = payload.get("senderStaffId") or payload.get("senderId")
        text = _dict(payload.get("text")).get("content")
        if isinstance(text, str):
            text = text.strip()
        return ChannelWebhookResponse(
            message=_channel_message(
                self.provider,
                (
                    user_id,
                    payload.get("senderNick") or user_id,
                    payload.get("conversationId"),
                    payload.get("msgId"),
                    text,
                ),
            ),
        )

    async def send(self, destination_id: str, text: str) -> str:
        if destination_id.startswith(("http://", "https://")):
            url = destination_id
        else:
            url = self._signed_robot_url()
        return await self._post(
            url=url,
            headers=None,
            payload={"msgtype": "text", "text": {"content": text}},
        )

    def _signed_robot_url(self) -> str:
        timestamp = str(int(self._now().timestamp() * 1000))
        signature = base64.b64encode(
            hmac.new(
                self._webhook_secret.encode(),
                f"{timestamp}\n{self._webhook_secret}".encode(),
                hashlib.sha256,
            ).digest(),
        ).decode()
        raw_url = self._outbound_url.replace(
            "{token}",
            quote(self._token, safe=""),
        )
        parsed = urlsplit(raw_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "{token}" not in self._outbound_url:
            query.setdefault("access_token", self._token)
        query.update({"timestamp": timestamp, "sign": signature})
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(query),
                parsed.fragment,
            ),
        )


class DiscordChannelAdapter(_HttpChannelAdapter):
    provider: Provider = "discord"

    def __init__(
        self,
        *,
        outbound_url: str,
        token: str,
        public_key: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(
            outbound_url=outbound_url,
            token=token,
            client=client,
        )
        try:
            key_bytes = bytes.fromhex(public_key)
            if len(key_bytes) != 32:
                raise ValueError
            subject_public_key_info = bytes.fromhex(
                "302a300506032b6570032100",
            ) + key_bytes
            self._public_key = ECC.import_key(subject_public_key_info)
        except ValueError as exc:
            raise ValueError("invalid Discord public key") from exc

    def handle_webhook(
        self,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        _require_post(request, self.provider)
        timestamp = _header(
            request.headers,
            "x-signature-timestamp",
        )
        signature = _header(
            request.headers,
            "x-signature-ed25519",
        )
        try:
            eddsa.new(self._public_key, "rfc8032").verify(
                timestamp.encode() + request.body,
                bytes.fromhex(signature),
            )
        except ValueError as exc:
            raise PermissionError("invalid Discord signature") from exc
        payload = _json_object(request.body)
        if payload.get("type") == 1 and "event" not in payload:
            return _json_response({"type": 1})
        if payload.get("type") == 0 and "event" in payload:
            return ChannelWebhookResponse(
                status_code=204,
                content_type="application/json",
            )

        if "event" in payload:
            event = _dict(payload.get("event"))
            event_data = _dict(event.get("data"))
            if event.get("type") not in {
                "LOBBY_MESSAGE_CREATE",
                "GAME_DIRECT_MESSAGE_CREATE",
            }:
                return ChannelWebhookResponse(status_code=204)
            author = _dict(event_data.get("author"))
            values = (
                author.get("id"),
                author.get("global_name") or author.get("username"),
                event_data.get("channel_id")
                or event_data.get("lobby_id"),
                event_data.get("id"),
                event_data.get("content"),
            )
            return ChannelWebhookResponse(
                status_code=204,
                message=_channel_message(self.provider, values),
            )

        user = _dict(_dict(payload.get("member")).get("user")) or _dict(
            payload.get("user"),
        )
        data = _dict(payload.get("data"))
        text = _discord_interaction_text(data)
        return ChannelWebhookResponse(
            body=json.dumps(
                {"type": 5},
                separators=(",", ":"),
            ).encode(),
            message=_channel_message(
                self.provider,
                (
                    user.get("id"),
                    user.get("global_name") or user.get("username"),
                    payload.get("channel_id"),
                    payload.get("id"),
                    text,
                ),
            ),
        )

    async def send(self, destination_id: str, text: str) -> str:
        url = self._outbound_url.replace(
            "{destination_id}",
            quote(destination_id, safe=""),
        )
        return await self._post(
            url=url,
            headers={"Authorization": f"Bot {self._token}"},
            payload={"content": text},
        )


# Kept as an import-compatible name for old relay callers. New configuration
# uses ``build_channel_adapter`` and never reaches this alias in native mode.
HttpChannelAdapter = RelayChannelAdapter


def build_channel_adapter(
    *,
    provider: Provider,
    mode: ChannelMode,
    outbound_url: str,
    secrets: Mapping[str, str],
    options: Mapping[str, str] | None = None,
    client: httpx.AsyncClient | None = None,
) -> ChannelAdapter:
    """Build one adapter from resolved secrets without retaining env names."""
    del options
    if mode == "relay":
        return RelayChannelAdapter(
            provider=provider,
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            webhook_secret=_secret(
                secrets,
                "webhook_secret",
                provider,
            ),
            client=client,
        )
    if provider == "signal":
        raise ValueError("Signal supports relay mode only")
    constructors: dict[Provider, Callable[[], ChannelAdapter]] = {
        "telegram": lambda: TelegramChannelAdapter(
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            webhook_secret=_secret(
                secrets,
                "webhook_secret",
                provider,
            ),
            client=client,
        ),
        "slack": lambda: SlackChannelAdapter(
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            signing_secret=_secret(
                secrets,
                "signing_secret",
                provider,
            ),
            client=client,
        ),
        "whatsapp": lambda: WhatsAppChannelAdapter(
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            app_secret=_secret(secrets, "app_secret", provider),
            verify_token=_secret(
                secrets,
                "verify_token",
                provider,
            ),
            client=client,
        ),
        "feishu": lambda: FeishuChannelAdapter(
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            verification_token=_secret(
                secrets,
                "verification_token",
                provider,
            ),
            encrypt_key=secrets.get("encrypt_key", ""),
            client=client,
        ),
        "dingtalk": lambda: DingTalkChannelAdapter(
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            webhook_secret=_secret(
                secrets,
                "webhook_secret",
                provider,
            ),
            client=client,
        ),
        "discord": lambda: DiscordChannelAdapter(
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            public_key=_secret(secrets, "public_key", provider),
            client=client,
        ),
        "signal": lambda: RelayChannelAdapter(
            provider="signal",
            outbound_url=outbound_url,
            token=_secret(secrets, "token", provider),
            webhook_secret=_secret(
                secrets,
                "webhook_secret",
                provider,
            ),
            client=client,
        ),
    }
    try:
        return constructors[provider]()
    except KeyError as exc:
        raise ValueError(f"unsupported native provider {provider}") from exc


def _header(headers: Mapping[str, str], name: str) -> str:
    expected = name.casefold()
    for key, value in headers.items():
        if key.casefold() == expected:
            return value
    return ""


def _json_object(body: bytes | str) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid webhook JSON") from exc
    if not isinstance(payload, dict):
        raise TypeError("webhook payload must be an object")
    return payload


def _channel_message(
    provider: Provider,
    values: tuple[object, object, object, object, object],
) -> ChannelMessage:
    if not all(value is not None and str(value).strip() for value in values):
        raise ValueError("webhook payload is missing identity or text")
    return ChannelMessage(
        provider=provider,
        external_user_id=str(values[0]),
        display_name=str(values[1]),
        destination_id=str(values[2]),
        message_id=str(values[3]),
        text=str(values[4]),
    )


def _require_post(
    request: ChannelWebhookRequest,
    provider: Provider,
) -> None:
    if request.method.upper() != "POST":
        raise ValueError(f"{provider} webhook requires POST")


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"webhook payload is missing {field}")
    return value


def _json_response(payload: Mapping[str, object]) -> ChannelWebhookResponse:
    return ChannelWebhookResponse(
        body=json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode(),
        content_type="application/json",
    )


def _decrypt_feishu(encrypted: str, key: str) -> str:
    try:
        ciphertext = base64.b64decode(encrypted, validate=True)
        aes_key = hashlib.sha256(key.encode()).digest()
        padded = AES.new(
            aes_key,
            AES.MODE_CBC,
            iv=bytes(AES.block_size),
        ).decrypt(ciphertext)
        return unpad(padded, AES.block_size).decode()
    except (ValueError, UnicodeDecodeError) as exc:
        raise PermissionError("invalid Feishu encrypted payload") from exc


def _discord_interaction_text(data: Mapping[str, Any]) -> str:
    def values(options: object) -> list[str]:
        result: list[str] = []
        if not isinstance(options, list):
            return result
        for option in options:
            item = _dict(option)
            value = item.get("value")
            if isinstance(value, (str, int, float, bool)):
                result.append(str(value))
            result.extend(values(item.get("options")))
        return result

    arguments = values(data.get("options"))
    if arguments:
        return " ".join(arguments)
    return _required_text(data.get("name"), "interaction command")


def _secret(
    secrets: Mapping[str, str],
    name: str,
    provider: Provider,
) -> str:
    value = secrets.get(name, "")
    if not value:
        raise ValueError(f"{provider} requires secret {name!r}")
    return value


def _outbound_message_id(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return "sent"
    if not isinstance(payload, dict):
        return "sent"
    result = _dict(payload.get("result"))
    return str(
        payload.get("id")
        or payload.get("message_id")
        or payload.get("ts")
        or result.get("message_id")
        or int(time.time() * 1000),
    )


def _normalize_relay(
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
        text = _text_content(content)
        return (
            sender.get("open_id") or sender.get("user_id"),
            sender.get("open_id") or "Feishu user",
            message.get("chat_id"),
            message.get("message_id"),
            text,
        )
    if provider == "whatsapp":
        entry = _dict(_first(payload.get("entry")))
        change = _dict(_first(entry.get("changes")))
        value = _dict(change.get("value"))
        message = _dict(_first(value.get("messages")))
        contact = _dict(_first(value.get("contacts")))
        return (
            message.get("from"),
            _dict(contact.get("profile")).get("name") or message.get("from"),
            message.get("from"),
            message.get("id"),
            _dict(message.get("text")).get("body"),
        )
    if provider == "dingtalk":
        return (
            payload.get("senderStaffId") or payload.get("senderId"),
            payload.get("senderNick") or payload.get("senderId"),
            payload.get("conversationId"),
            payload.get("msgId"),
            _dict(payload.get("text")).get("content"),
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


def _relay_outbound_payload(
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
    if provider == "dingtalk":
        return {"msgtype": "text", "text": {"content": text}}
    return {"content": text}


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first(value: object) -> object:
    return value[0] if isinstance(value, list) and value else {}


def _text_content(content: object) -> object:
    if not isinstance(content, (str, bytes, bytearray)):
        return content
    try:
        return _dict(json.loads(content)).get("text")
    except json.JSONDecodeError:
        return content
