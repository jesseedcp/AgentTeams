from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime

import httpx
import pytest
from Crypto.Cipher import AES
from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa
from Crypto.Util.Padding import pad

from agentteams_manager.channels.base import ChannelWebhookRequest
from agentteams_manager.channels.http_providers import (
    DingTalkChannelAdapter,
    DiscordChannelAdapter,
    FeishuChannelAdapter,
    RelayChannelAdapter,
    SlackChannelAdapter,
    TelegramChannelAdapter,
    WhatsAppChannelAdapter,
)


def _request(
    body: bytes = b"",
    *,
    method: str = "POST",
    headers: dict[str, str] | None = None,
    query: dict[str, str] | None = None,
) -> ChannelWebhookRequest:
    return ChannelWebhookRequest(
        method=method,
        headers=headers or {},
        query=query or {},
        body=body,
    )


def _hmac_hex(secret: str, body: bytes) -> str:
    return hmac.new(
        secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()


def test_telegram_uses_native_secret_header_and_update_shape() -> None:
    body = json.dumps(
        {
            "update_id": 7001,
            "message": {
                "message_id": 42,
                "text": "hello",
                "from": {"id": 7, "username": "alice"},
                "chat": {"id": 99},
            },
        },
    ).encode()
    adapter = TelegramChannelAdapter(
        outbound_url="https://api.telegram.test/sendMessage",
        token="bot-token",
        webhook_secret="native-secret",
    )

    with pytest.raises(PermissionError):
        adapter.handle_webhook(_request(body))
    result = adapter.handle_webhook(
        _request(
            body,
            headers={
                "X-Telegram-Bot-Api-Secret-Token": "native-secret",
            },
        ),
    )

    assert result.message is not None
    assert result.message.external_user_id == "7"
    assert result.message.destination_id == "99"
    assert result.message.message_id == "7001"
    assert result.message.text == "hello"


def test_slack_verifies_timestamp_signature_and_answers_challenge() -> None:
    now = datetime(2026, 7, 28, 3, 0, tzinfo=UTC)
    timestamp = str(int(now.timestamp()))
    body = json.dumps(
        {"type": "url_verification", "challenge": "challenge-code"},
        separators=(",", ":"),
    ).encode()
    base = b"v0:" + timestamp.encode() + b":" + body
    signature = "v0=" + _hmac_hex("signing-secret", base)
    adapter = SlackChannelAdapter(
        outbound_url="https://slack.test/chat.postMessage",
        token="bot-token",
        signing_secret="signing-secret",
        now=lambda: now,
    )

    result = adapter.handle_webhook(
        _request(
            body,
            headers={
                "X-Slack-Request-Timestamp": timestamp,
                "X-Slack-Signature": signature,
            },
        ),
    )
    assert result.message is None
    assert json.loads(result.body) == {"challenge": "challenge-code"}

    stale = str(int(now.timestamp()) - 301)
    stale_base = b"v0:" + stale.encode() + b":" + body
    with pytest.raises(PermissionError, match="timestamp"):
        adapter.handle_webhook(
            _request(
                body,
                headers={
                    "X-Slack-Request-Timestamp": stale,
                    "X-Slack-Signature":
                        "v0=" + _hmac_hex("signing-secret", stale_base),
                },
            ),
        )


def test_slack_normalizes_event_callback() -> None:
    now = datetime(2026, 7, 28, 3, 0, tzinfo=UTC)
    timestamp = str(int(now.timestamp()))
    body = json.dumps(
        {
            "type": "event_callback",
            "event_id": "Ev01",
            "event": {
                "type": "message",
                "user": "U01",
                "channel": "C01",
                "client_msg_id": "M01",
                "text": "ship it",
            },
        },
        separators=(",", ":"),
    ).encode()
    signature = "v0=" + _hmac_hex(
        "signing-secret",
        b"v0:" + timestamp.encode() + b":" + body,
    )
    adapter = SlackChannelAdapter(
        outbound_url="https://slack.test/chat.postMessage",
        token="bot-token",
        signing_secret="signing-secret",
        now=lambda: now,
    )

    message = adapter.handle_webhook(
        _request(
            body,
            headers={
                "x-slack-request-timestamp": timestamp,
                "x-slack-signature": signature,
            },
        ),
    ).message
    assert message is not None
    assert (
        message.external_user_id,
        message.destination_id,
        message.message_id,
        message.text,
    ) == ("U01", "C01", "Ev01", "ship it")


def test_whatsapp_supports_get_verification_and_signed_messages() -> None:
    adapter = WhatsAppChannelAdapter(
        outbound_url="https://graph.test/messages",
        token="graph-token",
        app_secret="app-secret",
        verify_token="verify-me",
    )

    challenge = adapter.handle_webhook(
        _request(
            method="GET",
            query={
                "hub.mode": "subscribe",
                "hub.verify_token": "verify-me",
                "hub.challenge": "123456",
            },
        ),
    )
    assert challenge.body == b"123456"
    assert challenge.content_type == "text/plain; charset=utf-8"

    body = json.dumps(
        {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "contacts": [
                                    {
                                        "wa_id": "8613800000000",
                                        "profile": {"name": "Alice"},
                                    },
                                ],
                                "messages": [
                                    {
                                        "from": "8613800000000",
                                        "id": "wamid.1",
                                        "type": "text",
                                        "text": {"body": "hello"},
                                    },
                                ],
                            },
                        },
                    ],
                },
            ],
        },
        separators=(",", ":"),
    ).encode()
    result = adapter.handle_webhook(
        _request(
            body,
            headers={
                "X-Hub-Signature-256":
                    "sha256=" + _hmac_hex("app-secret", body),
            },
        ),
    )
    assert result.message is not None
    assert result.message.display_name == "Alice"
    assert result.message.message_id == "wamid.1"


def test_feishu_verifies_token_signature_and_answers_challenge() -> None:
    body = json.dumps(
        {
            "challenge": "feishu-challenge",
            "type": "url_verification",
            "token": "verification-token",
        },
        separators=(",", ":"),
    ).encode()
    timestamp = "1722135600"
    nonce = "nonce-1"
    signature = hashlib.sha256(
        timestamp.encode()
        + nonce.encode()
        + b"encrypt-key"
        + body,
    ).hexdigest()
    adapter = FeishuChannelAdapter(
        outbound_url="https://feishu.test/messages",
        token="tenant-token",
        verification_token="verification-token",
        encrypt_key="encrypt-key",
    )

    response = adapter.handle_webhook(
        _request(
            body,
            headers={
                "X-Lark-Request-Timestamp": timestamp,
                "X-Lark-Request-Nonce": nonce,
                "X-Lark-Signature": signature,
            },
        ),
    )
    assert json.loads(response.body) == {
        "challenge": "feishu-challenge",
    }


def test_feishu_normalizes_v2_message_event() -> None:
    body = json.dumps(
        {
            "schema": "2.0",
            "header": {
                "event_id": "feishu-event-1",
                "event_type": "im.message.receive_v1",
                "token": "verification-token",
            },
            "event": {
                "sender": {
                    "sender_id": {
                        "open_id": "ou_1",
                        "user_id": "u_1",
                    },
                    "sender_type": "user",
                },
                "message": {
                    "message_id": "om_1",
                    "chat_id": "oc_1",
                    "message_type": "text",
                    "content": '{"text":"你好"}',
                },
            },
        },
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    adapter = FeishuChannelAdapter(
        outbound_url="https://feishu.test/messages",
        token="tenant-token",
        verification_token="verification-token",
    )

    message = adapter.handle_webhook(_request(body)).message
    assert message is not None
    assert (
        message.external_user_id,
        message.destination_id,
        message.message_id,
        message.text,
    ) == ("ou_1", "oc_1", "feishu-event-1", "你好")


def test_feishu_decrypts_encrypted_challenge() -> None:
    plaintext = json.dumps(
        {
            "challenge": "encrypted-challenge",
            "type": "url_verification",
            "token": "verification-token",
        },
        separators=(",", ":"),
    ).encode()
    key = hashlib.sha256(b"encrypt-key").digest()
    encrypted = base64.b64encode(
        AES.new(
            key,
            AES.MODE_CBC,
            iv=bytes(AES.block_size),
        ).encrypt(pad(plaintext, AES.block_size)),
    ).decode()
    body = json.dumps(
        {"encrypt": encrypted},
        separators=(",", ":"),
    ).encode()
    timestamp = "1722135600"
    nonce = "nonce-encrypted"
    signature = hashlib.sha256(
        timestamp.encode()
        + nonce.encode()
        + b"encrypt-key"
        + body,
    ).hexdigest()
    adapter = FeishuChannelAdapter(
        outbound_url="https://feishu.test/messages",
        token="tenant-token",
        verification_token="verification-token",
        encrypt_key="encrypt-key",
    )

    response = adapter.handle_webhook(
        _request(
            body,
            headers={
                "X-Lark-Request-Timestamp": timestamp,
                "X-Lark-Request-Nonce": nonce,
                "X-Lark-Signature": signature,
            },
        ),
    )
    assert json.loads(response.body) == {
        "challenge": "encrypted-challenge",
    }


def test_dingtalk_verifies_callback_signature_and_preserves_session() -> None:
    timestamp = "1722135600000"
    secret = "ding-secret"
    signature = base64.b64encode(
        hmac.new(
            secret.encode(),
            f"{timestamp}\n{secret}".encode(),
            hashlib.sha256,
        ).digest(),
    ).decode()
    body = json.dumps(
        {
            "msgId": "ding-message-1",
            "msgtype": "text",
            "text": {"content": " 你好 "},
            "senderId": "sender-1",
            "senderStaffId": "staff-1",
            "senderNick": "张三",
            "conversationId": "cid-1",
            "sessionWebhook": "https://oapi.dingtalk.test/session-1",
        },
        ensure_ascii=False,
    ).encode()
    adapter = DingTalkChannelAdapter(
        outbound_url="https://oapi.dingtalk.test/robot/send",
        token="access-token",
        webhook_secret=secret,
        now=lambda: datetime.fromtimestamp(
            int(timestamp) / 1000,
            tz=UTC,
        ),
    )

    with pytest.raises(PermissionError):
        adapter.handle_webhook(_request(body))
    message = adapter.handle_webhook(
        _request(
            body,
            headers={"timestamp": timestamp, "sign": signature},
        ),
    ).message
    assert message is not None
    assert (
        message.external_user_id,
        message.destination_id,
        message.message_id,
        message.text,
    ) == ("staff-1", "cid-1", "ding-message-1", "你好")


def test_discord_verifies_ed25519_and_handles_ping_and_command() -> None:
    private_key = ECC.generate(curve="Ed25519")
    public_key = private_key.public_key().export_key(format="raw").hex()
    signer = eddsa.new(private_key, "rfc8032")
    adapter = DiscordChannelAdapter(
        outbound_url="https://discord.test/channels/{destination_id}/messages",
        token="discord-token",
        public_key=public_key,
    )

    timestamp = "1722135600"
    ping_body = b'{"type":1}'
    ping_signature = signer.sign(
        timestamp.encode() + ping_body,
    ).hex()
    ping = adapter.handle_webhook(
        _request(
            ping_body,
            headers={
                "X-Signature-Ed25519": ping_signature,
                "X-Signature-Timestamp": timestamp,
            },
        ),
    )
    assert json.loads(ping.body) == {"type": 1}
    assert ping.message is None

    command_body = json.dumps(
        {
            "id": "interaction-1",
            "type": 2,
            "channel_id": "channel-1",
            "member": {
                "user": {
                    "id": "user-1",
                    "username": "Alice",
                },
            },
            "data": {
                "name": "ask",
                "options": [{"name": "prompt", "value": "hello"}],
            },
        },
        separators=(",", ":"),
    ).encode()
    signature = signer.sign(
        timestamp.encode() + command_body,
    ).hex()
    message = adapter.handle_webhook(
        _request(
            command_body,
            headers={
                "x-signature-ed25519": signature,
                "x-signature-timestamp": timestamp,
            },
        ),
    ).message
    assert message is not None
    assert (
        message.external_user_id,
        message.destination_id,
        message.message_id,
        message.text,
    ) == ("user-1", "channel-1", "interaction-1", "hello")


def test_signal_and_legacy_integrations_remain_explicit_relay_mode() -> None:
    body = json.dumps(
        {
            "envelope": {
                "sourceUuid": "signal-user",
                "sourceNumber": "+8613800000000",
                "sourceName": "Alice",
                "timestamp": 42,
                "dataMessage": {"message": "hello"},
            },
        },
    ).encode()
    adapter = RelayChannelAdapter(
        provider="signal",
        outbound_url="https://signal.test/v2/send",
        token="relay-token",
        webhook_secret="relay-secret",
    )
    result = adapter.handle_webhook(
        _request(
            body,
            headers={
                "X-AgentTeams-Signature":
                    "sha256=" + _hmac_hex("relay-secret", body),
            },
        ),
    )
    assert result.message is not None
    assert result.message.external_user_id == "signal-user"
    assert result.message.text == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter", "destination", "expected_auth", "expected_payload"),
    [
        (
            TelegramChannelAdapter(
                outbound_url="https://telegram.test/send",
                token="telegram-token",
                webhook_secret="hook-secret",
            ),
            "chat-1",
            None,
            {"chat_id": "chat-1", "text": "hello"},
        ),
        (
            SlackChannelAdapter(
                outbound_url="https://slack.test/send",
                token="slack-token",
                signing_secret="signing-secret",
            ),
            "C01",
            "Bearer slack-token",
            {"channel": "C01", "text": "hello"},
        ),
        (
            WhatsAppChannelAdapter(
                outbound_url="https://graph.test/send",
                token="graph-token",
                app_secret="app-secret",
                verify_token="verify-token",
            ),
            "8613800000000",
            "Bearer graph-token",
            {
                "messaging_product": "whatsapp",
                "to": "8613800000000",
                "type": "text",
                "text": {"body": "hello"},
            },
        ),
        (
            FeishuChannelAdapter(
                outbound_url="https://feishu.test/send",
                token="tenant-token",
                verification_token="verify-token",
            ),
            "oc_1",
            "Bearer tenant-token",
            {
                "receive_id": "oc_1",
                "msg_type": "text",
                "content": '{"text": "hello"}',
            },
        ),
        (
            DingTalkChannelAdapter(
                outbound_url="https://dingtalk.test/send",
                token="ding-token",
                webhook_secret="ding-secret",
            ),
            "cid-1",
            None,
            {"msgtype": "text", "text": {"content": "hello"}},
        ),
        (
            DiscordChannelAdapter(
                outbound_url=(
                    "https://discord.test/channels/"
                    "{destination_id}/messages"
                ),
                token="discord-token",
                public_key="58" + "66" * 31,
            ),
            "channel-1",
            "Bot discord-token",
            {"content": "hello"},
        ),
    ],
)
async def test_native_outbound_shapes(
    adapter,
    destination: str,
    expected_auth: str | None,
    expected_payload: dict[str, object],
) -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("authorization")
        captured["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "ok": True,
                "result": {"message_id": 77},
                "ts": "123.4",
                "message_id": "out-1",
                "id": "discord-out-1",
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter._replace_client_for_test(client)
    try:
        assert await adapter.send(destination, "hello")
        assert captured["authorization"] == expected_auth
        assert captured["payload"] == expected_payload
        if isinstance(adapter, DiscordChannelAdapter):
            assert "/channel-1/messages" in str(captured["url"])
        if isinstance(adapter, DingTalkChannelAdapter):
            assert "access_token=ding-token" in str(captured["url"])
            assert "timestamp=" in str(captured["url"])
            assert "sign=" in str(captured["url"])
    finally:
        await client.aclose()
