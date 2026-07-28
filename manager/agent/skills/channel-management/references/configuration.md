# External Channel Configuration

Configure channels through `manager.externalChannels` in Helm. The Controller
serializes the list into `AGENTTEAMS_EXTERNAL_CHANNELS` and forwards only the
environment variables referenced by `secret_envs`. Never put a token or secret
value directly in this document.

## Version 2 shape

```json
{
  "schema_version": 2,
  "provider": "slack",
  "mode": "native",
  "outbound_url": "https://slack.com/api/chat.postMessage",
  "secret_envs": {
    "token": "env:SLACK_BOT_TOKEN",
    "signing_secret": "env:SLACK_SIGNING_SECRET"
  },
  "options": {}
}
```

The inbound URL is:

```text
https://<manager-public-host>/manager-admin/hooks/<provider>
```

WhatsApp uses `GET` on this URL for subscription verification. All other
native callbacks use `POST`.

## Required secret names

| Provider | Mode | `secret_envs` keys |
|---|---|---|
| Telegram | native | `token`, `webhook_secret` |
| Slack | native | `token`, `signing_secret` |
| WhatsApp | native | `token`, `app_secret`, `verify_token` |
| Feishu | native | `token`, `verification_token`; optional `encrypt_key` |
| DingTalk | native | `token`, `webhook_secret` |
| Discord | native | `token`, `public_key` |
| Signal | relay | `token`, `webhook_secret` |

Telegram `outbound_url` may contain a `{token}` placeholder. Discord may use
`{destination_id}`. Placeholders are expanded only in memory when a request is
sent; resolved credentials are never written to SQLite.

## Relay compatibility

Relay mode accepts the normalized JSON shapes used before schema version 2 and
requires:

```text
X-AgentTeams-Signature: sha256=<HMAC-SHA256(raw-body, webhook-secret)>
```

Legacy documents containing `token_env` and `webhook_secret_env` are migrated
to relay mode at startup with a deprecation warning. Migrate them explicitly;
relay security is an AgentTeams contract, not the provider's native protocol.

## Delivery behavior

- Provider signatures are checked against the unmodified request body.
- Slack and DingTalk timestamp windows reject replayed requests.
- Discord PING, Slack/Feishu challenges, and WhatsApp verification requests
  are answered directly and never create contacts.
- Provider event IDs are claimed in SQLite before dispatch. Retries return a
  successful acknowledgement without a second Manager turn.
- A valid first message creates a pending contact. Only an Admin can approve
  it; blocked contacts never reach AgentScope.
