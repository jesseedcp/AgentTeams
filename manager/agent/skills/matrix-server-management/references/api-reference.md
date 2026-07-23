# Matrix Tool Reference

## Read Operations

`list_matrix_rooms` takes no fields.

```json
{}
```

`list_matrix_members` and `get_matrix_room_state` take a room ID:

```json
{"room_id":"!release:example.org"}
```

`lookup_matrix_user` takes a full Matrix user ID:

```json
{"user_id":"@alice:example.org"}
```

## Membership

Use `invite_matrix_user` or `unban_matrix_user`:

```json
{
  "room_id": "!release:example.org",
  "user_id": "@alice:example.org"
}
```

Use `kick_matrix_user` or `ban_matrix_user` with an optional reason:

```json
{
  "room_id": "!release:example.org",
  "user_id": "@alice:example.org",
  "reason": "Access scope removed"
}
```

Never infer success from the request alone; report the typed success receipt.

## Media

Upload a local file with `upload_matrix_media`:

```json
{"path":"/workspace/reports/release.pdf"}
```

Download with `download_matrix_media`:

```json
{
  "mxc_uri": "mxc://example.org/media-id",
  "media_type": "application/pdf",
  "filename": "release.pdf"
}
```

For encrypted media, also pass the key, hash, and IV fields from the inbound
media reference. The adapter owns decryption and local size limits.

## Boundaries

- Use `create_channel` for a new private room.
- Use `send_notification` for an idempotent routed message.
- Use `create_human` for a real Human account.
- Task messages to Workers and Team Leaders must use their task workflows so
  structured mentions, threads, and durable operation IDs are preserved.
