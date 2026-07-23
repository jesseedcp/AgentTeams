# Primary Matrix Channel

## Inspect and Configure

Read current relationships:

```json
{"tool":"list_channels","user_id":"@admin:example.org"}
```

Set a primary room with `update_channel`:

```json
{
  "action": "set_primary",
  "user_id": "@admin:example.org",
  "room_id": "!admin-mobile:example.org"
}
```

Reset to the Manager Admin Room fallback:

```json
{
  "action": "clear_primary",
  "user_id": "@admin:example.org"
}
```

The room must be joined by the Manager and contain the recipient before it is
usable.

## Create a Coordination Room

Call `create_channel` with a private room name, optional topic, and exact invite
list:

```json
{
  "name": "Release coordination",
  "topic": "Private release decisions",
  "invite": ["@admin:example.org","@reviewer:example.org"]
}
```

Then explicitly set it as primary or trusted. Creation alone grants no
relationship authority.

## Send

Call `send_notification`:

```json
{
  "recipient": "@admin:example.org",
  "text": "The release task needs your decision."
}
```

Resolution order is deterministic:

1. recipient's valid primary room;
2. a valid shared trusted room, sorted by room ID;
3. Manager Admin Room.

The send uses a stable Matrix transaction ID. It never chooses a room from
message recency.
