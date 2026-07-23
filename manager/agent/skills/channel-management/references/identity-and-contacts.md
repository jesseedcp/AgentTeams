# Matrix Identity and Contacts

## Classification

| Sender fact | Authority |
|---|---|
| Configured admin Matrix ID in Admin DM | Full Manager tools |
| Controller Human level 1 | Admin-equivalent; secrets remain withheld |
| Controller Human level 2 | Declared Team and Worker read scope |
| Controller Human level 3 | Declared Worker read scope |
| Controller Worker in Worker Room | Worker task/report tools |
| Controller Team Leader in Leader Room | Team delegation/report tools |
| Explicit trusted Matrix relationship | General read-only tools |
| No matching fact | No tools; group messages are silent |

The policy resolver checks both the Matrix sender ID and the room binding. A
correct-looking display name, mention, or claimed role grants nothing.

## Trusted Contact Changes

First use `list_channels` for the contact. To trust a shared room, call
`update_channel`:

```json
{
  "action": "trust",
  "user_id": "@admin:example.org",
  "peer_user_id": "@reviewer:example.org",
  "room_id": "!shared:example.org"
}
```

To remove that relationship, call `delete_channel`:

```json
{
  "action": "remove_trusted",
  "user_id": "@admin:example.org",
  "peer_user_id": "@reviewer:example.org"
}
```

Trust permits conversation, not resource mutation, credential access, or task
assignment.
