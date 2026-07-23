---
name: channel-management
description: Use when identifying a Matrix sender, creating a coordination room, setting primary or trusted Matrix channels, or sending a cross-room notification.
assign_when: Not assigned to workers; this is a Manager-only capability.
---

# Channel Management

Channel authority comes from Controller identities, Matrix membership, and the
SQLite topology materialization. Never infer trust from recent traffic.

## Tools

| Intent | Tool |
|---|---|
| Inspect a user's primary and trusted rooms | `list_channels` |
| Create a private Matrix coordination room | `create_channel` |
| Set/clear primary or add trusted relationship | `update_channel` |
| Remove primary or trusted relationship | `delete_channel` |
| Notify via primary, trusted, then Admin Room fallback | `send_notification` |

All relationship changes and sends require AgentScope confirmation. Removing a
channel relationship does not delete the Matrix room.

## Authority Rules

- Admin and level-1 Human: full policy, with confirmation for changes.
- Level-2 Human: read-only within declared Teams and Workers.
- Level-3 Human: read-only within declared Workers.
- Trusted contact: general read-only help; never secrets or management.
- Unknown group sender: silently ignore.
- Team Leader and Worker permissions come from the room topology, not names in
  message text.

Read `references/identity-and-contacts.md` for sender classification and
`references/primary-channel.md` for relationship schemas and fallback order.
