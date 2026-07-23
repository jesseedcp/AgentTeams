---
name: matrix-server-management
description: Use when an admin wants to inspect Matrix rooms or profiles, manage room membership, or upload and download Matrix media outside Worker and Team provisioning.
---

# Matrix Server Management

Use only the Manager's owned Matrix adapter. Worker, Team, Human, project, and
task workflows perform their own Matrix changes.

## Tools

| Intent | Tool |
|---|---|
| Joined rooms and membership | `list_matrix_rooms`, `list_matrix_members` |
| User profile | `lookup_matrix_user` |
| Room state | `get_matrix_room_state` |
| Invite, kick, ban, unban | `invite_matrix_user`, `kick_matrix_user`, `ban_matrix_user`, `unban_matrix_user` |
| Media | `upload_matrix_media`, `download_matrix_media` |

Membership mutations and uploads require AgentScope confirmation. Read-only
inspection and downloads do not.

Human account provisioning belongs to `human-management`; private room creation
and notification routing belong to `channel-management`. Do not create a second
Matrix client or bypass the Manager's E2EE store.

Read `references/api-reference.md` for exact typed inputs.
