---
name: team-management
description: Use when an admin wants to create, inspect, reconfigure, or delete a Team, or when work must be delegated through a Team Leader.
---

# Team Management

The Controller owns Team resources and member containers. The Manager
coordinates a Team only through its Leader Room and never enters or instructs
the Team-private room.

## Tools

| Intent | Tool |
|---|---|
| Inspect | `list_teams`, `get_team` |
| Create | `create_team` |
| Replace desired roster/configuration | `update_team` |
| Delete | `delete_team` |
| Assign finite work through the Leader | `delegate_team_task` |

Create, update, and delete require AgentScope confirmation. A successful
resource receipt means Controller readiness and Matrix topology both converged.

Teams may mix OpenClaw, CoPaw, Hermes, QwenPaw, and OpenHuman members. Preserve
the hierarchy: admin → Manager → Team Leader → Team Workers.

Read `references/create-team.md` for typed Team input,
`references/team-lifecycle.md` for changes, and
`references/team-task-delegation.md` for delegation.
