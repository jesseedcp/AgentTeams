---
name: team-management
description: Use when an admin wants to create, inspect, reconfigure, or delete a Team, or when work must be delegated through a Team Leader.
---

# Team Management

The Controller owns Team coordination resources. Every Team member is an
independent Worker resource whose model, runtime, image, skills, package, and
lifecycle are managed through Worker tools. The Manager coordinates a Team
only through its Leader Room and never enters or instructs the Team-private
room.

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
`get_team` also returns the effective `peerMentions` policy, Leader and Team
room identifiers, ready-member counts, and the current roster.

Create or update all required Workers first. Then compose the Team with
`leader_name` and `worker_names`; never embed Worker runtime configuration in
the Team request. Deleting a Team preserves its referenced Workers and returns
their names in `preservedWorkers`.

Preserve the hierarchy: admin → Manager → Team Leader → Team Workers.

Read `references/create-team.md` for typed Team input,
`references/team-lifecycle.md` for changes, and
`references/team-task-delegation.md` for delegation.
