---
name: human-management
description: Use when an admin wants to add, inspect, change, or remove a real Human identity and its AgentTeams permission scope.
---

# Human Management

Humans are Controller resources backed by Matrix identities. They do not run a
Worker container and do not receive Worker runtime configuration.

## Permission Levels

| Level | Effective scope |
|---|---|
| 1 | Manager, all Team Leaders, and all Workers |
| 2 | Declared Teams plus declared standalone Workers |
| 3 | Declared Workers only |

Level 1 ignores narrower lists. Level 3 ignores Team scope. Changes take effect
only after Controller and Matrix topology converge.

## Tools

| Intent | Tool |
|---|---|
| List or inspect | `list_humans`, `get_human` |
| Add identity and scope | `create_human` |
| Change display, email, level, scope, or note | `update_human` |
| Remove access | `delete_human` |

Every change requires AgentScope confirmation. Never create Matrix credentials
or edit room allowlists separately; the Controller reconciles identity and
membership from the Human resource.

Read `references/create-human.md` for exact typed requests and convergence
rules.
