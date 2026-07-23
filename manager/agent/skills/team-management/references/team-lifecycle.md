# Team Lifecycle

## Inspect

Use `get_team` for one Team or `list_teams` for all visible Teams. Report phase,
Leader, roster, readiness counts, and Leader Room.

## Add, Remove, or Reconfigure a Member

Call `update_team` with the complete desired Team spec. It is replacement-style
for the typed roster: include every member that must remain.

Before changing a busy Team, explain that affected member containers may be
reconciled. AgentScope requests confirmation before applying the new document.
The workflow returns only after the new roster and rooms converge.

## Delete

Call `delete_team`:

```json
{"name":"alpha"}
```

Deletion is complete only when the Controller no longer reports the Team and
the topology refresh removes its Leader and private-room bindings. Do not
individually delete Team members as a substitute for deleting the Team.
