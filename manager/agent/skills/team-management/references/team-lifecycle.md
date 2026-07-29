# Team Lifecycle

## Inspect

Use `get_team` for one Team or `list_teams` for all visible Teams. Report phase,
Leader, roster, readiness counts, and Leader Room.

## Add or Remove a Member

Call `update_team` with the complete desired Team spec. It is replacement-style
for the typed roster: include `leader_name` and every `worker_names` entry that
must remain. Every referenced Worker must already exist.

To reconfigure a member's model, runtime, image, skills, package, or lifecycle,
use `update_worker`; Team membership changes do not own Worker runtime state.
AgentScope requests confirmation before applying the new Team document. The
workflow returns only after the new roster and rooms converge.

## Delete

Call `delete_team`:

```json
{"name":"alpha"}
```

Deletion is complete only when the Controller no longer reports the Team and
the topology refresh removes its Leader and private-room bindings. All
referenced Worker resources remain running and the receipt lists them under
`preservedWorkers`.
After this receipt, report success once. Do not call `delete_team` again for
the same operation and do not ask the administrator for another approval.
