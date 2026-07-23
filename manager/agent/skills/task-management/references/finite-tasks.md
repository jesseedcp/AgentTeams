# Finite Task Workflow

A finite task has a clear terminal deliverable.

## Assign

1. Select a Worker or Team from authoritative resource reads.
2. Call `create_task` or `delegate_task` with title, specification, assignee,
   and optional project identity. Use `delegate_team_task` for a Team Leader.
3. The workflow inserts SQLite `prepared`, uploads and verifies `meta.json`
   and `spec.md`, sends one stable assignment to the authoritative Worker or
   Leader room, then publishes `assigned`.
4. Confirm the returned task ID to the admin; do not repeat the assignment in
   the admin room.

## Complete

When the assigned Worker reports completion, call `complete_task` with the
task ID and optional structured result. The tool takes the Worker event ID
from the bound Matrix turn; callers cannot forge it.

Completion pulls the full task prefix, requires `result.md` or a structured
result, updates remote metadata before SQLite, appends daily memory, and sends
one terminal notification. A replay returns the canonical receipt.

Use `get_task` to inspect status. Use `update_task` only for a deliberate
transition. Use `delete_task` to cancel; cancellation does not erase artifacts.
