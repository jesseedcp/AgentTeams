# Project Task Lifecycle

## Assign

1. Call `get_project` and verify status is `active`.
2. Check the project graph. Every dependency for the new node must already be
   terminal and successful.
3. Call `update_project` with `action="add_task"`, a complete specification,
   the selected Worker, and optional Team.
4. The workflow delegates through the normal task service, updates project
   metadata and `plan.md`, then posts a stable project-room progress event.

For multi-phase work, the specification must require an explicit Manager
mention and phase completion marker. The next phase is never inferred from
unaddressed room chatter.

## Complete

1. Accept completion only from the assigned Worker or Team Leader room.
2. Call `update_project` with `action="complete_task"` and the task ID.
3. The workflow pulls and verifies Worker artifacts, records task completion,
   updates the project index, and announces progress exactly once.
4. On `REVISION_NEEDED`, add a revision task before any dependent task.

## Close

Call `delete_project` after every task is terminal. `force=true` is reserved
for an explicitly confirmed administrative close and records the nonterminal
tasks it bypassed.
