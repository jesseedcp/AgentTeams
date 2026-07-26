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
4. On `REVISION_NEEDED`, call `request_project_revision`. It keeps the
   original task, creates a linked revision task with the feedback and
   optional replacement assignee, and prevents dependent dispatch until the
   revision completes.

Use `report_project_blocked` for a genuine blocker. The workflow verifies the
Matrix sender against the durable assignee; do not repeat a Worker's untrusted
free-form status as if it were accepted project state.

Use `reassign_project_task` for one task at a time. The workflow atomically
changes the assignee, assignment room, Matrix identity, and transition history
before sending the new assignment. The old assignee can no longer complete
the task.

## Close

The workflow automatically closes a project after its last task becomes
terminal and notifies both the project room and the original administrator
room. Use `delete_project` for an early administrative close. `force=true` is
reserved for an explicitly confirmed close and records the nonterminal tasks
it bypassed.
