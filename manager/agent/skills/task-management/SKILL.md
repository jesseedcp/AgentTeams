---
name: task-management
description: Use to inspect, delegate, schedule, complete, or cancel durable Worker tasks.
---

# Task Management

Use the typed task tools; never construct task registry files or send a Worker
assignment only in an admin room.

- Inspect with `list_tasks` and `get_task`.
- Prepare a finite task with `create_task` or its explicit delegation alias
  `delegate_task`.
- Send Team work only through `delegate_team_task`.
- Create recurring work with `schedule_task`.
- Record a Worker event with `complete_task`.
- Use `update_task` for an explicit complete, recurring execution, or cancel
  transition.
- Use `delete_task` as the confirmed cancellation surface.

Task artifacts are verified in MinIO before Matrix dispatch. The Matrix
transaction ID, operation ID, and Worker event ID are durable and repeat-safe.

Read the relevant reference:

- `references/worker-selection.md`
- `references/finite-tasks.md`
- `references/infinite-tasks.md`
- `references/state-management.md`
