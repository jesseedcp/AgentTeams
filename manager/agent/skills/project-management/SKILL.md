---
name: project-management
description: Use to create durable multi-Worker projects, advance their task graph, report progress, or close them.
---

# Project Management

A project has a SQLite record, canonical MinIO `meta.json` and `plan.md`, an
immutable project ID, and one private Matrix room. The room always includes
the requesting admin, Manager, and selected Workers.

Use only these typed operations:

- `create_project` prepares a `planning` project, canonical plan, and private
  room. It does not activate work.
- `confirm_project_plan` records the administrator's plan decision, publishes
  the confirmed plan in the project room, and changes the project to `active`.
- `list_projects` and `get_project` inspect durable status.
- `update_project` adds a ready task or records a project task completion.
- `report_project_blocked` records a blocker only for the assigned Worker or
  administrator.
- `request_project_revision` preserves the original task and creates a linked
  revision task before downstream work can continue.
- `reassign_project_task` revokes one live assignment and dispatches it to
  another existing project participant.
- `revise_project_plan` applies a minor, versioned plan change immediately.
- `revise_project_plan_major` applies a major plan change only after global
  administrator confirmation.
- `update_project_participants` adds or removes participants only after global
  administrator confirmation and synchronizes Matrix membership.
- `delete_project` closes the project after AgentScope confirmation.

Assignments still go to Worker or Team Leader rooms. The project room receives
progress summaries; it is not a substitute for the assignment room.

In every non-YOLO mode, including `/elevated full`, present the returned
planning project to the administrator in the Admin DM and stop. Do not add
tasks until a later message explicitly confirms the plan. `/elevated full`
changes tool-level authorization only and does not confirm the plan. Only
configured YOLO mode auto-confirms project creation and may proceed in the same
turn. If the administrator explicitly requests `planning` while YOLO is
configured, report the conflict before calling `create_project`.

Read only the relevant reference:

- `references/create-project.md`
- `references/task-lifecycle.md`
- `references/plan-format.md`
- `references/plan-changes.md`
