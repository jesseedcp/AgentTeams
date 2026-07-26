---
name: project-management
description: Use to create durable multi-Worker projects, advance their task graph, report progress, or close them.
---

# Project Management

A project has a SQLite record, canonical MinIO `meta.json` and `plan.md`, an
immutable project ID, and one private Matrix room. The room always includes
the requesting admin, Manager, and selected Workers.

Use only these typed operations:

- `create_project` prepares metadata and the plan before room creation.
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

Read only the relevant reference:

- `references/create-project.md`
- `references/task-lifecycle.md`
- `references/plan-format.md`
- `references/plan-changes.md`
