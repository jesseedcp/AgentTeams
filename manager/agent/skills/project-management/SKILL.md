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
- `delete_project` closes the project after AgentScope confirmation.

Assignments still go to Worker or Team Leader rooms. The project room receives
progress summaries; it is not a substitute for the assignment room.

Read only the relevant reference:

- `references/create-project.md`
- `references/task-lifecycle.md`
- `references/plan-format.md`
- `references/plan-changes.md`
