# Canonical Project Plan

`create_project` accepts a human-readable plan body. The workflow renders it
with a single canonical header and task section:

```markdown
# <title>

**ID**: <project-id>

**Status**: planning | active | completed

## Goal

<description>

## Plan

<phases, dependencies, acceptance criteria>

## Tasks

- [ ] <task-id>
```

Task markers are derived from durable task status:

- `[ ]` pending or not yet dispatched
- `[~]` assigned or active
- `[x]` completed
- `[-]` failed or cancelled

The plan must describe a directed acyclic graph. Dependencies refer to stable
task IDs after creation. A Worker result belongs in `result.md` and should
state `SUCCESS`, `SUCCESS_WITH_NOTES`, `REVISION_NEEDED`, or `BLOCKED`.

Use `get_project` to inspect the structured record. Use `update_project` for
task lifecycle changes; never hand-edit the rendered task status list.
