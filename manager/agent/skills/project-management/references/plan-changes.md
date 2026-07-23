# Plan Changes, Blockers, and Participants

Use `get_project` and `list_projects` before changing a project. SQLite and
the versioned MinIO documents are the durable facts; conversational summaries
are not.

## Task changes

Use `update_project` with `action="add_task"` only when all declared
dependencies are completed. For a completion event, use
`action="complete_task"` with the existing task ID. Recovery updates project
metadata and `plan.md` before it posts a stable project-room summary.

Do not advance a dependent task while its predecessor reports
`REVISION_NEEDED` or `BLOCKED`. Refine the next task's specification, assign a
separate revision task, or escalate the missing decision.

## Material changes

Changing the project goal, participants, deliverables, or forced closure is a
material decision. Obtain the confirmation required by room policy and make
the change through the relevant typed resource/project operation. Do not edit
Matrix membership or canonical project objects behind the workflow.

Use `delete_project` only to close. It refuses nonterminal tasks unless
`force=true`; forced close remains a confirmed, auditable operation.
