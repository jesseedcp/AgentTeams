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
`REVISION_NEEDED` or `BLOCKED`. Call `request_project_revision` to preserve the
original and create the linked rework task. Call `report_project_blocked` only
for the current assignee's report. Use `reassign_project_task` to transfer one
live task; the old assignee loses completion authority immediately.

## Material changes

Use `revise_project_plan` for a minor change: reordering work within a phase,
slightly refining scope, or adding explanatory subtasks. It versions and
exports the plan without a second tool-level gate. If the project is still
`planning`, every revision still needs the administrator's final plan
confirmation before execution starts.

Changing the project goal, overall deliverables, phase structure, more than
two assignments, participants, or forced closure is a material decision. Use
`revise_project_plan_major` so the global Admin-DM confirmation resumes the
original project room only after approval.

Use `update_project_participants` for every participant addition or removal.
It always requires global administrator confirmation, updates SQLite
participants and project metadata together, then invites or kicks the Matrix
user. A Worker with a nonterminal task cannot be removed; reassign or finish
the task first. Do not edit Matrix membership or canonical project objects
behind the workflow.

Use `delete_project` only to close. It refuses nonterminal tasks unless
`force=true`; forced close remains a confirmed, auditable operation.
