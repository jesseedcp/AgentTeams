# Create a Project

1. Decompose the goal into a directed acyclic task graph. Each task needs a
   deliverable, assignee, dependencies, and acceptance criteria.
2. Resolve Workers with the resource read tools. Do not invent a Worker,
   Team, room, or Matrix identity.
3. Draft the complete plan, including phases, task deliverables, assignees,
   dependencies, and acceptance criteria.
4. Call `create_project` with the title, description, complete plan body, and
   participant names.
5. In every non-YOLO mode, including `/elevated full`, verify the receipt is
   `planning`, present the plan in the Admin DM, ask for explicit confirmation,
   and stop the turn. A room existing does not mean the plan is approved.
6. When a later administrator message confirms, call
   `confirm_project_plan`. Verify the receipt is `active`.
7. Only then add the first graph nodes. Call `update_project` only for nodes
   whose dependencies are terminal.

If the administrator requests changes while the project is `planning`, revise
and version the plan, present the new revision, and wait again.

`/elevated full` does not change this plan gate. Only configured YOLO mode
makes `create_project` automatically perform Step 6 and return `active`; send
one informational notice naming the YOLO policy source instead of asking a
question. If the administrator explicitly requests a `planning` result while
YOLO is configured, report the conflict before calling `create_project`.

Creation is ordered: SQLite preparation, verified MinIO metadata and plan,
private room reconciliation by immutable marker, membership verification, and
topology publication in `planning`. Confirmation is a separate journaled
operation that publishes `confirmed_at`, `confirmed_by`, and the active plan.

If room creation times out, do not create another room blindly. Recovery finds
the exact immutable project marker and continues with the existing room.
