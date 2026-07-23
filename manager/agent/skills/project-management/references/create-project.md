# Create a Project

1. Decompose the goal into a directed acyclic task graph. Each task needs a
   deliverable, assignee, dependencies, and acceptance criteria.
2. Resolve Workers with the resource read tools. Do not invent a Worker,
   Team, room, or Matrix identity.
3. Present material scope or participant choices to the admin when policy
   requires confirmation.
4. Call `create_project` with the title, description, complete plan body, and
   participant names.
5. Use `get_project` to confirm that the returned project is active and has a
   room ID.
6. Call `update_project` only for graph nodes whose dependencies are terminal.

Creation is ordered: SQLite preparation, verified MinIO metadata and plan,
private room reconciliation by immutable marker, membership verification,
topology update, then active metadata publication.

If room creation times out, do not create another room blindly. Recovery finds
the exact immutable project marker and continues with the existing room.
