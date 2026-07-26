# Create a Team

Create and configure each Worker separately with `create_worker` or
`update_worker`. After every referenced Worker exists, call `create_team`:

```json
{
  "name": "alpha",
  "description": "Release engineering",
  "leader_name": "alpha-lead",
  "worker_names": ["researcher", "coder"],
  "heartbeat_every": "30m",
  "admin_name": "reviewer",
  "admin_matrix_id": "@reviewer:example.com",
  "peer_mentions": true
}
```

Names must be unique. The Leader must not also appear in `worker_names`. If any
Worker is missing, the workflow reports the complete missing-name list and
does not create a partial Team.

The workflow sends only the current `workerMembers` contract, waits for Team
readiness, refreshes topology, and returns the Leader Room.

Use `get_team` after creation for a fresh status view. Configure Workers only
through Worker tools and never send instructions to Team Workers directly.
