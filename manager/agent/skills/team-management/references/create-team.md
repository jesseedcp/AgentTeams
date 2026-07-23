# Create a Team

Call `create_team` with one Leader and zero or more Workers:

```json
{
  "name": "alpha",
  "description": "Release engineering",
  "leader": {
    "name": "alpha-lead",
    "runtime": "qwenpaw",
    "model": "qwen3.6-plus"
  },
  "workers": [
    {
      "name": "researcher",
      "runtime": "hermes",
      "model": "qwen3.6-plus"
    },
    {
      "name": "coder",
      "runtime": "openclaw",
      "model": "qwen3.6-plus",
      "skills": ["github-operations"]
    }
  ],
  "leader_heartbeat_every": "30m",
  "worker_idle_timeout": "12h"
}
```

Names must be unique. Leader skills are package-managed; Worker skills may be
declared explicitly. Each member may also declare image, identity, soul, or a
package URI.

The workflow chooses the simple Controller create form only when the request is
representable there; otherwise it applies the full v1beta1 Team document. It
then waits for every member, validates Matrix membership, refreshes topology,
and returns the Leader Room.

Use `get_team` after creation for a fresh status view. Never send instructions
to Team Workers from the Manager.
