# Create a Worker

Use this path for a hand-authored Worker. For a discovered or direct Nacos
package, use the registry import workflow.

## Gather the desired state

Ask for or derive only from explicit administrator instructions:

- `name`: lowercase resource identity
- `runtime`: `openclaw`, `copaw`, `hermes`, `qwenpaw`, or `openhuman`
- `model`: non-empty model identifier
- `identity` and `soul`: optional persona and operating instructions
- `skills`: the complete desired skill-name array
- `image`: optional approved runtime image
- `package_uri`: optional verified package reference
- `expose`: desired service-port array
- `team` and `role`: optional initial Team placement; role is `team_leader`
  or `worker`

Do not choose a runtime from a stale default. If the administrator did not
specify one, present the five supported choices.

## Create

Call `create_worker` once with the complete typed request:

```json
{
  "name": "researcher",
  "runtime": "copaw",
  "model": "qwen3.5-plus",
  "identity": "Evidence-focused research Worker",
  "soul": "Verify sources, state uncertainty, and keep handoffs concise.",
  "skills": ["web-research"],
  "expose": [],
  "team": "analysis",
  "role": "worker"
}
```

The tool rejects unknown fields. It durably records the operation, asks the
Controller to create the Worker, waits for the Worker room, refreshes
topology, and sends an idempotent greeting. Its successful receipt is the
completion signal; no local follow-up queue is required.

If the result says the resource already exists, use `get_worker`. Apply an
intentional configuration change with `update_worker`; never duplicate the
create request as a retry.

## Report

Return the Worker's name, runtime, phase, room identifier when available, and
the operation result. Tell the administrator that tasks must target the
Worker or its Team explicitly.
