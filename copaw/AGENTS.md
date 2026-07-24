# CoPaw Worker Subsystem Guide

Read the root [AGENTS.md](../AGENTS.md) first. This file adds rules only for
the `copaw/` Worker runtime.

## Scope and architecture boundary

CoPaw is one of the five supported Worker runtimes. It is not a Manager
runtime. The Manager is always the AgentScope 2.0 service under
`manager-agentscope/`; do not add CoPaw imports, startup branches, images, or
prompt overlays to the Manager.

This subtree owns:

- the `copaw-worker` Python package;
- its standard and lite runtime modes;
- the CoPaw Worker Docker image and entrypoint;
- the Worker-only Matrix channel overlay;
- translation of Controller-generated Worker configuration into CoPaw files;
- CoPaw Worker tests.

OpenClaw, Hermes, QwenPaw, and OpenHuman Workers live in their respective
runtime paths. Manager tools and policy live under `manager-agentscope/`.

## Runtime data flow

The Controller is authoritative for Worker identity, model, Matrix policy,
skills, and lifecycle. It publishes a Worker package to MinIO:

```text
Controller
  -> agents/<worker>/openclaw.json + prompts + skills + credentials
  -> CoPaw Worker startup mirror
  -> bridge.py converts structured configuration
  -> .copaw/config.json, providers.json, and agent.json
  -> CoPaw runtime
```

`openclaw.json` is retained as the Controller's cross-runtime Worker
configuration format. Its presence here does not make OpenClaw or CoPaw a
Manager dependency.

Use these terms consistently:

| Term | Direction | Responsibility |
|---|---|---|
| `pull` / `push` | MinIO ↔ standard workspace | Durable Worker files |
| `bridge` | `openclaw.json` → CoPaw JSON | Structured runtime configuration |
| `propagate` | standard workspace → CoPaw workspace | Prompts, skills, MCP config |
| `save` | CoPaw workspace → standard workspace | Worker-created memory/session data |

The standard workspace is `/root/.agentteams-worker/<name>/`. CoPaw runtime
files live below its `.copaw/` directory.

## Key files

```text
copaw/
├── Dockerfile
├── pyproject.toml
├── scripts/
│   ├── copaw-worker-entrypoint.sh
│   └── patch_*_lazy.py
├── src/
│   ├── copaw_worker/
│   │   ├── cli.py
│   │   ├── config.py
│   │   ├── sync.py
│   │   ├── bridge.py
│   │   └── worker.py
│   └── matrix/
└── tests/

manager/agent/copaw-worker-agent/   # Controller-published Worker prompts
```

- `bridge.py` owns Controller-to-CoPaw field conversion.
- `sync.py` owns MinIO and local persistence boundaries.
- `worker.py` owns Worker startup and runtime orchestration.
- `src/matrix/` is overlaid only into CoPaw Worker environments.
- `manager/agent/copaw-worker-agent/` is agent-facing content; write it in the
  second person. Developer documentation stays in the third person.

## Bridge invariants

`bridge()` writes CoPaw configuration and also adjusts CoPaw module paths and
environment variables because upstream resolves several paths at import time.
Tests that bridge multiple workspaces in one process must reset those globals.

The Controller owns model routing, credentials, Matrix policy, and generated
prompt sections. Preserve user-owned runtime data, but do not let a Worker
overwrite Controller-owned fields during a restart or push.

Do not move cross-runtime merge logic into the Worker. If the rule must be
identical for multiple Worker runtimes, implement it in the Controller and
test the generated package there.

## Build and test

Use the repository's normal targets:

```bash
cd copaw
pytest

cd ..
make build-copaw-worker
```

For a local Kubernetes deployment:

```bash
kind load docker-image agentteams/copaw-worker:latest --name agentteams
kubectl patch worker <name> -n agentteams --subresource=status \
  --type merge -p '{"status":{"phase":"Pending"}}'
```

Manager runtime selection is not part of these commands.
`AGENTTEAMS_MANAGER_RUNTIME` must remain `agentscope`.

## Debugging order

For a non-responsive CoPaw Worker, check:

1. Controller Worker status and generated MinIO package.
2. Worker startup logs and selected standard/lite mode.
3. bridged `.copaw/config.json` and workspace `agent.json`.
4. Matrix sync/allow-list decisions in Worker logs.
5. Higress model request and response.
6. Matrix outbound event.

Useful paths:

| Purpose | Path |
|---|---|
| Controller-format config | `/root/.agentteams-worker/<name>/openclaw.json` |
| CoPaw channel/security config | `/root/.agentteams-worker/<name>/.copaw/config.json` |
| Active agent config | `/root/.agentteams-worker/<name>/.copaw/workspaces/default/agent.json` |
| Matrix sync token | `/root/.agentteams-worker/<name>/.copaw/workspaces/default/matrix_sync_token` |
| Worker log | `/root/.agentteams-worker/logs/` |

Remote Controller configuration is applied by restarting the Worker and
running the startup mirror/bridge path. Shared files may also be synchronized
explicitly. Never diagnose Manager behavior from these Worker-local files.

## Change discipline

- Keep standard and lite modes working unless the task explicitly narrows
  runtime support.
- Rebuild the CoPaw Worker image after changes to dependencies, Dockerfile,
  entrypoint, patches, or Matrix overlay.
- Add regression tests for bridge, sync, or workspace changes before editing
  implementation.
- Do not reintroduce `manager-copaw`, `Dockerfile.copaw`,
  `copaw-manager-agent`, or a CoPaw Manager session format.
- Preserve the typed AgentScope Manager ↔ Controller boundary.
