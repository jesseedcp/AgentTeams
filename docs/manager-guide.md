# AgentScope Manager Guide

The AgentTeams Manager is a long-running Python service built on
**AgentScope 2.0.4.post1**. AgentScope owns the model/tool loop; deterministic
workflows own persistence, retries, reconciliation, authorization, and
external side effects.

There is one Manager runtime: `agentscope`. OpenClaw, CoPaw, Hermes, QwenPaw,
and OpenHuman are Worker runtimes.

## Runtime boundaries

| System | Authoritative state |
|---|---|
| Controller | Manager, Worker, Team, and Human desired state |
| Matrix | rooms, membership, messages, threads, and media |
| SQLite WAL | active sessions, operation journal, schedules, topology cache |
| MinIO/S3 | immutable operation events, verified snapshots, task/project artifacts |
| Higress | model routes, MCP servers/consumers, published service routes |

The Manager never mutates Controller resources with ad-hoc shell commands.
All Controller access goes through the typed `AgtClient`. Matrix turns call
AgentScope `reply_stream`, and runtime changes activate only between turns.

## Installation settings

The installers write `agentteams-manager.env` and force
`AGENTTEAMS_MANAGER_RUNTIME=agentscope`.

| Variable | Default | Purpose |
|---|---|---|
| `AGENTTEAMS_WORKSPACE_DIR` | `~/agentteams-manager` | Host persistence mounted at `/var/lib/agentteams-manager` |
| `AGENTTEAMS_DEFAULT_WORKER_RUNTIME` | `openclaw` | Default Worker runtime |
| `AGENTTEAMS_GITHUB_TOKEN` | empty | Optional GitHub PAT; normalized to `AGENTTEAMS_MCP_GITHUB_TOKEN` |
| `AGENTTEAMS_MATRIX_E2EE` | installer choice | Enables encrypted Matrix sessions |
| `AGENTTEAMS_YOLO` | unset | `1` bypasses interactive confirmation for trusted unattended runs |
| `AGENTTEAMS_MATRIX_DEBUG` | unset | `1` enables additional structured Matrix traces |

For Helm, use `manager.runtime=agentscope` and optionally
`credentials.githubToken`. The token is stored in the runtime Secret, consumed
by the Controller, and injected into the Manager only. Worker resources and
runtime documents never contain it.

## First conversation and identity

On a fresh install, the Manager asks the administrator for:

1. the Manager's name;
2. communication style;
3. behavior guidelines;
4. default language.

After showing the complete proposal and receiving confirmation, the Manager
calls `update_manager_identity`. The Controller stores the identity in
`Manager.spec.identity`, merges only the `Identity & Personality` section of
SOUL, publishes a higher runtime revision, and the Manager hot-reloads it
between turns. It never writes `SOUL.md` or a marker file directly.

Existing OpenClaw/CoPaw Manager sessions are intentionally not imported.
Controller resources and remote artifacts remain available.

## Room authorization

The tool set is constructed separately for every Matrix room:

- **Admin DM**: full Manager capability; risky mutations require confirmation.
- **Worker room**: scoped task and Worker communication.
- **Leader room**: Team Leader delegation and Team-scoped inspection.
- **Project room**: project tasks, members, and artifacts only.
- **Human/channel room**: Controller Human permission scope.
- **Unknown room**: no management mutation tools.

The model cannot recover a hidden tool through MCP, generated commands, or a
different room. Authorization is checked again at tool invocation time.

## Capabilities

The 16 retained Manager skills cover Worker discovery/import and lifecycle,
Team and Human management, tasks and recurring schedules, projects, channels,
Matrix administration, file synchronization, Git delegation, model changes,
MCP management, service publishing, and task coordination.

The machine-readable coverage map is
[`tests/manager-skill-parity.json`](../tests/manager-skill-parity.json). Every
listed skill maps to registered AgentScope tools and executable evidence.

Supported Worker runtimes:

| Runtime | Value |
|---|---|
| OpenClaw | `openclaw` |
| CoPaw compatibility runtime | `copaw` |
| Hermes | `hermes` |
| QwenPaw | `qwenpaw` |
| OpenHuman | `openhuman` |

## Model and MCP management

`switch_model` first probes the requested model through the OpenAI-compatible
gateway. Failed preflight does not change desired state. A successful update
waits for a higher Controller runtime revision; the current turn finishes on
its original model.

GitHub MCP can be bootstrapped with the optional installation token. The
Controller reconciles the secret into Higress and publishes only a secret-free
descriptor to the Manager. Dynamic MCP tools use AgentScope's native
`MCPRegistry`; Worker MCP consumers remain independently authorized.

## Persistence and recovery

The local database is:

```text
/var/lib/agentteams-manager/state/manager.db
```

It uses standard-library SQLite in WAL mode. This is deliberate: there is one
active Manager writer, so Redis would add a network dependency and another
failure mode without improving correctness.

Before external side effects, the workflow records intent locally and appends
redacted immutable events under `manager/journal/` in object storage.
Checksummed SQLite snapshots are published under `manager/snapshots/`.
At startup, the Manager restores the latest verified snapshot, replays later
events, and reconciles incomplete operations against external facts. A timeout
is therefore treated as ambiguous rather than blindly retried.

## Health and diagnostics

The Manager exposes operational HTTP on container port `18799`:

- `GET /healthz` — process liveness;
- `GET /readyz` — dependency and runtime readiness;
- `GET /metrics` — Prometheus text metrics.

Embedded installs bind this to loopback host port `18888` by default. It is a
health/metrics endpoint, not an OpenClaw console.

```bash
docker logs agentteams-manager -f
curl -fsS http://127.0.0.1:18888/readyz
curl -fsS http://127.0.0.1:18888/metrics
python scripts/export-debug-log.py --range 1h --container agentteams-manager
```

The debug exporter reads AgentScope session rows through SQLite read-only mode,
emits JSONL, and redacts credentials and PII by default. It also understands
the supported Worker session layouts.

## Operating notes

- Change identity with `update_manager_identity`, not by editing SOUL.
- Change the model with `switch_model`, not by editing provider files.
- Use `AGENTTEAMS_YOLO=1` only in a trusted, isolated environment; there is no
  runtime marker-file toggle.
- Keep the Manager workspace private. It contains Matrix E2EE material and
  active SQLite state.
- Back up the object-storage bucket and host workspace together when taking an
  operator-level backup.

For exact health checks, corrupt-database recovery, secret rotation, and
runtime-revision troubleshooting, see
[`agentscope-manager-operations.md`](agentscope-manager-operations.md).
