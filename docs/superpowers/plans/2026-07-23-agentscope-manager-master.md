# AgentScope Manager Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the AgentTeams Manager runtime with a directly embedded AgentScope 2.0 daemon while preserving the latest upstream Manager behavior, Controller authority, Matrix topology, MinIO artifacts, and all five Worker runtimes.

**Architecture:** One Python Manager process owns Matrix sync, room-scoped AgentScope sessions, typed tools, deterministic workflows, a SQLite operation journal, and MinIO recovery records. Controller remains authoritative for AgentTeams resources; Matrix remains authoritative for messages and rooms; MinIO remains authoritative for artifacts.

**Tech Stack:** Python 3.11+, `agentscope[s3]==2.0.4.post1`, `matrix-nio[e2e]>=0.24.0`, Pydantic 2, HTTPX, Python `sqlite3`, Go Controller, Helm, Bash, PowerShell, MinIO/S3, Matrix/Tuwunel, Higress.

## Global Constraints

- Start from AgentTeams upstream commit `0ff89f07a205b82cd81d18385c7095ec352a083f`.
- Copy a curated upstream source snapshot into `jesseedcp/AgentTeams`: retain
  Controller, Helm, installers, documentation/test foundations, Matrix,
  MinIO/OSS, Higress, Element, infrastructure assets, and all five Worker
  runtimes.
- Exclude upstream components replaced by this project before creating the
  root commit: both legacy Manager images, both legacy Manager entrypoints,
  the CoPaw Manager prompt overlay, the OpenClaw Manager config template,
  Manager supervisord/legacy-all-in-one files, and Manager-only bootstrap
  scripts. Retain shared Worker templates, the 16 Manager skill names and
  useful business-rule references, and infrastructure startup scripts. Rewrite
  every retained Manager skill document to invoke typed AgentScope tools; do
  not retain its legacy shell executor.
- Start `jesseedcp/main` from one new root commit with no parent; do not retain
  or graft the upstream Git history.
- Commit and push only to `jesseedcp/main`; do not create a feature branch.
- Pin `agentscope[s3]==2.0.4.post1`.
- Require Python `>=3.11`.
- The production Manager runtime is only `agentscope`; do not retain OpenClaw or CoPaw as alternate Manager startup paths.
- Do not use AgentScope `create_app`, Redis, AgentScope Team, or AgentScope AgentCreate.
- Call `await agent.reply_stream(...)` directly from the Matrix session runner.
- Use Python standard-library `sqlite3` with WAL; do not add a database service.
- Send all Controller-backed queries and mutations through typed `AgtClient` calls to `agt -o json`; Agent-facing tools must not call Controller HTTP directly.
- Preserve Worker runtimes `openclaw`, `copaw`, `hermes`, `qwenpaw`, and `openhuman`.
- Preserve Controller, Matrix, MinIO/OSS, Higress, Element, Worker, Team, and Human external contracts.
- Do not migrate old OpenClaw/CoPaw Manager sessions, `state.json`, or `pending-workers.json`.
- Do not add DingTalk, Feishu, QQ, or other QwenPaw-only Manager channels.
- A first release is complete only when all six subsystem plans and the complete parity matrix pass.
- No new dependency beyond the approved AgentScope package and dependencies already used by current AgentTeams Manager/CoPaw code.
- Any built-image change must update `changelog/current.md`.
- Every commit must follow the repository Lore commit protocol in `AGENTS.md`.

---

## Repository Bootstrap Before Plan 01

Before implementation, remove the excluded legacy Manager-only files, stage
the curated source plus the approved specification and these plans, and create
one parentless root commit with `git write-tree` and `git commit-tree`. Update
local `main`, then replace `jesseedcp/main` with
`git push --force-with-lease jesseedcp main`.

The root snapshot excludes exactly:

```text
manager/Dockerfile
manager/Dockerfile.copaw
manager/README.md
manager/supervisord.conf
manager/docker-legacy/
manager/scripts/init/start-manager-agent.sh
manager/scripts/init/start-copaw-manager.sh
manager/scripts/init/setup-higress.sh
manager/scripts/init/upgrade-builtins.sh
manager/agent/copaw-manager-agent/
manager/configs/manager-openclaw.json.tmpl
manager/agent/skills-alpha/
manager/agent/state.json
manager/agent/workers-registry.json
manager/agent/skills/*/scripts/
manager/agent/skills/worker-management/references/worker-openclaw.json.tmpl
```

Ignore a listed path that does not exist at the pinned upstream commit.
Retain `manager/agent/skills/*/SKILL.md`, their useful references and MCP YAML
templates, `manager/agent/worker-skills/`, every Worker agent directory,
`manager/scripts/lib/`, and the infrastructure scripts copied by the embedded
Controller image. A retained skill reference is useful only when it describes
an external contract, safety rule, or workflow invariant. Rewrite or remove
references that prescribe OpenClaw/CoPaw commands, direct shell scripts,
legacy JSON registries, or hand-authored Worker runtime payloads.

Verify:

```bash
git rev-list --max-parents=0 HEAD
git cat-file -p HEAD
git merge-base --is-ancestor 0ff89f07a205b82cd81d18385c7095ec352a083f HEAD
git ls-remote --heads jesseedcp main
```

Expected: exactly one root commit; `HEAD` has no `parent` line; the
`merge-base --is-ancestor` command exits nonzero; remote `main` equals local
`HEAD`. This history replacement is performed once, before Plan 01. All later
implementation commits are normal, non-force pushes on this new project
history.

---

## Upstream Evidence

Implementation must continually compare behavior with these paths at the
pinned upstream commit. Excluded files remain reference evidence at
`agentscope-ai/AgentTeams@0ff89f07a205b82cd81d18385c7095ec352a083f`;
they are not copied into the new root snapshot:

- `manager/agent/AGENTS.md`
- `manager/agent/HEARTBEAT.md`
- `manager/agent/skills/*/SKILL.md`
- `manager/agent/skills/task-management/references/finite-tasks.md`
- `manager/agent/skills/task-management/references/infinite-tasks.md`
- `manager/agent/skills/project-management/scripts/create-project.sh`
- `manager/agent/skills/task-management/scripts/manage-state.sh`
- `copaw/src/matrix/channel.py`
- `agentteams-controller/cmd/agt/*.go`
- `agentteams-controller/internal/controller/manager_*.go`
- `agentteams-controller/internal/service/deployer.go`
- `tests/test-01-manager-boot.sh` through `tests/test-26-qwenpaw-teamharness-plugin-mode.sh`

## Locked File Structure

```text
manager-agentscope/
├── pyproject.toml
├── src/agentteams_manager/
│   ├── __init__.py
│   ├── main.py
│   ├── application.py
│   ├── config.py
│   ├── health.py
│   ├── domain/
│   │   ├── errors.py
│   │   ├── ids.py
│   │   ├── models.py
│   │   └── ports.py
│   ├── state/
│   │   ├── database.py
│   │   ├── schema.py
│   │   ├── operations.py
│   │   ├── sessions.py
│   │   ├── tasks.py
│   │   ├── topology.py
│   │   ├── journal.py
│   │   └── recovery.py
│   ├── runtime/
│   │   ├── agent_factory.py
│   │   ├── config_watcher.py
│   │   ├── event_stream.py
│   │   ├── permissions.py
│   │   ├── session_manager.py
│   │   └── skills.py
│   ├── matrix/
│   │   ├── client.py
│   │   ├── crypto.py
│   │   ├── media.py
│   │   ├── policy.py
│   │   ├── router.py
│   │   └── threads.py
│   ├── clients/
│   │   ├── agt.py
│   │   ├── higress.py
│   │   ├── minio.py
│   │   ├── nacos.py
│   │   └── process.py
│   ├── tools/
│   │   ├── base.py
│   │   ├── configuration.py
│   │   ├── integrations.py
│   │   ├── resources.py
│   │   ├── storage.py
│   │   └── tasks.py
│   ├── workflows/
│   │   ├── heartbeat.py
│   │   ├── integrations.py
│   │   ├── notifications.py
│   │   ├── projects.py
│   │   ├── resources.py
│   │   ├── supervisor.py
│   │   └── tasks.py
│   └── observability/
│       ├── logging.py
│       └── metrics.py
└── tests/
    ├── unit/
    ├── contract/
    ├── integration/
    ├── fault_injection/
    └── fixtures/
```

Files remain responsibility-focused. No module may import the Matrix `nio.AsyncClient` outside `matrix/`, invoke `agt` outside `clients/agt.py`, or access SQLite outside `state/`.

## Cross-Plan Interfaces

The Runtime Core plan owns the following signatures. Later plans consume them without renaming:

```python
class MatrixPort(Protocol):
    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str: ...

class MatrixAdministrationPort(Protocol):
    async def create_private_room(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        creation_marker: dict[str, str | int],
    ) -> str: ...
    async def joined_rooms(self) -> tuple[str, ...]: ...
    async def members(self, room_id: str) -> tuple[str, ...]: ...
    async def invite_user(self, room_id: str, user_id: str) -> None: ...
    async def kick_user(
        self, room_id: str, user_id: str, *, reason: str
    ) -> None: ...
    async def ban_user(
        self, room_id: str, user_id: str, *, reason: str
    ) -> None: ...
    async def unban_user(self, room_id: str, user_id: str) -> None: ...

class ArtifactPort(Protocol):
    async def put_bytes(
        self, key: str, data: bytes, *, content_type: str
    ) -> ObjectReceipt: ...
    async def get_bytes(self, key: str) -> bytes: ...
    async def mirror_down(
        self, prefix: str, destination: Path
    ) -> MirrorReceipt: ...
    async def mirror_up(self, source: Path, prefix: str) -> MirrorReceipt: ...

class ControllerPort(Protocol):
    async def get_worker(self, name: str) -> WorkerResource | None: ...
    async def list_workers(self) -> tuple[WorkerResource, ...]: ...
    async def get_team(self, name: str) -> TeamResource | None: ...

class OperationSupervisor:
    async def begin(
        self,
        *,
        operation_id: str,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> OperationRecord: ...
    async def before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> JournalEvent: ...
    async def effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...
    async def recover_all(self) -> RecoveryReport: ...
```

Stable identifiers:

```text
session_id  = matrix:<room_id>
operation_id = sha256(room_id + NUL + event_id + NUL + tool_call_id)[:32]
matrix txn_id = agentteams:<operation_id>:<effect-sequence>
task_id = task-YYYYMMDD-HHMMSS-<6-char-suffix>
project_id = project-YYYYMMDD-HHMMSS-<6-char-suffix>
```

## Plan Order and Release Gates

```text
01 Runtime Core
       │
       ├──────────────┐
       ▼              ▼
02 Matrix        03 Resources
       │              │
       └──────┬───────┘
              ▼
        04 Tasks/Projects
              │
              ▼
        05 Integrations
              │
              ▼
        06 Deployment/Parity
```

Plans 02 and 03 may be implemented after Plan 01 and can proceed independently. Plan 04 requires both. Plan 05 requires Runtime Core and resource/config clients. Plan 06 consumes all other plans.

| Gate | Required evidence |
| --- | --- |
| Core gate | Unit tests prove SQLite recovery, operation transitions, AgentState persistence, permission checks, and health lifecycle. |
| Transport gate | Matrix contract tests prove sync resume, event deduplication, E2EE, media, threads, mentions, and confirmation continuation. |
| Resource gate | Fake-`agt` and Controller integration tests prove Worker, Team, Human, topology, and Nacos import workflows. |
| Task gate | MinIO/Matrix fault injection proves prepared-before-dispatch, completion recovery, recurring schedules, projects, and Git leases. |
| Integration gate | Model switching, MCP discovery/calls, service publishing, file sync, and Matrix-only channel policy tests pass. |
| Release gate | New image boots without OpenClaw/CoPaw Manager, five Worker runtimes pass, Helm/install paths pass, all 16 skills have parity tests. |

## Skill Coverage Matrix

| Upstream skill | Owning plan | Acceptance test |
| --- | --- | --- |
| `agentteams-find-worker` | 03 Resources | Search, confirm, import, and failed-import-no-fallback |
| `channel-management` | 03 Resources + 05 Integrations | Admin/Worker/Human/trusted/unknown policy and Matrix notification fallback |
| `file-sync-management` | 04 Tasks/Projects | Explicit push/pull, checksum, Worker notification |
| `git-delegation-management` | 04 Tasks/Projects | Lease, allowed Git argv, high-risk confirmation, result message |
| `human-management` | 03 Resources | Permission levels 1/2/3 and room scope |
| `matrix-server-management` | 02 Matrix + 03 Resources | User/room/member/media operations |
| `mcp-server-management` | 05 Integrations | Create/update/proxy/access replacement/end-to-end verification |
| `mcporter` | 05 Integrations | AgentScope MCP discovery and tool call |
| `model-switch` | 05 Integrations | Preflight, Controller update, next-turn hot reload |
| `project-management` | 04 Tasks/Projects | Project room, metadata, confirmation, task lifecycle |
| `service-publishing` | 05 Integrations | Replace exposed ports through `agt update`, status verification |
| `task-coordination` | 04 Tasks/Projects | Expiring processing lease and conflict refusal |
| `task-management` | 04 Tasks/Projects | Finite/infinite dispatch, progress, completion, recovery |
| `team-management` | 03 Resources | Controller team creation, topology validation, Leader-only delegation |
| `worker-management` | 03 Resources | Create/list/update/stop/delete, pending reconciliation, greeting |
| `worker-model-switch` | 05 Integrations | `agt update worker`, Controller convergence |

## Subsystem Plans

1. [`2026-07-23-agentscope-manager-01-runtime-core.md`](2026-07-23-agentscope-manager-01-runtime-core.md)
2. [`2026-07-23-agentscope-manager-02-matrix.md`](2026-07-23-agentscope-manager-02-matrix.md)
3. [`2026-07-23-agentscope-manager-03-resources.md`](2026-07-23-agentscope-manager-03-resources.md)
4. [`2026-07-23-agentscope-manager-04-tasks-projects.md`](2026-07-23-agentscope-manager-04-tasks-projects.md)
5. [`2026-07-23-agentscope-manager-05-integrations.md`](2026-07-23-agentscope-manager-05-integrations.md)
6. [`2026-07-23-agentscope-manager-06-deployment-parity.md`](2026-07-23-agentscope-manager-06-deployment-parity.md)

## Commit and Review Contract

Each numbered task produces one reviewable commit. Use this Lore structure:

```text
<intent line explaining why>

<short rationale>

Constraint: <relevant fixed architecture constraint>
Rejected: <alternative> | <reason>
Confidence: high
Scope-risk: narrow|moderate|broad
Directive: <future-maintainer warning>
Tested: <exact commands>
Not-tested: <remaining higher-level verification>
```

After every task:

1. Run the task-specific test.
2. Run the subsystem test directory.
3. Run `git diff --check`.
4. Review only the task diff.
5. Commit using the Lore format.

## Final Verification

Run from the repository root:

```bash
python -m pytest manager-agentscope/tests/unit -q
python -m pytest manager-agentscope/tests/contract -q
python -m pytest manager-agentscope/tests/integration -q
python -m pytest manager-agentscope/tests/fault_injection -q
python -m compileall -q manager-agentscope/src
go test ./... 
bash tests/check-helm-agentteams.sh
bash tests/check-agentteams-rename-defaults.sh
bash manager/tests/smoke-test.sh
bash tests/run-all-tests.sh
git diff --check
```

The Go command runs from `agentteams-controller/`; all others run from the repository root. The full shell E2E suite requires the documented Docker/Podman test environment.

The release is blocked if any of the following remain:

- A Manager process launches OpenClaw Gateway or CoPaw App.
- A Controller-backed tool bypasses `AgtClient`.
- A mutating tool is visible in an unauthorized room.
- A crash boundary can create an uncontrolled duplicate Worker, Team, task notification, or Matrix assignment.
- Any of the 16 skill acceptance tests is absent.
- Any Worker runtime in the five-runtime matrix is skipped without a recorded environment failure.
