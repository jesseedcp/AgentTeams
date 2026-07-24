# AgentTeams Manager Repository Guide

This repository contains a Controller-managed Agent Teams platform with one
AgentScope Manager and five independently selectable Worker runtimes.

## Runtime contract

- Manager runtime: `agentscope` only, pinned to AgentScope `2.0.4.post1`.
- Worker runtimes: `openclaw`, `copaw`, `hermes`, `qwenpaw`, `openhuman`.
- Controller is authoritative for Manager, Worker, Team, and Human resources.
- Matrix is authoritative for conversations, rooms, membership, and media.
- Higress is authoritative for model, MCP, consumer, and service routes.
- MinIO/S3 is authoritative for remote journals, snapshots, and artifacts.
- SQLite WAL is the active Manager's transactional session/operation store.

Do not reintroduce OpenClaw or CoPaw as Manager startup branches. Their code and
images remain only where required by Worker runtimes.

## Repository map

```text
agentteams-controller/   Go Controller, CRDs, REST API, agt CLI
manager-agentscope/      AgentScope Manager Python package and tests
manager/                 Manager image, entrypoint, prompts, 16 skills
worker/                  OpenClaw Worker image
copaw/                   CoPaw Worker package/image
hermes/                  Hermes Worker package/image
qwenpaw/                 QwenPaw Worker image
openhuman/               OpenHuman Worker image
openclaw-base/           Shared OpenClaw Worker base
helm/agentteams/         Kubernetes chart and mirrored CRDs
install/                 Bash and PowerShell embedded installers
scripts/                 Replay, debug export, and maintenance utilities
tests/                   Shell integration and release checks
docs/                    English documentation
docs/zh-cn/              Chinese documentation
.github/workflows/       Build, integration, and release automation
```

## Manager architecture

Matrix events are converted to room-scoped AgentScope turns. Each turn receives
only the tools permitted by the room policy. AgentScope `reply_stream` owns the
reasoning/tool loop; typed workflows own sequencing, confirmation, persistence,
idempotency, retries, recovery, and external side effects.

Python reaches the Controller only through `clients/agt.py`. The client invokes
the typed `agt` CLI with exact argv values and validates JSON responses. Do not
add raw HTTP calls or generated management shell commands in workflows.

Mutations must:

1. derive a stable operation ID from the Matrix event and tool call;
2. persist intent before an external effect;
3. append only redacted events to the remote journal;
4. treat timeouts as ambiguous;
5. verify desired external state before returning success;
6. provide a recovery path in the appropriate `resume_operation`.

## Agent-facing content

The Manager prompt root is `manager/agent/`:

- `AGENTS.md` — operating contract;
- `SOUL.md` — identity and delegation behavior;
- `HEARTBEAT.md` — periodic reconciliation guidance;
- `TOOLS.md` — tool boundary;
- `skills/<name>/SKILL.md` — the 16 retained Manager skills.

Skills are guidance, not executable capabilities. Any capability change must
update the registered typed tool, room policy, deterministic workflow, recovery
handler, tests, and `tests/manager-skill-parity.json`.

Identity is Controller desired state. After administrator confirmation,
`update_manager_identity` writes `Manager.spec.identity`; the Controller merges
only the SOUL identity section and publishes a higher runtime revision. Never
write SOUL or an onboarding marker directly.

## Important entry points

### AgentScope Manager

- `manager-agentscope/src/agentteams_manager/bootstrap.py`
- `manager-agentscope/src/agentteams_manager/matrix/session_runner.py`
- `manager-agentscope/src/agentteams_manager/matrix/policy.py`
- `manager-agentscope/src/agentteams_manager/tools/`
- `manager-agentscope/src/agentteams_manager/workflows/`
- `manager-agentscope/src/agentteams_manager/state/`

### Controller

- `agentteams-controller/api/v1beta1/types.go`
- `agentteams-controller/internal/controller/`
- `agentteams-controller/internal/service/`
- `agentteams-controller/internal/server/`
- `agentteams-controller/cmd/agt/`

### Packaging and deployment

- `manager/Dockerfile`
- `manager/entrypoint.sh`
- `Makefile`
- `helm/agentteams/`
- `install/agentteams-install.sh`
- `install/agentteams-install.ps1`

## Build and verification

```bash
# Manager Python suite
python -m pytest -q manager-agentscope/tests

# Controller
cd agentteams-controller
go test ./...

# Release-support checks
bash tests/check-agentteams-rename-defaults.sh
bash tests/install/test-agentscope-manager-install.sh
bash tests/check-helm-agentteams.sh

# Built Manager image parity (requires Docker and the local image)
bash tests/test-28-agentscope-manager-parity.sh

# Full image set: one Manager + five Worker runtimes + Controller
make build
```

On Windows without a C compiler, the Controller-wide `go test ./...` may fail
in kine's CGO SQLite package. Run unaffected packages locally and rely on the
Linux CI job for the CGO-dependent package; do not hide unrelated failures.

Before completion, also run `git diff --check`, parse modified YAML/JSON, check
shell syntax, and verify the two CRD copies remain synchronized.

## Change rules

- Preserve unrelated user changes.
- Keep secrets out of prompts, logs, SQLite, journals, CRs, and runtime
  documents.
- Do not give Workers the Manager's GitHub token.
- Use standard-library SQLite; do not add Redis unless the single-writer
  architecture changes and a measured requirement justifies it.
- Keep active turns stable during model, MCP, prompt, and identity reloads.
- Add dependencies only when the existing stack cannot safely provide the
  required behavior.
- Update English and Chinese user documentation together.
- Record image-affecting release notes in `changelog/current.md`.

## Further reading

- `docs/architecture.md`
- `docs/manager-guide.md`
- `docs/development.md`
- `docs/superpowers/specs/2026-07-23-agentscope-manager-design.md`
- `tests/manager-skill-parity.json`
