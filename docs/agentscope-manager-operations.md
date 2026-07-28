# AgentScope Manager Operations

This runbook covers the production `agentscope` Manager. OpenClaw, CoPaw,
Hermes and QwenPaw are Worker runtimes and have separate
operational state.

## Service checks

The Manager exposes operational HTTP on container port `18799`. Embedded
installs bind it to loopback port `18888` by default.

```bash
curl -fsS http://127.0.0.1:18888/healthz
curl -fsS http://127.0.0.1:18888/readyz
curl -fsS http://127.0.0.1:18888/metrics
docker logs --tail 200 agentteams-manager
```

`/healthz` only proves that the event loop is alive. Treat the Manager as
available only when `/readyz` reports the database, recovery, runtime
configuration, Matrix transport, and heartbeat as ready.

Useful metrics include:

- `agentteams_manager_runtime_revision`;
- `agentteams_manager_runtime_reloads_total`;
- `agentteams_manager_matrix_events_total`;
- `agentteams_manager_tool_calls_total`;
- `agentteams_manager_recovery_reconciled_total`;
- `agentteams_manager_recovery_errors_total`.

Use the redacting exporter when collecting a support bundle:

```bash
python scripts/export-debug-log.py \
  --range 1h \
  --container agentteams-manager
```

The exporter writes a timestamped bundle below `debug-log/`. Review the output
before sharing it. Redaction is a safety layer, not a substitute for handling
the bundle as sensitive data.

## State and backup boundaries

The active database is:

```text
<AGENTTEAMS_WORKSPACE_DIR>/state/manager.db
```

SQLite WAL is the local transactional authority. MinIO/S3 stores immutable
operation events at `manager/journal/` and checksummed database snapshots at
`manager/snapshots/`. Matrix remains authoritative for messages and rooms;
the Controller remains authoritative for Manager, Worker, Team, and Human
desired state.

Back up both the Manager workspace and object-storage bucket. A workspace-only
backup lacks the remote operation journal; an object-storage-only backup may
restore to the latest published snapshot rather than the latest local
conversation state.

## Recover a corrupt local SQLite database

Do not delete the object-storage journal or disable snapshot checksum
validation.

1. Stop the Manager and save diagnostics:

   ```bash
   docker logs agentteams-manager > manager-before-recovery.log 2>&1
   docker stop agentteams-manager
   ```

2. Resolve the exact workspace path from the protected
   `agentteams-manager.env` file. Confirm it is the intended Manager workspace
   before moving anything.

3. Create a timestamped quarantine directory beside `state/` and move only
   these files into it when present:

   ```text
   state/manager.db
   state/manager.db-wal
   state/manager.db-shm
   ```

   Keep the quarantined files until recovery has been verified. Do not remove
   the workspace, Matrix E2EE directory, object-storage bucket, or Controller
   data.

4. Verify that `manager/snapshots/latest.json` and the snapshot object named
   by its `key` field exist in the configured bucket. The Manager validates
   both byte length and SHA-256 before using the snapshot.

5. Start the Manager through the same installer, Compose, or Kubernetes
   deployment that created it. Startup creates a clean local schema, restores
   the latest verified snapshot, replays later immutable journal events, and
   reconciles incomplete effects against Controller, Matrix, MinIO, and
   Higress facts.

6. Wait for `/readyz`, then inspect recovery metrics and logs:

   ```bash
   curl -fsS http://127.0.0.1:18888/readyz
   curl -fsS http://127.0.0.1:18888/metrics |
     grep 'agentteams_manager_recovery_'
   docker logs --tail 200 agentteams-manager
   ```

If snapshot validation fails, leave the Manager stopped. Restore the affected
objects from a known-good bucket backup or investigate the storage failure;
never edit `latest.json` to bypass its checksum metadata. Operations that
cannot be reconciled automatically remain visible as recovery errors or
`needs_attention` instead of being blindly repeated.

## Rotate credentials

Secrets belong in the local protected environment file or Kubernetes Secret,
not in the Controller-generated Manager runtime document, SQLite payloads,
skill documents, or chat messages.

For an embedded install:

1. back up `agentteams-manager.env` with owner-only permissions;
2. replace the affected value in that file;
3. run the installer upgrade path so the Controller and Manager containers
   are recreated with the new environment;
4. wait for Manager readiness and exercise the affected integration;
5. revoke the old credential only after the new one is verified.

For Helm:

1. update the protected values source or external-secret input;
2. run `helm upgrade --install` with that source;
3. wait for the Controller and Manager rollout;
4. verify `/readyz`, reconciliation status, and the affected integration;
5. revoke the old credential.

Model, Matrix, MinIO, Higress, GitHub MCP, and Controller credentials are
process environment inputs. A simple runtime-document revision does not
replace them. Never place a literal secret in an `env:NAME` reference:
AgentScope resolves the referenced uppercase environment variable at tool
execution time.

## Optional coding CLI delegation

The base Manager image does not install Claude Code, Gemini CLI, or Qoder CLI.
Enable only the providers you operate and either use a derived Manager image
or mount a node directory read-only:

```yaml
manager:
  codingCLI:
    enabled: true
    providers: [claude]
    hostPath: /srv/agentteams-coding-cli
    mountPath: /opt/agentteams/coding-cli
    trustedDirectory: /opt/agentteams/coding-cli/bin
```

The mounted directory must contain `bin/claude`, `bin/gemini`, or
`bin/qodercli` as selected. A `hostPath` must exist on every node that can run
the Manager; a derived image is more portable. Provide provider credentials
through `envFrom` Secret references in the Manager Pod template or a
read-only vendor login mount. Do not put provider tokens in Helm values,
Manager resources, prompts, or task artifacts.

Changing `codingCLI` recreates the Manager Pod because its environment and
mounts change. The tools remain admin-room-only and confirmation-gated.
`coding_cli_status` reports configured and actually available providers
separately.

## Runtime revisions

Model, MCP, service, and Manager identity changes are submitted through typed
Manager tools and Controller resources. The Controller publishes an immutable,
secret-free runtime document with a higher revision. The current AgentScope
turn finishes on its original runtime; the new revision activates between
turns without replacing the Manager container.

When a revision does not activate:

1. compare the Controller-reported revision with
   `agentteams_manager_runtime_revision`;
2. inspect `agentteams_manager_runtime_reloads_total`;
3. verify access to the configured MinIO runtime-document key;
4. inspect Manager logs for validation errors;
5. correct desired state through `agt` or the typed Manager tool rather than
editing files inside the container.

## Project changes

SQLite is authoritative for project tasks, dependencies, participants,
transitions, and plan revisions. `plan.md` is a readable export, not a second
state store.

- `request_project_revision` creates a linked rework task and holds dependent
  work until it completes.
- `reassign_project_task` atomically changes the assignee, Worker Room, Matrix
  identity, and transition record before redispatch.
- `report_project_blocked` accepts reports only from the durable assignee or
  administrator.
- `revise_project_plan` versions a minor plan change immediately.
- `revise_project_plan_major` requires global Admin-DM confirmation.
- `update_project_participants` also requires global confirmation and keeps
  SQLite membership and Matrix membership aligned. Reassign active tasks
  before removing their assignee.

Completing the final task closes the project and emits idempotent completion
messages to the project room and original administrator room.

## Session commands and memory

Every model-visible Matrix input is delimited by `[Current message]` and
includes verified sender, room, and thread metadata. Recent Matrix history is
used only as a bounded cold-session projection; it is removed before
AgentScope state is persisted, so prior messages are not copied into every
new `UserMsg`.

- `/new` starts a clean session on the runtime default model.
- `/new <model>` starts a clean room session with a persisted model override.
- `/reset` clears AgentScope context while preserving the room model setting.
- `/compact` folds older context into a bounded summary and records the
  summary in daily and curated long-term SQLite memory.
- `/status` reports the session ID, model, context and summary sizes, next
  reset boundary, and durable pending confirmations.

Each room has a persisted IANA timezone and resets at the next local 04:00
boundary. Rooms parked on a pending global confirmation are excluded until
that approval is resolved or expires. SQLite also stores bounded daily
entries, curated long-term memory, project decisions, and Worker capability
assessments; no Redis service is required.

The active tool list shown to the model comes from the concrete AgentScope
Toolkit for that room. The checked-in `manager/agent/TOOLS.md` catalog is a
generated, CI-validated projection of the canonical Manager tool registry.
Required built-in skills must remain present, while additional valid local
skills are allowed.

## Semantic supervision and synchronized files

The heartbeat always runs deterministic recovery first. Its later semantic
phase uses explicit thresholds to notify the administrator about overdue
active tasks, long-lived project blockers, nonresponsive running Workers, and
waiting-task capacity shortages. Alert IDs include the durable fact version,
so an unchanged condition reuses the same exactly-once notification while a
new state transition can create a new alert.

`sync_files` supports three confined roots: `task_artifacts`,
`worker_workspace`, and `shared_knowledge`. Worker names and task IDs are
validated before path construction; resolved paths must remain below the
configured cache root, and symlinks are rejected. For a task push, conditional
MinIO upload and the assigned Worker mention are one durable `FILE_SYNC`
operation with a stable Matrix transaction and restart recovery.

`reset_worker` persists a complete typed Worker create request before
deletion, then recreates, proves readiness, and refreshes topology from that
saved request. `get_worker` exposes observed container and service-port state;
`get_team` exposes the effective `peerMentions` policy and coordination-room
facts.

## Admin console and external channels

Cinny remains at `/`. The authenticated operations console is routed at
`/manager-admin/`; its API accepts the Manager admin token as a Bearer token.
Health, readiness, and metrics remain unauthenticated on their dedicated
paths for Kubernetes probes and scraping.

Optional Discord, Telegram, Slack, Feishu, WhatsApp, DingTalk, and Signal
adapters are configured by `manager.externalChannels`. Native mode verifies
each platform's own webhook contract and uses its outbound API; Signal uses
the explicitly declared relay mode because it has no common hosted Bot
webhook. Legacy custom-HMAC documents migrate to relay mode. Every secret is
an `env:NAME` reference whose value is injected from one of
`manager.externalChannelSecretRefs`. First contact creates a durable `pending`
record and posts an approval request to the Matrix Admin DM. Pending, blocked,
and unknown contacts never enter an AgentScope turn. Only an Admin DM can use
the external-contact approval, primary-channel, block, and send tools.

Host files are disabled by default. Enabling `manager.hostFiles` mounts only
the exact configured Kubernetes node path at `/host-share`; read and write
operations still require separate relative-path allowlists. Absolute paths,
parent traversal, oversized files, and writes outside the write allowlist are
rejected. Host writes are atomic and confirmation-gated.

Manager SQLite and AgentScope session/E2EE state use the Manager PVC. Tuwunel
and MinIO retain their own StatefulSet PVCs. MinIO journal snapshots are
disaster recovery, not a substitute for the primary SQLite volume.

Tuwunel is pinned to 1.8.2. Local account creation uses its registration
token for ordinary names and its authenticated shared-secret endpoint when
the AgentTeams AppService exclusive namespace rejects normal registration.
