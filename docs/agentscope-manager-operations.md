# AgentScope Manager Operations

This runbook covers the production `agentscope` Manager. OpenClaw, CoPaw,
Hermes, QwenPaw, and OpenHuman are Worker runtimes and have separate
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
