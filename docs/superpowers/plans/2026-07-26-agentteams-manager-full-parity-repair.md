# AgentTeams Manager Full-Parity Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task in the current session. The user explicitly prohibited subagents.

**Goal:** Make the AgentScope 2.0 Manager behaviorally compatible with the latest official AgentTeams Team/Worker contract while retaining Cinny, Kubernetes, SQLite, MinIO recovery, and the new AgentScope runtime.

**Architecture:** Treat official AgentTeams commit `82cbd5fe78d294b1018fc8a037e4f91879dce9e7` as the audited resource-contract baseline, with no legacy Team compatibility path. Keep AgentScope as the conversational and tool runtime. Put durable orchestration state in SQLite, external-effect recovery in the existing MinIO journal/outbox, and resolve all Matrix actors and confirmations through topology-aware services rather than room-local prompt state.

**Tech Stack:** Go controller and CLI, Kubernetes CRDs and Helm, Python 3.11+, AgentScope `2.0.4.post1`, SQLite, MinIO/S3, Matrix/Tuwunel, Cinny, pytest, Go test.

## Global Constraints

- Do not create a Git branch; commit directly to `main` in `jesseedcp/AgentTeams`.
- Do not import the original Git history.
- Do not use subagents.
- Keep AgentScope `2.0.4.post1` as the only Manager runtime.
- Keep Cinny at `http://127.0.0.1:18388`; do not restore Element.
- Keep Kubernetes as the deployment target.
- Use Python standard-library SQLite; do not introduce Redis.
- Do not deploy or retain OpenHuman support.
- Do not migrate old AgentScope conversations.
- Preserve QwenPaw Worker support.
- Every commit must use the repository Lore commit protocol.
- Every risky external effect must be idempotent and recoverable after restart.

---

### Task 1: Hard-cut the latest Team and Worker resource contract

**Files:**
- Modify: `agentteams-controller/api/v1beta1/types.go`
- Modify: `agentteams-controller/api/v1beta1/zz_generated.deepcopy.go`
- Modify: `agentteams-controller/config/crd/teams.agentteams.io.yaml`
- Modify: `helm/agentteams/crds/teams.agentteams.io.yaml`
- Modify: `agentteams-controller/cmd/agt/create.go`
- Modify: `agentteams-controller/cmd/agt/update.go`
- Modify: `agentteams-controller/internal/controller/team_controller.go`
- Delete: `agentteams-controller/internal/migration/registry_migration.go`
- Delete: `agentteams-controller/internal/migration/registry_migration_test.go`
- Delete: `agentteams-controller/internal/service/legacy.go`
- Test: `agentteams-controller/api/v1beta1/types_test.go`
- Test: `agentteams-controller/cmd/agt/create_test.go`
- Test: `agentteams-controller/internal/controller/team_controller_test.go`
- Test: `agentteams-controller/test/integration/controller/team_test.go`

**Interfaces:**
- Consumes: existing standalone `Worker` CRs.
- Produces: `TeamSpec.WorkerMembers []TeamWorkerRef`, with exactly one `role=team_leader`; Team deletion preserves referenced Workers.

- [ ] Write failing Go tests that reject empty `workerMembers`, duplicate members, zero or multiple leaders, and legacy `leader`/`workers` JSON.
- [ ] Write a failing deletion test proving that deleting a Team preserves all referenced Worker CRs and runtimes.
- [ ] Replace the local Team/Worker contract with the exact latest upstream contract, copying source without importing upstream Git history.
- [ ] Remove all legacy reconciliation, registry migration, embedded member creation, and legacy deletion branches.
- [ ] Remove obsolete CLI flags `--leader-model`, `--worker-idle-timeout`, and Worker-create `--team`/`--role`.
- [ ] Regenerate CRD and deepcopy outputs, then verify the two checked-in Team CRDs are byte-equivalent in schema.
- [ ] Run:

```powershell
go test ./agentteams-controller/api/v1beta1 ./agentteams-controller/cmd/agt ./agentteams-controller/internal/controller
go test ./agentteams-controller/test/integration/controller -run Team
```

- [ ] Commit with a Lore message whose intent line explains that independent Worker lifecycles must survive Team deletion.

### Task 2: Align AgentScope resource tools with the hard-cut contract

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/clients/agt.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/resources.py`
- Modify: `manager/agent/TOOLS.md`
- Modify: `manager/agent/skills/team-management/SKILL.md`
- Test: `manager-agentscope/tests/unit/clients/test_agt.py`
- Test: `manager-agentscope/tests/unit/workflows/test_teams.py`
- Test: `manager-agentscope/tests/unit/tools/test_resources.py`
- Test: `manager-agentscope/tests/integration/test_worker_lifecycle.py`
- Test: `manager-agentscope/tests/integration/test_team_topology.py`

**Interfaces:**
- Consumes: existing Worker names plus Team metadata.
- Produces: `TeamCreateRequest(name, leader_name, worker_names, team_name, heartbeat_every, description, admin_name, admin_matrix_id, peer_mentions)`.

- [ ] Add failing request-construction tests proving Manager never emits removed CLI flags.
- [ ] Replace embedded leader/worker runtime specifications with `leader_name` and `worker_names`.
- [ ] Make `create_team` preflight every referenced Worker and return a clear missing-Worker list without creating a partial Team.
- [ ] Keep Worker creation as a separate typed operation; let the AgentScope model create missing Workers first and then compose the Team.
- [ ] Make Team deletion report that Workers were preserved.
- [ ] Update skill and tool documentation to use the exact registered tool and field names.
- [ ] Run:

```powershell
python -m pytest manager-agentscope/tests/unit/clients/test_agt.py manager-agentscope/tests/unit/workflows/test_teams.py manager-agentscope/tests/unit/tools/test_resources.py manager-agentscope/tests/integration/test_team_topology.py -q
```

- [ ] Commit the Manager-side protocol cut separately from the Go controller commit.

### Task 3: Introduce topology-aware Matrix wake and actor resolution

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/state/topology.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/policy.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/router.py`
- Modify: `manager-agentscope/src/agentteams_manager/bootstrap.py`
- Test: `manager-agentscope/tests/unit/state/test_topology.py`
- Test: `manager-agentscope/tests/unit/matrix/test_policy.py`
- Test: `manager-agentscope/tests/unit/matrix/test_router.py`
- Test: `manager-agentscope/tests/integration/test_project_lifecycle.py`

**Interfaces:**
- Produces: `TopologyRepository.actor_for_sender(matrix_user_id) -> Actor | None`.
- Produces: `RoomPolicyResolver.should_wake(event, binding, actor) -> bool`.
- Rule: direct rooms wake without a mention; group/project/leader rooms wake only when `m.mentions` contains the Manager user ID.

- [x] Add failing tests for a Project Worker mentioning Manager, a Worker message without a mention, Manager self-events, bot acknowledgements, Human access, and persisted trusted contacts.
- [x] Index Worker, Team Leader, Human, Admin, and trusted-contact identities by Matrix user ID during topology refresh.
- [x] Query persisted trusted relationships from SQLite instead of constructor-only static contacts.
- [x] Apply mention gating before claiming or recording a Matrix event.
- [x] Permit a Project participant Worker to use only project-reporting tools; never grant global resource-management tools.
- [x] Ignore Manager self-events, edit echoes, redactions, and bot-only acknowledgements to prevent loops.
- [x] Run the Matrix policy, router, topology, and project integration tests.

### Task 4: Replace room-local confirmation with a global approval workflow

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/state/database.py`
- Create: `manager-agentscope/src/agentteams_manager/state/confirmations.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/session_runner.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/session_manager.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/notifications.py`
- Test: `manager-agentscope/tests/unit/state/test_confirmations.py`
- Test: `manager-agentscope/tests/integration/test_matrix_confirmation.py`
- Test: `manager-agentscope/tests/fault_injection/test_matrix_restart.py`

**Interfaces:**
- Produces durable `ConfirmationRequest(id, source_room_id, source_event_id, source_reply_id, requester_id, tool_calls, status, created_at, expires_at)`.
- Produces `ConfirmationService.resolve(id, admin_id, decision)` that resumes the source room's AgentScope continuation.

- [x] Add failing tests proving a risky call from Project Room sends approval to Admin DM and resumes the Project Room session after Admin confirmation.
- [x] Move pending confirmation metadata out of room-local `AgentState` into SQLite while retaining the AgentScope continuation reference.
- [x] Send the Admin a Chinese approval message containing source room, requester, exact tool names, summarized arguments, `/confirm <id>`, and `/deny <id>`.
- [x] Accept slash commands globally in Admin DM; when exactly one approval is pending, also accept deterministic Chinese `确认/同意/拒绝/取消`.
- [x] Notify the source room that approval is pending and notify it again after approval or denial.
- [x] Add expiry, cancellation, `/status`, and `/reset` handling so no room can remain permanently wedged.
- [x] Preserve YOLO behavior by bypassing creation of a confirmation request when `AGENTTEAMS_YOLO=true`.
- [x] Test restart between request and approval.

### Task 5: Implement the complete Project DAG state machine

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/state/database.py`
- Modify: `manager-agentscope/src/agentteams_manager/state/tasks.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/projects.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/tasks.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/tasks.py`
- Modify: `manager/agent/skills/project-management/SKILL.md`
- Test: `manager-agentscope/tests/unit/workflows/test_projects.py`
- Test: `manager-agentscope/tests/integration/test_project_lifecycle.py`
- Test: `manager-agentscope/tests/fault_injection/test_project_room_recovery.py`
- Test: `manager-agentscope/tests/fault_injection/test_task_dispatch_recovery.py`

**Interfaces:**
- Project task states: `pending`, `ready`, `dispatched`, `in_progress`, `blocked`, `revision_needed`, `completed`, `cancelled`.
- Project actions: `add_task`, `update_task`, `report_progress`, `report_blocked`, `request_revision`, `complete_task`, `reassign_task`, `add_participant`, `remove_participant`, `revise_plan`, `close_project`.
- Dependencies form an acyclic directed graph.

- [x] Add failing tests for cycle rejection, automatic ready-task dispatch, blocked tasks, revision tasks, reassignment, participant changes, major plan confirmation, and project completion.
- [x] Store tasks, dependencies, participants, plan revisions, and state transitions as normalized SQLite records.
- [x] Validate sender identity against the task assignee before accepting Worker completion or blocker reports.
- [x] Dispatch every newly ready task exactly once through the existing outbox/effect boundary.
- [x] Make minor plan changes immediate and major changes use the global confirmation service.
- [x] On completion, update the plan artifact, announce in Project Room, and send an idempotent Admin DM milestone notification.
- [x] Recover safely when the process stops after state transition but before Matrix delivery.

### Task 6: Make prompt, tool, history, session, skill, and memory state truthful

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/runtime/prompts.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/skills.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/threads.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/session_runner.py`
- Modify: `manager-agentscope/src/agentteams_manager/state/sessions.py`
- Create: `manager-agentscope/src/agentteams_manager/state/memory.py`
- Modify: `manager/agent/TOOLS.md`
- Test: `manager-agentscope/tests/unit/runtime/test_prompts.py`
- Test: `manager-agentscope/tests/unit/runtime/test_skills.py`
- Test: `manager-agentscope/tests/unit/runtime/test_session_manager.py`
- Test: `manager-agentscope/tests/unit/state/test_sessions.py`

**Interfaces:**
- Tool documentation is generated from the registered AgentScope Toolkit names.
- Current input is always delimited by `[Current message]`.
- Commands: `/new`, `/new <model>`, `/reset`, `/compact`, `/status`.

- [x] Add a contract test that fails whenever documented tool names differ from registered tool names.
- [x] Generate the active tool section from the registry instead of maintaining a second manual name list.
- [x] Stop copying recent history into every durable UserMsg; use AgentScope session memory and a transient bounded context projection.
- [x] Add explicit current-message delimiters and sender metadata.
- [x] Implement the five session commands and daily 04:00 reset using persisted timezone-aware scheduling.
- [x] Replace the exact-16-skill guard with required-builtins validation plus allowed additional skills.
- [x] Add daily memory, curated long-term memory, project decisions, and Worker capability assessments with bounded compaction.

### Task 7: Restore supervision, file synchronization, notifications, and Worker controls

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/workflows/heartbeat.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/storage.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/notifications.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Test: `manager-agentscope/tests/integration/test_recurring_heartbeat.py`
- Test: `manager-agentscope/tests/integration/test_file_sync.py`
- Test: `manager-agentscope/tests/fault_injection/test_completion_notification.py`
- Test: `manager-agentscope/tests/integration/test_worker_lifecycle.py`

- [x] Keep deterministic recovery heartbeat as the first phase.
- [x] Add threshold-based semantic supervision for overdue tasks, nonresponsive Workers, project blockers, and capacity shortages.
- [x] Add Worker workspace, shared knowledge, and task-artifact sync roots with path traversal protection.
- [x] Make a successful upload and Worker mention one durable operation with retry-safe outbox records.
- [x] Add atomic Worker reset/recreate while preserving desired Worker CR configuration.
- [x] Expose peer-mention policy and Worker service state through typed Manager tools.

### Task 8: Complete Matrix UX, persistence, Admin UI, and approved external access

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/matrix/client.py`
- Create: `manager-agentscope/src/agentteams_manager/channels/base.py`
- Create: `manager-agentscope/src/agentteams_manager/channels/matrix.py`
- Create: `manager-agentscope/src/agentteams_manager/channels/http_providers.py`
- Create: `manager-agentscope/src/agentteams_manager/channels/service.py`
- Modify: `manager-agentscope/src/agentteams_manager/main.py`
- Modify: `manager-agentscope/src/agentteams_manager/config.py`
- Modify: `helm/agentteams/values.yaml`
- Modify: `helm/agentteams/values-kind.yaml`
- Modify: `helm/agentteams/templates/matrix/tuwunel-statefulset.yaml`
- Modify: `helm/agentteams/templates/storage/minio-statefulset.yaml`
- Modify: `agentteams-controller/internal/controller/manager_reconcile_container.go`
- Create: `manager-agentscope/src/agentteams_manager/admin/`
- Delete: `openhuman/Dockerfile`
- Delete: `openhuman/scripts/openhuman-worker-entrypoint.sh`

- [ ] Add Markdown-to-sanitized-HTML output, typing indicators, and read receipts while retaining plain-text fallback.
- [ ] Add authenticated Matrix user registration through Tuwunel's supported administrative API; restrict it to Admin DM and the global confirmation workflow.
- [ ] Introduce a `ChannelAdapter` contract and `ChannelService` for Matrix plus the original channel identifiers `discord`, `telegram`, `slack`, `feishu`, `whatsapp`, and `signal`, using the existing `httpx` dependency and Kubernetes Secrets for credentials.
- [ ] Implement inbound webhook identity mapping, first-contact approval, primary-channel selection, trusted-contact restrictions, and cross-channel escalation back to the originating Matrix workflow.
- [ ] Add configurable PVC-backed persistence for Tuwunel, MinIO, Manager SQLite, AgentScope sessions, and Matrix E2EE state; keep ephemeral storage only in explicit test values.
- [ ] Keep MinIO journal/snapshots as disaster recovery rather than the only persistence layer.
- [ ] Add a minimal authenticated Admin UI/API for health, sessions, confirmations, projects, Workers, Teams, heartbeat, and model configuration.
- [ ] Route the Admin UI under `/manager-admin/` without replacing Cinny at `/`.
- [ ] Implement host-file access only through an explicit read/write allowlist and a disabled-by-default Kubernetes mount.
- [ ] Remove every OpenHuman runtime option, image, installer branch, test, and document reference.

### Task 9: Add upstream differential and full Kubernetes acceptance tests

**Files:**
- Create: `manager-agentscope/tests/contract/test_upstream_resource_contract.py`
- Create: `manager-agentscope/tests/e2e/test_k8s_manager_parity.py`
- Modify: `.github/workflows/test-manager-agentscope.yml`
- Modify: `.github/workflows/test-controller.yml`
- Modify: `.github/workflows/test-integration.yml`
- Modify: `install/agentteams-verify.sh`
- Modify: `install/agentteams-install.ps1`

- [ ] Pin the audited upstream commit SHA in a contract fixture and compare CRD fields, CLI flags, Team validation, and deletion semantics.
- [ ] Add K8s acceptance scenarios for identity setup, Worker creation, Team composition, Project Worker mention, group mention gating, cross-room approval, DAG completion, Team deletion preserving Workers, and Pod restart persistence.
- [ ] Test both YOLO and non-YOLO modes.
- [ ] Run all Python tests, all Go tests, Helm lint/template, CRD sync, container contract tests, and the K8s acceptance suite.
- [ ] Require zero skipped parity scenarios before deployment.

### Task 10: Canary rollout, credential rotation, and direct-main delivery

**Files:**
- Modify only deployment values or generated deployment scripts required by the verified images.

- [ ] Build immutable Manager and Controller images from the tested commit.
- [ ] Deploy a temporary K8s canary namespace and run Task 9 acceptance tests against it.
- [ ] Export the current test resources, recreate standalone Workers first, then recreate Teams with `workerMembers`; do not translate old AgentScope conversation state.
- [ ] Rotate the Matrix access token, MinIO secret, Higress password, and Manager Gateway key that were exposed during diagnostics.
- [ ] Switch ports `18388` and `18480` only after the canary passes.
- [ ] Verify Cinny login, Manager health, Matrix room interaction, Worker/Team/Project flows, Prometheus metrics, PVCs, and restart recovery.
- [ ] Commit each independently verified phase directly to `main` with Lore trailers and push to `jesseedcp/AgentTeams`.

## Final Acceptance Gate

- [ ] Current official Team/Worker contract is matched without compatibility fields or obsolete CLI flags.
- [ ] Deleting a Team preserves all referenced Workers.
- [ ] Project Worker mentions reach Manager and non-mentioned group messages remain silent.
- [ ] Confirmations from any source room can be approved safely from Admin DM.
- [ ] Project DAG, blockers, revisions, reassignment, and completion notifications work end to end.
- [ ] Tool documentation exactly matches the registered AgentScope toolset.
- [ ] Sessions, SQLite state, and E2EE data survive Manager Pod restart.
- [ ] First-contact, primary notification channel, trusted contacts, and cross-channel escalation work without granting external contacts management tools.
- [ ] Cinny remains available at `http://127.0.0.1:18388`.
- [ ] OpenHuman is absent from source, images, Helm, installers, and runtime choices.
- [ ] Full Python, Go, Helm, container, and K8s parity suites pass.
