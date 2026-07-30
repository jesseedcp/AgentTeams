# Changelog (Unreleased)

Record image-affecting changes to `manager/`, `worker/`, `copaw/`, `hermes/`, `openclaw-base/`, `agentteams-controller/`, and release-facing install/chart changes here before the next release.

---

**Features**

- **Single AgentScope Manager image**: Ship one Python 3.11 Manager process
  with AgentScope 2.0, Matrix E2EE, `agt`, the retained 19-skill catalog, and
  standard-library health/readiness/metrics on port 18799. OpenClaw, CoPaw,
  Hermes and QwenPaw remain Worker runtimes.
- **Role-specific runtimes**: Make `agentscope` the Manager-only runtime while
  preserving OpenClaw, CoPaw, Hermes, and QwenPaw as independent
  Worker choices with their existing images.
- **AgentScope resource administration**: Expose policy-scoped typed tools for
  Worker, Team, Human, Matrix, channel, and Nacos operations, with durable
  recovery and deterministic heartbeat reconciliation.
- **Restart-free Manager desired state**: Publish integrity-tagged prompt and
  skill artifacts before a secret-free, generation-stamped AgentScope runtime
  document, so model, MCP, prompt, skill, and timing changes activate
  without deleting the Manager container.
- **Controller-owned Manager identity**: Collect administrator-confirmed name,
  language, communication style, and behavior guidelines through a typed
  AgentScope tool, persist them in `Manager.spec.identity`, and hot-reload only
  the identity section of `SOUL.md`.
- **Transactional project changes**: Preserve original work through linked
  revision tasks, atomically reassign live tasks, synchronize confirmed
  participant changes with Matrix, version minor and major plans in SQLite,
  and close completed projects with idempotent project/Admin notifications.
- **Truthful AgentScope sessions**: Render active tools from the concrete
  room Toolkit, delimit current Matrix input, keep cold-history projection
  transient, allow additive skills, persist per-room model and 04:00 reset
  settings, and bound daily, long-term, project, and Worker memory in SQLite.
- **Recoverable supervision and file control**: Detect overdue, blocked,
  nonresponsive, and capacity conditions after deterministic recovery; confine
  task, Worker-workspace, and shared-knowledge sync roots; join task upload and
  Worker mention in one durable operation; and recreate Workers from a saved
  desired-state copy.
- **Authenticated Matrix operations UI**: Keep Cinny at `/`, serve a
  token-protected Manager console under `/manager-admin/`, and expose health,
  session, confirmation, project, Worker, Team, heartbeat, and runtime facts.
- **Authenticated local account provisioning**: Pin Tuwunel 1.8.2 so the
  Manager can use registration-token UIA for ordinary names and the
  shared-secret administrative endpoint when the AppService correctly owns
  an exclusive user namespace.
- **Approved external channels**: Add signed Discord, Telegram, Slack, Feishu,
  WhatsApp, and Signal adapters with durable first-contact approval, trusted
  contact blocking, one primary contact, and Matrix Admin-DM escalation.
- **PVC and host-file boundaries**: Persist Manager SQLite and AgentScope
  state on a dedicated PVC; keep optional Kubernetes host files disabled by
  default and enforce separate read/write path allowlists when enabled.
- **Five-role release parity**: Build and inject OpenClaw, CoPaw, Hermes,
  and QwenPaw Worker images consistently across Make, local kind,
  Helm, and both installers while keeping the Manager fixed to AgentScope.

**Bug Fixes**

- **QwenPaw 2.0 runtime compatibility**: Move the Worker to QwenPaw 2.0.1,
  configure models, channels, MCP clients, policies, agents, and skills through
  the public local API, and install TeamHarness, Workerflow, and Matrix as
  native QwenPaw plugins.
- **QwenPaw startup and Team convergence**: Apply desired state only after the
  QwenPaw API is ready, establish built-in MCP policies before accepting work,
  preserve effective Team storage during independent Worker reconciliation,
  and project inline prompts and Team context into the active workspace.
- **Manager diagnostic convergence**: Stop repeated no-op diagnostics and treat
  Controller-confirmed Worker absence as the deletion boundary instead of
  probing stale Matrix rooms.
- **Worker storage sync I/O amplification**: Upload changed OpenClaw workspace
  files once per successful watermark, retry failed uploads, collapse large
  change sets to one mirror, keep jq 1.7 fallback pulls alive, and limit the
  embedded Controller mirror to control-plane configuration. CoPaw, QwenPaw,
  and Hermes now also refuse to advance their watermark after any partial
  upload failure, so every retained Worker runtime retries unsaved state.
  ([#1110](https://github.com/agentscope-ai/AgentTeams/pull/1110))
- **PVC-first restart recovery**: Preserve an existing Manager SQLite database
  across Pod restarts and replay only newer immutable journal events; use the
  MinIO snapshot only when the local database is absent, matching the declared
  primary-storage and disaster-recovery roles.
- **Fork-safe integration CI**: Port the latest upstream PR security boundary:
  run untrusted contributions through `pull_request`, check out the reviewed
  merge ref, keep permissions read-only by default, and filter every
  secret-dependent LLM/runtime shard for fork and Dependabot PRs.
  ([0ff89f0](https://github.com/agentscope-ai/AgentTeams/commit/0ff89f07a205b82cd81d18385c7095ec352a083f))
- **Provable resource updates**: Return complete Worker desired state and add
  typed Human updates so Manager mutations can be confirmed from Controller
  facts, including explicit skill, exposure, and permission-scope clearing.
- **AgentScope-aware diagnostics**: Export redacted SQLite turn, operation, and
  tool-call records instead of relying on deleted OpenClaw Manager log files.
- **CoPaw Team coordination routing**: Route Team Leader worker assignments sent through the `message` tool from Leader DM to Team Room, matching the Matrix channel send path. ([92c8145](https://github.com/agentscope-ai/AgentTeams/commit/92c8145))
- **Pinned OpenClaw source fetch**: Fetch the pinned OpenClaw commit directly so the base image build does not depend on a retired-brand external branch name. ([b0081c2](https://github.com/agentscope-ai/AgentTeams/commit/b0081c2))

**Branding and Compatibility**

- **Complete AgentTeams runtime rename**: Rename installer and Helm entrypoints, the controller Go module and CLI, and container filesystem paths to AgentTeams while preserving thin compatibility aliases and upgrade migration for existing HiClaw installations. ([3121f5f](https://github.com/agentscope-ai/AgentTeams/commit/3121f5f))
- **Hard-cut AgentTeams naming**: Remove retired-brand installer wrappers, environment fallbacks, CLI aliases, Helm naming branches, runtime path migrations, and active source paths so fresh AgentTeams deployments use one canonical contract end to end. ([d20e606](https://github.com/agentscope-ai/AgentTeams/commit/d20e606617edefbbc42c28c1201c5629fa73fd88))
