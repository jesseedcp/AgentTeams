# Changelog (Unreleased)

Record image-affecting changes to `manager/`, `worker/`, `copaw/`, `hermes/`, `openclaw-base/`, `agentteams-controller/`, and release-facing install/chart changes here before the next release.

---

**Features**

- **Single AgentScope Manager image**: Ship one Python 3.11 Manager process
  with AgentScope 2.0, Matrix E2EE, `agt`, the retained 16-skill catalog, and
  standard-library health/readiness/metrics on port 18799. OpenClaw, CoPaw,
  Hermes, QwenPaw, and OpenHuman remain Worker runtimes.
- **Role-specific runtimes**: Make `agentscope` the Manager-only runtime while
  preserving OpenClaw, CoPaw, Hermes, QwenPaw, and OpenHuman as independent
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
- **Five-runtime release parity**: Build and inject OpenClaw, CoPaw, Hermes,
  QwenPaw, and OpenHuman Worker images consistently across Make, local kind,
  Helm, and both installers while keeping the Manager fixed to AgentScope.

**Bug Fixes**

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
