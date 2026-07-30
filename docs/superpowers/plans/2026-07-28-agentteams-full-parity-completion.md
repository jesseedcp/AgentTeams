# AgentTeams Full Parity Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the eight verified gaps between the current AgentScope 2.0 Manager fork and the latest upstream AgentTeams behavior, while preserving the project’s intentional AgentScope/Cinny/SQLite architecture.

**Architecture:** Every new mutation enters through an authenticated adapter, is validated by Pydantic/Go types, and is executed by the existing workflow or Controller service. External protocols receive provider-specific adapters; no generic shell or arbitrary HTTP tool is exposed to the model. Compatibility code remains at installer/CRD boundaries, while durable user state stays in SQLite.

**Tech Stack:** Python 3.11, AgentScope 2.x, Pydantic 2, asyncio/httpx, SQLite, Go/controller-runtime, Kubernetes CRDs, Helm, Bash, PowerShell, GitHub Actions, GHCR.

**Approved design:** `docs/superpowers/specs/2026-07-28-agentteams-full-parity-completion-design.md`

---

## Task 1: Make the upstream contract and release chain truthful

**Files:**

- Modify: `.github/workflows/build.yml`
- Modify: `.github/workflows/build-rc.yml`
- Modify: `.github/workflows/test-integration.yml`
- Modify: `.github/workflows/release.yml`
- Modify: `Makefile`
- Modify: `install/agentteams-install.sh`
- Modify: `install/agentteams-install.ps1`
- Modify: `manager-agentscope/tests/contract/fixtures/upstream-agentteams.json`
- Modify: `manager-agentscope/tests/contract/test_upstream_resource_contract.py`
- Modify: `tests/test-28-agentscope-manager-parity.sh`
- Create: `tests/check-fork-release-chain.sh`
- Modify: `README.md`

### Step 1: Write failing release-chain tests

Add a shell contract test that asserts:

- no workflow or Make target builds/pushes OpenHuman;
- release assets and release API default to `jesseedcp/AgentTeams`;
- self-owned application images default to the Fork GHCR path;
- every workflow target exists in the Makefile;
- the upstream fixture reports the actual upstream legacy Manager and five Workers, while local intentional differences remain explicit.

Run:

```bash
bash tests/check-fork-release-chain.sh
pytest manager-agentscope/tests/contract/test_upstream_resource_contract.py -q
```

Expected: FAIL on the stale OpenHuman targets, official release URL/image defaults, and inaccurate fixture.

### Step 2: Repair the CI and release graph

- Remove OpenHuman matrices and targets from build, RC, integration, and release workflows.
- Add `packages: write`, GHCR login, and consistent image names.
- Publish immutable version tags; publish `latest` only from a stable release tag.
- Keep registry/repository override variables for mirrors and private installations.
- Verify that each workflow calls an existing Make target.

### Step 3: Make installers select the Fork release and images

- Add `AGENTTEAMS_RELEASE_REPOSITORY`, defaulting to `jesseedcp/AgentTeams`.
- Default the application image registry/repository to the Fork GHCR namespace.
- Keep explicit registry override behavior.
- For source revisions without a release, use source build mode or return a clear release-not-found error; never silently pull the upstream Manager.
- Mirror behavior in Bash and PowerShell.

### Step 4: Correct the parity contract and documentation

- Record upstream runtime/Worker facts accurately.
- Add explicit `intentional_differences` for AgentScope Manager, Cinny, SQLite, and removed OpenHuman.
- Make contract tests verify both upstream truth and local replacement coverage.
- Correct Worker counts and install examples in README.

### Step 5: Verify and commit

Run:

```bash
bash tests/check-fork-release-chain.sh
pytest manager-agentscope/tests/contract/test_upstream_resource_contract.py -q
make -n push-manager-agentscope push-controller push-copaw-worker push-openclaw-worker push-hermes-worker push-qwenpaw-worker
```

Commit intent: `Ensure releases install this AgentScope fork`

---

## Task 2: Restore pre-v1.2 installer compatibility

**Files:**

- Modify: `install/agentteams-install.sh`
- Modify: `install/agentteams-install.ps1`
- Create: `tests/install/test-pre-1.2-env-compat.sh`
- Create: `tests/install/test-pre-1.2-env-compat.ps1`
- Modify: `tests/install/test-agentscope-manager-install.sh`
- Modify: `tests/install/test-agentscope-manager-install.ps1`

### Step 1: Write failing version-boundary tests

Cover:

- `v1.1.9`, `1.0.0`, and `1.2.0-rc.1`;
- `v1.2.0`, `1.2.1`, `2.0.0`;
- leading `v`, missing patch segment, and invalid values;
- generated Controller environment keys for the old and current prefixes;
- the selected image version, not merely installer version, controls the prefix.

Run both tests. Expected: FAIL because helper functions and legacy rendering are absent.

### Step 2: Port normalized semantic comparison

Implement in both installers:

- version normalization;
- three-part integer comparison;
- `_use_legacy_image_env`/PowerShell equivalent;
- Controller prefix selection;
- invalid-version warning and safe current-prefix fallback.

Do not scatter prefix conditionals; generate the entire Controller environment block through one prefix helper.

### Step 3: Verify and commit

Run:

```bash
bash tests/install/test-pre-1.2-env-compat.sh
bash tests/install/test-agentscope-manager-install.sh
powershell -NoProfile -File tests/install/test-pre-1.2-env-compat.ps1
powershell -NoProfile -File tests/install/test-agentscope-manager-install.ps1
```

Commit intent: `Keep upgrades compatible with pre-v1.2 images`

---

## Task 3: Complete IM command parity and real cancellation

**Files:**

- Modify: `manager-agentscope/src/agentteams_manager/state/schema.py`
- Modify: `manager-agentscope/src/agentteams_manager/state/sessions.py`
- Modify: `manager-agentscope/src/agentteams_manager/domain/models.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/agent_factory.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/session_manager.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/router.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/session_runner.py`
- Modify: `manager-agentscope/tests/unit/state/test_sessions.py`
- Modify: `manager-agentscope/tests/unit/matrix/test_router.py`
- Modify: `manager-agentscope/tests/integration/test_session_commands.py`
- Create: `manager-agentscope/tests/integration/test_session_command_cancellation.py`

### Step 1: Write failing command behavior tests

Test:

- `/help` and `/commands` enumerate all supported commands;
- `/models`, `/model list`, status, numeric selection, and full model ID;
- model switch rebuilds the Agent but retains conversation state;
- `/think`, `/reasoning`, `/verbose`, `/elevated`, `/queue` parsing and persistence;
- an ordinary member cannot elevate privileges;
- unknown slash commands do not reach the LLM;
- `/stop` cancels an in-flight turn before the room queue drains.

Run the focused tests. Expected: FAIL on every newly specified command and cancellation.

### Step 2: Add session settings migration

Add durable columns or a versioned JSON settings document for:

- thinking mode/effort;
- reasoning-summary visibility;
- verbose tool summaries;
- elevated confirmation mode;
- queue mode and limit.

Migration must be idempotent against existing SQLite databases. Repository methods update one setting without overwriting the others.

### Step 3: Implement a typed command parser

Move command recognition away from a long conditional into a small typed parser/result model. Normalize aliases and arguments, and return deterministic help/errors.

### Step 4: Wire runtime effects

- Feed thinking settings into supported AgentScope model parameters.
- Rebuild per-room Agent instances after model/thinking changes and restore state.
- Apply verbose/reasoning only to user-visible summaries, never hidden chain-of-thought.
- Apply elevated mode after Matrix role checks and confirmation policy.
- Apply queue settings at room queue admission.

### Step 5: Implement `/stop` as a control-plane fast path

Track the active asyncio task per room. The router recognizes an authorized `/stop` before enqueueing and calls a cancellation method. Convert `CancelledError` into a stable user response and guaranteed cleanup; do not write a partial assistant message to history.

### Step 6: Verify and commit

Run:

```bash
pytest manager-agentscope/tests/unit/state/test_sessions.py manager-agentscope/tests/unit/matrix/test_router.py manager-agentscope/tests/integration/test_session_commands.py manager-agentscope/tests/integration/test_session_command_cancellation.py -q
```

Commit intent: `Make Matrix commands control real AgentScope sessions`

---

## Task 4: Replace generic channel HMAC with native adapters

**Files:**

- Modify: `manager-agentscope/src/agentteams_manager/config.py`
- Modify: `manager-agentscope/src/agentteams_manager/channels/base.py`
- Rewrite: `manager-agentscope/src/agentteams_manager/channels/http_providers.py`
- Modify: `manager-agentscope/src/agentteams_manager/channels/service.py`
- Modify: `manager-agentscope/src/agentteams_manager/bootstrap.py`
- Modify: `manager-agentscope/src/agentteams_manager/health.py`
- Modify: `manager-agentscope/tests/unit/channels/test_http_providers.py`
- Modify: `manager-agentscope/tests/unit/channels/test_service.py`
- Create: `manager-agentscope/tests/integration/test_native_channel_webhooks.py`
- Modify: `manager/agent/skills/channel-management/SKILL.md`
- Create: `manager/agent/skills/channel-management/references/configuration.md`

### Step 1: Write failing provider-native tests

For each provider, construct valid and invalid signed requests without network access:

- Telegram secret-token header and Update;
- Slack HMAC/time window and URL challenge;
- WhatsApp HMAC, GET verification, and message event;
- Feishu token/challenge/signature;
- DingTalk signature and callback;
- Discord signed PING/message interaction;
- Signal relay mode;
- backward-compatible custom-HMAC relay mode.

Assert normalized contact/conversation/message IDs and platform-specific outbound request shape. Expected: FAIL because only custom `x-agentteams-signature` exists and DingTalk is invalid.

### Step 2: Version the channel configuration

Add `mode`, provider-specific environment references, and options. Convert legacy fields to relay mode during validation and emit one sanitized warning. Ensure secrets are resolved at runtime and excluded from model dumps/logging.

### Step 3: Split adapters by protocol

Create one adapter implementation per protocol behind the existing channel interface. Share only body-size, JSON, constant-time comparison, outbound timeout, and error-envelope helpers. Avoid an arbitrary URL/method API.

### Step 4: Support verification HTTP methods and event deduplication

- Route WhatsApp GET verification and provider POSTs.
- Record provider event IDs before dispatch; duplicate delivery returns success without a second Agent turn.
- Keep pending/trusted/blocked contacts and primary-contact logic unchanged.

### Step 5: Verify and commit

Run:

```bash
pytest manager-agentscope/tests/unit/channels manager-agentscope/tests/integration/test_native_channel_webhooks.py -q
```

Commit intent: `Speak each external channel's native webhook protocol`

---

## Task 5: Add a declarative CoPaw Console switch end-to-end

**Files:**

- Modify: `agentteams-controller/api/v1beta1/types.go`
- Modify: `agentteams-controller/api/v1beta1/zz_generated.deepcopy.go`
- Modify: `agentteams-controller/config/crd/workers.agentteams.io.yaml`
- Modify: `helm/agentteams/crds/workers.agentteams.io.yaml`
- Modify: `agentteams-controller/internal/service/worker_env.go`
- Modify: `agentteams-controller/internal/service/worker_env_test.go`
- Modify: `agentteams-controller/internal/server/types.go`
- Modify: `agentteams-controller/internal/server/resource_handler.go`
- Modify: `agentteams-controller/internal/server/resource_handler_test.go`
- Modify: `agentteams-controller/cmd/agt/create.go`
- Modify: `agentteams-controller/cmd/agt/create_test.go`
- Modify: `agentteams-controller/cmd/agt/update.go`
- Modify: `agentteams-controller/cmd/agt/update_test.go`
- Modify: `manager-agentscope/src/agentteams_manager/clients/agt.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/resources.py`
- Modify: `manager-agentscope/tests/unit/clients/test_agt.py`
- Modify: `manager-agentscope/tests/unit/workflows/test_workers.py`
- Modify: `manager-agentscope/tests/integration/test_worker_lifecycle.py`
- Modify: `manager/agent/skills/worker-management/references/console.md`

### Step 1: Write failing Go and Python tests

Assert:

- absent console spec means disabled;
- CoPaw/QwenPaw with `enabled=true` gets the configured console env;
- disabling removes the env;
- unsupported runtime is rejected;
- API and CLI round-trip the field;
- Manager tool invokes the typed update and returns status;
- repeated enable/disable is idempotent.

Run focused Go/Python tests. Expected: FAIL because console is currently unconditional and no typed switch exists.

### Step 2: Add CRD and API types

Add optional `WorkerConsoleSpec` with validation (`port` 1–65535). Regenerate or carefully update deep-copy and CRD schemas, then verify both CRD copies remain identical.

### Step 3: Make Controller reconciliation authoritative

Remove unconditional console env injection. Add it only for enabled compatible runtimes. Ensure an update changes the pod template so Kubernetes rolls the Worker, and Docker reconciliation recreates its container.

### Step 4: Add CLI, client, workflow, and AgentScope tool

Expose create/update flags and `set_worker_console`. Require Admin authorization and confirmation only if the operation makes a new network surface reachable; ordinary disable is safe and immediate.

### Step 5: Verify and commit

Run:

```bash
(cd agentteams-controller && go test ./internal/service ./internal/server ./cmd/agt)
pytest manager-agentscope/tests/unit/clients/test_agt.py manager-agentscope/tests/unit/workflows/test_workers.py manager-agentscope/tests/integration/test_worker_lifecycle.py -q
helm template agentteams helm/agentteams -f helm/agentteams/values-kind.yaml > /dev/null
```

Commit intent: `Make CoPaw console state declarative and reversible`

---

## Task 6: Turn the read-only admin page into a safe management console

**Files:**

- Modify: `manager-agentscope/src/agentteams_manager/admin/service.py`
- Create: `manager-agentscope/src/agentteams_manager/admin/commands.py`
- Modify: `manager-agentscope/src/agentteams_manager/admin/ui.py`
- Modify: `manager-agentscope/src/agentteams_manager/health.py`
- Modify: `manager-agentscope/src/agentteams_manager/bootstrap.py`
- Modify: `manager-agentscope/tests/integration/test_health_server.py`
- Create: `manager-agentscope/tests/integration/test_admin_resource_api.py`
- Create: `manager-agentscope/tests/unit/admin/test_commands.py`

### Step 1: Write failing API tests

Cover:

- no/incorrect bearer token;
- JSON/body-size/content-type errors;
- list/get/create/patch/delete Worker, Team, Project;
- Pydantic validation details;
- required confirmation for destructive changes;
- idempotency key propagation;
- service errors mapped to stable status/error codes;
- no direct SQLite/Controller mutation from HTTP handler.

Expected: FAIL because admin API only accepts GET.

### Step 2: Build an Admin command facade

Inject the existing Resource, Team, and Project workflow services. Parse explicit command DTOs, apply admin actor identity, and return serializable operation results. Keep it independent of the HTTP server to avoid circular imports.

### Step 3: Add versioned HTTP routes

Teach the lightweight server to read bounded request bodies and dispatch `GET/POST/PATCH/DELETE /manager-admin/api/v1/...`. Keep existing read endpoints as compatibility aliases. Use one JSON error envelope.

### Step 4: Upgrade the dependency-free UI

Add accessible tables/forms, edit actions, resource-name delete confirmation, pending-operation feedback, and automatic refresh. Keep the page usable without a Node build pipeline.

### Step 5: Verify and commit

Run:

```bash
pytest manager-agentscope/tests/unit/admin manager-agentscope/tests/integration/test_health_server.py manager-agentscope/tests/integration/test_admin_resource_api.py -q
```

Commit intent: `Let administrators manage resources without leaving Manager`

---

## Task 7: Add typed Higress gateway administration

**Files:**

- Modify: `manager-agentscope/src/agentteams_manager/clients/higress.py`
- Create: `manager-agentscope/src/agentteams_manager/workflows/gateway.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/gateway.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/__init__.py`
- Modify: `manager-agentscope/src/agentteams_manager/application.py`
- Modify: `manager-agentscope/src/agentteams_manager/bootstrap.py`
- Modify: `manager-agentscope/tests/unit/clients/test_higress.py`
- Create: `manager-agentscope/tests/unit/workflows/test_gateway.py`
- Create: `manager-agentscope/tests/integration/test_gateway_management.py`
- Create: `manager/agent/skills-alpha/higress-gateway-management/SKILL.md`
- Create: `manager/agent/skills-alpha/higress-gateway-management/references/resources.md`

### Step 1: Write failing client and workflow tests

Test list/get/upsert/delete for Provider, AI Route, and Consumer; secret redaction; confirmation for destructive/replacement operations; idempotency; 401/403/404/conflict mapping; and retry boundaries.

Expected: FAIL because current client only supports MCP and service publishing.

### Step 2: Add constrained Higress models and endpoints

Represent only supported resource fields. Keep `_request` private. Do not add a generic method/path/body tool. Wrap secret inputs in redacting types and scrub recorded request summaries.

### Step 3: Add gateway workflow and tools

Require Admin role, use operation journal/confirmation service, and return stable resource summaries. Register tools in the AgentScope tool group with concise tool docs.

### Step 4: Restore the alpha skill as typed guidance

Explain provider → route → consumer relationships, safe ordering, and the exact Manager tools. Do not include raw curl or the upstream bulk API dump.

### Step 5: Verify and commit

Run:

```bash
pytest manager-agentscope/tests/unit/clients/test_higress.py manager-agentscope/tests/unit/workflows/test_gateway.py manager-agentscope/tests/integration/test_gateway_management.py -q
pytest manager-agentscope/tests/contract/test_skill_documents.py -q
```

Commit intent: `Manage Higress resources through typed audited tools`

---

## Task 8: Add bounded Coding CLI delegation

**Files:**

- Modify: `manager-agentscope/src/agentteams_manager/config.py`
- Create: `manager-agentscope/src/agentteams_manager/clients/coding_cli.py`
- Create: `manager-agentscope/src/agentteams_manager/workflows/coding_cli.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/coding_cli.py`
- Modify: `manager-agentscope/src/agentteams_manager/state/operations.py`
- Modify: `manager-agentscope/src/agentteams_manager/application.py`
- Modify: `manager-agentscope/src/agentteams_manager/bootstrap.py`
- Modify: `manager-agentscope/src/agentteams_manager/health.py`
- Create: `manager-agentscope/tests/unit/clients/test_coding_cli.py`
- Create: `manager-agentscope/tests/unit/workflows/test_coding_cli.py`
- Create: `manager-agentscope/tests/integration/test_coding_cli_delegation.py`
- Create: `manager/agent/skills-alpha/coding-cli-management/SKILL.md`
- Create: `manager/agent/skills-alpha/coding-cli-management/references/providers.md`
- Create: `worker/agent/skills/coding-cli/SKILL.md`
- Modify: `manager/Dockerfile`
- Modify: `helm/agentteams/values.yaml`
- Modify: `helm/agentteams/templates/controller/deployment.yaml`
- Modify: `agentteams-controller/cmd/controller/main.go`
- Modify: `agentteams-controller/internal/initializer/initializer.go`
- Modify: `agentteams-controller/api/v1beta1/types.go`

### Step 1: Write failing runner tests

Use temporary fake executables to cover:

- exact provider allowlist and argument templates;
- prompt passed through stdin, never shell interpolation;
- executable detection;
- working-directory containment;
- timeout/cancellation and child-process cleanup;
- output truncation/redaction;
- nonzero exit and missing CLI results.

Expected: FAIL because no Coding CLI client exists.

### Step 2: Write failing workflow tests

Cover admin authorization, feature flag, confirmation, processing lease, workspace sync, operation journal, success/failure notification, artifact mirror, and guaranteed lease release.

### Step 3: Implement the bounded client and service

Build direct subprocess argv for the three named CLIs only. Resolve executables only from configured trusted directories. Reuse Git delegation’s lease/sync primitives instead of duplicating repository orchestration.

### Step 4: Make deployment availability explicit

Add disabled-by-default Helm values for allowed providers and a read-only CLI mount. Do not embed tokens. Health/admin status reports configured/available separately. The base image contains the framework; operator-provided or derived images supply the vendor CLIs.

### Step 5: Restore alpha skills

Document the typed delegation flow, supported providers, required credentials, result handling, and why arbitrary shell is unavailable.

### Step 6: Verify and commit

Run:

```bash
pytest manager-agentscope/tests/unit/clients/test_coding_cli.py manager-agentscope/tests/unit/workflows/test_coding_cli.py manager-agentscope/tests/integration/test_coding_cli_delegation.py -q
pytest manager-agentscope/tests/contract/test_skill_documents.py -q
```

Commit intent: `Delegate coding work without exposing an arbitrary shell`

---

## Task 9: Add behavioral parity and Kubernetes acceptance coverage

**Files:**

- Modify: `manager-agentscope/tests/e2e/test_k8s_manager_parity.py`
- Create: `manager-agentscope/tests/e2e/test_k8s_admin_and_console.py`
- Create: `manager-agentscope/tests/e2e/test_k8s_matrix_commands.py`
- Modify: `tests/test-28-agentscope-manager-parity.sh`
- Modify: `tests/run-all-tests.sh`
- Modify: `tests/README.md`
- Modify: `docs/superpowers/plans/2026-07-26-agentteams-manager-full-parity-repair.md`
- Create: `docs/parity/upstream-agentteams-fb3a40b.md`
- Modify: `README.md`

### Step 1: Replace structural-only checks with behaviors

Test against a disposable namespace:

- Admin creates/updates/deletes a Worker, Team, Project.
- Console enable/disable changes pod environment and rollout revision.
- Matrix `/model`, `/status`, `/help`, and `/stop` produce expected replies.
- At least one Manager tool call changes Controller state.
- a confirmation-required operation pauses and resumes through Matrix.
- persistence survives Manager restart.

### Step 2: Generate a human-readable parity report

Report all upstream features as implemented, intentionally replaced, intentionally removed, or externally unverified. Pin the exact upstream commit. Do not classify OpenHuman removal as an accidental missing implementation.

### Step 3: Verify and commit

Run static tests locally and behavioral tests after deployment.

Commit intent: `Prove parity through behavior instead of source claims`

---

## Task 10: Full verification, deploy, and publish the implementation

**Files:**

- Update only files needed to fix failures discovered by verification.

### Step 1: Run all local quality gates

```bash
pytest manager-agentscope/tests -q
ruff check manager-agentscope/src manager-agentscope/tests
mypy manager-agentscope/src
go test ./...
bash tests/check-fork-release-chain.sh
bash tests/test-28-agentscope-manager-parity.sh
helm lint helm/agentteams
helm template agentteams helm/agentteams -f helm/agentteams/values-kind.yaml > rendered.yaml
git diff --check
```

Use the repository’s supported Windows equivalents where Bash tooling is unavailable. Fix every regression; do not weaken tests to pass.

### Step 2: Build new immutable local images

Tag images with the short Git commit SHA. Reuse unchanged base and infrastructure layers. Confirm no old OpenHuman image is built or referenced.

### Step 3: Deploy to the existing Kubernetes test namespace

- Upgrade the existing release, preserving PVCs and SQLite data.
- Wait for rollout readiness.
- Re-establish the stable local port `18388` for Cinny and required backend forwards.
- Run Task 9 behavioral tests.
- Verify the existing admin and Manager room still work after migration.

### Step 4: Inspect runtime evidence

Collect:

- pod readiness and restart counts;
- Manager `/readyz`, metrics, and migration logs;
- Matrix/Cinny login and room response;
- Controller resources and Worker console state;
- image digests actually running.

### Step 5: Final Lore commit and push

Commit any final deployment/test fixes with a Lore message. Push current `main` to `jesseedcp/AgentTeams.git`. Do not create a new branch, import upstream Git history, create a version tag, or create a GitHub Release.

### Step 6: Completion report

Report:

- the eight implemented capabilities;
- changed architectural boundaries;
- exact test/deployment evidence;
- current URLs;
- any external validation not possible without platform credentials or installed vendor CLIs;
- commit SHA and push result.
