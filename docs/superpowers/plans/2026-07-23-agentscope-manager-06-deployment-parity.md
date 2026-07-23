# AgentScope Manager Deployment and Full-Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship one AgentScope-only Manager image and wire it through Controller, Docker/Podman, Kubernetes, Helm, installers, CI, documentation, and the complete upstream capability test matrix.

**Architecture:** Controller provisions Matrix/MinIO/Higress credentials and writes a secret-free AgentScope runtime document. One Python Manager image runs the direct AgentScope daemon on every deployment surface. OpenClaw and CoPaw remain Worker runtimes but are removed as Manager runtime choices and startup paths.

**Tech Stack:** Python 3.11, AgentScope 2.0, Docker/Podman BuildKit, Go Controller, Kubernetes CRDs, Helm, Bash, PowerShell, GitHub Actions, pytest, Go test, shell E2E tests.

## Global Constraints

- Apply every constraint and release gate from `2026-07-23-agentscope-manager-master.md`.
- The only valid Manager runtime is `agentscope`.
- The valid Worker runtimes remain `openclaw`, `copaw`, `hermes`, `qwenpaw`, and `openhuman`.
- Publish one Manager image name: `agentteams/agentteams-manager`.
- Do not publish or select an `agentteams-manager-copaw` image.
- Do not launch OpenClaw Gateway, CoPaw App, AgentScope `create_app`, or Redis from the Manager image.
- Preserve embedded Controller infrastructure: Higress, Tuwunel, MinIO, Element Web, Controller, and `agt`.
- Health/readiness/metrics use port 18799; this is not an AgentScope Web UI or replacement chat console.
- Element remains the human chat UI.
- Existing OpenClaw/CoPaw Manager session files are not imported.
- Preserve the manager `agent/skills` source because AgentScope loads those 16 skills.
- Preserve all Worker-specific templates and images.
- Any image-affecting commit updates `changelog/current.md`.

---

### Task 1: Build One Minimal AgentScope Manager Image

**Files:**
- Create: `manager/Dockerfile`
- Create: `manager/entrypoint.sh`
- Create: `manager/README.md`
- Create: `manager-agentscope/tests/container/test_image_contract.py`
- Modify: `changelog/current.md`

The repository bootstrap in the master plan has already omitted the legacy
Manager Dockerfiles, entrypoints, CoPaw Manager overlay, OpenClaw template,
supervisord file, legacy-all-in-one directory, `setup-higress.sh`, and
`upgrade-builtins.sh`. Do not restore them.

**Interfaces:**
- Produces the `agentteams/agentteams-manager` image.
- Entrypoint executes `agentteams-manager`.
- Exposes port 18799 and a `/readyz` health check.

- [ ] **Step 1: Write static image-contract tests**

```python
from pathlib import Path


def test_manager_image_contains_agentscope_and_no_legacy_gateway():
    dockerfile = Path("manager/Dockerfile").read_text("utf-8")
    assert "manager-agentscope" in dockerfile
    assert "agentscope" in dockerfile
    assert "openclaw gateway" not in dockerfile
    assert "copaw app" not in dockerfile
    assert "Dockerfile.copaw" not in dockerfile


def test_entrypoint_execs_one_python_manager():
    script = Path("manager/entrypoint.sh").read_text("utf-8")
    assert 'exec agentteams-manager' in script
    assert "supervisord" not in script
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/container/test_image_contract.py -q
```

Expected: FAIL against the legacy Manager image and entrypoints.

- [ ] **Step 3: Replace the image**

Use a Python 3.11 builder to install the locked package, and a Python 3.11 slim final stage. Copy `agt` from `AGENTTEAMS_CONTROLLER_IMAGE`; install only runtime OS packages required by E2EE and Git delegation. Copy:

```text
manager-agentscope/
manager/agent/SOUL.md
manager/agent/AGENTS.md
manager/agent/TOOLS.md
manager/agent/HEARTBEAT.md
manager/agent/skills/
manager/configs/known-models.json
manager/entrypoint.sh
```

The final image must contain:

- `agentscope==2.0.4.post1`;
- `matrix-nio` E2EE support;
- `agt`;
- `git`;
- CA certificates and `tini`;
- no Node/OpenClaw package;
- no CoPaw package;
- no Redis server or client service;
- no embedded Higress, Matrix, MinIO, or Element process.

Use a standard-library health command:

```dockerfile
EXPOSE 18799
HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD \
  python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:18799/readyz', timeout=2)"
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/agentteams/entrypoint.sh"]
```

`entrypoint.sh` validates required paths without printing values, creates state/media/E2EE directories, and calls `exec agentteams-manager`. Do not run infrastructure setup or password login.

- [ ] **Step 4: Build and inspect the image**

Run:

```bash
docker build --build-arg AGENTTEAMS_CONTROLLER_IMAGE=agentteams/controller:latest -f manager/Dockerfile -t agentteams/manager:agentscope-test .
docker run --rm agentteams/manager:agentscope-test python -c "import agentscope; print(agentscope.__version__)"
docker run --rm --entrypoint sh agentteams/manager:agentscope-test -c "! command -v openclaw && ! command -v copaw && ! command -v redis-server"
python -m pytest manager-agentscope/tests/container/test_image_contract.py -q
```

Expected: image builds, reports `2.0.4.post1`, legacy commands are absent, and tests PASS.

- [ ] **Step 5: Commit**

```bash
git add manager manager-agentscope/tests/container changelog/current.md
git commit -m "Ship one direct AgentScope Manager process" \
  -m "Constraint: OpenClaw and CoPaw stay Worker runtimes but cannot launch as Manager." \
  -m "Rejected: Keep parallel Manager images | that preserves split behavior and duplicate release paths." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: The Manager image must remain infrastructure-free and expose only daemon health on 18799." \
  -m "Tested: image contract, image build, AgentScope version, and legacy executable inspection"
```

### Task 2: Separate Manager Runtime Validation from Worker Runtimes

**Files:**
- Modify: `agentteams-controller/internal/backend/interface.go`
- Modify: `agentteams-controller/internal/backend/interface_test.go`
- Modify: `agentteams-controller/internal/backend/docker.go`
- Modify: `agentteams-controller/internal/backend/docker_test.go`
- Modify: `agentteams-controller/internal/backend/kubernetes.go`
- Modify: `agentteams-controller/internal/backend/kubernetes_test.go`
- Modify: `agentteams-controller/internal/backend/sandbox.go`
- Modify: `agentteams-controller/internal/backend/sandbox_test.go`
- Modify: `agentteams-controller/internal/config/config.go`
- Modify: `agentteams-controller/internal/config/config_test.go`
- Modify: `agentteams-controller/internal/server/resource_handler.go`
- Modify: `agentteams-controller/internal/server/resource_handler_test.go`
- Modify: `agentteams-controller/cmd/agt/create.go`
- Modify: `agentteams-controller/cmd/agt/update.go`

**Interfaces:**
- Adds: `RuntimeAgentScope = "agentscope"`.
- Adds: `ValidManagerRuntime` and `ResolveManagerRuntime`.
- Leaves Worker `ValidRuntime` and `ResolveRuntime` behavior intact.

- [ ] **Step 1: Write Manager/Worker separation tests**

```go
func TestRuntimeSetsAreSeparated(t *testing.T) {
    if !ValidManagerRuntime(RuntimeAgentScope) {
        t.Fatal("agentscope must be valid for Manager")
    }
    if ValidManagerRuntime(RuntimeOpenClaw) || ValidManagerRuntime(RuntimeCopaw) {
        t.Fatal("legacy runtimes must be invalid for Manager")
    }
    for _, runtime := range []string{
        RuntimeOpenClaw, RuntimeCopaw, RuntimeHermes,
        RuntimeQwenPaw, RuntimeOpenHuman,
    } {
        if !ValidRuntime(runtime) {
            t.Fatalf("%s must remain valid for Worker", runtime)
        }
    }
    if ValidRuntime(RuntimeAgentScope) {
        t.Fatal("agentscope is not a Worker runtime")
    }
}
```

Add backend tests proving an AgentScope Manager with no custom image selects `ManagerImage`, while every Worker runtime still selects its existing image.

- [ ] **Step 2: Verify failure**

Run:

```bash
cd agentteams-controller && go test ./internal/backend ./internal/config ./internal/server ./cmd/agt
```

Expected: FAIL because AgentScope Manager runtime separation is absent.

- [ ] **Step 3: Implement role-specific defaults**

`ValidRuntime` remains Worker-only. Add:

```go
const RuntimeAgentScope = "agentscope"

func ValidManagerRuntime(runtime string) bool {
    return runtime == "" || runtime == RuntimeAgentScope
}

func ResolveManagerRuntime(runtime string) string {
    if runtime == "" {
        return RuntimeAgentScope
    }
    return runtime
}
```

Set `AGENTTEAMS_MANAGER_RUNTIME` default to `agentscope`. Extend backend configuration with `ManagerImage`; backend image selection uses it only for `RuntimeAgentScope`. Manager APIs reject any other nonempty runtime. Worker APIs continue to reject `agentscope` and accept all five existing values.

Update Manager CLI help to `agentscope`; update Worker help to list all five Worker runtimes, including `qwenpaw`.

- [ ] **Step 4: Run runtime tests**

Run:

```bash
cd agentteams-controller && go test ./internal/backend ./internal/config ./internal/server ./cmd/agt
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add agentteams-controller
git commit -m "Prevent Manager replacement from changing Worker runtime support" \
  -m "Constraint: AgentScope is Manager-only; five historical runtimes remain Worker-only." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: backend, config, server, and agt runtime tests"
```

### Task 3: Generate Secret-Free AgentScope Desired State and Avoid Restart

**Files:**
- Modify: `agentteams-controller/internal/agentconfig/generator.go`
- Modify: `agentteams-controller/internal/agentconfig/generator_test.go`
- Modify: `agentteams-controller/internal/service/interfaces.go`
- Modify: `agentteams-controller/internal/service/deployer.go`
- Modify: `agentteams-controller/internal/service/deployer_test.go`
- Modify: `agentteams-controller/internal/controller/manager_reconcile_config.go`
- Modify: `agentteams-controller/internal/controller/manager_reconcile_container.go`
- Modify: `agentteams-controller/internal/controller/manager_container_test.go`
- Modify: `agentteams-controller/internal/service/worker_env.go`
- Modify: `agentteams-controller/internal/service/worker_env_test.go`
- Modify: `agentteams-controller/api/v1beta1/types.go`

**Interfaces:**
- Produces `manager/agentscope-manager.json`.
- Adds `RuntimeRevision` and effective model parameters to Controller deployment.
- Changes Manager pod hash to pod-affecting fields only.
- Adds explicit Matrix user ID and runtime-document key environment variables.

- [ ] **Step 1: Write document secrecy, revision, and no-restart tests**

```go
func TestDeployManagerAgentScopeDocumentContainsNoSecrets(t *testing.T) {
    payload := deployManagerDocument(t, ManagerDeployRequest{
        Name: "default", Generation: 7,
        MatrixToken: "matrix-secret",
        GatewayKey: "gateway-secret",
        MinIOPassword: "minio-secret",
    })
    text := string(payload)
    for _, secret := range []string{
        "matrix-secret", "gateway-secret", "minio-secret",
    } {
        if strings.Contains(text, secret) {
            t.Fatalf("runtime document leaked %q", secret)
        }
    }
    require.Contains(t, text, `"revision":7`)
}

func TestModelAndMCPChangeDoNotChangeManagerPodHash(t *testing.T) {
    before := managerSpecForHash()
    after := before
    after.Model = "new-model"
    after.McpServers = []v1beta1.MCPServer{{Name: "github"}}
    require.Equal(t,
        hashAppliedManagerSpec(before),
        hashAppliedManagerSpec(after),
    )
}
```

- [ ] **Step 2: Verify failure**

Run:

```bash
cd agentteams-controller && go test ./internal/agentconfig ./internal/service ./internal/controller
```

Expected: FAIL because the AgentScope document and narrowed hash are absent.

- [ ] **Step 3: Export model resolution and generate the runtime document**

Export a single model-resolution function from `internal/agentconfig` so OpenClaw Worker configuration and AgentScope Manager configuration use the same known-model table and unknown-model defaults.

Generate canonical JSON with:

```json
{
  "schema_version": 1,
  "revision": 7,
  "manager_name": "default",
  "model": "qwen3.6-plus",
  "context_window": 150000,
  "max_tokens": 128000,
  "reasoning": true,
  "input_modalities": ["text"],
  "skills": ["worker-management"],
  "mcp_servers": [
    {"name": "github", "url": "http://higress/mcp", "transport": "http"}
  ],
  "prompt_sources": {
    "soul": "manager/SOUL.md",
    "agents": "manager/AGENTS.md",
    "tools": "manager/TOOLS.md",
    "heartbeat": "manager/HEARTBEAT.md"
  },
  "heartbeat_interval_seconds": 1800,
  "worker_idle_timeout_seconds": 43200
}
```

Use `generation` as monotonic revision. The document contains endpoint descriptors and object keys, never credential values. Upload prompt/skill artifacts first and the runtime document last so its revision is the activation barrier.

Set:

```text
AGENTTEAMS_MANAGER_MATRIX_USER_ID=<provisioned Matrix user>
AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY=manager/agentscope-manager.json
AGENTTEAMS_MANAGER_RUNTIME=agentscope
```

Remove Manager-only `OPENCLAW_*` environment variables. Preserve Worker environment behavior.

- [ ] **Step 4: Narrow pod recreation and prove hot reload**

Hash exactly pod-affecting fields:

```go
type managerPodSpec struct {
    Runtime       string
    Image         string
    Resources     *v1beta1.AgentResourceRequirements
    AccessEntries []v1beta1.AccessEntry
    Env           map[string]string
    Labels        map[string]string
}
```

Model, Soul, Agents, Skills, MCP servers, Package, and Manager timing config trigger `DeployManagerConfig` and runtime revision activation, not container deletion. Image/runtime/resources/access/env/labels still recreate the container. Ensure `ObservedGeneration` advances after a config-only change.

- [ ] **Step 5: Run Controller configuration tests**

Run:

```bash
cd agentteams-controller && go test ./internal/agentconfig ./internal/service ./internal/controller
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add agentteams-controller
git commit -m "Let Controller desired state hot-reload the AgentScope Manager" \
  -m "Constraint: Runtime documents are secret-free and pod recreation is reserved for pod-affecting changes." \
  -m "Rejected: Keep hashing the entire Manager spec | model and MCP changes would destroy active sessions." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Upload referenced artifacts before publishing a higher runtime revision." \
  -m "Tested: agentconfig, deployer, environment, and Manager reconcile tests"
```

### Task 4: Update CRDs, Helm, and Kubernetes Health

**Files:**
- Modify: `agentteams-controller/config/crd/managers.agentteams.io.yaml`
- Modify: `helm/agentteams/crds/managers.agentteams.io.yaml`
- Modify: `helm/agentteams/values.yaml`
- Modify: `helm/agentteams/templates/00-validate.yaml`
- Modify: `helm/agentteams/templates/controller/deployment.yaml`
- Modify: `agentteams-controller/internal/backend/kubernetes.go`
- Modify: `agentteams-controller/internal/backend/kubernetes_test.go`
- Modify: `tests/check-helm-agentteams.sh`

**Interfaces:**
- Manager CRD runtime enum becomes `[agentscope]`.
- Helm Manager runtime default becomes `agentscope`.
- Manager Pod probes `/healthz` and `/readyz` on 18799.

- [ ] **Step 1: Write render and probe tests**

Add shell assertions:

```bash
helm template agentteams helm/agentteams \
  --set manager.runtime=agentscope >"$rendered"
grep -q 'AGENTTEAMS_MANAGER_RUNTIME' "$rendered"
grep -q 'agentscope' "$rendered"
! grep -q 'agentteams-manager-copaw' "$rendered"
```

Add a Go Pod test asserting AgentScope Manager liveness/readiness HTTP probes use named port `manager-health`, paths `/healthz` and `/readyz`, and port 18799.

- [ ] **Step 2: Verify failure**

Run:

```bash
bash tests/check-helm-agentteams.sh
cd agentteams-controller && go test ./internal/backend
```

Expected: FAIL because Helm/CRDs still expose legacy Manager choices and probes are absent.

- [ ] **Step 3: Apply AgentScope-only deployment values**

Change both checked-in CRD copies together. Update descriptions to distinguish Manager `agentscope` from Worker runtime values. Helm validation must reject any Manager runtime other than `agentscope`.

Keep Manager image repository `agentteams/agentteams-manager`; remove runtime-specific repository selection. Controller deployment receives the one runtime and one image.

For AgentScope Manager CreateRequests, Kubernetes adds:

```yaml
ports:
  - name: manager-health
    containerPort: 18799
livenessProbe:
  httpGet: {path: /healthz, port: manager-health}
readinessProbe:
  httpGet: {path: /readyz, port: manager-health}
```

Do not add these probes to Worker pods.

- [ ] **Step 4: Run Helm and backend tests**

Run:

```bash
bash tests/check-helm-agentteams.sh
bash tests/check-agentteams-rename-defaults.sh
cd agentteams-controller && go test ./internal/backend
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add agentteams-controller/config/crd helm agentteams-controller/internal/backend tests/check-helm-agentteams.sh
git commit -m "Make AgentScope the only Manager choice on Kubernetes" \
  -m "Constraint: Worker runtime values and images remain unchanged." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Helm contract, rename defaults, and backend Pod tests"
```

### Task 5: Simplify Bash and PowerShell Installers

**Files:**
- Modify: `install/agentteams-install.sh`
- Modify: `install/agentteams-install.ps1`
- Modify: `install/agentteams-verify.sh`
- Modify: `Makefile`
- Create: `tests/install/test-agentscope-manager-install.sh`
- Create: `tests/install/test-agentscope-manager-install.ps1`

**Interfaces:**
- Removes Manager runtime selection.
- Uses one Manager image variable.
- Readiness calls `/readyz`.
- Keeps the independent default Worker runtime selection.

- [ ] **Step 1: Write static installer tests**

Bash:

```bash
grep -q 'AGENTTEAMS_MANAGER_RUNTIME=agentscope' install/agentteams-install.sh
! grep -q 'MANAGER_COPAW_IMAGE' install/agentteams-install.sh
! grep -q 'step_manager_runtime' install/agentteams-install.sh
grep -q '/readyz' install/agentteams-install.sh
```

PowerShell performs equivalent `Select-String` assertions, and Makefile assertions prove there is no `build-manager-copaw`, `push-manager-copaw`, or `LOCAL_MANAGER_COPAW`.

- [ ] **Step 2: Verify failure**

Run:

```bash
bash tests/install/test-agentscope-manager-install.sh
pwsh -NoProfile -File tests/install/test-agentscope-manager-install.ps1
```

Expected: FAIL while installers and Makefile still branch on OpenClaw/CoPaw Manager.

- [ ] **Step 3: Remove runtime branching**

Keep the Worker runtime prompt unchanged and extend its displayed values to all supported Worker runtimes where the installer currently exposes fewer choices. Remove:

- Manager runtime prompt/state;
- `AGENTTEAMS_INSTALL_MANAGER_COPAW_IMAGE`;
- `MANAGER_COPAW_IMAGE`;
- runtime-specific console messages;
- `openclaw gateway health`;
- CoPaw App readiness;
- legacy all-in-one Manager fallback and `AGENTTEAMS_FORCE_LEGACY`;
- creation of OpenClaw/CoPaw Manager state files.

Set runtime to `agentscope` internally and always use the single Manager image. `wait_manager_ready` executes a Python standard-library GET of `http://127.0.0.1:18799/readyz` inside the container. Continue polling Controller `welcomeSent` after process readiness.

Advertise Element as the chat UI and port 18799 only as Manager health/metrics; remove claims that it is an OpenClaw or CoPaw console.

Remove Manager-CoPaw variables and build/push/tag targets from Makefile and CI dependencies. `build`, `push`, and install/test targets build exactly one Manager plus all five Worker images.

- [ ] **Step 4: Run installer syntax and contract tests**

Run:

```bash
bash -n install/agentteams-install.sh
bash -n install/agentteams-verify.sh
bash tests/install/test-agentscope-manager-install.sh
pwsh -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw install/agentteams-install.ps1))'
pwsh -NoProfile -File tests/install/test-agentscope-manager-install.ps1
make -n build-manager
```

Expected: all commands PASS and dry-run contains one Manager build.

- [ ] **Step 5: Commit**

```bash
git add install Makefile tests/install
git commit -m "Remove obsolete Manager choices from installation" \
  -m "Constraint: Users still choose Worker runtime independently." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Bash syntax, PowerShell parse, installer contracts, and Make dry-run"
```

### Task 6: Update CI and End-to-End Tests for Direct AgentScope

**Files:**
- Modify: `.github/workflows/build.yml`
- Modify: `.github/workflows/build-rc.yml`
- Modify: `.github/workflows/test-integration.yml`
- Modify: `tests/lib/test-helpers.sh`
- Modify: `tests/lib/agent-metrics.sh`
- Modify: `tests/test-01-manager-boot.sh`
- Modify: `tests/test-03-assign-task.sh`
- Modify: `tests/test-05-heartbeat.sh`
- Modify: `tests/test-06-multi-worker.sh`
- Modify: `tests/test-08-github-mcp.sh`
- Modify: `tests/test-10-mcp-permission.sh`
- Modify: `tests/test-12-github-mcp-tools.sh`
- Modify: `tests/test-13-git-delegation.sh`
- Modify: `tests/test-19-human-and-team-admin.sh`
- Modify: `tests/test-21-team-project-dag.sh`
- Modify: `tests/test-23-runtime-switch.sh`
- Modify: `tests/run-all-tests.sh`
- Create: `tests/test-27-worker-runtime-matrix.sh`
- Create: `tests/test-28-agentscope-manager-parity.sh`
- Create: `tests/manager-skill-parity.json`
- Create: `manager-agentscope/tests/contract/test_skill_documents.py`

**Interfaces:**
- CI builds one Manager image.
- E2E helpers identify Manager by `/readyz`, not runtime-specific process names.
- Adds a five-Worker-runtime matrix and 16-skill evidence registry.

- [ ] **Step 1: Write a failing Manager boot assertion**

`test-01-manager-boot.sh` must assert:

```bash
docker exec agentteams-manager python -c \
  'import agentscope; assert agentscope.__version__ == "2.0.4.post1"'
docker exec agentteams-manager python -c \
  'import urllib.request; urllib.request.urlopen("http://127.0.0.1:18799/readyz")'
! docker exec agentteams-manager sh -c \
  'pgrep -f "openclaw gateway|copaw.*app|redis-server"'
```

Run:

```bash
bash -n tests/test-01-manager-boot.sh
bash tests/test-01-manager-boot.sh
```

Expected before implementation: FAIL against a legacy Manager installation.

- [ ] **Step 2: Port runtime-specific assertions to behavior**

Search every E2E helper/test for `openclaw`, `copaw`, `manager-copaw`, `openclaw.json`, `state.json`, `pending-workers.json`, and Manager CLI send branches. For Manager assertions, replace implementation checks with:

- health/readiness;
- Matrix visible response;
- Controller resource convergence;
- MinIO artifact receipts;
- SQLite/journal recovery;
- AgentScope model/MCP event evidence.

Keep runtime-specific checks that intentionally test Workers.

`test-23-runtime-switch.sh` continues testing Worker runtime switching, and adds Manager model hot reload without container ID change. It must no longer switch Manager between OpenClaw and CoPaw.

- [ ] **Step 3: Add the five-runtime Worker matrix**

`test-27-worker-runtime-matrix.sh` creates one Worker for each:

```text
openclaw
copaw
hermes
qwenpaw
openhuman
```

For each Worker, assert Controller phase, runtime, Matrix room, mention/response, one finite task artifact pull, completion, and cleanup. A runtime may be reported as an environment failure only when its image is demonstrably unavailable; release CI provides all images and allows no skip.

- [ ] **Step 4: Add machine-readable 16-skill parity**

`tests/manager-skill-parity.json` has exactly 16 entries. Each entry contains:

```json
{
  "skill": "worker-management",
  "unit": ["manager-agentscope/tests/unit/workflows/test_workers.py"],
  "integration": ["manager-agentscope/tests/integration/test_worker_lifecycle.py"],
  "e2e": ["tests/test-02-create-worker.sh"]
}
```

`test-28-agentscope-manager-parity.sh` validates:

- no duplicate/missing skill names;
- every referenced file exists;
- every skill has unit, integration/contract, and E2E evidence;
- all 16 retained `SKILL.md` files load and reference registered typed tools;
- useful references contain no deleted script path, Manager OpenClaw/CoPaw
  command, legacy JSON registry, or `mcporter` executable invocation;
- Manager image lacks legacy processes;
- Controller reports runtime `agentscope`;
- `/metrics` includes Matrix, model, tool, recovery, and error counters.

- [ ] **Step 5: Run CI/static and selected E2E tests**

Run:

```bash
bash -n tests/test-01-manager-boot.sh
bash -n tests/test-27-worker-runtime-matrix.sh
bash -n tests/test-28-agentscope-manager-parity.sh
bash tests/test-28-agentscope-manager-parity.sh --static-only
python -m pytest manager-agentscope/tests/contract -q
bash tests/test-01-manager-boot.sh
bash tests/test-23-runtime-switch.sh
bash tests/test-27-worker-runtime-matrix.sh
```

Expected: syntax/static tests PASS; runtime tests PASS in the documented Docker/Podman environment.

- [ ] **Step 6: Commit**

```bash
git add .github tests
git commit -m "Test Manager behavior independently from Worker runtime choice" \
  -m "Constraint: Release CI must exercise all five Worker runtimes and all 16 Manager skills." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: workflow/static contracts, AgentScope boot, model reload, and five-runtime matrix"
```

### Task 7: Rewrite Manager Documentation and Remove Legacy Claims

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `README.ja-JP.md`
- Modify: `AGENTS.md`
- Modify: `docs/architecture.md`
- Modify: `docs/zh-cn/architecture.md`
- Modify: `docs/manager-guide.md`
- Modify: `docs/zh-cn/manager-guide.md`
- Modify: `docs/development.md`
- Modify: `docs/zh-cn/development.md`
- Modify: `docs/quickstart.md`
- Modify: `docs/zh-cn/quickstart.md`
- Modify: `docs/faq.md`
- Modify: `docs/zh-cn/faq.md`
- Modify: `docs/cms-integration.md`
- Modify: `docs/zh-cn/cms-integration.md`
- Modify: `docs/declarative-resource-management.md`
- Modify: `changelog/current.md`
- Create: `docs/agentscope-manager-operations.md`
- Create: `docs/zh-cn/agentscope-manager-operations.md`

**Interfaces:**
- Documents one Manager runtime and five Worker runtimes.
- Documents fresh-install boundary, recovery stores, health, metrics, and operations.

- [ ] **Step 1: Write a stale-reference scanner**

Create assertions in `tests/test-28-agentscope-manager-parity.sh --static-only` that production docs/config do not contain:

```text
Manager runtime: OpenClaw
Manager runtime: CoPaw
agentteams-manager-copaw
AGENTTEAMS_FORCE_LEGACY
openclaw gateway restart
```

Exclude historical changelog files and the design/implementation records that intentionally explain removed behavior.

The same scanner and `test_skill_documents.py` must reject current Manager
skill/prompt documents containing `/opt/agentteams/agent/skills/*/scripts`,
`openclaw gateway`, `copaw channels send`, `state.json`,
`workers-registry.json`, `pending-workers.json`, a manual
`worker-openclaw.json.tmpl`, or a `mcporter` command. The word `mcporter` is
allowed only as the retained compatibility skill name and in explanatory text
that directs execution through AgentScope `Toolkit`.

- [ ] **Step 2: Run scanner and verify failure**

Run:

```bash
bash tests/test-28-agentscope-manager-parity.sh --static-only
```

Expected: FAIL while current docs and configuration describe legacy Manager choices.

- [ ] **Step 3: Document the actual architecture and migration boundary**

Explain:

- AgentScope `2.0.4.post1` is embedded directly;
- Matrix events call `Agent.reply_stream`;
- Controller/Matrix/MinIO/SQLite/Higress authority boundaries;
- why SQLite WAL is local durable state and MinIO is remote recovery;
- why Redis is absent;
- how to inspect `/healthz`, `/readyz`, and `/metrics`;
- how runtime revisions hot reload;
- how to recover from a corrupt local SQLite file using verified snapshot/journal replay;
- how to rotate secrets without placing them in the runtime document;
- how all five Worker runtimes remain supported;
- no old Manager session/config migration;
- Element is the user chat interface and port 18799 is operational HTTP only.

Update CMS documentation to describe AgentScope/OpenTelemetry spans and Manager metrics instead of the OpenClaw CMS plugin. Preserve Worker plugin documentation where it still applies.

Remove legacy all-in-one Dockerfiles and current-version installer guidance; historical changelog content remains unchanged.

- [ ] **Step 4: Run documentation and link checks**

Run:

```bash
bash tests/test-28-agentscope-manager-parity.sh --static-only
rg -n "agentteams-manager-copaw|AGENTTEAMS_FORCE_LEGACY|Manager runtime.*(OpenClaw|CoPaw)" README.md README.zh-CN.md README.ja-JP.md docs install helm Makefile .github
git diff --check
```

Expected: parity scanner PASS; `rg` has no current-production matches; diff check has no output.

- [ ] **Step 5: Commit**

```bash
git add README.md README.zh-CN.md README.ja-JP.md AGENTS.md docs changelog/current.md tests/test-28-agentscope-manager-parity.sh manager-agentscope/tests/contract/test_skill_documents.py
git commit -m "Make the documented Manager match the shipped AgentScope runtime" \
  -m "Constraint: Historical changelogs remain historical; current deployment guidance has one Manager path." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: stale-reference scanner, documentation links, and diff check"
```

### Task 8: Full Release Gate and Direct Mainline Delivery

**Files:**
- Modify only files required by failures found in this gate.

**Interfaces:**
- Produces one release candidate with all six subsystem gates closed.

- [ ] **Step 1: Run Python static and subsystem gates**

Run:

```bash
python -m pytest manager-agentscope/tests/unit -q
python -m pytest manager-agentscope/tests/contract -q
python -m pytest manager-agentscope/tests/integration -q
python -m pytest manager-agentscope/tests/fault_injection -q
python -m pytest manager-agentscope/tests/container -q
python -m compileall -q manager-agentscope/src
```

Expected: all tests PASS and compileall exits 0.

- [ ] **Step 2: Run Controller, Helm, installer, and repository gates**

Run:

```bash
cd agentteams-controller && go test ./...
cd .. && bash tests/check-helm-agentteams.sh
bash tests/check-agentteams-rename-defaults.sh
bash -n install/agentteams-install.sh
bash -n install/agentteams-verify.sh
pwsh -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw install/agentteams-install.ps1))'
bash manager/tests/smoke-test.sh
bash tests/test-28-agentscope-manager-parity.sh --static-only
git diff --check
```

Expected: all commands PASS and diff check has no output.

- [ ] **Step 3: Build all release images**

Run:

```bash
make build
docker image inspect agentteams/manager:latest
docker run --rm --entrypoint sh agentteams/manager:latest -c \
  '! command -v openclaw && ! command -v copaw && ! command -v redis-server'
```

Expected: one Manager plus Controller/embedded and five Worker images build; legacy Manager executables are absent.

- [ ] **Step 4: Run full end-to-end suite**

Run:

```bash
bash tests/run-all-tests.sh
```

Expected: tests 01–28 and cleanup PASS in the documented integration environment, including AgentScope boot, all 16 skills, and all five Worker runtimes.

- [ ] **Step 5: Audit final release invariants**

Run:

```bash
rg -n "Runtime(OpenClaw|Copaw).*manager|build-manager-copaw|push-manager-copaw|agentteams-manager-copaw" Makefile install helm agentteams-controller .github manager
rg -n "create_app|redis://|Redis" manager-agentscope manager/Dockerfile
git status --short
git log -1 --format=full
git rev-list --max-parents=0 HEAD
git cat-file -p "$(git rev-list --max-parents=0 HEAD)"
git merge-base --is-ancestor 2540c968a642845c4b9382afd75d8c80ed861137 HEAD
```

Expected:

- first search has no production Manager path;
- second search has no runtime use of AgentScope app server or Redis;
- worktree contains only intended release changes;
- the final commit follows Lore trailers.
- exactly one root commit exists, it has no `parent` line, and the pinned
  upstream commit is not an ancestor (the final command exits nonzero).

- [ ] **Step 6: Commit release-gate fixes**

```bash
git add -A
git commit -m "Close the AgentScope Manager full-release gate" \
  -m "Constraint: One release must cover every upstream Manager skill and all five Worker runtimes." \
  -m "Rejected: Partial rollout or alternate Manager runtime | the approved specification requires a hard replacement." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Do not reintroduce OpenClaw/CoPaw Manager paths without a new architecture decision." \
  -m "Tested: Python, Go, Helm, installers, images, full E2E, 16-skill parity, and five-runtime matrix" \
  -m "Not-tested: none"
```

- [ ] **Step 7: Push the current `main` directly**

Run:

```bash
git branch --show-current
git push jesseedcp main
git ls-remote --heads jesseedcp main
```

Expected: current branch is `main`, push succeeds without creating another branch, and the remote `main` hash equals local `HEAD`.
