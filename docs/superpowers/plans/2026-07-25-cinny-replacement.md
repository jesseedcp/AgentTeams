# Cinny Replacement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every bundled Element Web runtime with Cinny v4.12.3 while preserving Matrix data, network addresses, Manager/Worker behavior, and upgrade compatibility.

**Architecture:** Docker embedded mode copies Cinny static assets into the existing infrastructure image and serves them on internal port 8088. Kubernetes runs Cinny as a dedicated Deployment and routes the existing Higress root route to it. Canonical configuration names become Cinny-specific, while installers and the controller accept old Element environment variables only as upgrade-time input fallbacks.

**Tech Stack:** Cinny v4.12.3, Matrix Client-Server API, Nginx, Docker, Supervisor, Go, Helm/Kubernetes, Bash, PowerShell.

## Global Constraints

- Keep Docker UI address `http://127.0.0.1:18388`.
- Keep the current Kubernetes test UI address `http://127.0.0.1:18480`.
- Keep the default install UI port `18088` and embedded container port `8088`.
- Do not recreate or migrate Tuwunel, MinIO, Controller, Manager, Worker, rooms, users, or messages.
- Pin the client image to `ghcr.io/cinnyapp/cinny:v4.12.3`; do not use `latest`.
- Enable Cinny hash routing and expose the current gateway URL as homeserver index `0`.
- Do not add Redis, a database, or another runtime dependency.
- Do not create a branch or use subagents; execute inline on `main`.
- Commit messages must follow the repository Lore Commit Protocol.

---

### Task 1: Lock the Helm Cinny contract with failing tests

**Files:**
- Modify: `tests/check-helm-agentteams.sh`

**Interfaces:**
- Consumes: the existing `helm template` test harness and `gateway.publicURL`.
- Produces: assertions for `agentteams-cinny`, `AGENTTEAMS_CINNY_URL`, Cinny v4.12.3, and the exact Cinny `config.json` fields.

- [ ] **Step 1: Add the failing assertions**

Add checks equivalent to:

```bash
grep -q 'name: agentteams-cinny' "${render}"
grep -q 'name: AGENTTEAMS_CINNY_URL' "${render}"
grep -q 'ghcr.io/cinnyapp/cinny:v4.12.3' "${render}"
grep -q '"defaultHomeserver": 0' "${render}"
grep -Fq '"homeserverList": ["http://localhost:18080"]' "${render}"
grep -q '"allowCustomHomeservers": true' "${render}"
grep -q '"enabled": true' "${render}"
! grep -q 'agentteams-element-web' "${render}"
! grep -q 'AGENTTEAMS_ELEMENT_WEB_URL' "${render}"
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
bash tests/check-helm-agentteams.sh
```

Expected: failure because the current chart still renders Element resources.

### Task 2: Replace Helm Element resources with Cinny

**Files:**
- Modify: `helm/agentteams/values.yaml`
- Modify: `helm/agentteams/templates/_helpers.tpl`
- Modify: `helm/agentteams/templates/controller/deployment.yaml`
- Modify: `helm/agentteams/templates/NOTES.txt`
- Delete: `helm/agentteams/templates/element-web/configmap.yaml`
- Delete: `helm/agentteams/templates/element-web/deployment.yaml`
- Delete: `helm/agentteams/templates/element-web/service.yaml`
- Create: `helm/agentteams/templates/cinny/configmap.yaml`
- Create: `helm/agentteams/templates/cinny/deployment.yaml`
- Create: `helm/agentteams/templates/cinny/service.yaml`
- Modify: `hack/local-k8s-up.sh`
- Test: `tests/check-helm-agentteams.sh`

**Interfaces:**
- Consumes: `.Values.gateway.publicURL`.
- Produces: `.Values.cinny`, helper `agentteams.cinny.fullname`, and controller environment variable `AGENTTEAMS_CINNY_URL`.

- [ ] **Step 1: Add canonical Cinny values**

Use:

```yaml
cinny:
  enabled: true
  image:
    repository: ghcr.io/cinnyapp/cinny
    tag: "v4.12.3"
    pullPolicy: IfNotPresent
  replicaCount: 1
  service:
    type: ClusterIP
    port: 8080
```

- [ ] **Step 2: Create Cinny ConfigMap**

Render:

```json
{
  "defaultHomeserver": 0,
  "homeserverList": ["<gateway.publicURL>"],
  "allowCustomHomeservers": true,
  "featuredCommunities": {
    "openAsDefault": false,
    "spaces": [],
    "rooms": [],
    "servers": []
  },
  "hashRouter": {
    "enabled": true,
    "basename": "/"
  }
}
```

- [ ] **Step 3: Create Deployment and Service**

Mount the ConfigMap at `/app/config.json`, expose container port `80`, and let
the ClusterIP Service expose the existing chart port `8080`.

- [ ] **Step 4: Wire the controller and local Kind loader**

Set:

```yaml
- name: AGENTTEAMS_CINNY_URL
  value: "http://agentteams-cinny.<namespace>.svc.cluster.local:8080"
```

Replace the Element preload image in `hack/local-k8s-up.sh` with
`ghcr.io/cinnyapp/cinny:v4.12.3`.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
helm lint helm/agentteams \
  --set credentials.registrationToken=test \
  --set credentials.adminPassword=test \
  --set credentials.llmApiKey=test
bash tests/check-helm-agentteams.sh
```

Expected: both commands pass and no Element workload renders.

### Task 3: Replace the controller routing contract

**Files:**
- Modify: `agentteams-controller/internal/config/config.go`
- Modify: `agentteams-controller/internal/initializer/initializer.go`
- Modify: `agentteams-controller/internal/app/app.go`
- Modify: controller tests that construct `initializer.Config` or assert route names.

**Interfaces:**
- Consumes: `AGENTTEAMS_CINNY_URL`, falling back to `AGENTTEAMS_ELEMENT_WEB_URL` only when the canonical variable is empty.
- Produces: `CinnyURL string` in controller and initializer configuration, plus Higress source/route name `cinny`.

- [ ] **Step 1: Write or update failing Go tests**

Cover both cases:

```go
// canonical variable wins
AGENTTEAMS_CINNY_URL=http://agentteams-cinny:8080

// legacy input is accepted only when canonical is absent
AGENTTEAMS_ELEMENT_WEB_URL=http://legacy-client:8080
```

Assert that initializer calls use service source and route name `cinny`.

- [ ] **Step 2: Run targeted tests and verify RED**

Run:

```bash
go test ./agentteams-controller/internal/config ./agentteams-controller/internal/initializer
```

Expected: failure until the new field and route are implemented.

- [ ] **Step 3: Implement the canonical field and legacy fallback**

Use a small environment helper that selects `AGENTTEAMS_CINNY_URL` first and
only then reads `AGENTTEAMS_ELEMENT_WEB_URL`. Propagate `CinnyURL` through
`app.go` and use `cinny` for Higress source and route identifiers.

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./agentteams-controller/internal/config ./agentteams-controller/internal/initializer ./agentteams-controller/internal/app
```

Expected: all packages pass.

### Task 4: Replace the embedded Docker client

**Files:**
- Modify: `agentteams-controller/Dockerfile.embedded`
- Modify: `agentteams-controller/supervisord.embedded.conf`
- Delete: `manager/scripts/init/start-element-web.sh`
- Create: `manager/scripts/init/start-cinny.sh`
- Create or modify: relevant static deployment test script.

**Interfaces:**
- Consumes: `AGENTTEAMS_CINNY_HOMESERVER_URL`; legacy
  `AGENTTEAMS_ELEMENT_HOMESERVER_URL` is a fallback.
- Produces: Cinny static files under `/opt/cinny`, `config.json`, Nginx on
  `8088`, and the unchanged Higress plugin server on `8002`.

- [ ] **Step 1: Add a failing static contract test**

Assert:

```bash
grep -Fq 'FROM ghcr.io/cinnyapp/cinny:v4.12.3 AS cinny' agentteams-controller/Dockerfile.embedded
grep -Fq 'COPY --from=cinny /app /opt/cinny' agentteams-controller/Dockerfile.embedded
grep -Fq '[program:cinny]' agentteams-controller/supervisord.embedded.conf
test -f manager/scripts/init/start-cinny.sh
test ! -f manager/scripts/init/start-element-web.sh
```

- [ ] **Step 2: Implement `start-cinny.sh`**

Generate the same Cinny JSON contract as Helm, configure SPA fallback, disable
cache for `config.json`/`index.html`, preserve the port `8002` plugin server,
remove the default site, and run `nginx -g 'daemon off;'`.

- [ ] **Step 3: Update the image and Supervisor**

Copy `/app` from the pinned Cinny stage to `/opt/cinny`, invoke
`start-cinny.sh`, and write logs to `cinny.log`/`cinny-error.log`.

- [ ] **Step 4: Run syntax and static tests**

Run:

```bash
bash -n manager/scripts/init/start-cinny.sh
bash tests/check-cinny-replacement.sh
```

Expected: both pass.

### Task 5: Migrate installers and user-facing output

**Files:**
- Modify: `install/agentteams-install.sh`
- Modify: `install/agentteams-install.ps1`
- Modify: `install/README.md`
- Modify: `tests/install/test-windows-upgrade-appservice.ps1`
- Modify: installer tests that assert ports or Docker run arguments.

**Interfaces:**
- Consumes: canonical `AGENTTEAMS_PORT_CINNY` and legacy
  `AGENTTEAMS_PORT_ELEMENT_WEB`.
- Produces: env files containing only `AGENTTEAMS_PORT_CINNY`, Docker
  `-p <port>:8088`, and `AGENTTEAMS_CINNY_HOMESERVER_URL`.

- [ ] **Step 1: Change the Windows upgrade fixture to legacy input**

Keep `AGENTTEAMS_PORT_ELEMENT_WEB=29388` in the input fixture, then assert:

```powershell
Assert-Equal $saved["AGENTTEAMS_PORT_CINNY"] "29388" "Cinny port"
if ($saved.ContainsKey("AGENTTEAMS_PORT_ELEMENT_WEB")) {
    throw "Legacy Element port was written back"
}
```

- [ ] **Step 2: Run the fixture and verify RED**

Run:

```powershell
pwsh -NoProfile -File tests/install/test-windows-upgrade-appservice.ps1
```

Expected: failure because the installer currently writes the old variable.

- [ ] **Step 3: Implement Bash and PowerShell migration**

Read canonical values first, fall back to old values only during import,
prompt and display Cinny names, write only canonical values, map the chosen
port to `8088`, and pass the Cinny homeserver variable to the container.

- [ ] **Step 4: Run installer verification**

Run:

```powershell
pwsh -NoProfile -File tests/install/test-windows-upgrade-appservice.ps1
pwsh -NoProfile -File tests/install/test-agentscope-manager-install.ps1
```

Run:

```bash
bash -n install/agentteams-install.sh
bash tests/install/test-agentscope-manager-install.sh
```

Expected: all tests pass.

### Task 6: Update active documentation and textual behavior

**Files:**
- Modify: `README.md`
- Modify: `manager/README.md`
- Modify: active English and Chinese documents under `docs/`
- Modify: controller comments and welcome email copy that name the bundled client.
- Do not modify: historical design/plan files dated before 2026-07-25.

**Interfaces:**
- Consumes: final Cinny addresses and environment variable names.
- Produces: documentation that calls Cinny the bundled client while still
  listing Element Mobile as an optional third-party Matrix client where useful.

- [ ] **Step 1: Replace active bundled-client references**

Change installation, login, troubleshooting, architecture, file upload, and
onboarding instructions from Element Web to Cinny. Keep statements about
Element Mobile only when describing optional Matrix clients.

- [ ] **Step 2: Add the one-time login note**

State that replacing Element does not delete Matrix data, but Cinny requires
one new browser login because local browser sessions are client-specific.

- [ ] **Step 3: Scan for unintended active references**

Run:

```bash
rg -n -i 'element web|elementWeb|AGENTTEAMS_ELEMENT' \
  README.md manager install helm agentteams-controller docs \
  --glob '!docs/superpowers/plans/2026-07-23-*' \
  --glob '!docs/superpowers/specs/2026-07-23-*'
```

Expected: only explicit legacy-input compatibility notes and optional Element
Mobile references remain.

### Task 7: Deploy, verify, commit, and push

**Files:**
- Modify only if verification exposes a defect.

**Interfaces:**
- Consumes: the current Docker volumes/env and Helm release.
- Produces: Cinny at `18388` and `18480`, with the existing Agent teams intact.

- [ ] **Step 1: Pull and smoke-test the pinned image**

Run:

```bash
docker pull ghcr.io/cinnyapp/cinny:v4.12.3
docker run --rm ghcr.io/cinnyapp/cinny:v4.12.3 \
  sh -c 'test -f /app/index.html && test -f /app/config.json'
```

- [ ] **Step 2: Build the embedded image**

Build the controller binary image and the embedded image with a new local tag,
using Docker cache and the current source tree.

- [ ] **Step 3: Replace only the Docker controller container**

Capture its mounts, network, published ports, restart policy, and non-secret
environment from inspect; remove and recreate only `agentteams-controller`
with the new image and the same persistent volume. Leave `agentteams-manager`
and Workers running.

- [ ] **Step 4: Upgrade the Kind release**

Load the Cinny image into `agentteams-b35deb9`, then run:

```bash
helm upgrade agentteams helm/agentteams \
  --kube-context kind-agentteams-b35deb9 \
  --namespace agentteams-k8s-b35deb9 \
  --reuse-values
```

Wait for the controller, Cinny, Manager, and Worker to be Ready.

- [ ] **Step 5: Verify both deployments**

Check:

```text
GET http://127.0.0.1:18388/                         -> 200, Cinny HTML
GET http://127.0.0.1:18388/config.json              -> Cinny config
GET http://127.0.0.1:18380/_matrix/client/versions  -> 200
GET http://127.0.0.1:18480/                         -> 200, Cinny HTML
GET http://127.0.0.1:18480/config.json              -> Cinny config
GET http://127.0.0.1:18480/_matrix/client/versions  -> 200
```

Use the browser to confirm the Cinny login/welcome screen and complete one
existing-admin login without printing credentials.

- [ ] **Step 6: Run the full relevant verification suite**

Run Helm, Go, installer, shell syntax, git diff check, and runtime smoke tests.

- [ ] **Step 7: Commit and push**

Create Lore-protocol commits and push directly:

```bash
git push jesseedcp main
```

Confirm the remote branch points at the local final commit.
