# AgentScope Manager Integrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve Manager and Worker model switching, MCP administration and calls, Higress integration, service publishing, runtime hot reload, and integration-facing channel behavior.

**Architecture:** Controller remains authoritative for Manager/Worker desired configuration; Higress remains authoritative for MCP definitions, consumers, and exposed routes. The Manager preflights mutations, records their intent, changes desired state through `agt`, polls the Controller-generated runtime document from MinIO, and replaces AgentScope model/toolkit dependencies only between room turns.

**Tech Stack:** Python 3.11+, AgentScope 2.0 `MCPClient`, `HttpMCPConfig`, `Toolkit`, OpenAI-compatible model API, HTTPX, Pydantic 2, `agt`, Higress Console API, MinIO, pytest, Go tests.

## Global Constraints

- Apply every constraint from `2026-07-23-agentscope-manager-master.md`.
- The Controller-generated `manager/agentscope-manager.json` contains no secret values.
- Gateway, Matrix, and MinIO credentials remain environment-injected `SecretStr` values.
- Apply model or MCP configuration to a room only before its next turn; never replace an Agent model/toolkit during `reply_stream`.
- Preserve `AgentState` when rebuilding a room Agent after configuration changes.
- Preflight a requested Manager model through the OpenAI-compatible Higress route before changing desired state.
- Update Manager and Worker resources only through `AgtClient`.
- Use AgentScope `MCPClient` directly; do not install or invoke the `mcporter` CLI.
- MCP consumer authorization is a replace operation. Always send the complete intended consumer set.
- Local Higress MCP administration must stop before mutation in `AGENTTEAMS_RUNTIME=aliyun`.
- Never place upstream API credentials in chat, logs, SQLite, runtime documents, or Worker manifests.
- Service publishing is unauthenticated by the current Controller contract and therefore always requires explicit confirmation.

---

### Task 1: Runtime Document Polling and Turn-Boundary Hot Reload

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/runtime/config_watcher.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/session_manager.py`
- Modify: `manager-agentscope/src/agentteams_manager/config.py`
- Create: `manager-agentscope/tests/unit/runtime/test_config_watcher.py`
- Create: `manager-agentscope/tests/integration/test_turn_boundary_reload.py`
- Create: `manager-agentscope/tests/fault_injection/test_invalid_runtime_document.py`

**Interfaces:**
- Produces: `ConfigWatcher.poll_once() -> ConfigChange | None`.
- Produces: `RuntimeRegistry.current`, `activate(change)`, `revision`.
- Extends: `RoomSessionManager` with turn-boundary generation checks.

- [ ] **Step 1: Write monotonic-revision and active-turn tests**

```python
import pytest


@pytest.mark.asyncio
async def test_new_revision_is_downloaded_and_validated(config_fixture):
    config_fixture.remote_document(
        revision=2,
        model="claude-sonnet-4-6",
    )

    change = await config_fixture.watcher.poll_once()

    assert change is not None
    assert change.revision == 2
    assert config_fixture.registry.current.model == "claude-sonnet-4-6"


@pytest.mark.asyncio
async def test_active_turn_finishes_before_model_replacement(reload_fixture):
    turn = await reload_fixture.start_blocked_turn(model="old")
    await reload_fixture.publish_revision(model="new")

    assert turn.agent_model == "old"
    await turn.finish()
    next_turn = await reload_fixture.start_turn()
    assert next_turn.agent_model == "new"
    assert next_turn.session_state == turn.final_state
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_config_watcher.py manager-agentscope/tests/integration/test_turn_boundary_reload.py manager-agentscope/tests/fault_injection/test_invalid_runtime_document.py -q
```

Expected: FAIL because runtime polling and generation-aware sessions are absent.

- [ ] **Step 3: Implement conditional MinIO polling**

Poll `manager/agentscope-manager.json` with `head` and download only when ETag changes. Validate with `RuntimeDocument`, require a strictly increasing revision, calculate a SHA-256 over canonical JSON, then atomically replace the local cache file.

`RuntimeRegistry.activate()` stores an immutable generation:

```python
@dataclass(frozen=True, slots=True)
class RuntimeGeneration:
    revision: int
    digest: str
    document: RuntimeDocument
    activated_at: datetime
```

If validation, model preconstruction, or MCP discovery fails, keep the previous generation, emit a redacted error, and report readiness as degraded but still serving. Never roll back to a lower remote revision.

At room-lock acquisition, `RoomSessionManager` compares the cached Agent generation with the registry. On change it saves `AgentState`, closes old MCP clients after the prior turn has ended, constructs a new Agent, restores state, and only then accepts the event.

- [ ] **Step 4: Run hot-reload tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_config_watcher.py manager-agentscope/tests/integration/test_turn_boundary_reload.py manager-agentscope/tests/fault_injection/test_invalid_runtime_document.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/runtime manager-agentscope/src/agentteams_manager/config.py manager-agentscope/tests
git commit -m "Apply runtime changes between turns without losing room state" \
  -m "Constraint: Active AgentScope streams cannot be mutated and Controller desired state remains authoritative." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Runtime revisions are monotonic; never activate an unverified document." \
  -m "Tested: config watcher, turn-boundary reload, and invalid-document tests"
```

### Task 2: Manager and Worker Model Switching

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/clients/model_gateway.py`
- Create: `manager-agentscope/src/agentteams_manager/workflows/integrations.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/configuration.py`
- Modify: `manager-agentscope/src/agentteams_manager/clients/agt.py`
- Create: `manager-agentscope/tests/unit/clients/test_model_gateway.py`
- Create: `manager-agentscope/tests/unit/workflows/test_model_switch.py`
- Create: `manager-agentscope/tests/integration/test_model_hot_reload.py`

**Interfaces:**
- Produces: `ModelGatewayClient.preflight(spec) -> ModelCapabilities`.
- Produces: `IntegrationService.switch_manager_model`, `switch_worker_model`.
- Consumes: `AgtClient.update_manager`, `update_worker`, `ConfigWatcher`, and Controller resource queries.

- [ ] **Step 1: Write preflight-before-update and convergence tests**

```python
import pytest


@pytest.mark.asyncio
async def test_unreachable_manager_model_does_not_update_controller(
    model_fixture,
):
    model_fixture.gateway.rejects("unknown-model", status=404)

    with pytest.raises(ModelNotReachable):
        await model_fixture.service.switch_manager_model(
            model_fixture.manager_request("unknown-model")
        )

    assert model_fixture.agt.update_manager_calls == 0


@pytest.mark.asyncio
async def test_worker_switch_waits_for_controller_model(model_fixture):
    model_fixture.gateway.accepts("deepseek-chat")
    model_fixture.agt.worker_models("old", "deepseek-chat")

    worker = await model_fixture.service.switch_worker_model(
        "alice", "deepseek-chat"
    )

    assert worker.model == "deepseek-chat"
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_model_gateway.py manager-agentscope/tests/unit/workflows/test_model_switch.py manager-agentscope/tests/integration/test_model_hot_reload.py -q
```

Expected: FAIL because model gateway and switch workflows are absent.

- [ ] **Step 3: Implement secret-safe model preflight**

POST one minimal request to `<ai_gateway_url>/v1/chat/completions` using the Manager gateway key:

```python
payload = {
    "model": request.model.removeprefix("agentteams-gateway/"),
    "messages": [{"role": "user", "content": "Reply with OK."}],
    "max_tokens": 8,
    "stream": False,
}
```

Use a 15-second timeout and a generated idempotency/trace header. On non-2xx, return a redacted `ModelNotReachable` with the guidance to create a new provider and prefix route rather than editing the initialization-managed default provider.

For a recognized model, use Controller-generated context, token, reasoning, and modality parameters. For an unknown model, accept explicit optional context window and reasoning values from the Admin; default to 150,000 / 128,000 / reasoning enabled when omitted. The Controller runtime-document generator in Plan 06 persists the effective values.

After preflight:

```text
agt update manager --name <manager> --model <model>
agt update worker --name <worker> --model <model>
```

Manager switching waits for a higher runtime document revision and reports that active turns kept their prior model. Worker switching polls `agt get workers <name> -o json` until the requested model and a nonfailed phase converge; Worker container recreation remains Controller behavior.

- [ ] **Step 4: Run model tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_model_gateway.py manager-agentscope/tests/unit/workflows/test_model_switch.py manager-agentscope/tests/integration/test_model_hot_reload.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/clients manager-agentscope/src/agentteams_manager/workflows/integrations.py manager-agentscope/src/agentteams_manager/tools/configuration.py manager-agentscope/tests
git commit -m "Validate model routes before changing desired state" \
  -m "Constraint: Manager reloads on its next turn while Worker recreation remains Controller-owned." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: model gateway, workflow, and hot-reload tests"
```

### Task 3: Native AgentScope MCP Discovery and Calls

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/runtime/mcp.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/agent_factory.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/config_watcher.py`
- Create: `manager-agentscope/tests/unit/runtime/test_mcp.py`
- Create: `manager-agentscope/tests/contract/test_mcporter_parity.py`
- Create: `manager-agentscope/tests/integration/test_agentscope_mcp.py`

**Interfaces:**
- Produces: `MCPRegistry.prepare(runtime)`, `clients_for(policy)`, `close_generation`.
- Produces equivalent discovery and call behavior for the upstream `mcporter` skill.
- Consumes: AgentScope `MCPClient`, `HttpMCPConfig`, and `Toolkit`.

- [ ] **Step 1: Write discovery, authorization-header, and tool-call tests**

```python
import pytest


@pytest.mark.asyncio
async def test_registry_uses_runtime_descriptor_and_secret_header(
    mcp_fixture,
):
    await mcp_fixture.registry.prepare(
        mcp_fixture.runtime(
            name="github",
            url="http://higress:8080/mcp-servers/mcp-github/mcp",
        )
    )

    client = mcp_fixture.created_client("github")
    assert client.mcp_config.url.endswith("/mcp-github/mcp")
    assert client.mcp_config.headers["Authorization"] == "Bearer secret"
    assert "secret" not in mcp_fixture.runtime_json


@pytest.mark.asyncio
async def test_agent_can_discover_schema_and_call_mcp_tool(mcp_fixture):
    toolkit = await mcp_fixture.toolkit()
    names = {
        schema["function"]["name"]
        for schema in await toolkit.get_tool_schemas()
    }
    assert "github_search_issues" in names
    result = await mcp_fixture.call(
        toolkit,
        "github_search_issues",
        {"query": "is:open label:bug"},
    )
    assert result.output
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_mcp.py manager-agentscope/tests/contract/test_mcporter_parity.py manager-agentscope/tests/integration/test_agentscope_mcp.py -q
```

Expected: FAIL because native MCP registration is absent.

- [ ] **Step 3: Construct AgentScope MCP clients directly**

For each allowed runtime descriptor:

```python
client = MCPClient(
    name=descriptor.name,
    is_stateful=False,
    mcp_config=HttpMCPConfig(
        url=descriptor.url,
        headers={
            "Authorization": (
                "Bearer "
                + config.gateway_key.get_secret_value()
            )
        },
    ),
)
tools = await client.list_tools()
```

Only `http` and `sse` descriptors are accepted; `stdio` is rejected. Prefix or otherwise collision-proof MCP tool names according to AgentScope's returned `ToolBase.name`, and reject collisions with built-in Manager tools.

`MCPRegistry.prepare` calls `list_tools()` for all proposed clients before activation. The Toolkit receives only clients allowed by room policy. Admin rooms may use Manager-authorized MCPs; Worker/Human rooms receive only descriptors explicitly granted by Controller topology. Close clients only after all room Agents using the old generation are idle.

The `mcporter` parity test proves list-all, list-schema, and call-with-structured-JSON behavior through AgentScope, with no subprocess.

- [ ] **Step 4: Run native MCP tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_mcp.py manager-agentscope/tests/contract/test_mcporter_parity.py manager-agentscope/tests/integration/test_agentscope_mcp.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/runtime manager-agentscope/tests
git commit -m "Call MCP tools through AgentScope instead of a sidecar CLI" \
  -m "Constraint: MCP headers come from runtime secrets and descriptors remain secret-free." \
  -m "Rejected: Retain mcporter subprocesses | AgentScope 2.0 already provides typed discovery and calls." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: AgentScope MCP unit, contract, and integration tests"
```

### Task 4: Typed MCP Desired-State Support in `agt`

**Files:**
- Modify: `agentteams-controller/internal/server/types.go`
- Modify: `agentteams-controller/internal/server/resource_handler.go`
- Modify: `agentteams-controller/cmd/agt/update.go`
- Modify: `agentteams-controller/cmd/agt/get.go`
- Modify: `agentteams-controller/cmd/agt/create_test.go`
- Modify: `agentteams-controller/cmd/agt/main_test.go`
- Modify: `manager-agentscope/src/agentteams_manager/clients/agt.py`
- Create: `manager-agentscope/tests/unit/clients/test_agt_mcp.py`

**Interfaces:**
- Adds `mcpServers` to Manager and Worker JSON responses.
- Adds `--mcp-servers-file <path|->` to `agt update manager` and `agt update worker`.
- Produces: `AgtClient.replace_manager_mcp_servers`, `replace_worker_mcp_servers`.

- [ ] **Step 1: Write Go CLI and Python argv/stdin tests**

Go test:

```go
func TestUpdateManagerMCPServersFromStdin(t *testing.T) {
    stdin := `[{"name":"github","url":"http://gateway/mcp","transport":"http"}]`
    result := runCLIWithStdin(t,
        []string{"update", "manager", "--name", "default",
            "--mcp-servers-file", "-"},
        stdin,
    )
    require.Equal(t, "github", result.Request.McpServers[0].Name)
}
```

Python test:

```python
import pytest


@pytest.mark.asyncio
async def test_replace_manager_mcp_uses_json_stdin(fake_process):
    await fake_process.agt.replace_manager_mcp_servers(
        "default",
        (fake_process.mcp("github"),),
    )
    assert fake_process.argv == (
        "agt", "update", "manager", "--name", "default",
        "--mcp-servers-file", "-",
    )
    assert b'"name":"github"' in fake_process.stdin
```

- [ ] **Step 2: Verify failure**

Run:

```bash
cd agentteams-controller && go test ./cmd/agt ./internal/server
cd .. && python -m pytest manager-agentscope/tests/unit/clients/test_agt_mcp.py -q
```

Expected: FAIL because response and CLI support are absent.

- [ ] **Step 3: Add strict descriptor replacement**

Manager and Worker responses include the current desired `[]MCPServer`; they never include consumer keys or upstream credentials. `--mcp-servers-file`:

1. reads at most 1 MiB from a named file or stdin;
2. decodes a JSON array with unknown fields rejected;
3. validates unique DNS-label-like names;
4. accepts only `http` or `sse`;
5. requires `http://` or `https://` URLs;
6. sends the complete array in the existing update request.

An empty JSON array explicitly clears all descriptors. Omitting the flag leaves them unchanged.

`AgtClient` serializes its typed tuple directly to stdin and queries the updated resource after success or ambiguous timeout.

- [ ] **Step 4: Run Go and Python tests**

Run:

```bash
cd agentteams-controller && go test ./cmd/agt ./internal/server
cd .. && python -m pytest manager-agentscope/tests/unit/clients/test_agt_mcp.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add agentteams-controller manager-agentscope/src/agentteams_manager/clients/agt.py manager-agentscope/tests/unit/clients/test_agt_mcp.py
git commit -m "Make MCP access replaceable through the typed Controller boundary" \
  -m "Constraint: MCP desired state must travel through agt and replacement must preserve the full consumer view." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: Go agt/server tests and Python AgtClient MCP tests"
```

### Task 5: Higress MCP Administration and End-to-End Verification

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/clients/higress.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/integrations.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/integrations.py`
- Create: `manager-agentscope/tests/unit/clients/test_higress.py`
- Create: `manager-agentscope/tests/unit/workflows/test_mcp_management.py`
- Create: `manager-agentscope/tests/integration/test_mcp_management_e2e.py`
- Create: `manager-agentscope/tests/fault_injection/test_mcp_replace_consumers.py`

**Interfaces:**
- Produces: `HigressClient.list_mcp_servers`, `upsert_rest_server`, `upsert_proxy`, `replace_consumers`, `delete_server`.
- Produces: `IntegrationService.configure_mcp`, `grant_mcp`, `revoke_mcp`, `delete_mcp`.
- Consumes: `AgtClient`, `MCPRegistry`, Worker resources, and runtime config polling.

- [ ] **Step 1: Write cloud refusal, replacement, and verify-before-notify tests**

```python
import pytest


@pytest.mark.asyncio
async def test_cloud_mode_refuses_before_higress_mutation(mcp_admin_fixture):
    mcp_admin_fixture.runtime = "aliyun"
    with pytest.raises(CloudMCPManagementUnsupported):
        await mcp_admin_fixture.service.configure_mcp(
            mcp_admin_fixture.rest_request()
        )
    assert mcp_admin_fixture.higress.calls == []


@pytest.mark.asyncio
async def test_consumer_update_sends_complete_set(mcp_admin_fixture):
    mcp_admin_fixture.existing_consumers("manager", "worker-alice")

    await mcp_admin_fixture.service.grant_mcp(
        "github", workers=("bob",)
    )

    assert mcp_admin_fixture.higress.replacement == {
        "manager", "worker-alice", "worker-bob"
    }


@pytest.mark.asyncio
async def test_worker_notification_occurs_after_real_tool_call(
    mcp_admin_fixture,
):
    await mcp_admin_fixture.service.configure_mcp(
        mcp_admin_fixture.rest_request(workers=("alice",))
    )
    assert mcp_admin_fixture.effect_order[-2:] == [
        "mcp.tool_call.verify",
        "matrix.worker_notification",
    ]
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_higress.py manager-agentscope/tests/unit/workflows/test_mcp_management.py manager-agentscope/tests/integration/test_mcp_management_e2e.py manager-agentscope/tests/fault_injection/test_mcp_replace_consumers.py -q
```

Expected: FAIL because Higress administration is absent.

- [ ] **Step 3: Implement the typed Higress API**

Preserve the current API contract:

```text
POST /v1/service-sources
PUT  /v1/mcpServer
GET  /v1/mcpServer
GET  /v1/mcpServer/consumers
PUT  /v1/mcpServer/consumers
DELETE /v1/mcpServer
```

Authenticate the configured Console session without logging cookie or credential data. Validate response status and `success:false`; do not treat warnings as success.

For REST-to-MCP templates, require exactly one `accessToken: ""` credential slot. Insert the credential with JSON-compatible double-quoted escaping, hold the resulting raw configuration only in memory, and send it to Higress. For proxies, accept only `http` and `sse`, parse the backend URL with `urllib.parse`, and render headers into the known Higress security schema without string concatenation of YAML syntax.

The operation sequence is:

1. refuse cloud mode;
2. require confirmation because credentials and gateway state change;
3. register/reconcile the service source;
4. upsert the MCP server;
5. read current consumers and send the complete replacement set;
6. update Manager and selected Worker MCP descriptors through `agt`;
7. poll the new Manager runtime revision;
8. poll AgentScope `list_tools` until auth propagation succeeds, bounded to 30 seconds;
9. invoke the configured verification tool with a safe request;
10. notify only selected Workers.

If any effect is ambiguous, reconcile Higress, Controller, and AgentScope facts before repeating it. Never include upstream credentials in the Controller descriptor.

- [ ] **Step 4: Run MCP administration tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_higress.py manager-agentscope/tests/unit/workflows/test_mcp_management.py manager-agentscope/tests/integration/test_mcp_management_e2e.py manager-agentscope/tests/fault_injection/test_mcp_replace_consumers.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/clients/higress.py manager-agentscope/src/agentteams_manager/workflows/integrations.py manager-agentscope/src/agentteams_manager/tools/integrations.py manager-agentscope/tests
git commit -m "Verify MCP access end to end before giving it to Workers" \
  -m "Constraint: Higress consumer updates replace the complete set and cloud mode uses its console." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Never persist upstream MCP credentials outside Higress." \
  -m "Tested: Higress, MCP workflow, end-to-end, and replacement fault tests"
```

### Task 6: Public Service Publishing Through Controller Reconciliation

**Files:**
- Modify: `agentteams-controller/cmd/agt/update.go`
- Modify: `agentteams-controller/cmd/agt/main_test.go`
- Modify: `manager-agentscope/src/agentteams_manager/clients/agt.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/integrations.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/integrations.py`
- Create: `manager-agentscope/tests/unit/workflows/test_service_publishing.py`
- Create: `manager-agentscope/tests/integration/test_service_publishing.py`

**Interfaces:**
- Adds: `agt update worker --clear-expose`.
- Produces: `IntegrationService.publish_service`, `unpublish_service`.
- Consumes: Worker `exposedPorts` status and `AgtClient.update_worker_expose`.

- [ ] **Step 1: Write public-warning and status-convergence tests**

```python
import pytest


@pytest.mark.asyncio
async def test_publish_requires_confirmation(service_fixture):
    decision = await service_fixture.tool.check_permissions(
        {"worker": "alice", "ports": [8080]},
        service_fixture.permission_context(),
    )
    assert decision.behavior.value == "ask"
    assert "public" in decision.message.lower()


@pytest.mark.asyncio
async def test_publish_returns_controller_observed_domain(service_fixture):
    service_fixture.agt.worker_status_sequence(
        exposed=(),
        exposed=((8080, "worker-alice-8080-local.agentteams.io"),),
    )

    receipt = await service_fixture.service.publish_service(
        worker="alice", ports=(8080,)
    )

    assert receipt.domains == (
        "worker-alice-8080-local.agentteams.io",
    )
```

- [ ] **Step 2: Verify failure**

Run:

```bash
cd agentteams-controller && go test ./cmd/agt
cd .. && python -m pytest manager-agentscope/tests/unit/workflows/test_service_publishing.py manager-agentscope/tests/integration/test_service_publishing.py -q
```

Expected: FAIL because explicit clearing and publishing workflows are incomplete.

- [ ] **Step 3: Implement replace-all expose semantics**

Add `--clear-expose` as mutually exclusive with `--expose`; it sends an explicit empty array. Existing `--expose` continues to replace the complete desired list.

`publish_service`:

1. validates ports are unique integers in 1–65535;
2. reads current Controller-observed exposed ports;
3. computes the full desired union;
4. warns that routes are public and waits for confirmation;
5. journals and calls `agt update worker --name <name> --expose <csv>`;
6. polls the Worker until every port has an observed domain;
7. returns domains reported by Controller rather than predicting them.

`unpublish_service` computes the full remainder and uses `--clear-expose` when empty. Cloud providers that report unsupported exposure return a typed unsupported result instead of claiming a public route exists.

- [ ] **Step 4: Run publishing tests**

Run:

```bash
cd agentteams-controller && go test ./cmd/agt
cd .. && python -m pytest manager-agentscope/tests/unit/workflows/test_service_publishing.py manager-agentscope/tests/integration/test_service_publishing.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add agentteams-controller/cmd/agt manager-agentscope
git commit -m "Publish Worker services only after Controller proves the route" \
  -m "Constraint: Current exposed routes are public and expose updates replace the full port set." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: agt CLI and service publishing tests"
```

### Task 7: Integration Tools, Cross-System Recovery, and Integration Gate

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/tools/configuration.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/integrations.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/heartbeat.py`
- Modify: `manager/agent/skills/mcp-server-management/SKILL.md`
- Modify: `manager/agent/skills/mcp-server-management/references/api-commands.md`
- Modify: `manager/agent/skills/mcp-server-management/references/create-update-server.md`
- Modify: `manager/agent/skills/mcp-server-management/references/custom-yaml-guide.md`
- Modify: `manager/agent/skills/mcp-server-management/references/setup-mcp-proxy.md`
- Modify: `manager/agent/skills/mcporter/SKILL.md`
- Modify: `manager/agent/skills/model-switch/SKILL.md`
- Modify: `manager/agent/skills/service-publishing/SKILL.md`
- Modify: `manager/agent/skills/worker-model-switch/SKILL.md`
- Create: `manager-agentscope/tests/contract/test_integration_skill_parity.py`
- Create: `manager-agentscope/tests/integration/test_integration_recovery.py`
- Create: `manager-agentscope/tests/fault_injection/test_integration_effect_boundaries.py`

**Interfaces:**
- Registers policy-bound AgentScope model, MCP, service, and channel integration tools.
- Extends heartbeat with configuration and integration reconciliation.

- [ ] **Step 1: Write skill coverage and crash-boundary tests**

```python
import pytest


def test_integration_skills_have_owned_acceptance_tests(skill_registry):
    assert skill_registry.covered({
        "channel-management",
        "mcp-server-management",
        "mcporter",
        "model-switch",
        "service-publishing",
        "worker-model-switch",
    })


@pytest.mark.parametrize(
    "boundary",
    (
        "after_higress_upsert",
        "after_consumer_replace",
        "after_manager_descriptor_update",
        "after_worker_descriptor_update",
        "after_service_expose_update",
    ),
)
@pytest.mark.asyncio
async def test_restart_reconciles_integration_boundary(
    boundary, integration_fault_fixture
):
    integration_fault_fixture.crash_at(boundary)
    await integration_fault_fixture.run_and_restart()
    assert integration_fault_fixture.desired_state_converged()
```

The parity test loads all five integration skill families, keeps the useful
`mcp-github.yaml` declarative example, verifies exact typed tool names, and
rejects `mcporter` command execution, direct Higress mutation from the model,
deleted shell scripts, and legacy Manager channel commands.

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/contract/test_integration_skill_parity.py manager-agentscope/tests/integration/test_integration_recovery.py manager-agentscope/tests/fault_injection/test_integration_effect_boundaries.py -q
```

Expected: FAIL because integration recovery and parity evidence are incomplete.

- [ ] **Step 3: Register reconcilers and enforce room policy**

Heartbeat integration order:

1. poll the Controller runtime document;
2. prepare and activate valid model/MCP generations;
3. reconcile incomplete Higress MCP operations;
4. reconcile Controller MCP descriptor sets;
5. retry bounded end-to-end MCP verification;
6. reconcile service exposure status;
7. send pending Worker/Admin notifications exactly once.

Only an Admin Room may receive Manager model, MCP administration, or service publishing tools. A Leader Room may request a Worker model change only for its Team members if topology policy permits it. Matrix remains the sole Manager channel; the tool set contains no DingTalk, Feishu, QQ, or generic channel adapter.

Rewrite the listed integration skills so AgentScope-native MCP discovery,
Controller desired-state updates, Higress reconciliation, model preflight, and
service publishing are the only execution paths. Preserve upstream permission,
consumer-replacement, provider-routing, and endpoint-verification rules. The
`mcporter` skill name remains for behavioral compatibility, but its document
must explicitly use AgentScope `Toolkit` discovery/call tools instead of the
`mcporter` executable.

- [ ] **Step 4: Run the complete integration gate**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_config_watcher.py manager-agentscope/tests/unit/runtime/test_mcp.py -q
python -m pytest manager-agentscope/tests/unit/clients/test_model_gateway.py manager-agentscope/tests/unit/clients/test_higress.py manager-agentscope/tests/unit/clients/test_agt_mcp.py -q
python -m pytest manager-agentscope/tests/unit/workflows/test_model_switch.py manager-agentscope/tests/unit/workflows/test_mcp_management.py manager-agentscope/tests/unit/workflows/test_service_publishing.py -q
python -m pytest manager-agentscope/tests/integration/test_turn_boundary_reload.py manager-agentscope/tests/integration/test_model_hot_reload.py manager-agentscope/tests/integration/test_agentscope_mcp.py manager-agentscope/tests/integration/test_mcp_management_e2e.py manager-agentscope/tests/integration/test_service_publishing.py manager-agentscope/tests/integration/test_integration_recovery.py -q
python -m pytest manager-agentscope/tests/fault_injection/test_invalid_runtime_document.py manager-agentscope/tests/fault_injection/test_mcp_replace_consumers.py manager-agentscope/tests/fault_injection/test_integration_effect_boundaries.py -q
python -m pytest manager-agentscope/tests/contract/test_mcporter_parity.py manager-agentscope/tests/contract/test_integration_skill_parity.py -q
cd agentteams-controller && go test ./cmd/agt ./internal/server
cd .. && git diff --check
```

Expected: all tests PASS and diff check has no output.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope agentteams-controller manager/agent/skills
git commit -m "Close integration parity behind typed desired state" \
  -m "Constraint: Controller, Higress, MinIO, and AgentScope each remain authoritative for their own facts." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: complete Integration gate"
```
