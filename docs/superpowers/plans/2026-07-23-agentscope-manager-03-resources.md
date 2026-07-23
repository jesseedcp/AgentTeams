# AgentScope Manager Resource Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproduce Manager control of Workers, Teams, Humans, Matrix topology, and Nacos-discovered Workers through typed Controller operations and deterministic reconciliation.

**Architecture:** Agent-facing tools call workflow services; workflow services call one typed `AgtClient`; only `AgtClient` executes the `agt` binary. Resource mutations use stable resource names as Controller idempotency keys, journal intent before execution, and reconcile ambiguous results by querying the Controller rather than repeating commands.

**Tech Stack:** Python 3.11+, AgentScope 2.0 tools and permissions, Pydantic 2, `asyncio.create_subprocess_exec`, `agt`, SQLite operation/topology repositories, Matrix client from Plan 02, pytest.

## Global Constraints

- Apply every constraint and cross-plan interface from `2026-07-23-agentscope-manager-master.md`.
- Controller resources remain authoritative; never synthesize a successful Worker, Team, or Human from local state alone.
- Invoke `agt` with an argv tuple and `asyncio.create_subprocess_exec`; never use a shell.
- Parse JSON into strict Pydantic models before returning it to a workflow or model.
- Use `agt get ... -o json`, `agt create ... -o json`, and `agt worker status ... -o json` where supported.
- `agt apply -f` does not support JSON output; treat its stdout only as diagnostic text and prove success with a subsequent `agt get`.
- A timeout or lost process result is ambiguous. Query by stable resource name before deciding whether to retry.
- Keep OpenClaw, CoPaw, Hermes, QwenPaw, and OpenHuman as valid Worker runtimes.
- A Team topology is valid only when the Manager is in the Leader Room and is absent from the Team Room, Leader DM Room, and Team Worker rooms.
- Channel management in this release is Matrix-only.

---

### Task 1: Safe Process Runner and Typed `AgtClient`

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/clients/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/clients/process.py`
- Create: `manager-agentscope/src/agentteams_manager/clients/agt.py`
- Create: `manager-agentscope/tests/unit/clients/test_process.py`
- Create: `manager-agentscope/tests/unit/clients/test_agt.py`
- Create: `manager-agentscope/tests/fixtures/fake_agt.py`

**Interfaces:**
- Produces: `ProcessRunner.run(argv, *, stdin, cwd, timeout) -> ProcessResult`
- Produces: `AgtClient.get_worker`, `list_workers`, `create_worker`, `update_worker`, `sleep_worker`, `wake_worker`, `delete_worker`
- Produces: `AgtClient.get_team`, `list_teams`, `create_team`, `apply_team`, `delete_team`
- Produces: `AgtClient.get_human`, `list_humans`, `create_human`, `delete_human`
- Consumes: the `ControllerPort` contract from Plan 01.

- [ ] **Step 1: Write argv, redaction, and typed-output tests**

```python
import pytest

from agentteams_manager.clients.agt import AgtClient
from agentteams_manager.clients.process import ProcessTimeout


@pytest.mark.asyncio
async def test_get_worker_uses_json_and_parses_runtime(fake_process) -> None:
    fake_process.queue_json(
        {
            "name": "alice",
            "phase": "Running",
            "model": "qwen3.6-plus",
            "runtime": "qwenpaw",
            "image": "agentteams-worker:qwenpaw",
            "containerState": "running",
            "matrixUserID": "@worker-alice:matrix.local",
            "roomID": "!leader:matrix.local",
            "message": "",
            "team": "",
            "role": "",
        }
    )
    client = AgtClient(fake_process)

    worker = await client.get_worker("alice")

    assert worker is not None
    assert worker.runtime == "qwenpaw"
    assert fake_process.argv == (
        "agt", "get", "workers", "alice", "-o", "json"
    )


@pytest.mark.asyncio
async def test_process_timeout_does_not_log_stdin_secret(
    caplog, fake_process_factory
) -> None:
    runner = fake_process_factory(timeout=True)
    with pytest.raises(ProcessTimeout):
        await runner.run(
            ("agt", "apply", "-f", "-"),
            stdin=b'{"token":"never-log-this"}',
            timeout=0.01,
        )
    assert "never-log-this" not in caplog.text
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_process.py manager-agentscope/tests/unit/clients/test_agt.py -q
```

Expected: FAIL because the process and `agt` clients are absent.

- [ ] **Step 3: Implement an allowlisted, shell-free process boundary**

`ProcessRunner` rejects empty argv, path separators in `argv[0]`, and executables outside the constructor allowlist. Its production instance allows only `agt`:

```python
process = await asyncio.create_subprocess_exec(
    *argv,
    stdin=asyncio.subprocess.PIPE if stdin is not None else None,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    env=self._environment,
)
stdout, stderr = await asyncio.wait_for(
    process.communicate(stdin),
    timeout=timeout,
)
```

On timeout, terminate, wait for two seconds, then kill and wait. Raise `ProcessTimeout` without embedding stdin or secret environment values.

Define strict resource models with the exact current CLI field aliases:

```python
class WorkerResource(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    phase: str
    model: str
    runtime: Literal[
        "openclaw", "copaw", "hermes", "qwenpaw", "openhuman"
    ]
    image: str = ""
    container_state: str = Field(default="", alias="containerState")
    matrix_user_id: str = Field(default="", alias="matrixUserID")
    room_id: str = Field(default="", alias="roomID")
    message: str = ""
    team: str = ""
    role: str = ""
```

Map only known request fields to argv. Never pass model-generated flags through unchanged. Treat CLI exit code 1 plus a normalized not-found response as `None`; all other nonzero exits raise `AgtCommandError` with redacted stderr.

- [ ] **Step 4: Run client tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/clients manager-agentscope/tests/unit/clients manager-agentscope/tests/fixtures
git commit -m "Keep Controller mutations behind one typed command boundary" \
  -m "Constraint: Manager tools may use the Controller only through agt argv calls." \
  -m "Rejected: Controller HTTP calls from Python | that would duplicate auth and resource contracts." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Add every new agt flag to a typed request model before exposing it." \
  -m "Tested: python -m pytest manager-agentscope/tests/unit/clients -q"
```

### Task 2: Resource Reconciliation and Matrix Topology Resolver

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Create: `manager-agentscope/tests/unit/workflows/test_resource_reconciliation.py`
- Create: `manager-agentscope/tests/unit/workflows/test_topology_resolver.py`
- Create: `manager-agentscope/tests/fault_injection/test_ambiguous_resource_create.py`

**Interfaces:**
- Produces: `ResourceReconciler.reconcile(operation) -> ReconcileResult`
- Produces: `TopologyResolver.refresh() -> TopologySnapshot`
- Produces: `TopologyResolver.policy_for(room_id, sender_id) -> RoomPolicy`
- Consumes: `AgtClient`, `MatrixPort`, `TopologyRepository`, and `OperationSupervisor`.

- [ ] **Step 1: Write ambiguity and topology-invariant tests**

```python
import pytest


@pytest.mark.asyncio
async def test_timed_out_worker_create_queries_before_retry(resource_fixture):
    resource_fixture.agt.create_worker_times_out()
    resource_fixture.agt.get_worker_returns_running("alice")

    worker = await resource_fixture.service.create_worker(
        resource_fixture.create_worker_request(name="alice")
    )

    assert worker.name == "alice"
    assert resource_fixture.agt.create_worker_calls == 1
    assert resource_fixture.agt.get_worker_calls == 1


@pytest.mark.asyncio
async def test_manager_is_not_allowed_in_team_worker_room(topology_fixture):
    topology_fixture.team(
        name="alpha",
        leader_room="!leader:example",
        team_room="!team:example",
        leader_dm_room="!dm:example",
        worker_rooms=("!worker:example",),
    )

    snapshot = await topology_fixture.resolver.refresh()

    assert snapshot.policy("!leader:example").kind == "leader"
    assert "!team:example" not in snapshot.manager_join_targets
    assert "!dm:example" not in snapshot.manager_join_targets
    assert "!worker:example" not in snapshot.manager_join_targets
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_resource_reconciliation.py manager-agentscope/tests/unit/workflows/test_topology_resolver.py manager-agentscope/tests/fault_injection/test_ambiguous_resource_create.py -q
```

Expected: FAIL because resource reconciliation and topology resolution are absent.

- [ ] **Step 3: Implement fact-based reconciliation**

Register reconcilers by `OperationKind`. For `CREATE_WORKER`, query `get_worker(target_name)`:

```python
async def reconcile_create_worker(
    self, operation: OperationRecord
) -> ReconcileResult:
    worker = await self._agt.get_worker(operation.target_name)
    if worker is None:
        return ReconcileResult.effect_absent()
    if worker.phase in {"Failed", "Deleting"}:
        return ReconcileResult.failed(worker.message)
    return ReconcileResult.succeeded(
        receipt=worker.model_dump(mode="json", by_alias=True)
    )
```

Use equivalent resource-specific proof for Team and Human creation. Never infer absence from a stale topology row.

`TopologyResolver.refresh()` gathers Controller Worker/Team/Human facts and Matrix memberships, then atomically replaces topology rows. It must reject:

- the same room assigned incompatible kinds;
- Manager membership in Team, Leader DM, or Team Worker rooms;
- a Leader Room that does not include the Manager;
- a claimed Worker/Human sender that does not match Controller identity.

Unknown rooms receive a hard-deny policy. Unknown senders in an Admin Room receive only read-only tools until explicitly trusted.

- [ ] **Step 4: Run reconciliation and topology tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_resource_reconciliation.py manager-agentscope/tests/unit/workflows/test_topology_resolver.py manager-agentscope/tests/fault_injection/test_ambiguous_resource_create.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows/resources.py manager-agentscope/tests
git commit -m "Recover resource changes from Controller facts" \
  -m "Constraint: Command timeouts are ambiguous and team room isolation is mandatory." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: resource reconciliation, topology, and ambiguous-create tests"
```

### Task 3: Worker Lifecycle Workflow

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Create: `manager-agentscope/tests/unit/workflows/test_workers.py`
- Create: `manager-agentscope/tests/integration/test_worker_lifecycle.py`

**Interfaces:**
- Produces: `ResourceService.create_worker`, `list_workers`, `update_worker`, `sleep_worker`, `wake_worker`, `delete_worker`
- Produces: `ResourceHeartbeat.reconcile_pending_workers()`
- Consumes: `AgtClient`, `MatrixPort`, `OperationSupervisor`, and `TopologyResolver`.

- [ ] **Step 1: Write Worker lifecycle tests**

```python
import pytest


@pytest.mark.asyncio
async def test_create_worker_waits_for_room_then_greets(worker_fixture):
    worker_fixture.agt.create_returns(
        name="alice",
        phase="Pending",
        runtime="copaw",
    )
    worker_fixture.agt.get_sequence(
        ("Pending", ""),
        ("Running", "!alice:example"),
    )

    worker = await worker_fixture.service.create_worker(
        worker_fixture.request(
            name="alice",
            runtime="copaw",
            model="qwen3.6-plus",
        )
    )

    assert worker.room_id == "!alice:example"
    assert worker_fixture.matrix.sent[-1].room_id == "!alice:example"
    assert worker_fixture.matrix.sent[-1].txn_id.startswith("agentteams:")


@pytest.mark.asyncio
async def test_all_five_worker_runtimes_are_accepted(worker_fixture):
    accepted = {
        "openclaw", "copaw", "hermes", "qwenpaw", "openhuman"
    }
    for runtime in accepted:
        worker_fixture.agt.reset_running(runtime)
        result = await worker_fixture.service.create_worker(
            worker_fixture.request(name=f"worker-{runtime}", runtime=runtime)
        )
        assert result.runtime == runtime
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_workers.py manager-agentscope/tests/integration/test_worker_lifecycle.py -q
```

Expected: FAIL because the Worker workflow is incomplete.

- [ ] **Step 3: Implement stable Worker operations**

Validate names with the Controller rule `^[a-z0-9][a-z0-9-]*$`. Build typed commands:

```text
agt create worker --name <name> --runtime <runtime> --model <model> --no-wait -o json
agt get workers <name> -o json
agt update worker --name <name> <typed changed flags>
agt worker sleep <name>
agt worker wake <name>
agt delete worker <name>
```

Creation sequence:

1. derive `operation_id` from the Matrix event and tool call;
2. reject a conflicting existing Worker;
3. journal Controller intent;
4. run `create --no-wait`;
5. poll `get worker` with bounded exponential backoff;
6. persist the Worker Room topology only after Controller returns `roomID`;
7. send one idempotent greeting using the operation transaction ID;
8. mark the operation complete.

If the bounded wait expires, leave the operation `RECONCILING`; `ResourceHeartbeat` continues checking without repeating create. Update, sleep, wake, and delete prove convergence with `get worker`. Delete also removes stale topology in the same local transaction after Controller proves absence.

- [ ] **Step 4: Run Worker tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_workers.py manager-agentscope/tests/integration/test_worker_lifecycle.py manager-agentscope/tests/fault_injection/test_ambiguous_resource_create.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows/resources.py manager-agentscope/tests
git commit -m "Preserve Worker lifecycle across every supported runtime" \
  -m "Constraint: OpenClaw, CoPaw, Hermes, QwenPaw, and OpenHuman remain Worker runtimes." \
  -m "Rejected: A local pending-workers file | SQLite operations and Controller reconciliation replace it." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Worker lifecycle unit, integration, and fault-injection tests"
```

### Task 4: Team Creation and Leader-Room Delegation

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Create: `manager-agentscope/tests/unit/workflows/test_teams.py`
- Create: `manager-agentscope/tests/integration/test_team_topology.py`

**Interfaces:**
- Produces: `ResourceService.create_team`, `apply_team`, `get_team`, `list_teams`, `delete_team`
- Produces: `TeamSpec.to_apply_document() -> bytes`
- Consumes: Controller Team and Worker resources plus Matrix membership facts.

- [ ] **Step 1: Write mixed-runtime and isolation tests**

```python
import pytest


@pytest.mark.asyncio
async def test_mixed_runtime_team_uses_apply_then_get(team_fixture):
    spec = team_fixture.spec(
        name="alpha",
        leader=("alpha-lead", "qwenpaw"),
        workers=(("researcher", "hermes"), ("coder", "openclaw")),
    )

    team = await team_fixture.service.apply_team(spec)

    assert team.name == "alpha"
    assert team_fixture.agt.apply_documents[0]["kind"] == "Team"
    assert team_fixture.agt.get_team_calls >= 1


@pytest.mark.asyncio
async def test_delegation_is_available_only_in_leader_room(team_fixture):
    topology = team_fixture.valid_topology()
    assert "delegate_team_task" in topology.leader_policy.allowed_tools
    assert "delegate_team_task" not in topology.team_policy.allowed_tools
    assert "delegate_team_task" not in topology.worker_policy.allowed_tools
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_teams.py manager-agentscope/tests/integration/test_team_topology.py -q
```

Expected: FAIL because Team workflows and apply documents are absent.

- [ ] **Step 3: Implement simple-create and full-spec paths**

Use `agt create team` only when the request is expressible by its flags. Use `agt apply -f -` for per-member runtime/image/model settings. Serialize a fixed schema; never pass through an arbitrary model-produced manifest:

```python
class TeamSpec(BaseModel):
    name: str
    description: str = ""
    leader: TeamMemberSpec
    workers: tuple[TeamMemberSpec, ...]
    leader_heartbeat_every: str = "30m"
    worker_idle_timeout: str = "12h"

    def to_apply_document(self) -> bytes:
        return json.dumps(
            {
                "apiVersion": "agentteams.ai/v1alpha1",
                "kind": "Team",
                "metadata": {"name": self.name},
                "spec": {
                    "teamName": self.name,
                    "description": self.description,
                    "leader": self.leader.to_document(),
                    "workers": [item.to_document() for item in self.workers],
                    "leaderHeartbeatEvery": self.leader_heartbeat_every,
                    "workerIdleTimeout": self.worker_idle_timeout,
                },
            },
            separators=(",", ":"),
        ).encode()
```

After create/apply, poll `agt get teams <name> -o json`, load every named Worker, and validate Matrix memberships before marking complete. Manager joins only the Leader Room. Delegation sends the finite task protocol to the Team Leader and records the Team name on the task; it never sends the instruction directly to Team Workers.

- [ ] **Step 4: Run Team tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_teams.py manager-agentscope/tests/integration/test_team_topology.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows/resources.py manager-agentscope/tests
git commit -m "Preserve Team hierarchy while allowing mixed Worker runtimes" \
  -m "Constraint: Manager delegates only through the Leader Room and never enters Team-private rooms." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Team workflow and topology integration tests"
```

### Task 5: Human Access, Matrix Administration, and Channel Policy

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Modify: `manager-agentscope/src/agentteams_manager/domain/ports.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/resources.py`
- Create: `manager-agentscope/tests/unit/workflows/test_humans.py`
- Create: `manager-agentscope/tests/unit/tools/test_resource_permissions.py`
- Create: `manager-agentscope/tests/integration/test_matrix_resource_admin.py`

**Interfaces:**
- Produces: Human create/list/delete workflows.
- Produces: Matrix user, room, membership, media, contact, and primary-channel management tools.
- Produces: policy sets for Admin, Leader, Worker, Human, trusted, and unknown contexts.

- [ ] **Step 1: Write Human scope and channel fallback tests**

```python
import pytest


@pytest.mark.asyncio
async def test_level_two_human_cannot_manage_workers(human_fixture):
    policy = human_fixture.policy(permission_level=2)
    assert "create_worker" not in policy.allowed_tools
    assert "delete_worker" not in policy.allowed_tools
    assert policy.allowed_rooms == {
        "!selected-team:example",
        "!selected-worker:example",
    }


@pytest.mark.asyncio
async def test_missing_primary_room_falls_back_to_manager_admin_room(
    channel_fixture,
):
    channel_fixture.primary_room_missing()

    room_id = await channel_fixture.resolver.notification_room(
        recipient="@human-alice:example"
    )

    assert room_id == channel_fixture.manager_admin_room
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_humans.py manager-agentscope/tests/unit/tools/test_resource_permissions.py manager-agentscope/tests/integration/test_matrix_resource_admin.py -q
```

Expected: FAIL because Human and channel policies are absent.

- [ ] **Step 3: Implement deterministic policy tiers**

Map the existing three Human permission levels into immutable tool sets:

| Context | Read resources | Create/update resources | Delete resources | Matrix admin |
| --- | --- | --- | --- | --- |
| Admin-equivalent / level 1 | all scoped resources | ask | ask | ask |
| Team-scoped / level 2 | selected Teams and Workers | no | no | no |
| Worker-only / level 3 | selected Workers | no | no | no |
| Unknown | no | no | no | no |

Human creation calls:

```text
agt create human --name <name> --display-name <display> --email <email> --permission-level <level>
```

The create command emits text, so prove the result with
`agt get humans <name> -o json`.

Matrix administrative tools use only Plan 02's Matrix adapter and cover user lookup, room creation, invite, kick, ban, unban, membership, room state, and media upload/download. Every mutation requires AgentScope confirmation unless `yolo` is explicitly configured.

Store primary channel and trusted-contact relationships in the `topology` table, keyed by Matrix user ID and room ID. Notification resolution checks the recipient primary room, then a shared trusted room, then the Manager Admin Room; it never selects an arbitrary room from recent traffic.

- [ ] **Step 4: Run Human, policy, and Matrix administration tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_humans.py manager-agentscope/tests/unit/tools/test_resource_permissions.py manager-agentscope/tests/integration/test_matrix_resource_admin.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows/resources.py manager-agentscope/src/agentteams_manager/tools/resources.py manager-agentscope/tests
git commit -m "Make Human and channel authority explicit in room policy" \
  -m "Constraint: Manager channels are Matrix-only and permission levels remain three-tiered." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Human, resource permission, and Matrix administration tests"
```

### Task 6: Nacos Worker Discovery, Confirmation, and Import

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/clients/nacos.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/resources.py`
- Create: `manager-agentscope/tests/unit/clients/test_nacos.py`
- Create: `manager-agentscope/tests/integration/test_find_worker_import.py`

**Interfaces:**
- Produces: `NacosClient.search_workers(query) -> tuple[NacosWorker, ...]`
- Produces: `ResourceService.find_worker`, `confirm_import`, `import_worker`
- Consumes: Nacos credentials from process environment and `AgtClient.apply_worker`.

- [ ] **Step 1: Write search and no-fallback tests**

```python
import pytest


@pytest.mark.asyncio
async def test_find_worker_requires_confirmation_before_import(find_fixture):
    find_fixture.nacos.search_returns(
        name="remote-coder",
        package_uri="nacos://registry/remote-coder",
    )

    result = await find_fixture.service.find_worker("coder")

    assert result.requires_confirmation
    assert find_fixture.agt.apply_worker_calls == 0


@pytest.mark.asyncio
async def test_failed_import_does_not_create_generic_worker(find_fixture):
    find_fixture.nacos.search_returns(
        name="remote-coder",
        package_uri="nacos://registry/remote-coder",
    )
    find_fixture.agt.apply_worker_fails("package signature invalid")

    with pytest.raises(WorkerImportError):
        await find_fixture.service.import_worker(
            find_fixture.confirmed_result()
        )

    assert find_fixture.agt.create_worker_calls == 0
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_nacos.py manager-agentscope/tests/integration/test_find_worker_import.py -q
```

Expected: FAIL because Nacos discovery and import are absent.

- [ ] **Step 3: Implement typed Nacos discovery**

`NacosClient` sends typed HTTP requests to the configured registry, validates every response, limits result count, and strips credentials from errors. A result contains only:

```python
class NacosWorker(BaseModel):
    name: str
    display_name: str
    description: str
    runtime: Literal[
        "openclaw", "copaw", "hermes", "qwenpaw", "openhuman"
    ]
    package_uri: str
    version: str
    digest: str
```

The AgentScope tool first returns candidates as a confirmation event. After confirmation, `AgtClient.apply_worker_package(package_uri, expected_digest)` calls the existing `agt apply worker --package ...` path, then queries `agt get workers <name> -o json`. A failed import reports the exact redacted failure and stops; it must not silently create a generic Worker.

- [ ] **Step 4: Run discovery tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_nacos.py manager-agentscope/tests/integration/test_find_worker_import.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/clients/nacos.py manager-agentscope/src/agentteams_manager/workflows/resources.py manager-agentscope/tests
git commit -m "Keep discovered Workers auditable from search through import" \
  -m "Constraint: Nacos import requires confirmation and must not fall back to generic creation." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: Nacos client and Worker import integration tests"
```

### Task 7: AgentScope Resource Tools, Heartbeat, and Resource Gate

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/tools/resources.py`
- Create: `manager-agentscope/src/agentteams_manager/workflows/heartbeat.py`
- Modify: `manager/agent/skills/agentteams-find-worker/SKILL.md`
- Modify: `manager/agent/skills/agentteams-find-worker/references/import-worker-template.md`
- Modify: `manager/agent/skills/channel-management/SKILL.md`
- Modify: `manager/agent/skills/channel-management/references/identity-and-contacts.md`
- Modify: `manager/agent/skills/channel-management/references/primary-channel.md`
- Modify: `manager/agent/skills/human-management/SKILL.md`
- Modify: `manager/agent/skills/human-management/references/create-human.md`
- Modify: `manager/agent/skills/matrix-server-management/SKILL.md`
- Modify: `manager/agent/skills/matrix-server-management/references/api-reference.md`
- Modify: `manager/agent/skills/team-management/SKILL.md`
- Modify: `manager/agent/skills/team-management/references/create-team.md`
- Modify: `manager/agent/skills/team-management/references/team-lifecycle.md`
- Modify: `manager/agent/skills/team-management/references/team-task-delegation.md`
- Modify: `manager/agent/skills/worker-management/SKILL.md`
- Modify: `manager/agent/skills/worker-management/references/console.md`
- Modify: `manager/agent/skills/worker-management/references/create-worker.md`
- Modify: `manager/agent/skills/worker-management/references/lifecycle.md`
- Modify: `manager/agent/skills/worker-management/references/peer-mentions.md`
- Modify: `manager/agent/skills/worker-management/references/skills-management.md`
- Create: `manager-agentscope/tests/unit/tools/test_resources.py`
- Create: `manager-agentscope/tests/integration/test_resource_heartbeat.py`
- Create: `manager-agentscope/tests/contract/test_resource_skill_parity.py`

**Interfaces:**
- Produces registered AgentScope tools for Worker, Team, Human, Matrix resource, channel, and Nacos workflows.
- Produces: `Heartbeat.run_once() -> HeartbeatReport`.

- [ ] **Step 1: Write tool-schema, heartbeat, and skill-parity tests**

```python
import pytest


def test_resource_tools_have_closed_input_schemas(resource_toolkit):
    for tool in resource_toolkit.tools:
        schema = tool.input_schema
        assert schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_heartbeat_reconciles_without_model_call(heartbeat_fixture):
    heartbeat_fixture.pending_create("worker/alice")

    report = await heartbeat_fixture.heartbeat.run_once()

    assert report.reconciled == 1
    assert heartbeat_fixture.model.calls == 0


def test_resource_skills_have_owned_acceptance_tests(skill_registry):
    assert skill_registry.covered({"agentteams-find-worker",
                                   "channel-management",
                                   "human-management",
                                   "matrix-server-management",
                                   "team-management",
                                   "worker-management"})
```

The parity test also loads each listed `SKILL.md` and reference, verifies that
every named operation maps to a registered typed tool, and rejects legacy
executor paths, OpenClaw/CoPaw Manager commands, `state.json`,
`workers-registry.json`, `pending-workers.json`, and manual Worker JSON
templates.

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/tools/test_resources.py manager-agentscope/tests/integration/test_resource_heartbeat.py manager-agentscope/tests/contract/test_resource_skill_parity.py -q
```

Expected: FAIL because tool registration and resource heartbeat are incomplete.

- [ ] **Step 3: Register tools and deterministic heartbeat jobs**

Every mutating tool:

1. receives a strict Pydantic request;
2. rechecks room policy;
3. uses the Matrix event and tool-call identifiers to derive `operation_id`;
4. invokes exactly one workflow method;
5. returns a typed receipt that omits secrets and raw subprocess output.

`Heartbeat.run_once()` performs deterministic jobs in this order:

1. recover pending resource operations;
2. refresh Controller and Matrix topology;
3. continue Worker readiness polling and greeting;
4. remove topology rows for proven-deleted resources;
5. emit one admin notification for newly terminal failures.

The model is not called for reconciliation. A separate conversational heartbeat may summarize the typed report only when policy requests it.

Rewrite the six resource skill families and useful references in the file list.
Keep their upstream business rules, permission boundaries, confirmation points,
topology invariants, and user-facing examples. Replace script invocation and
handwritten runtime payload instructions with the exact AgentScope tool schema
and typed receipt. References that add no contract beyond a deleted script are
removed rather than paraphrased.

- [ ] **Step 4: Run the complete resource gate**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients -q
python -m pytest manager-agentscope/tests/unit/workflows/test_resource_reconciliation.py manager-agentscope/tests/unit/workflows/test_topology_resolver.py manager-agentscope/tests/unit/workflows/test_workers.py manager-agentscope/tests/unit/workflows/test_teams.py manager-agentscope/tests/unit/workflows/test_humans.py -q
python -m pytest manager-agentscope/tests/unit/tools/test_resources.py manager-agentscope/tests/unit/tools/test_resource_permissions.py -q
python -m pytest manager-agentscope/tests/integration/test_worker_lifecycle.py manager-agentscope/tests/integration/test_team_topology.py manager-agentscope/tests/integration/test_matrix_resource_admin.py manager-agentscope/tests/integration/test_find_worker_import.py manager-agentscope/tests/integration/test_resource_heartbeat.py -q
python -m pytest manager-agentscope/tests/fault_injection/test_ambiguous_resource_create.py -q
python -m pytest manager-agentscope/tests/contract/test_resource_skill_parity.py -q
git diff --check
```

Expected: all tests PASS and diff check has no output.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope manager/agent/skills
git commit -m "Expose complete resource management through policy-bound tools" \
  -m "Constraint: Reconciliation is deterministic and never depends on a model turn." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: complete Resource gate"
```
