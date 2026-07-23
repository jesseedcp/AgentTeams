# AgentScope Manager Matrix Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CoPaw/OpenClaw Matrix bridge with a runtime-neutral Matrix adapter that preserves upstream sync, E2EE, history, media, mention, thread, retry, and room-session behavior.

**Architecture:** `MatrixClient` is the only module that owns `nio.AsyncClient`. It emits normalized `InboundEvent` records to `EventRouter`; `RoomPolicyResolver` binds each event to hard permissions; `MatrixSessionRunner` calls the room-scoped AgentScope session and projects streamed AgentScope events back into Matrix.

**Tech Stack:** `matrix-nio[e2e]>=0.24.0`, AgentScope events, asyncio queues and locks, SQLite repositories from Plan 01, pytest.

## Global Constraints

- Complete Plan 01 before this plan.
- Preserve `session_id = matrix:<room_id>`.
- Serialize events inside one room and allow different rooms to run concurrently.
- Persist Matrix sync token, E2EE store, event IDs, outbound transaction IDs, and pending confirmations.
- Unknown group senders receive no response.
- Manager must not join Team Room, Leader DM, or Team Worker Rooms during normal Team operation.
- Do not import CoPaw `BaseChannel` or `agentscope_runtime`.
- Port protocol behavior from `copaw/src/matrix/channel.py`; do not retain its runtime-specific request or response types.

---

### Task 1: Matrix Event and Configuration Models

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/matrix/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/matrix/client.py`
- Create: `manager-agentscope/tests/unit/matrix/test_config.py`
- Create: `manager-agentscope/tests/fixtures/matrix_events.py`

**Interfaces:**
- Produces: `MatrixClientConfig`
- Produces: `MatrixClient.start(handler)`, `stop()`, and the `MatrixPort` methods.
- Consumes: `ManagerConfig`, `InboundEvent`, `ObjectReceipt`.

- [ ] **Step 1: Write configuration and normalization tests**

```python
from agentteams_manager.matrix.client import MatrixClientConfig


def test_matrix_config_uses_manager_identity(manager_config) -> None:
    config = MatrixClientConfig.from_manager_config(manager_config)
    assert config.user_id == "@manager:matrix.local"
    assert config.sync_timeout_ms == 30_000
    assert config.history_limit == 50
    assert config.crypto_store.name == "matrix-e2ee"


def test_event_fixture_keeps_real_sender(matrix_text_event) -> None:
    event = matrix_text_event(
        room_id="!room:local",
        event_id="$one",
        sender="@alice:local",
        body="finished",
    )
    assert event.sender_id == "@alice:local"
    assert event.room_id == "!room:local"
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/matrix/test_config.py -q
```

Expected: FAIL because the Matrix package is absent.

- [ ] **Step 3: Implement configuration and a narrow client shell**

```python
@dataclass(frozen=True, slots=True)
class MatrixClientConfig:
    homeserver: str
    user_id: str
    access_token: SecretStr
    device_name: str
    crypto_store: Path
    media_dir: Path
    sync_timeout_ms: int = 30_000
    history_limit: int = 50
    encryption: bool = True
    vision_enabled: bool = True
    mention_pill_in_body: bool = False
    outbound_structured_mentions: bool = True

    @classmethod
    def from_manager_config(
        cls, config: ManagerConfig
    ) -> "MatrixClientConfig":
        return cls(
            homeserver=config.matrix_url,
            user_id=config.manager_user_id,
            access_token=config.matrix_access_token,
            device_name="agentteams-manager",
            crypto_store=config.workspace / "matrix-e2ee",
            media_dir=config.workspace / "media",
        )
```

Create `MatrixClient` with injected `AsyncClient` support for tests. Its constructor performs no network I/O.

- [ ] **Step 4: Run tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/matrix/test_config.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/matrix manager-agentscope/tests
git commit -m "Give Matrix a runtime-neutral boundary" \
  -m "Constraint: No CoPaw or agentscope_runtime types may cross the adapter." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: Matrix configuration unit tests"
```

### Task 2: Sync Loop, Token Resume, Invite Join, and E2EE

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/matrix/client.py`
- Create: `manager-agentscope/src/agentteams_manager/matrix/crypto.py`
- Create: `manager-agentscope/tests/contract/matrix/test_sync.py`
- Create: `manager-agentscope/tests/contract/matrix/test_crypto.py`
- Reference: `copaw/src/matrix/channel.py:518`
- Reference: `copaw/src/matrix/channel.py:724`
- Reference: `copaw/src/matrix/channel.py:788`
- Reference: `copaw/src/matrix/channel.py:817`
- Reference: `copaw/src/matrix/channel.py:879`

**Interfaces:**
- Produces: `MatrixClient.ready: asyncio.Event`
- Produces: `CryptoStore.prepare() -> Path`
- Consumes: `OperationRepository` key/value and event-claim operations.

- [ ] **Step 1: Write resume and invite tests**

```python
import pytest


@pytest.mark.asyncio
async def test_sync_resumes_from_persisted_token(matrix_harness) -> None:
    await matrix_harness.repository.set_value(
        "matrix.sync_token", "saved-token"
    )
    await matrix_harness.client.sync_once()
    assert matrix_harness.nio.sync_calls[0]["since"] == "saved-token"


@pytest.mark.asyncio
async def test_invites_are_joined_before_timeline_dispatch(matrix_harness) -> None:
    matrix_harness.nio.next_sync = matrix_harness.sync_with_invite(
        "!worker:local"
    )
    await matrix_harness.client.sync_once()
    assert matrix_harness.nio.joined_rooms == ["!worker:local"]


@pytest.mark.asyncio
async def test_unknown_token_refresh_is_bounded(matrix_harness) -> None:
    matrix_harness.nio.fail_sync_with_unknown_token(times=4)
    with pytest.raises(RuntimeError, match="three token refresh attempts"):
        await matrix_harness.client.run_sync_loop()
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/contract/matrix/test_sync.py manager-agentscope/tests/contract/matrix/test_crypto.py -q
```

Expected: FAIL because sync and crypto behavior are absent.

- [ ] **Step 3: Port protocol behavior without CoPaw dependencies**

Use:

```python
nio_config = AsyncClientConfig(
    encryption_enabled=config.encryption,
    store_sync_tokens=False,
)
client = AsyncClient(
    config.homeserver,
    config.user_id,
    device_id=None,
    store_path=str(config.crypto_store),
    config=nio_config,
)
client.access_token = config.access_token.get_secret_value()
client.user_id = config.user_id
```

One sync iteration must execute:

```python
since = await state.get_value("matrix.sync_token")
response = await nio.sync(
    timeout=config.sync_timeout_ms,
    since=since,
    full_state=since is None,
)
await join_all_invites(response.rooms.invite)
await dispatch_joined_timelines(response.rooms.join)
await state.set_value("matrix.sync_token", response.next_batch)
ready.set()
```

Do not delete the E2EE store at startup. `CryptoStore.prepare()` creates it with mode `0700`; E2EE maintenance uploads missing device keys and retries withheld sessions using the bounded behavior from the upstream channel.

On `M_UNKNOWN_TOKEN`, attempt password-based token refresh only when the Manager Matrix password is available. Use three attempts with delays 5s, 10s, and 20s. Otherwise mark readiness false and terminate so the container supervisor restarts with refreshed Controller credentials.

- [ ] **Step 4: Run sync tests**

Run:

```bash
python -m pytest manager-agentscope/tests/contract/matrix/test_sync.py manager-agentscope/tests/contract/matrix/test_crypto.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/matrix manager-agentscope/tests/contract/matrix
git commit -m "Resume Matrix safely across Manager restarts" \
  -m "Constraint: Sync tokens and E2EE keys persist; historical replay must not trigger duplicate work." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Matrix sync and E2EE contract tests"
```

### Task 3: Event Router and Room Policy Resolver

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/matrix/router.py`
- Create: `manager-agentscope/src/agentteams_manager/matrix/policy.py`
- Create: `manager-agentscope/tests/unit/matrix/test_router.py`
- Create: `manager-agentscope/tests/unit/matrix/test_policy.py`
- Reference: `copaw/src/matrix/channel.py:462`
- Reference: `copaw/src/matrix/channel.py:1020`
- Reference: `copaw/src/matrix/channel.py:1806`
- Reference: `copaw/src/matrix/channel.py:1885`

**Interfaces:**
- Produces: `EventRouter.submit(event)`, `start()`, `stop()`
- Produces: `RoomPolicyResolver.resolve(event) -> RoomPolicy`
- Consumes: `TopologyRepository`, Human resources, trusted contacts, and `RoomSessionManager`.

- [ ] **Step 1: Write serialization and policy tests**

```python
import asyncio
import pytest


@pytest.mark.asyncio
async def test_same_room_is_serial_but_rooms_are_parallel(router_harness) -> None:
    await router_harness.submit("!a", "$1", block=True)
    await router_harness.submit("!a", "$2")
    await router_harness.submit("!b", "$3")

    assert router_harness.started == [
        ("!a", "$1"),
        ("!b", "$3"),
    ]
    router_harness.release("!a", "$1")
    await asyncio.sleep(0)
    assert ("!a", "$2") in router_harness.started


@pytest.mark.asyncio
async def test_unknown_group_sender_is_silent(policy_resolver, event_factory) -> None:
    policy = await policy_resolver.resolve(
        event_factory(
            room_id="!group:local",
            sender_id="@unknown:local",
            is_direct=False,
        )
    )
    assert policy.silent
    assert not policy.allowed_tools


@pytest.mark.asyncio
async def test_admin_dm_gets_management_tools(
    policy_resolver, admin_dm_event
) -> None:
    policy = await policy_resolver.resolve(admin_dm_event)
    assert policy.kind.value == "admin_dm"
    assert "create_worker" in policy.allowed_tools
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/matrix/test_router.py manager-agentscope/tests/unit/matrix/test_policy.py -q
```

Expected: FAIL because router and policy modules are absent.

- [ ] **Step 3: Implement room queues and policy precedence**

Policy precedence is exact:

```text
admin DM
→ known Leader Room
→ known Worker Room
→ known Team Room
→ scoped Human
→ trusted contact
→ unknown
```

`EventRouter.submit` claims `(room_id, event_id)` in SQLite before enqueuing. A rejected duplicate returns without invoking the model. Each room owns an `asyncio.Queue`; a worker task drains it serially. The router removes idle queues after five minutes with no pending events.

Tool sets:

```python
ADMIN_TOOLS = frozenset(ALL_MANAGER_TOOLS)
WORKER_TOOLS = frozenset(
    {"delegate_task", "complete_task", "sync_files", "git_result"}
)
LEADER_TOOLS = frozenset(
    {"delegate_team_task", "complete_task", "sync_files"}
)
HUMAN_TOOLS = frozenset({"list_workers", "list_tasks", "sync_files"})
UNKNOWN_TOOLS = frozenset()
```

Level 1 Human receives read and task tools but never credential, runtime, Worker-create, Team-create, MCP-admin, or model-switch tools. Levels 2 and 3 are additionally restricted to the Controller-declared Worker/Team scope.

- [ ] **Step 4: Run router and policy tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/matrix/test_router.py manager-agentscope/tests/unit/matrix/test_policy.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/matrix manager-agentscope/tests/unit/matrix
git commit -m "Keep room concurrency without weakening room authority" \
  -m "Constraint: One room is serial; unknown group senders are silent." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Matrix router and room-policy unit tests"
```

### Task 4: Mentions, Media, History, and Threads

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/matrix/client.py`
- Create: `manager-agentscope/src/agentteams_manager/matrix/media.py`
- Create: `manager-agentscope/src/agentteams_manager/matrix/threads.py`
- Create: `manager-agentscope/tests/contract/matrix/test_mentions.py`
- Create: `manager-agentscope/tests/contract/matrix/test_media.py`
- Create: `manager-agentscope/tests/contract/matrix/test_threads.py`
- Reference: `copaw/src/matrix/channel.py:1110`
- Reference: `copaw/src/matrix/channel.py:1290`
- Reference: `copaw/src/matrix/channel.py:1457`
- Reference: `copaw/src/matrix/channel.py:1761`
- Reference: `copaw/src/matrix/channel.py:2396`
- Reference: `copaw/src/matrix/channel.py:2450`
- Reference: `copaw/src/matrix/channel.py:2562`

**Interfaces:**
- Implements all `MatrixPort` outbound methods.
- Produces: `MediaAdapter.download(event) -> tuple[DataBlock, ...]`
- Produces: `ThreadProjector.relation(thread_id, replace_event_id) -> dict`.

- [ ] **Step 1: Port behavior-focused tests**

Start from assertions in `copaw/tests/test_channel_mention.py`, replacing construction through `MatrixChannel.__new__` with the new test harness. Cover:

```python
assert content["m.mentions"]["user_ids"] == ["@alice:local"]
assert content["m.relates_to"] == {
    "rel_type": "m.thread",
    "event_id": "$root",
    "is_falling_back": True,
    "m.in_reply_to": {"event_id": "$root"},
}
```

Also verify:

- encrypted and plain image downloads produce the same `DataBlock` media type;
- uploaded files return an `mxc://` URI;
- `history_limit=50` evicts the oldest entry;
- a 10 MiB decoded-media limit rejects oversized payloads;
- one `txn_id` is reused after a send timeout.

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/contract/matrix/test_mentions.py manager-agentscope/tests/contract/matrix/test_media.py manager-agentscope/tests/contract/matrix/test_threads.py -q
```

Expected: FAIL because media and thread adapters are absent.

- [ ] **Step 3: Implement protocol-preserving outbound content**

`send_text` must call:

```python
response = await self._client.room_send(
    room_id=room_id,
    message_type="m.room.message",
    content=content,
    txn_id=txn_id,
    ignore_unverified_devices=True,
)
```

Mention content always includes `m.mentions.user_ids`; the optional HTML pill follows `mention_pill_in_body`. Thread replies include the exact relation in the assertion above. Streaming edits use:

```python
content["m.new_content"] = final_content
content["m.relates_to"] = {
    "rel_type": "m.replace",
    "event_id": original_event_id,
}
```

Media code reuses the upstream encrypted-file decryption rules and MIME/size validation but returns AgentScope `DataBlock` objects.

- [ ] **Step 4: Run transport contract tests**

Run:

```bash
python -m pytest manager-agentscope/tests/contract/matrix -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/matrix manager-agentscope/tests/contract/matrix
git commit -m "Preserve visible Matrix collaboration semantics" \
  -m "Constraint: Mentions, media, E2EE, history, and thread relations match the latest upstream channel." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: complete Matrix transport contract suite"
```

### Task 5: Direct AgentScope Session Runner and Confirmations

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/matrix/session_runner.py`
- Modify: `manager-agentscope/src/agentteams_manager/matrix/router.py`
- Modify: `manager-agentscope/src/agentteams_manager/runtime/event_stream.py`
- Modify: `manager-agentscope/src/agentteams_manager/state/sessions.py`
- Create: `manager-agentscope/tests/integration/test_matrix_agent_turn.py`
- Create: `manager-agentscope/tests/integration/test_matrix_confirmation.py`

**Interfaces:**
- Produces: `MatrixSessionRunner.handle(event, policy) -> None`
- Consumes: `RoomSessionManager`, `MatrixPort`, and AgentScope confirmation events.

- [ ] **Step 1: Write direct-call and confirmation tests**

```python
import pytest


@pytest.mark.asyncio
async def test_runner_calls_reply_stream_directly(session_harness) -> None:
    await session_harness.runner.handle(
        session_harness.admin_event("list workers"),
        session_harness.admin_policy(),
    )
    assert session_harness.agent.reply_stream_inputs[0].name == "@admin:local"
    assert session_harness.matrix.sent[-1].text == "There are 2 workers."


@pytest.mark.asyncio
async def test_confirmation_continues_same_reply(session_harness) -> None:
    await session_harness.runner.handle(
        session_harness.admin_event("delete alice"),
        session_harness.admin_policy(),
    )
    prompt = session_harness.matrix.sent[-1]
    assert "/confirm " in prompt.text

    await session_harness.runner.handle(
        session_harness.admin_event(
            f"/confirm {session_harness.pending_reply_id}"
        ),
        session_harness.admin_policy(),
    )
    assert session_harness.agent.confirmation_results[0].confirmed
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/integration/test_matrix_agent_turn.py manager-agentscope/tests/integration/test_matrix_confirmation.py -q
```

Expected: FAIL because `MatrixSessionRunner` is absent.

- [ ] **Step 3: Implement input and output mapping**

Convert an inbound event to AgentScope:

```python
message = UserMsg(
    name=event.sender_id,
    content=[
        TextBlock(text=history_prefix + event.body),
        *event.attachments,
    ],
    metadata={
        "room_id": event.room_id,
        "event_id": event.event_id,
        "sender_id": event.sender_id,
        "thread_id": event.thread_id,
    },
)
async for agent_event in agent.reply_stream(inputs=message):
    await projector.accept(agent_event)
```

Throttle Matrix streaming edits to at most one every 500 ms. Always send the final accumulated text. Save AgentState after the generator ends or parks.

For `RequireUserConfirmEvent`, save the tool calls and reply ID with the session, then send `/confirm <reply_id>` and `/deny <reply_id>` instructions to the Admin DM. On a matching command, construct `ConfirmResult` values and pass `UserConfirmResultEvent` back to the same Agent. Reject confirmation from any non-admin sender.

- [ ] **Step 4: Run integration tests**

Run:

```bash
python -m pytest manager-agentscope/tests/integration/test_matrix_agent_turn.py manager-agentscope/tests/integration/test_matrix_confirmation.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/matrix manager-agentscope/src/agentteams_manager/runtime manager-agentscope/src/agentteams_manager/state manager-agentscope/tests/integration
git commit -m "Connect Matrix rooms directly to AgentScope turns" \
  -m "Constraint: Confirmations continue the same parked AgentScope reply and only the admin may resolve them." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: Matrix-to-AgentScope integration tests"
```

### Task 6: Matrix Fault Injection and Transport Gate

**Files:**
- Create: `manager-agentscope/tests/fault_injection/test_matrix_send_timeout.py`
- Create: `manager-agentscope/tests/fault_injection/test_matrix_replay.py`
- Create: `manager-agentscope/tests/fault_injection/test_matrix_restart.py`
- Modify: `copaw/tests/test_channel_mention.py` only if shared fixtures need import-path updates; do not weaken Worker coverage.

**Interfaces:**
- Verifies the complete Matrix adapter contract.

- [ ] **Step 1: Add crash-boundary tests**

Cover these exact cases:

```text
send accepted by Matrix, response times out
→ retry uses the same txn_id
→ one event exists

event processed, process exits before sync token save
→ same event arrives after restart
→ processed_matrix_events rejects it

confirmation prompt sent, process exits
→ AgentState restores awaiting tool calls
→ admin confirmation continues the parked reply

E2EE store exists, unclean shutdown occurs
→ restart reuses keys
→ encrypted room remains readable
```

Use fake Matrix state with event counts and explicit process-restart fixture construction.

- [ ] **Step 2: Run and fix only evidenced failures**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/matrix -q
python -m pytest manager-agentscope/tests/contract/matrix -q
python -m pytest manager-agentscope/tests/integration/test_matrix_agent_turn.py manager-agentscope/tests/integration/test_matrix_confirmation.py -q
python -m pytest manager-agentscope/tests/fault_injection/test_matrix_send_timeout.py manager-agentscope/tests/fault_injection/test_matrix_replay.py manager-agentscope/tests/fault_injection/test_matrix_restart.py -q
python -m pytest copaw/tests/test_channel_mention.py copaw/tests/test_worker_matrix_channel.py -q
```

Expected: all listed tests PASS.

- [ ] **Step 3: Verify static quality**

Run:

```bash
python -m compileall -q manager-agentscope/src/agentteams_manager/matrix
git diff --check
```

Expected: exit 0 and no diff-check output.

- [ ] **Step 4: Commit**

```bash
git add manager-agentscope copaw/tests
git commit -m "Prove Matrix effects remain singular across failures" \
  -m "Constraint: Replay and ambiguous sends cannot produce duplicate Manager work or messages." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: complete Matrix gate plus retained CoPaw Worker Matrix tests"
```
