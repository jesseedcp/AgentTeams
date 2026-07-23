# AgentScope Manager Runtime Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the direct AgentScope process, typed domain contracts, SQLite state, MinIO journal, operation supervisor, room sessions, permissions, health, and observability foundation.

**Architecture:** A dependency-injected `ManagerApplication` constructs one durable SQLite store, one remote append-only journal, one AgentScope agent cache partitioned by Matrix room, and deterministic workflow services. SQLite methods complete one transaction per call on a worker thread; external effects are journaled before execution.

**Tech Stack:** Python 3.11+, `agentscope[s3]==2.0.4.post1`, Pydantic 2, standard-library `sqlite3`, `asyncio`, `aioboto3` supplied by the AgentScope S3 extra, pytest.

## Global Constraints

- Apply every constraint from `2026-07-23-agentscope-manager-master.md`.
- Keep all I/O behind Protocols in `domain/ports.py`.
- Use `AgentState.model_dump_json()` and `AgentState.model_validate_json()` for AgentScope session state.
- Use SQLite WAL, `foreign_keys=ON`, `busy_timeout=5000`, and compare-and-swap transitions.
- Write a remote journal intent before each Controller, Matrix, or MinIO business side effect.
- Never place access tokens, gateway keys, or storage secrets in SQLite payloads or logs.

---

### Task 1: Package and Environment Configuration

**Files:**
- Create: `manager-agentscope/pyproject.toml`
- Create: `manager-agentscope/src/agentteams_manager/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/config.py`
- Create: `manager-agentscope/tests/unit/test_config.py`

**Interfaces:**
- Produces: `ManagerConfig.from_env() -> ManagerConfig`
- Produces: `RuntimeDocument.load(path: Path) -> RuntimeDocument`
- Consumes: environment variables already produced by `WorkerEnvBuilder.BuildManager`

- [ ] **Step 1: Write the configuration tests**

```python
from pathlib import Path

import pytest

from agentteams_manager.config import ManagerConfig, RuntimeDocument


def test_manager_config_reads_secret_values_without_exposing_them(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = {
        "AGENTTEAMS_MANAGER_NAME": "default",
        "AGENTTEAMS_MANAGER_MATRIX_USER_ID": "@manager:matrix.local",
        "AGENTTEAMS_MANAGER_MATRIX_TOKEN": "matrix-secret",
        "AGENTTEAMS_MANAGER_GATEWAY_KEY": "gateway-secret",
        "AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY":
            "manager/agentscope-manager.json",
        "AGENTTEAMS_MATRIX_URL": "http://matrix:6167",
        "AGENTTEAMS_MATRIX_DOMAIN": "matrix.local",
        "AGENTTEAMS_CONTROLLER_URL": "http://controller:8080",
        "AGENTTEAMS_AI_GATEWAY_URL": "http://higress:8080",
        "AGENTTEAMS_FS_ENDPOINT": "http://minio:9000",
        "AGENTTEAMS_FS_BUCKET": "agentteams",
        "AGENTTEAMS_FS_ACCESS_KEY": "default",
        "AGENTTEAMS_FS_SECRET_KEY": "minio-secret",
        "AGENTTEAMS_DEFAULT_MODEL": "qwen3.6-plus",
        "HOME": str(tmp_path),
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    config = ManagerConfig.from_env()

    assert config.session_database == tmp_path / "state" / "manager.db"
    assert config.matrix_access_token.get_secret_value() == "matrix-secret"
    assert "matrix-secret" not in repr(config)


def test_runtime_document_rejects_non_monotonic_schema(tmp_path: Path) -> None:
    path = tmp_path / "agentscope-manager.json"
    path.write_text('{"schema_version": 2, "revision": 1, "model": "x"}')

    with pytest.raises(ValueError, match="schema_version 1"):
        RuntimeDocument.load(path)
```

- [ ] **Step 2: Run the test and verify the package does not exist**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/test_config.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'agentteams_manager'`.

- [ ] **Step 3: Add the package metadata and configuration models**

`manager-agentscope/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[project]
name = "agentteams-manager"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "agentscope[s3]==2.0.4.post1",
  "matrix-nio[e2e]>=0.24.0",
  "httpx>=0.27.0,<1.0",
]

[project.optional-dependencies]
test = [
  "pytest>=8.0",
  "pytest-asyncio>=0.23",
]

[project.scripts]
agentteams-manager = "agentteams_manager.main:main"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.setuptools.packages.find]
where = ["src"]
```

`config.py` must define secret-safe models:

```python
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class MCPServerDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    url: str = Field(pattern=r"^https?://")
    transport: Literal["http", "sse"] = "http"


class PromptSources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    soul: str = Field(min_length=1)
    agents: str = Field(min_length=1)
    tools: str = Field(min_length=1)
    heartbeat: str = Field(min_length=1)


class RuntimeDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = 1
    revision: int = Field(ge=0)
    manager_name: str = Field(min_length=1)
    model: str = Field(min_length=1)
    context_window: int = Field(default=150_000, gt=0)
    max_tokens: int = Field(default=128_000, gt=0)
    reasoning: bool = True
    input_modalities: tuple[str, ...] = ("text",)
    skills: tuple[str, ...] = ()
    mcp_servers: tuple[MCPServerDocument, ...] = ()
    prompt_sources: PromptSources
    heartbeat_interval_seconds: int = Field(default=1_800, gt=0)
    worker_idle_timeout_seconds: int = Field(default=43_200, gt=0)

    @classmethod
    def load(cls, path: Path) -> "RuntimeDocument":
        raw = json.loads(path.read_text("utf-8"))
        if raw.get("schema_version") != 1:
            raise ValueError(
                f"unsupported schema_version {raw.get('schema_version')}; "
                "expected schema_version 1"
            )
        return cls.model_validate(raw)


class ManagerConfig(BaseModel):
    model_config = ConfigDict(repr=False)

    manager_name: str
    manager_user_id: str
    matrix_url: str
    matrix_domain: str
    matrix_access_token: SecretStr
    controller_url: str
    controller_auth_token: SecretStr | None
    ai_gateway_url: str
    gateway_key: SecretStr
    fs_endpoint: str
    fs_bucket: str
    fs_access_key: str
    fs_secret_key: SecretStr
    storage_prefix: str
    default_model: str
    workspace: Path
    runtime_document_path: Path
    runtime_document_key: str
    session_database: Path
    health_port: int = 18799
    heartbeat_interval_seconds: int = 1_800
    worker_idle_timeout_seconds: int = 43_200
    yolo: bool = False

    @classmethod
    def from_env(cls) -> "ManagerConfig":
        env = os.environ
        workspace = Path(env["HOME"]).resolve()
        return cls(
            manager_name=env["AGENTTEAMS_MANAGER_NAME"],
            manager_user_id=env["AGENTTEAMS_MANAGER_MATRIX_USER_ID"],
            matrix_url=env["AGENTTEAMS_MATRIX_URL"].rstrip("/"),
            matrix_domain=env["AGENTTEAMS_MATRIX_DOMAIN"],
            matrix_access_token=SecretStr(
                env["AGENTTEAMS_MANAGER_MATRIX_TOKEN"]
            ),
            controller_url=env["AGENTTEAMS_CONTROLLER_URL"].rstrip("/"),
            controller_auth_token=(
                SecretStr(env["AGENTTEAMS_AUTH_TOKEN"])
                if env.get("AGENTTEAMS_AUTH_TOKEN")
                else None
            ),
            ai_gateway_url=env["AGENTTEAMS_AI_GATEWAY_URL"].rstrip("/"),
            gateway_key=SecretStr(env["AGENTTEAMS_MANAGER_GATEWAY_KEY"]),
            fs_endpoint=env["AGENTTEAMS_FS_ENDPOINT"].rstrip("/"),
            fs_bucket=env["AGENTTEAMS_FS_BUCKET"],
            fs_access_key=env["AGENTTEAMS_FS_ACCESS_KEY"],
            fs_secret_key=SecretStr(env["AGENTTEAMS_FS_SECRET_KEY"]),
            storage_prefix=env.get(
                "AGENTTEAMS_STORAGE_PREFIX", "agentteams"
            ).strip("/"),
            default_model=env["AGENTTEAMS_DEFAULT_MODEL"],
            workspace=workspace,
            runtime_document_path=workspace / "agentscope-manager.json",
            runtime_document_key=env[
                "AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY"
            ],
            session_database=workspace / "state" / "manager.db",
            health_port=int(
                env.get("AGENTTEAMS_MANAGER_HEALTH_PORT", "18799")
            ),
            heartbeat_interval_seconds=int(
                env.get(
                    "AGENTTEAMS_MANAGER_HEARTBEAT_INTERVAL_SECONDS", "1800"
                )
            ),
            worker_idle_timeout_seconds=int(
                env.get(
                    "AGENTTEAMS_MANAGER_WORKER_IDLE_TIMEOUT_SECONDS", "43200"
                )
            ),
            yolo=env.get("AGENTTEAMS_YOLO") == "1",
        )
```

- [ ] **Step 4: Install the package in editable mode and rerun**

Run:

```bash
python -m pip install -e "manager-agentscope[test]"
python -m pytest manager-agentscope/tests/unit/test_config.py -q
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope
git commit -m "Give the new Manager one validated runtime contract" \
  -m "Constraint: AgentScope is pinned to 2.0.4.post1 and secrets remain environment-only." \
  -m "Confidence: high" \
  -m "Scope-risk: narrow" \
  -m "Tested: python -m pytest manager-agentscope/tests/unit/test_config.py -q"
```

### Task 2: Domain Types, Stable IDs, and Ports

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/domain/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/domain/errors.py`
- Create: `manager-agentscope/src/agentteams_manager/domain/ids.py`
- Create: `manager-agentscope/src/agentteams_manager/domain/models.py`
- Create: `manager-agentscope/src/agentteams_manager/domain/ports.py`
- Create: `manager-agentscope/tests/unit/domain/test_ids.py`
- Create: `manager-agentscope/tests/unit/domain/test_models.py`

**Interfaces:**
- Produces all cross-plan records and Protocols listed in the master plan.
- Produces `operation_id_for(room_id, event_id, tool_call_id) -> str`.

- [ ] **Step 1: Write ID and transition tests**

```python
from agentteams_manager.domain.ids import operation_id_for
from agentteams_manager.domain.models import (
    OperationRecord,
    OperationStatus,
)


def test_operation_id_is_stable_and_tool_call_specific() -> None:
    first = operation_id_for("!room:a", "$event", "call-1")
    same = operation_id_for("!room:a", "$event", "call-1")
    other = operation_id_for("!room:a", "$event", "call-2")

    assert first == same
    assert first != other
    assert len(first) == 32


def test_operation_record_rejects_illegal_transition() -> None:
    record = OperationRecord.new(
        operation_id="a" * 32,
        kind="create_worker",
        target_key="worker/alice",
        request={"name": "alice"},
    )

    assert record.can_transition_to(OperationStatus.PREPARED)
    assert not record.can_transition_to(OperationStatus.SUCCEEDED)
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/domain -q
```

Expected: FAIL because `agentteams_manager.domain` is absent.

- [ ] **Step 3: Implement the stable domain contract**

`ids.py`:

```python
from hashlib import sha256


def operation_id_for(
    room_id: str, event_id: str, tool_call_id: str
) -> str:
    raw = "\0".join((room_id, event_id, tool_call_id)).encode()
    return sha256(raw).hexdigest()[:32]
```

`models.py` must include:

```python
from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RoomKind(StrEnum):
    ADMIN_DM = "admin_dm"
    WORKER_ROOM = "worker_room"
    LEADER_ROOM = "leader_room"
    TEAM_ROOM = "team_room"
    HUMAN_OR_CHANNEL_ROOM = "human_or_channel_room"
    UNKNOWN = "unknown"


class OperationStatus(StrEnum):
    PLANNED = "planned"
    PREPARED = "prepared"
    DISPATCHED = "dispatched"
    ACKNOWLEDGED = "acknowledged"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    RECONCILING = "reconciling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NEEDS_ATTENTION = "needs_attention"


class ExternalEffect(StrEnum):
    CONTROLLER = "controller"
    MATRIX = "matrix"
    STORAGE = "storage"
    PROCESS = "process"


class OperationKind(StrEnum):
    CREATE_WORKER = "create_worker"
    CREATE_TEAM = "create_team"
    CREATE_HUMAN = "create_human"
    DELEGATE_TASK = "delegate_task"
    COMPLETE_TASK = "complete_task"
    CREATE_PROJECT = "create_project"
    GIT_DELEGATION = "git_delegation"
    CONFIGURE_MCP = "configure_mcp"
    SWITCH_MODEL = "switch_model"
    PUBLISH_SERVICE = "publish_service"


_TRANSITIONS: dict[OperationStatus, frozenset[OperationStatus]] = {
    OperationStatus.PLANNED: frozenset(
        {OperationStatus.PREPARED, OperationStatus.FAILED}
    ),
    OperationStatus.PREPARED: frozenset(
        {
            OperationStatus.DISPATCHED,
            OperationStatus.RECONCILING,
            OperationStatus.RETRY_WAIT,
            OperationStatus.FAILED,
        }
    ),
    OperationStatus.DISPATCHED: frozenset(
        {
            OperationStatus.ACKNOWLEDGED,
            OperationStatus.RUNNING,
            OperationStatus.RECONCILING,
            OperationStatus.RETRY_WAIT,
        }
    ),
    OperationStatus.ACKNOWLEDGED: frozenset(
        {OperationStatus.RUNNING, OperationStatus.SUCCEEDED}
    ),
    OperationStatus.RUNNING: frozenset(
        {
            OperationStatus.SUCCEEDED,
            OperationStatus.RETRY_WAIT,
            OperationStatus.RECONCILING,
            OperationStatus.FAILED,
            OperationStatus.NEEDS_ATTENTION,
        }
    ),
    OperationStatus.RETRY_WAIT: frozenset(
        {OperationStatus.PREPARED, OperationStatus.RECONCILING}
    ),
    OperationStatus.RECONCILING: frozenset(
        {
            OperationStatus.RUNNING,
            OperationStatus.SUCCEEDED,
            OperationStatus.RETRY_WAIT,
            OperationStatus.FAILED,
            OperationStatus.NEEDS_ATTENTION,
        }
    ),
    OperationStatus.SUCCEEDED: frozenset(),
    OperationStatus.FAILED: frozenset(),
    OperationStatus.NEEDS_ATTENTION: frozenset(
        {OperationStatus.RECONCILING, OperationStatus.FAILED}
    ),
}


class OperationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_id: str = Field(min_length=32, max_length=32)
    kind: OperationKind
    target_key: str
    status: OperationStatus
    request: dict[str, Any]
    result: dict[str, Any] = Field(default_factory=dict)
    retry_count: int = 0
    created_at: datetime
    updated_at: datetime

    @classmethod
    def new(
        cls,
        *,
        operation_id: str,
        kind: OperationKind | str,
        target_key: str,
        request: dict[str, Any],
    ) -> "OperationRecord":
        now = datetime.now(UTC)
        return cls(
            operation_id=operation_id,
            kind=kind,
            target_key=target_key,
            status=OperationStatus.PLANNED,
            request=request,
            created_at=now,
            updated_at=now,
        )

    def can_transition_to(self, status: OperationStatus) -> bool:
        return status in _TRANSITIONS[self.status]
```

Also define `InboundEvent`, `RoomPolicy`, `WorkerResource`, `TeamResource`, `HumanResource`, `TaskRecord`, `ProjectRecord`, `JournalEvent`, `ObjectReceipt`, and `RecoveryReport` as Pydantic models with `extra="forbid"`. `ports.py` must contain the exact Protocol signatures from the master plan and a `Clock.now() -> datetime` Protocol.

- [ ] **Step 4: Rerun tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/domain -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/domain manager-agentscope/tests/unit/domain
git commit -m "Make every Manager side effect nameable and testable" \
  -m "Constraint: Cross-subsystem interfaces are fixed before Matrix and workflow implementation." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: python -m pytest manager-agentscope/tests/unit/domain -q"
```

### Task 3: SQLite Schema and Operation Repository

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/state/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/state/schema.py`
- Create: `manager-agentscope/src/agentteams_manager/state/database.py`
- Create: `manager-agentscope/src/agentteams_manager/state/operations.py`
- Create: `manager-agentscope/tests/unit/state/test_database.py`
- Create: `manager-agentscope/tests/unit/state/test_operations.py`

**Interfaces:**
- Produces: `Database.open()`, `Database.read()`, `Database.write()`, `Database.backup_to()`
- Produces: `OperationRepository.create()`, `get()`, `transition()`, `list_recoverable()`, `claim_matrix_event()`

- [ ] **Step 1: Write transaction and idempotency tests**

```python
from pathlib import Path

import pytest

from agentteams_manager.domain.models import (
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.state.database import Database
from agentteams_manager.state.operations import OperationRepository


@pytest.mark.asyncio
async def test_operation_transition_is_compare_and_swap(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)
    record = OperationRecord.new(
        operation_id="b" * 32,
        kind="create_worker",
        target_key="worker/alice",
        request={"name": "alice"},
    )
    await repository.create(record)

    changed = await repository.transition(
        record.operation_id,
        expected={OperationStatus.PLANNED},
        target=OperationStatus.PREPARED,
    )
    stale = await repository.transition(
        record.operation_id,
        expected={OperationStatus.PLANNED},
        target=OperationStatus.FAILED,
    )

    assert changed.status is OperationStatus.PREPARED
    assert stale is None


@pytest.mark.asyncio
async def test_matrix_event_is_claimed_once(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = OperationRepository(database)

    assert await repository.claim_matrix_event("!room:a", "$event")
    assert not await repository.claim_matrix_event("!room:a", "$event")
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/state/test_database.py manager-agentscope/tests/unit/state/test_operations.py -q
```

Expected: FAIL because state modules do not exist.

- [ ] **Step 3: Implement the database worker and schema**

`schema.py` must expose `SCHEMA_VERSION = 1` and a migration that creates:

```sql
CREATE TABLE operations (
  operation_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  target_key TEXT NOT NULL,
  status TEXT NOT NULL,
  request_json TEXT NOT NULL,
  result_json TEXT NOT NULL DEFAULT '{}',
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX operations_recovery_idx
  ON operations(status, updated_at);
CREATE TABLE operation_events (
  operation_id TEXT NOT NULL REFERENCES operations(operation_id),
  sequence INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(operation_id, sequence)
);
CREATE TABLE processed_matrix_events (
  room_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY(room_id, event_id)
);
CREATE TABLE key_values (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
PRAGMA user_version = 1;
```

`Database` opens a new connection for each synchronous transaction:

```python
class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    async def write(self, callback: Callable[[sqlite3.Connection], T]) -> T:
        def run() -> T:
            with self._connect() as connection:
                return callback(connection)
        return await asyncio.to_thread(run)
```

`OperationRepository.transition` performs one SQL statement with `WHERE operation_id=? AND status IN (...)`, validates the domain transition before issuing SQL, and returns `None` when `rowcount == 0`.

- [ ] **Step 4: Run state tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/state/test_database.py manager-agentscope/tests/unit/state/test_operations.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/state manager-agentscope/tests/unit/state
git commit -m "Keep Manager operations durable without another service" \
  -m "Constraint: Standard-library SQLite with WAL is the only transactional store." \
  -m "Rejected: Redis | one active Manager does not need distributed coordination." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: python -m pytest manager-agentscope/tests/unit/state -q"
```

### Task 4: Session, Task, and Topology Repositories

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/state/sessions.py`
- Create: `manager-agentscope/src/agentteams_manager/state/tasks.py`
- Create: `manager-agentscope/src/agentteams_manager/state/topology.py`
- Modify: `manager-agentscope/src/agentteams_manager/state/schema.py`
- Create: `manager-agentscope/tests/unit/state/test_sessions.py`
- Create: `manager-agentscope/tests/unit/state/test_tasks.py`
- Create: `manager-agentscope/tests/unit/state/test_topology.py`

**Interfaces:**
- Produces: `SessionRepository.load(room_id)`, `save(room_id, AgentState, policy_revision, last_event_id)`
- Produces: `TaskRepository.create()`, `transition()`, `due_schedules()`
- Produces: `TopologyRepository.replace_snapshot()`, `room_binding()`

- [ ] **Step 1: Add failing round-trip tests**

```python
import pytest
from agentscope.state import AgentState


@pytest.mark.asyncio
async def test_agent_state_round_trip(session_repository) -> None:
    state = AgentState(session_id="matrix:!room:example")
    state.summary = "compressed context"

    await session_repository.save(
        room_id="!room:example",
        state=state,
        policy_revision=3,
        last_event_id="$event",
    )
    restored = await session_repository.load("!room:example")

    assert restored is not None
    assert restored.state.session_id == "matrix:!room:example"
    assert restored.state.summary == "compressed context"
    assert restored.policy_revision == 3
```

Also test that one room cannot be simultaneously bound as a Worker Room and Team Room, and that `due_schedules(now)` excludes already-executed occurrences.

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/state/test_sessions.py manager-agentscope/tests/unit/state/test_tasks.py manager-agentscope/tests/unit/state/test_topology.py -q
```

Expected: FAIL because repository modules are absent.

- [ ] **Step 3: Extend schema and implement repositories**

Add tables:

```sql
CREATE TABLE sessions (
  room_id TEXT PRIMARY KEY,
  agent_state_json TEXT NOT NULL,
  policy_revision INTEGER NOT NULL,
  last_event_id TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  status TEXT NOT NULL,
  title TEXT NOT NULL,
  assigned_to TEXT NOT NULL,
  room_id TEXT NOT NULL,
  project_id TEXT,
  delegated_to_team TEXT,
  schedule TEXT,
  timezone TEXT,
  last_executed_at TEXT,
  next_scheduled_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX tasks_due_idx ON tasks(status, next_scheduled_at);
CREATE TABLE topology (
  resource_type TEXT NOT NULL,
  resource_name TEXT NOT NULL,
  room_kind TEXT NOT NULL,
  room_id TEXT NOT NULL,
  matrix_user_id TEXT,
  payload_json TEXT NOT NULL,
  refreshed_at TEXT NOT NULL,
  PRIMARY KEY(resource_type, resource_name, room_kind)
);
CREATE UNIQUE INDEX topology_room_kind_idx
  ON topology(room_id, room_kind);
```

Serialize AgentScope state exactly:

```python
serialized = state.model_dump_json()
restored = AgentState.model_validate_json(row["agent_state_json"])
```

All repository updates use the `Database.write()` transaction boundary.

- [ ] **Step 4: Run repository tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/state -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/state manager-agentscope/tests/unit/state
git commit -m "Let room sessions and tasks survive Manager restarts" \
  -m "Constraint: Room session identity remains matrix:<room_id>." \
  -m "Confidence: high" \
  -m "Scope-risk: moderate" \
  -m "Tested: python -m pytest manager-agentscope/tests/unit/state -q"
```

### Task 5: Append-Only MinIO Journal and Recovery

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/state/journal.py`
- Create: `manager-agentscope/src/agentteams_manager/state/recovery.py`
- Create: `manager-agentscope/tests/unit/state/test_journal.py`
- Create: `manager-agentscope/tests/fault_injection/test_snapshot_recovery.py`

**Interfaces:**
- Produces: `S3Journal.append(event)`, `list_after(snapshot_sequence)`, `upload_snapshot(path, sequence)`
- Produces: `RecoveryCoordinator.restore() -> RecoveryReport`

- [ ] **Step 1: Write immutable-journal and replay tests**

```python
import pytest

from agentteams_manager.domain.models import JournalEvent


@pytest.mark.asyncio
async def test_journal_never_overwrites_an_existing_sequence(fake_s3) -> None:
    journal = fake_s3.journal()
    event = JournalEvent.example(
        operation_id="c" * 32,
        sequence=1,
        event_type="effect_planned",
    )

    await journal.append(event)
    with pytest.raises(FileExistsError):
        await journal.append(event)


@pytest.mark.asyncio
async def test_restore_replays_events_after_snapshot(recovery_fixture) -> None:
    await recovery_fixture.seed_snapshot(sequence=4)
    await recovery_fixture.seed_event(sequence=5, event_type="effect_planned")

    report = await recovery_fixture.coordinator.restore()

    assert report.snapshot_sequence == 4
    assert report.replayed_events == 1
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/state/test_journal.py manager-agentscope/tests/fault_injection/test_snapshot_recovery.py -q
```

Expected: FAIL because journal and recovery modules are absent.

- [ ] **Step 3: Implement immutable S3 keys and snapshot restore**

Use the key format:

```python
def event_key(prefix: str, event: JournalEvent) -> str:
    return (
        f"{prefix}/manager/journal/{event.operation_id}/"
        f"{event.sequence:020d}.json"
    )
```

Call S3 `put_object` with `IfNoneMatch="*"` and translate a precondition failure to `FileExistsError`. Snapshot creation must:

1. call `Database.backup_to(temp_path)` using `sqlite3.Connection.backup`;
2. calculate SHA-256;
3. upload `manager/snapshots/<sequence>.db`;
4. upload `manager/snapshots/<sequence>.json` containing sequence, checksum, byte length, and timestamp;
5. update `manager/snapshots/latest.json` only after both immutable objects exist.

Restore verifies length and SHA-256 before replacing the local database, then replays journal events in ascending sequence and asks registered reconcilers to compare Controller, Matrix, and MinIO external facts.

- [ ] **Step 4: Run recovery tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/state/test_journal.py manager-agentscope/tests/fault_injection/test_snapshot_recovery.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/state manager-agentscope/tests
git commit -m "Preserve recovery intent before external effects" \
  -m "Constraint: MinIO stores immutable journal records and verified SQLite snapshots." \
  -m "Rejected: Treat MinIO as a transactional database | object storage cannot provide the required local concurrency semantics." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: journal and snapshot fault-injection tests"
```

### Task 6: Operation Supervisor

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/workflows/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/workflows/supervisor.py`
- Create: `manager-agentscope/tests/unit/workflows/test_supervisor.py`
- Create: `manager-agentscope/tests/fault_injection/test_ambiguous_effect.py`

**Interfaces:**
- Produces the `OperationSupervisor` methods fixed in the master plan.
- Consumes `OperationRepository`, `S3Journal`, and effect-specific reconcilers.

- [ ] **Step 1: Write ambiguous-result recovery tests**

```python
import pytest

from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationStatus,
)


@pytest.mark.asyncio
async def test_timeout_moves_operation_to_reconciling(supervisor) -> None:
    operation = await supervisor.begin(
        operation_id="d" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    await supervisor.before_effect(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        {"argv": ["agt", "create", "worker", "--name", "alice"]},
    )

    changed = await supervisor.effect_ambiguous(
        operation.operation_id,
        ExternalEffect.CONTROLLER,
        "timeout waiting for agt",
    )

    assert changed.status is OperationStatus.RECONCILING


@pytest.mark.asyncio
async def test_repeated_begin_returns_existing_operation(supervisor) -> None:
    first = await supervisor.begin(
        operation_id="e" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    second = await supervisor.begin(
        operation_id="e" * 32,
        kind=OperationKind.CREATE_WORKER,
        target_key="worker/alice",
        request={"name": "alice"},
    )
    assert first.operation_id == second.operation_id
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_supervisor.py manager-agentscope/tests/fault_injection/test_ambiguous_effect.py -q
```

Expected: FAIL because `OperationSupervisor` is absent.

- [ ] **Step 3: Implement supervisor sequencing**

`before_effect` must execute in this order:

```python
async def before_effect(
    self,
    operation_id: str,
    effect: ExternalEffect,
    request: dict[str, object],
) -> JournalEvent:
    sequence = await self._operations.next_sequence(operation_id)
    event = JournalEvent(
        operation_id=operation_id,
        sequence=sequence,
        event_type="effect_planned",
        payload={"effect": effect, "request": redact(request)},
        created_at=self._clock.now(),
    )
    await self._journal.append(event)
    await self._operations.append_event(event)
    return event
```

`effect_ambiguous` always enters `RECONCILING`; it must not consume the normal retry counter until reconciliation proves the effect absent. `recover_all` loads statuses `PREPARED`, `DISPATCHED`, `RUNNING`, `RETRY_WAIT`, and `RECONCILING`, acquires a lock per `target_key`, and invokes the registered handler for the operation kind.

- [ ] **Step 4: Run supervisor and all core state tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_supervisor.py manager-agentscope/tests/fault_injection/test_ambiguous_effect.py manager-agentscope/tests/unit/state -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows manager-agentscope/tests
git commit -m "Reconcile uncertain effects instead of repeating them" \
  -m "Constraint: A timeout never proves a Controller, Matrix, or MinIO effect failed." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: supervisor unit and ambiguous-effect fault-injection tests"
```

### Task 7: AgentScope Skills, Agent Factory, and Session Manager

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/runtime/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/runtime/skills.py`
- Create: `manager-agentscope/src/agentteams_manager/runtime/prompts.py`
- Create: `manager-agentscope/src/agentteams_manager/runtime/agent_factory.py`
- Create: `manager-agentscope/src/agentteams_manager/runtime/session_manager.py`
- Modify: `manager/agent/AGENTS.md`
- Modify: `manager/agent/TOOLS.md`
- Create: `manager-agentscope/tests/unit/runtime/test_skills.py`
- Create: `manager-agentscope/tests/unit/runtime/test_prompts.py`
- Create: `manager-agentscope/tests/unit/runtime/test_agent_factory.py`
- Create: `manager-agentscope/tests/unit/runtime/test_session_manager.py`

**Interfaces:**
- Produces: `SkillRegistry.load() -> tuple[Skill, ...]`
- Produces: `PromptBuilder.build(policy, runtime) -> str`
- Produces: `AgentFactory.create(room_id, policy, state) -> Agent`
- Produces: `RoomSessionManager.run(event, policy) -> AsyncIterator[AgentEvent]`

- [ ] **Step 1: Write skill-count and session tests**

```python
import pytest
from agentscope.state import AgentState


@pytest.mark.asyncio
async def test_all_upstream_manager_skills_load(skill_registry) -> None:
    skills = await skill_registry.load()
    assert {skill.name for skill in skills} == {
        "agentteams-find-worker",
        "channel-management",
        "file-sync-management",
        "git-delegation-management",
        "human-management",
        "matrix-server-management",
        "mcp-server-management",
        "mcporter",
        "model-switch",
        "project-management",
        "service-publishing",
        "task-coordination",
        "task-management",
        "team-management",
        "worker-management",
        "worker-model-switch",
    }


@pytest.mark.asyncio
async def test_session_id_is_matrix_room_id(session_manager) -> None:
    session = await session_manager.get_or_create("!room:example")
    assert session.agent.state.session_id == "matrix:!room:example"


def test_prompt_uses_agentscope_tools_not_legacy_manager_commands(
    prompt_builder,
) -> None:
    prompt = prompt_builder.build(
        prompt_builder.admin_policy,
        prompt_builder.runtime,
    )
    assert "typed AgentScope tools" in prompt
    assert "openclaw gateway" not in prompt
    assert "copaw channels send" not in prompt
    assert "/opt/agentteams/agent/skills/" not in prompt
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_skills.py manager-agentscope/tests/unit/runtime/test_prompts.py manager-agentscope/tests/unit/runtime/test_agent_factory.py manager-agentscope/tests/unit/runtime/test_session_manager.py -q
```

Expected: FAIL because runtime modules are absent.

- [ ] **Step 3: Implement direct AgentScope construction**

`SkillRegistry` uses:

```python
loader = LocalSkillLoader(
    directory="/opt/agentteams/agent/skills",
    scan_subdir=True,
)
skills = tuple(await loader.list_skills())
```

Rewrite `manager/agent/AGENTS.md` and `manager/agent/TOOLS.md` as
runtime-independent Manager policy. Preserve upstream topology, delegation,
confirmation, and authority rules, but replace every legacy shell/CLI execution
instruction with the exact typed tool name exposed by this project.
`PromptBuilder` resolves and checksum-verifies all four runtime-document prompt
sources (`SOUL.md`, `AGENTS.md`, `TOOLS.md`, and `HEARTBEAT.md`), concatenates
them in that order, and appends only the current room policy and enabled tool
summary. Missing or path-escaping prompt sources fail the generation before an
Agent is created.

`AgentFactory` creates:

```python
credential = OpenAICredential(
    api_key=config.gateway_key,
    base_url=f"{config.ai_gateway_url}/v1",
)
model = OpenAIChatModel(
    credential=credential,
    model=runtime.model,
    context_size=runtime.context_window,
    parameters=OpenAIChatModel.Parameters(
        max_tokens=runtime.max_tokens,
        thinking_enable=runtime.reasoning,
    ),
)
agent = Agent(
    name="manager",
    system_prompt=prompt_builder.build(policy),
    model=model,
    toolkit=toolkit_factory.for_policy(policy),
    state=state
    or AgentState(session_id=f"matrix:{room_id}"),
)
```

`RoomSessionManager` stores one `asyncio.Lock` and one cached Agent per room. It loads persisted `AgentState` before creation and saves state after every completed or parked `reply_stream` run.

- [ ] **Step 4: Run runtime tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/runtime manager-agentscope/tests/unit/runtime manager/agent/AGENTS.md manager/agent/TOOLS.md
git commit -m "Run each Matrix room as a native AgentScope session" \
  -m "Constraint: The runtime calls Agent.reply_stream directly and loads exactly 16 upstream skills." \
  -m "Rejected: AgentScope create_app | the Manager owns its Matrix transport and lifecycle." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: python -m pytest manager-agentscope/tests/unit/runtime -q"
```

### Task 8: Permission-Aware Tools and Event Stream Mapping

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/runtime/permissions.py`
- Create: `manager-agentscope/src/agentteams_manager/runtime/event_stream.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/base.py`
- Create: `manager-agentscope/tests/unit/runtime/test_permissions.py`
- Create: `manager-agentscope/tests/unit/runtime/test_event_stream.py`

**Interfaces:**
- Produces: `ManagerTool.check_permissions()`
- Produces: `EventStreamProjector.consume(events) -> StreamProjection`
- Consumes: `RoomPolicy` and AgentScope `RequireUserConfirmEvent`, `TextBlockDeltaEvent`, and tool events.

- [ ] **Step 1: Write hard-deny and streaming tests**

```python
import pytest
from agentscope.event import TextBlockDeltaEvent
from agentscope.permission import PermissionBehavior, PermissionContext


@pytest.mark.asyncio
async def test_worker_room_cannot_create_worker(create_worker_tool) -> None:
    decision = await create_worker_tool.check_permissions(
        {"name": "bob"},
        PermissionContext(),
    )
    assert decision.behavior is PermissionBehavior.DENY


@pytest.mark.asyncio
async def test_text_deltas_are_accumulated() -> None:
    projector = EventStreamProjector()
    projection = await projector.consume(
        async_events(
            TextBlockDeltaEvent(
                reply_id="reply",
                block_id="block",
                delta="hello ",
            ),
            TextBlockDeltaEvent(
                reply_id="reply",
                block_id="block",
                delta="world",
            ),
        )
    )
    assert projection.text == "hello world"
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime/test_permissions.py manager-agentscope/tests/unit/runtime/test_event_stream.py -q
```

Expected: FAIL because permission and stream modules are absent.

- [ ] **Step 3: Implement two-layer authorization**

`ManagerTool` must first check the immutable room policy:

```python
async def check_permissions(
    self,
    tool_input: dict[str, object],
    context: PermissionContext,
) -> PermissionDecision:
    if self.name not in self._policy.allowed_tools:
        return PermissionDecision(
            behavior=PermissionBehavior.DENY,
            message=f"{self.name} is not allowed in {self._policy.kind}",
            decision_reason="room policy",
        )
    if self.name in self._policy.confirm_tools and not self._yolo:
        return PermissionDecision(
            behavior=PermissionBehavior.ASK,
            message=f"Confirm {self.name}",
            decision_reason="mutating management operation",
        )
    return PermissionDecision(
        behavior=PermissionBehavior.ALLOW,
        message="allowed by room policy",
        decision_reason="room policy",
    )
```

The Toolkit only contains tools in `policy.allowed_tools`; the check above remains as defense in depth.

`EventStreamProjector` accumulates text deltas, records tool start/end metadata, ignores thinking deltas, exposes binary result blocks for Matrix media, and returns pending confirmation tool calls when it sees `RequireUserConfirmEvent`.

- [ ] **Step 4: Run tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/runtime -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/runtime manager-agentscope/src/agentteams_manager/tools manager-agentscope/tests/unit/runtime
git commit -m "Enforce room authority below the model prompt" \
  -m "Constraint: Unauthorized mutation tools are absent from the Toolkit and deny if invoked directly." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: permission and AgentScope event-stream tests"
```

### Task 9: Application Lifecycle, Health, Metrics, and Core Gate

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/application.py`
- Create: `manager-agentscope/src/agentteams_manager/main.py`
- Create: `manager-agentscope/src/agentteams_manager/health.py`
- Create: `manager-agentscope/src/agentteams_manager/observability/__init__.py`
- Create: `manager-agentscope/src/agentteams_manager/observability/logging.py`
- Create: `manager-agentscope/src/agentteams_manager/observability/metrics.py`
- Create: `manager-agentscope/src/agentteams_manager/observability/tracing.py`
- Create: `manager-agentscope/tests/unit/test_application.py`
- Create: `manager-agentscope/tests/unit/observability/test_tracing.py`
- Create: `manager-agentscope/tests/integration/test_health_server.py`

**Interfaces:**
- Produces: `ManagerApplication.start()`, `run()`, `stop()`
- Produces HTTP `GET /healthz`, `GET /readyz`, and `GET /metrics` on port 18799.

- [ ] **Step 1: Write lifecycle tests**

```python
import pytest


@pytest.mark.asyncio
async def test_application_starts_in_dependency_order(application) -> None:
    await application.start()
    assert application.start_log == [
        "database",
        "recovery",
        "config_watcher",
        "matrix",
        "heartbeat",
        "health",
    ]
    assert application.readiness.ready


@pytest.mark.asyncio
async def test_ready_endpoint_is_503_until_matrix_is_ready(http_client, app) -> None:
    response = await http_client.get("/readyz")
    assert response.status_code == 503
    app.readiness.matrix_ready = True
    response = await http_client.get("/readyz")
    assert response.status_code == 200


def test_cms_disabled_uses_noop_tracer(monkeypatch) -> None:
    monkeypatch.delenv("AGENTTEAMS_CMS_TRACES_ENABLED", raising=False)
    assert build_tracer_from_env().is_noop
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/test_application.py manager-agentscope/tests/integration/test_health_server.py -q
```

Expected: FAIL because application and health modules are absent.

- [ ] **Step 3: Implement lifecycle and standard-library health HTTP**

Use `asyncio.start_server`; accept only `GET`, close after one response, and return JSON for health endpoints. `ManagerApplication.run()` uses `asyncio.TaskGroup` and installs SIGTERM/SIGINT handlers. Shutdown order is the reverse of startup and always saves dirty sessions before closing SQLite.

Structured logs include:

```python
{
    "timestamp": "...",
    "level": "INFO",
    "event": "operation.transition",
    "trace_id": "...",
    "operation_id": "...",
    "room_id": "...",
    "task_id": "...",
}
```

The redaction filter replaces values for keys matching `token`, `secret`, `password`, `authorization`, and `api_key`. `/metrics` emits counters for Matrix events, model turns, tool calls, retries, recovery operations, and errors in Prometheus text format without adding a metrics dependency.

When `AGENTTEAMS_CMS_TRACES_ENABLED=true`, initialize AgentScope's installed
OpenTelemetry SDK and OTLP exporter from the existing CMS environment
variables. Emit spans for Matrix receive/send, model turns, tool calls,
Controller operations, recovery, and scheduled work. With CMS disabled, use a
no-op tracer and create no background exporter.

- [ ] **Step 4: Run the complete core gate**

Run:

```bash
python -m pytest manager-agentscope/tests/unit -q
python -m pytest manager-agentscope/tests/fault_injection -q
python -m pytest manager-agentscope/tests/integration/test_health_server.py -q
python -m compileall -q manager-agentscope/src
git diff --check
```

Expected: all tests PASS, compileall exits 0, and diff check has no output.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope
git commit -m "Make the AgentScope Manager observable and restartable" \
  -m "Constraint: One daemon owns state, recovery, Matrix lifecycle, scheduling, and health." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: complete Runtime Core gate"
```
