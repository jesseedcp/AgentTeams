# AgentScope Manager Tasks and Projects Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve finite tasks, recurring tasks, projects, explicit MinIO file synchronization, shared-workspace coordination, Git delegation, completion memory, and notifications with crash-safe ordering.

**Architecture:** SQLite is the local scheduler and operation ledger; MinIO holds canonical task/project artifacts; Matrix carries dispatch and completion messages. Workflows prepare durable artifacts before dispatch, use stable Matrix transaction IDs, and reconcile each external effect independently after restart.

**Tech Stack:** Python 3.11+, standard-library `sqlite3`, `datetime`, `zoneinfo`, `shlex`, `hashlib`, AgentScope confirmations, MinIO/S3 client, Matrix adapter, `git`, pytest.

## Global Constraints

- Apply every constraint and identifier from `2026-07-23-agentscope-manager-master.md`.
- Store task/project structured state in SQLite and their human-readable artifacts in MinIO; do not recreate `state.json`.
- Push and verify `meta.json` and `spec.md` before sending a Worker assignment.
- Send Worker instructions only to the Worker Room or Team Leader Room, never only in an Admin DM.
- Use the five-field cron format and an IANA timezone; do not add a cron dependency.
- Recurring completion records execution only; it must not immediately trigger the next execution.
- Pull before reading and push after writing. Local `/root/agentteams-fs` is a cache, not the source of truth.
- Acquire an expiring processing lease before modifying a shared task workspace.
- Parse Git requests to argv and execute `git` directly without a shell.
- Resolve and confine every workspace path beneath `shared/tasks/<task-id>/workspace`.
- Remote deletion/overwrite, arbitrary executables, shell operators, and path escape remain denied even after confirmation.

---

### Task 1: MinIO Artifact Client and Versioned Task Documents

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/clients/minio.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/storage.py`
- Create: `manager-agentscope/tests/unit/clients/test_minio.py`
- Create: `manager-agentscope/tests/unit/tools/test_storage.py`
- Create: `manager-agentscope/tests/fixtures/fake_s3.py`

**Interfaces:**
- Implements the `ArtifactPort` methods fixed in the master plan.
- Produces: `MinioClient.put_json_if_version`, `head`, `list_prefix`, `delete_if_version`.
- Produces: `TaskArtifactSet.write_prepared() -> tuple[ObjectReceipt, ...]`.

- [ ] **Step 1: Write checksum and conditional-write tests**

```python
import hashlib

import pytest


@pytest.mark.asyncio
async def test_put_bytes_returns_verified_receipt(minio_client, fake_s3):
    data = b"requirements"

    receipt = await minio_client.put_bytes(
        "shared/tasks/task-1/spec.md",
        data,
        content_type="text/markdown",
    )

    assert receipt.sha256 == hashlib.sha256(data).hexdigest()
    assert receipt.size == len(data)
    assert fake_s3.head(receipt.key).metadata["sha256"] == receipt.sha256


@pytest.mark.asyncio
async def test_version_mismatch_refuses_overwrite(minio_client):
    first = await minio_client.put_json_if_version(
        "shared/tasks/task-1/meta.json",
        {"status": "assigned"},
        expected_etag=None,
    )
    with pytest.raises(ObjectVersionConflict):
        await minio_client.put_json_if_version(
            first.key,
            {"status": "completed"},
            expected_etag='"wrong"',
        )
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_minio.py manager-agentscope/tests/unit/tools/test_storage.py -q
```

Expected: FAIL because the MinIO client and storage tools are absent.

- [ ] **Step 3: Implement verified object operations**

`put_bytes` uploads SHA-256 as object metadata, reads object metadata back, and returns:

```python
class ObjectReceipt(BaseModel):
    key: str
    etag: str
    version_id: str | None
    sha256: str
    size: int
    content_type: str
```

`mirror_down` downloads into a sibling temporary directory, verifies every object receipt, then replaces only the requested cache directory. `mirror_up` walks resolved regular files under the supplied source, rejects symlinks that leave the source, uploads changed objects, and returns a manifest containing key, ETag, checksum, and size.

Create task documents with an explicit version:

```python
class TaskMetadata(BaseModel):
    schema_version: Literal[1] = 1
    task_id: str
    task_type: Literal["finite", "infinite"]
    status: Literal[
        "prepared", "assigned", "active", "completed", "failed", "cancelled"
    ]
    title: str
    assigned_to: str
    room_id: str
    project_id: str | None = None
    schedule: str | None = None
    timezone: str | None = None
    created_at: datetime
    completed_at: datetime | None = None
```

Write `meta.json` and `spec.md` beneath `shared/tasks/<task-id>/`; preserve `base/`, `plan.md`, `result.md`, `workspace/`, and `notes/` as compatible paths.

- [ ] **Step 4: Run artifact tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_minio.py manager-agentscope/tests/unit/tools/test_storage.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/clients/minio.py manager-agentscope/src/agentteams_manager/tools/storage.py manager-agentscope/tests
git commit -m "Make task artifacts verifiable before Workers consume them" \
  -m "Constraint: MinIO is authoritative for shared task and project files." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: Never replace pull-before-read or receipt verification with cache assumptions." \
  -m "Tested: MinIO client and storage tool tests"
```

### Task 2: Finite Task Prepare, Dispatch, and Completion

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/workflows/tasks.py`
- Create: `manager-agentscope/src/agentteams_manager/tools/tasks.py`
- Create: `manager-agentscope/tests/unit/workflows/test_finite_tasks.py`
- Create: `manager-agentscope/tests/integration/test_finite_task_lifecycle.py`
- Create: `manager-agentscope/tests/fault_injection/test_task_dispatch_recovery.py`

**Interfaces:**
- Produces: `TaskService.create_finite`, `dispatch`, `record_completion`, `cancel`.
- Produces: `TaskMessageFormatter.assignment`, `completion`.
- Consumes: `TaskRepository`, `ArtifactPort`, `ControllerPort`, `MatrixPort`, `NotificationService`, and `OperationSupervisor`.

- [ ] **Step 1: Write ordering and idempotency tests**

```python
import pytest


@pytest.mark.asyncio
async def test_artifacts_and_sqlite_exist_before_dispatch(task_fixture):
    await task_fixture.service.create_finite(
        title="Fix login",
        spec="Acceptance: tests pass",
        assigned_to="alice",
        source_event=task_fixture.event(),
    )

    assert task_fixture.effect_order[:3] == [
        "sqlite.prepare",
        "minio.meta",
        "minio.spec",
    ]
    assert task_fixture.effect_order[3] == "matrix.assignment"


@pytest.mark.asyncio
async def test_restart_reuses_assignment_transaction_id(task_fixture):
    task_fixture.matrix.timeout_after_accepting_first_send()
    operation_id = await task_fixture.begin_assignment()

    await task_fixture.restart_and_recover()

    sends = task_fixture.matrix.sends_for(operation_id)
    assert len({send.txn_id for send in sends}) == 1
    assert task_fixture.matrix.visible_message_count == 1
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_finite_tasks.py manager-agentscope/tests/integration/test_finite_task_lifecycle.py manager-agentscope/tests/fault_injection/test_task_dispatch_recovery.py -q
```

Expected: FAIL because finite task workflows are absent.

- [ ] **Step 3: Implement prepared-before-dispatch state transitions**

Use these transitions:

```text
new -> prepared -> assigned -> completed
                  ├──────────> failed
                  └──────────> cancelled
```

Creation order is fixed:

1. validate the Worker or Team Leader and obtain the authoritative room;
2. insert the SQLite task with `prepared`;
3. journal and upload `meta.json`;
4. journal and upload `spec.md`;
5. journal and send the assignment with a stable Matrix transaction ID;
6. compare-and-swap SQLite to `assigned`;
7. conditionally update MinIO metadata to `assigned`.

Use the upstream-compatible instruction:

```text
@<worker>:<domain> New task [<task-id>]: <title>. Use your file-sync
skill to pull the spec: shared/tasks/<task-id>/spec.md.
@mention me when complete.
```

Team tasks address only the Team Leader. The Admin Room receives a short acknowledgement, not a duplicate Worker assignment.

On completion, pull and verify the full task prefix, require `result.md` or an explicit structured result, update MinIO then SQLite using the operation journal, append daily memory, and notify the resolved Admin channel exactly once. Duplicate Worker completion events return the existing completion receipt.

- [ ] **Step 4: Run finite task tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_finite_tasks.py manager-agentscope/tests/integration/test_finite_task_lifecycle.py manager-agentscope/tests/fault_injection/test_task_dispatch_recovery.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows/tasks.py manager-agentscope/src/agentteams_manager/tools/tasks.py manager-agentscope/tests
git commit -m "Prevent task dispatch before its durable specification exists" \
  -m "Constraint: Workers pull task specifications from MinIO and receive assignments in their own rooms." \
  -m "Rejected: Preserve state.json ordering | SQLite plus the operation journal closes its crash windows." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: finite task unit, lifecycle, and dispatch recovery tests"
```

### Task 3: Five-Field Cron and Recurring Task Heartbeat

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/domain/cron.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/tasks.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/heartbeat.py`
- Create: `manager-agentscope/tests/unit/domain/test_cron.py`
- Create: `manager-agentscope/tests/unit/workflows/test_recurring_tasks.py`
- Create: `manager-agentscope/tests/integration/test_recurring_heartbeat.py`

**Interfaces:**
- Produces: `CronSchedule.parse(expression)`, `next_after(instant, timezone)`.
- Produces: `TaskService.create_recurring`, `record_execution`.
- Produces: `TaskHeartbeat.dispatch_due(now) -> DispatchReport`.

- [ ] **Step 1: Write cron, DST, and no-loop tests**

```python
from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.cron import CronSchedule


def test_cron_supports_lists_ranges_and_steps():
    schedule = CronSchedule.parse("*/15 9-17 * * 1,3,5")
    assert schedule.minute == frozenset({0, 15, 30, 45})
    assert 9 in schedule.hour and 17 in schedule.hour
    assert schedule.weekday == frozenset({1, 3, 5})


def test_next_after_crosses_dst_with_zoneinfo():
    schedule = CronSchedule.parse("30 9 * * *")
    result = schedule.next_after(
        datetime(2026, 3, 7, 15, tzinfo=UTC),
        "America/New_York",
    )
    assert result.tzinfo is UTC


@pytest.mark.asyncio
async def test_record_execution_does_not_dispatch_again(recurring_fixture):
    await recurring_fixture.service.record_execution(
        task_id="task-1",
        worker_event_id="$done",
    )
    assert recurring_fixture.matrix.send_count == 0
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/domain/test_cron.py manager-agentscope/tests/unit/workflows/test_recurring_tasks.py manager-agentscope/tests/integration/test_recurring_heartbeat.py -q
```

Expected: FAIL because cron parsing and recurring workflows are absent.

- [ ] **Step 3: Implement a bounded standard-library cron evaluator**

Accept exactly five fields: minute, hour, day of month, month, weekday. Each field supports `*`, comma lists, inclusive ranges, and `/step`. Reject names, seconds, macros, zero steps, out-of-range values, and expressions longer than 128 characters.

`next_after`:

1. converts the instant to the named `ZoneInfo`;
2. rounds to the next local minute;
3. scans at most 527,040 minutes;
4. applies standard cron day-of-month/weekday OR semantics when both are restricted;
5. converts the matching aware local instant to UTC;
6. rejects nonexistent local times and chooses the first occurrence of an ambiguous local time.

Recurring task state is `active`, with `last_executed_at` and `next_scheduled_at`. Heartbeat dispatches only when:

```python
due = (
    task.status == "active"
    and task.next_scheduled_at <= now
    and (
        task.last_executed_at is None
        or task.last_executed_at < task.next_scheduled_at
    )
)
```

Preserve the upstream 30-minute late grace as a reported warning, not a reason for multiple catch-up sends. After an `executed` report, calculate one future schedule and do not send anything until a later heartbeat observes it due.

- [ ] **Step 4: Run recurring task tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/domain/test_cron.py manager-agentscope/tests/unit/workflows/test_recurring_tasks.py manager-agentscope/tests/integration/test_recurring_heartbeat.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/domain/cron.py manager-agentscope/src/agentteams_manager/workflows manager-agentscope/tests
git commit -m "Schedule recurring work without rapid-fire completion loops" \
  -m "Constraint: Five-field cron and IANA timezones remain the public contract." \
  -m "Rejected: Add croniter | the standard library implementation covers the accepted grammar." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: cron, recurring task, and heartbeat tests"
```

### Task 4: Project Rooms, Metadata, and Project Task Lifecycle

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/workflows/projects.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/tasks.py`
- Create: `manager-agentscope/tests/unit/workflows/test_projects.py`
- Create: `manager-agentscope/tests/integration/test_project_lifecycle.py`
- Create: `manager-agentscope/tests/fault_injection/test_project_room_recovery.py`

**Interfaces:**
- Produces: `ProjectService.create`, `add_task`, `complete_task`, `close`.
- Produces: `ProjectMetadata` and `ProjectPlan`.
- Consumes: `MatrixAdministrationPort`, `ArtifactPort`, `TaskService`, `TopologyRepository`, and `OperationSupervisor`.

- [ ] **Step 1: Write project ordering and recovery tests**

```python
import pytest


@pytest.mark.asyncio
async def test_project_room_contains_admin_and_selected_workers(project_fixture):
    project = await project_fixture.service.create(
        title="Release 2",
        description="Ship the new runtime",
        participants=("alice", "bob"),
        source_event=project_fixture.event(),
    )
    members = project_fixture.matrix.members(project.room_id)
    assert project_fixture.admin_user_id in members
    assert project_fixture.manager_user_id in members
    assert "@worker-alice:example" in members
    assert "@worker-bob:example" in members


@pytest.mark.asyncio
async def test_room_timeout_is_reconciled_by_project_marker(project_fixture):
    project_fixture.matrix.create_room_times_out_after_success()

    project = await project_fixture.create_then_restart()

    assert project_fixture.matrix.project_rooms(project.project_id) == 1
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_projects.py manager-agentscope/tests/integration/test_project_lifecycle.py manager-agentscope/tests/fault_injection/test_project_room_recovery.py -q
```

Expected: FAIL because project workflows are absent.

- [ ] **Step 3: Implement project preparation and Matrix room reconciliation**

Generate `project_id` using the master identifier rule. Prepare SQLite and MinIO `meta.json` plus `plan.md` before inviting Workers. Create a private Matrix room with state:

```python
{
    "name": title,
    "topic": f"AgentTeams project {project_id}",
    "creation_content": {
        "m.agentteams.project_id": project_id,
        "m.agentteams.schema_version": 1,
    },
}
```

Invite the requesting Admin, selected Workers, and Manager. Record `room_kind="project"` in topology; do not patch OpenClaw channel configuration. If room creation is ambiguous, find a joined room with the exact immutable project marker before retrying.

Project tasks use `TaskService` with `project_id` and `project_room_id`. Assignment still goes to each Worker Room; progress summaries go to the Project Room. Closing requires confirmation, refuses while nonterminal tasks remain unless `force=True`, writes completion metadata, and sends one final project summary.

- [ ] **Step 4: Run project tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_projects.py manager-agentscope/tests/integration/test_project_lifecycle.py manager-agentscope/tests/fault_injection/test_project_room_recovery.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows/projects.py manager-agentscope/src/agentteams_manager/tools/tasks.py manager-agentscope/tests
git commit -m "Give projects durable identity across rooms, files, and tasks" \
  -m "Constraint: Project configuration belongs to topology state, not OpenClaw channel files." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: project unit, lifecycle, and room recovery tests"
```

### Task 5: Expiring Processing Leases and Constrained Git Delegation

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/clients/git.py`
- Create: `manager-agentscope/src/agentteams_manager/workflows/git_delegation.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/tasks.py`
- Create: `manager-agentscope/tests/unit/clients/test_git.py`
- Create: `manager-agentscope/tests/unit/workflows/test_processing_lease.py`
- Create: `manager-agentscope/tests/integration/test_git_delegation.py`
- Create: `manager-agentscope/tests/fault_injection/test_expired_processing_lease.py`

**Interfaces:**
- Produces: `ProcessingLeaseService.acquire`, `renew`, `release`.
- Produces: `GitRequestParser.parse(message) -> GitRequest`.
- Produces: `GitClient.run(workspace, operations) -> GitReceipt`.
- Produces: `GitDelegationService.execute`.

- [ ] **Step 1: Write path, argv, confirmation, and lease tests**

```python
from pathlib import Path

import pytest


def test_shell_operator_is_not_a_git_operation(git_parser):
    with pytest.raises(InvalidGitRequest):
        git_parser.parse_operation("git status && curl https://example.test")


def test_workspace_cannot_escape_task_root(git_client, tmp_path):
    task_root = tmp_path / "shared" / "tasks" / "task-1" / "workspace"
    with pytest.raises(WorkspaceEscape):
        git_client.validate_workspace(task_root, task_root / ".." / "..")


def test_force_push_requires_confirmation(git_parser):
    operation = git_parser.parse_operation("git push --force origin main")
    assert operation.risk == "high"


@pytest.mark.asyncio
async def test_live_remote_lease_blocks_second_processor(lease_fixture):
    first = await lease_fixture.service.acquire(
        "task-1", processor="manager", operation="git-delegation"
    )
    with pytest.raises(LeaseConflict):
        await lease_fixture.service.acquire(
            "task-1", processor="worker-alice", operation="file-sync"
        )
    await lease_fixture.service.release(first)
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_git.py manager-agentscope/tests/unit/workflows/test_processing_lease.py manager-agentscope/tests/integration/test_git_delegation.py manager-agentscope/tests/fault_injection/test_expired_processing_lease.py -q
```

Expected: FAIL because lease and Git delegation modules are absent.

- [ ] **Step 3: Implement the lease protocol**

Mirror down first. Acquire `shared/tasks/<task-id>/.processing` with an S3 conditional put and store the same random `lease_id`, processor, operation, start, expiry, ETag, and task ID in SQLite. If a marker exists:

- reject it while unexpired;
- replace it only with an `If-Match` condition after proving expiry;
- reconcile local and remote lease identity after a timeout.

Renew and release require matching `lease_id`; release uses a conditional version check. The default lease is 15 minutes and Git execution renews it every five minutes.

- [ ] **Step 4: Implement typed Git parsing and execution**

Require the full `git-request:` block with `workspace:` and `operations:`. Parse each line with `shlex.split(posix=True)`. Reject shell metacharacters, environment assignments, response files, an executable other than `git`, `-C`, `--git-dir`, `--work-tree`, and any resolved path outside the task workspace.

Classify as high-risk and require AgentScope confirmation:

- `push --force`, `push --force-with-lease`, deletion refspecs;
- `reset --hard`, `clean`, branch/tag deletion;
- `rebase`, `filter-branch`, `filter-repo`;
- checkout/restore paths that discard changes;
- remote removal or URL changes;
- destructive submodule operations.

Always deny overwriting/deleting a remote repository, `git init --bare` over an existing path, and `ext::` transports. Execute:

```python
argv = (
    "git",
    "-c", "core.hooksPath=/dev/null",
    "-c", "protocol.ext.allow=never",
    *operation.argv[1:],
)
result = await self._process.run(
    argv,
    cwd=validated_workspace,
    timeout=self._timeout_for(operation),
)
```

After all commands, mirror up, release the lease, and send exactly one `git-result:` or `git-failed:` to the requesting Worker Room. A Git receipt is not task completion.

- [ ] **Step 5: Run Git and lease tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/clients/test_git.py manager-agentscope/tests/unit/workflows/test_processing_lease.py manager-agentscope/tests/integration/test_git_delegation.py manager-agentscope/tests/fault_injection/test_expired_processing_lease.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/clients/git.py manager-agentscope/src/agentteams_manager/workflows/git_delegation.py manager-agentscope/src/agentteams_manager/tools/tasks.py manager-agentscope/tests
git commit -m "Preserve Git delegation without granting shell execution" \
  -m "Constraint: Shared workspaces require expiring leases and Git credentials stay in Manager." \
  -m "Rejected: Execute the original command strings through a shell | it permits command and path escape." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Directive: New destructive Git forms must be risk-classified before support." \
  -m "Tested: Git parser, lease, integration, and expiry fault tests"
```

### Task 6: Explicit File Sync, Daily Memory, and Exactly-Once Notifications

**Files:**
- Create: `manager-agentscope/src/agentteams_manager/workflows/notifications.py`
- Modify: `manager-agentscope/src/agentteams_manager/workflows/tasks.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/storage.py`
- Create: `manager-agentscope/tests/unit/workflows/test_notifications.py`
- Create: `manager-agentscope/tests/integration/test_file_sync.py`
- Create: `manager-agentscope/tests/fault_injection/test_completion_notification.py`

**Interfaces:**
- Produces: `FileSyncService.pull_task`, `push_task`, `push_files`.
- Produces: `NotificationService.resolve_room`, `send_once`.
- Produces: `DailyMemory.append_once(date, entry)`.

- [ ] **Step 1: Write stale-cache and duplicate-notification tests**

```python
import pytest


@pytest.mark.asyncio
async def test_worker_uploaded_result_is_pulled_before_read(sync_fixture):
    sync_fixture.local_result("stale")
    sync_fixture.remote_result("fresh")

    result = await sync_fixture.service.pull_task("task-1")

    assert result.read_text("utf-8") == "fresh"
    assert sync_fixture.calls[0] == "mirror_down"


@pytest.mark.asyncio
async def test_crash_after_send_does_not_duplicate_completion(
    notification_fixture,
):
    notification_fixture.matrix.timeout_after_accepting()
    await notification_fixture.send_then_restart()

    assert notification_fixture.matrix.visible_message_count == 1
    assert notification_fixture.memory.entry_count("task-1") == 1
```

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_notifications.py manager-agentscope/tests/integration/test_file_sync.py manager-agentscope/tests/fault_injection/test_completion_notification.py -q
```

Expected: FAIL because notification and explicit sync workflows are absent.

- [ ] **Step 3: Implement explicit sync and receipt-driven notification**

`pull_task` always mirrors the remote prefix down before returning a path. `push_task` acquires a processing lease unless it already owns one, uploads changed files plus a checksum manifest, and tells the Worker to run `agentteams-sync`. It never claims automatic real-time synchronization.

`NotificationService` resolves the primary Matrix channel through the policy from Plan 03 and records a journal event before send. Use:

```text
[Task Completed] <task-id>: <title> — assigned to <worker>. <summary>
```

The Matrix transaction ID is derived from the completion operation. A send timeout remains reconciling; repeated calls reuse the transaction ID.

Daily memory is stored at `manager/memory/YYYY-MM-DD.md` with an adjacent immutable entry object keyed by operation ID. Append only after the entry object is conditionally created, so restart cannot duplicate the entry.

- [ ] **Step 4: Run file, memory, and notification tests**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/workflows/test_notifications.py manager-agentscope/tests/integration/test_file_sync.py manager-agentscope/tests/fault_injection/test_completion_notification.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope/src/agentteams_manager/workflows manager-agentscope/src/agentteams_manager/tools/storage.py manager-agentscope/tests
git commit -m "Make file movement and completion notices explicit and repeat-safe" \
  -m "Constraint: Local task files are a cache and Manager channels are Matrix-only." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: notification, file-sync, and completion fault-injection tests"
```

### Task 7: Task Recovery, AgentScope Tools, and Task Gate

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/workflows/heartbeat.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/tasks.py`
- Modify: `manager-agentscope/src/agentteams_manager/tools/storage.py`
- Modify: `manager/agent/HEARTBEAT.md`
- Modify: `manager/agent/skills/file-sync-management/SKILL.md`
- Modify: `manager/agent/skills/file-sync-management/references/sync-guide.md`
- Modify: `manager/agent/skills/git-delegation-management/SKILL.md`
- Modify: `manager/agent/skills/project-management/SKILL.md`
- Modify: `manager/agent/skills/project-management/references/create-project.md`
- Modify: `manager/agent/skills/project-management/references/plan-changes.md`
- Modify: `manager/agent/skills/project-management/references/plan-format.md`
- Modify: `manager/agent/skills/project-management/references/task-lifecycle.md`
- Modify: `manager/agent/skills/task-coordination/SKILL.md`
- Modify: `manager/agent/skills/task-management/SKILL.md`
- Modify: `manager/agent/skills/task-management/references/finite-tasks.md`
- Modify: `manager/agent/skills/task-management/references/infinite-tasks.md`
- Modify: `manager/agent/skills/task-management/references/state-management.md`
- Modify: `manager/agent/skills/task-management/references/worker-selection.md`
- Create: `manager-agentscope/tests/integration/test_task_recovery.py`
- Create: `manager-agentscope/tests/contract/test_task_skill_parity.py`
- Create: `manager-agentscope/tests/fault_injection/test_task_effect_boundaries.py`

**Interfaces:**
- Registers AgentScope tools for task, project, file sync, coordination, and Git workflows.
- Extends `Heartbeat.run_once()` with due schedules and task recovery.

- [ ] **Step 1: Write boundary matrix and parity tests**

```python
import pytest


@pytest.mark.parametrize(
    "boundary",
    (
        "after_sqlite_prepare",
        "after_meta_upload",
        "after_spec_upload",
        "after_assignment_send",
        "after_completion_upload",
        "after_completion_notification",
    ),
)
@pytest.mark.asyncio
async def test_restart_converges_at_every_task_boundary(
    boundary, task_fault_fixture
):
    task_fault_fixture.crash_at(boundary)
    await task_fault_fixture.run_and_restart()
    assert task_fault_fixture.external_invariants_hold()


def test_task_skills_have_owned_acceptance_tests(skill_registry):
    assert skill_registry.covered({
        "file-sync-management",
        "git-delegation-management",
        "project-management",
        "task-coordination",
        "task-management",
    })
```

The parity test also scans the five task/project skill families and
`HEARTBEAT.md`. Every documented mutation must name a registered typed tool;
no retained document may call a deleted skill script, directly mutate the old
JSON registries, or describe model-driven reconciliation as the durable
heartbeat mechanism.

- [ ] **Step 2: Verify failure**

Run:

```bash
python -m pytest manager-agentscope/tests/integration/test_task_recovery.py manager-agentscope/tests/contract/test_task_skill_parity.py manager-agentscope/tests/fault_injection/test_task_effect_boundaries.py -q
```

Expected: FAIL because recovery registration and parity evidence are incomplete.

- [ ] **Step 3: Register task reconcilers and closed tool schemas**

Recovery proves:

- SQLite `prepared` plus absent objects resumes upload;
- present objects with matching checksum are reused;
- an ambiguous Matrix send reuses its transaction ID;
- a completion object is not overwritten by stale state;
- a live processing lease is respected;
- an expired lease can be reclaimed conditionally;
- due recurring tasks have at most one active dispatch schedule;
- project rooms are found by immutable project marker.

Heartbeat order after resource reconciliation is:

1. recover task/project/Git operations;
2. reclaim provably expired leases;
3. dispatch due recurring tasks;
4. reconcile outstanding finite-task completion reports;
5. send unsent terminal notifications;
6. snapshot SQLite and journal progress when the configured threshold is met.

AgentScope tools accept strict request models and expose high-risk Git/project actions as confirmation events.

Rewrite the listed skill documents around the new typed task, project, storage,
and Git tools. Preserve the upstream finite/recurring task protocol, project
DAG rules, Worker selection rules, lease safety, and MinIO object conventions.
Rewrite `HEARTBEAT.md` to describe the deterministic scheduler/recovery order
and the optional post-reconciliation conversational summary.

- [ ] **Step 4: Run the complete task gate**

Run:

```bash
python -m pytest manager-agentscope/tests/unit/domain/test_cron.py -q
python -m pytest manager-agentscope/tests/unit/workflows/test_finite_tasks.py manager-agentscope/tests/unit/workflows/test_recurring_tasks.py manager-agentscope/tests/unit/workflows/test_projects.py manager-agentscope/tests/unit/workflows/test_processing_lease.py manager-agentscope/tests/unit/workflows/test_notifications.py -q
python -m pytest manager-agentscope/tests/unit/clients/test_minio.py manager-agentscope/tests/unit/clients/test_git.py manager-agentscope/tests/unit/tools/test_storage.py -q
python -m pytest manager-agentscope/tests/integration/test_finite_task_lifecycle.py manager-agentscope/tests/integration/test_recurring_heartbeat.py manager-agentscope/tests/integration/test_project_lifecycle.py manager-agentscope/tests/integration/test_git_delegation.py manager-agentscope/tests/integration/test_file_sync.py manager-agentscope/tests/integration/test_task_recovery.py -q
python -m pytest manager-agentscope/tests/fault_injection/test_task_dispatch_recovery.py manager-agentscope/tests/fault_injection/test_project_room_recovery.py manager-agentscope/tests/fault_injection/test_expired_processing_lease.py manager-agentscope/tests/fault_injection/test_completion_notification.py manager-agentscope/tests/fault_injection/test_task_effect_boundaries.py -q
python -m pytest manager-agentscope/tests/contract/test_task_skill_parity.py -q
git diff --check
```

Expected: all tests PASS and diff check has no output.

- [ ] **Step 5: Commit**

```bash
git add manager-agentscope manager/agent/HEARTBEAT.md manager/agent/skills
git commit -m "Close every task and project crash boundary before release" \
  -m "Constraint: SQLite schedules, MinIO artifacts, and Matrix messages must converge independently." \
  -m "Confidence: high" \
  -m "Scope-risk: broad" \
  -m "Tested: complete Task and Project gate"
```
