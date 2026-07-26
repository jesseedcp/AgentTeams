# Matrix Pending Confirmation Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent ordinary Matrix messages and mismatched confirmation commands from entering an AgentScope reply that is waiting for a tool confirmation.

**Architecture:** Add a pre-dispatch guard in `MatrixSessionRunner`, before media download and `UserMsg` construction. The guard reads the durable pending confirmation from the room Agent state; only the matching formal command continues the existing AgentScope reply, while other administrator input receives a deterministic reminder and leaves state untouched.

**Tech Stack:** Python 3.11, AgentScope 2.0, asyncio, pytest, SQLite session persistence, Matrix.

## Global Constraints

- Do not auto-confirm, auto-deny, clear, or replace a pending operation.
- Do not expose tool arguments, credentials, or internal Agent state in the reminder.
- Preserve exact `/confirm <reply_id>` and `/deny <reply_id>` semantics.
- Preserve per-room serialization, deterministic Matrix transaction IDs, and restart recovery.
- Do not modify Cinny, Tuwunel, Controller, Matrix rooms, or stored messages.
- Execute inline in the current session; do not use subagents.

---

### Task 1: Lock the Failure with Integration Tests

**Files:**
- Modify: `manager-agentscope/tests/integration/test_matrix_confirmation.py`

**Interfaces:**
- Consumes: `MatrixSessionRunner.handle(event, policy)` and `pending_confirmation(AgentState)`.
- Produces: regression coverage proving invalid pending-confirmation input never reaches `ConfirmationAgent.reply_stream()`.

- [ ] **Step 1: Record all inputs received by the test Agent**

Add `self.inputs: list[object] = []` to `ConfirmationAgent` and append `inputs`
at the beginning of `reply_stream()`. Existing assertions continue to use
`confirmation_results`.

- [ ] **Step 2: Add the ordinary-message regression test**

Create a pending confirmation with `"delete alice"`, then send
`"告诉我你的名字"` as a second event. Assert:

```python
assert len(factory.agent.inputs) == 1
assert "Your message was not processed" in matrix.sent[-1].text
assert "/confirm reply-delete" in matrix.sent[-1].text
assert "/deny reply-delete" in matrix.sent[-1].text
assert pending_confirmation(stored.state).reply_id == "reply-delete"
```

- [ ] **Step 3: Add the mismatched-ID regression test**

After creating the same pending confirmation, send
`"/confirm wrong-reply"`. Assert that no `UserConfirmResultEvent` reached the
Agent, the reminder contains the current `reply-delete` commands, and the
durable pending state remains unchanged.

- [ ] **Step 4: Run the two tests and verify RED**

Run:

```powershell
python -m pytest `
  manager-agentscope/tests/integration/test_matrix_confirmation.py `
  -k 'ordinary_message or mismatched_id' -vv
```

Expected: both tests fail because the second input currently enters
`ConfirmationAgent.reply_stream()` instead of producing a reminder.

---

### Task 2: Guard the AgentScope Confirmation Boundary

**Files:**
- Modify: `manager-agentscope/src/agentteams_manager/matrix/session_runner.py`
- Test: `manager-agentscope/tests/integration/test_matrix_confirmation.py`

**Interfaces:**
- Consumes: `RoomSessionManager.get_or_create()`,
  `pending_confirmation(AgentState)`, `PendingConfirmation`, and
  `MatrixOutput.send_text()`.
- Produces: `_send_pending_confirmation_reminder(event, pending) -> None`.

- [ ] **Step 1: Read pending state before constructing `UserMsg`**

At the start of `handle()`, parse the command, load the room session, and read
its durable pending confirmation. When none exists, retain the existing
message path unchanged.

- [ ] **Step 2: Permit only the matching formal command**

When pending state exists:

```python
if command is not None and command[1] == pending.reply_id:
    await self._handle_confirmation(event, policy, *command)
    return
await self._send_pending_confirmation_reminder(event, pending)
return
```

Retain the existing administrator and `ADMIN_DM` permission check before
revealing or resolving confirmation data.

- [ ] **Step 3: Send a deterministic reminder**

Implement a helper that sends:

```text
A confirmation is still pending for: <tool names>
Your message was not processed. Resolve the pending request first:
/confirm <reply_id>
/deny <reply_id>
```

Use `operation_id_for(room_id, event_id, reply_id)` and
`matrix_transaction_id(..., 0)` so replaying the same Matrix event remains
idempotent. Include only tool names, never tool inputs.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```powershell
python -m pytest `
  manager-agentscope/tests/integration/test_matrix_confirmation.py -vv
```

Expected: ordinary text and a mismatched ID produce reminders; the correct
confirmation and non-admin permission tests still pass.

- [ ] **Step 5: Run restart recovery coverage**

Run:

```powershell
python -m pytest `
  manager-agentscope/tests/fault_injection/test_matrix_restart.py `
  -k confirmation -vv
```

Expected: confirmation still resumes after process reconstruction.

---

### Task 3: Verify, Deploy, and Publish

**Files:**
- Modify: `docs/superpowers/plans/2026-07-26-matrix-confirmation-guard.md`
- Runtime: current Docker Manager and Kind release

**Interfaces:**
- Consumes: the passing Manager package and existing deployment scripts/images.
- Produces: verified Docker/K8s Manager images and a pushed Lore commit on `jesseedcp/main`.

- [ ] **Step 1: Run Manager verification**

Run the Manager package test suite, formatting/lint checks already configured
by the repository, and `git diff --check`. Expected: zero failures.

- [ ] **Step 2: Build the AgentScope Manager image**

Build a new image from the repository’s existing Manager Docker target without
changing Controller, Cinny, Matrix, or data volumes.

- [ ] **Step 3: Upgrade Docker and Kind Manager only**

Replace only the Manager runtime image/container. Preserve the current
workspace volume, SQLite database, Matrix credentials, room IDs, and pending
confirmation.

- [ ] **Step 4: Run live regression**

In the current admin room, verify that ordinary text while the persisted
confirmation is pending receives the reminder and produces no Manager
traceback. Do not automatically resolve the user’s pending identity update.

- [ ] **Step 5: Final verification**

Confirm Manager readiness, all Kind Pods Ready, no new
`Matrix event processing failed` entry for the probe event, and the pending
SQLite record remains `awaiting`.

- [ ] **Step 6: Commit and push**

Create a Lore-format implementation commit, push directly to
`jesseedcp/main`, and verify `git ls-remote` matches local `HEAD`.
