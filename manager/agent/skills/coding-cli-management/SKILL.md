---
name: coding-cli-management
description: Use a configured Claude, Gemini, or Qoder CLI to perform one confirmed coding task inside a leased AgentTeams task workspace.
---

# Coding CLI Management

This optional workflow lets the AgentScope Manager delegate file edits to one
operator-installed coding CLI without exposing a general shell tool.

## Before delegating

1. Call `coding_cli_status`.
2. Continue only when the deployment reports `enabled: true` and the selected
   provider is both `configured` and `available`.
3. Confirm that the task already exists and its workspace has been uploaded.
4. Explain to the administrator that the provider can modify files inside that
   task workspace. `delegate_coding_cli` always requires confirmation.

## Delegation

Call `delegate_coding_cli` with:

- `task_id`: the existing durable task ID.
- `provider`: `claude`, `gemini`, or `qodercli`.
- `workspace`: `.` or a relative directory below the task's `workspace/`.
- `prompt`: exact target files, required behavior, constraints, and a
  verification command or acceptance criteria.
- `timeout_seconds`: optional; the deployment maximum still applies.

The workflow acquires the same processing lease used by file and Git
operations, mirrors the current task data down, stores the prompt as an
artifact, executes an immutable provider command with the prompt on stdin,
mirrors the result up, sends the Worker a Matrix result, and releases the
lease.

Never put provider tokens in the prompt. Credentials come only from the
operator-controlled Manager environment or provider login mount.

## Result handling

- A successful receipt means the provider process exited successfully and its
  workspace/log artifacts were mirrored. Ask the Worker to sync and review.
- A `coding-failed:` result is a definite provider failure. Keep the prompt
  artifact, inspect the redacted summary, and reassign or complete manually.
- If recovery reports that the process may have started, do not replay it
  blindly. Inspect the mirrored workspace before starting a new confirmed
  operation.

See [provider configuration](references/providers.md) for deployment and
permission details.
