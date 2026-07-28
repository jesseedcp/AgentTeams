---
name: coding-cli
description: Prepare a precise coding request for the Manager's bounded coding CLI and review the mirrored result.
assign_when: A Worker owns a coding task and the administrator has enabled Manager coding CLI delegation
---

# Coding CLI Delegation

The administrator, not the Worker, starts the confirmed CLI operation. Your
responsibility is to prepare a reviewable workspace and a precise prompt.

1. Sync the task directory and stop if `.processing` exists.
2. Put the repository under
   `shared/tasks/<task-id>/workspace/` and mirror it to storage.
3. Send the Manager a prompt containing target files, required behavior,
   constraints, and acceptance checks. Do not include credentials.
4. Wait for `coding-result:` or `coding-failed:`.
5. On success, run `agentteams-sync`, inspect the diff and logs under
   `coding-cli-logs/`, run the relevant tests, and report your review.
6. On failure, inspect the redacted diagnostic and finish the task through the
   normal Worker flow.

The Manager uses a durable processing lease. Do not edit or upload the same
task workspace while the lease marker exists.
