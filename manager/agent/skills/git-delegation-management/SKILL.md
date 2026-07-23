---
name: git-delegation-management
description: Use when a Worker submits a structured Git request for execution inside its task workspace.
---

# Git Delegation Management

Workers do not receive Manager credentials. Git requests therefore cross one
typed, journaled boundary.

## Required flow

1. Wait for the complete request block: task ID, workspace, operations, and
   optional context. A promise to send a request is not a request.
2. Call `inspect_git_request`. It parses arguments without a command shell,
   validates the task path, rejects escape mechanisms, and reports each
   operation's risk.
3. For low or medium risk, call `git_delegate`.
4. For high risk, call `git_delegate_high_risk`; AgentScope must emit and
   resolve the confirmation event before execution.
5. Treat the returned Git receipt as an intermediate result. It does not
   complete the Worker task.

## Safety boundary

Only the allowlisted Git executable and subcommands are accepted. Shell
operators, substitutions, response files, executable configuration, external
helpers, remote destruction, whole-remote overwrite, and paths outside
`shared/tasks/<task-id>/workspace` are denied.

The workflow pulls first, conditionally acquires a 15-minute processing lease,
renews it during execution, pushes verified workspace output, releases the
lease, and replies with one stable Matrix transaction.

If recovery evidence says a Git process may already have started, the Manager
does not replay it. It reports the operation as needing attention so an admin
can inspect repository facts safely.
