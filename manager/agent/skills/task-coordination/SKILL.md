---
name: task-coordination
description: Use when Manager and Workers could modify the same task workspace or when a processing lease conflicts.
---

# Task Coordination

Shared task workspaces use an authoritative MinIO processing lease and a local
SQLite materialization. Never create, edit, or remove `.processing` directly.

Before ordinary file work, use `sync_files`; its push path acquires and
releases the lease automatically. Before Git work, use
`inspect_git_request`; the selected Git delegation tool owns the full lease
lifecycle.

A live lease always wins over a local cache. Expiry alone is not enough to
delete it: the heartbeat must read the remote marker, prove task and lease
identity, and conditionally delete the exact observed version. A version
change is a conflict, not permission to retry destructively.

If a process may have crossed an irreversible boundary, escalate the operation
ID instead of replaying it.
