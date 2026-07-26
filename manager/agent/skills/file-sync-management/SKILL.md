---
name: file-sync-management
description: Use for verified task, Worker workspace, and shared-knowledge transfer between the local cache and authoritative MinIO storage.
---

# File Sync Management

MinIO is authoritative. Local sync directories are only caches.

Use `sync_files` with `direction="pull"` before reading Worker output. Use
`sync_files` with `direction="push"` after writing Worker-owned task files.
Choose `root="task_artifacts"` with `task_id`,
`root="worker_workspace"` with `worker_name`, or
`root="shared_knowledge"` with no target. The tool verifies object metadata,
checksums, room scope, symlinks, and path containment.

A task push and the resulting Worker mention share one durable `FILE_SYNC`
operation. A retry reuses the same MinIO versions and Matrix transaction.

Never overwrite Manager-owned `meta.json`, `spec.md`, `base/`, or the remote
processing lease through a task push. Never infer that a local file is
current merely because it exists.

Read `references/sync-guide.md` for the exact pull-before-read and
push-after-write protocol.
