---
name: file-sync-management
description: Use for verified task-file transfer between the local cache and authoritative MinIO storage.
---

# File Sync Management

MinIO is authoritative. The local task directory is only a cache.

Use `sync_files` with `direction="pull"` before reading Worker output. Use
`sync_files` with `direction="push"` after writing Worker-owned task files.
The tool verifies object metadata, checksums, and task scope.

Never overwrite Manager-owned `meta.json`, `spec.md`, `base/`, or the remote
processing lease through a Worker push. Never infer that a local file is
current merely because it exists.

Read `references/sync-guide.md` for the exact pull-before-read and
push-after-write protocol.
