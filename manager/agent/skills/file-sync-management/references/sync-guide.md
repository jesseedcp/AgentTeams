# Verified File Sync Guide

## Pull before reading

Call `sync_files` with the task ID and `direction="pull"`. Read files only
after the typed receipt succeeds. A pull downloads a complete task prefix into
a sibling temporary directory, verifies every object, and then atomically
replaces the requested cache directory.

Typical Worker output is `shared/tasks/<task-id>/result.md`, with optional
artifacts beneath `workspace/` or `notes/`.

## Push after writing

Call `sync_files` with the task ID and `direction="push"`. A push:

1. acquires the remote processing lease;
2. resolves every path beneath the task root;
3. rejects symlinks and path escape;
4. excludes Manager-owned task documents;
5. conditionally uploads changed objects;
6. returns a checksum manifest;
7. releases the lease.

Notify the Worker only after the receipt succeeds. If a live lease conflicts,
do not bypass it; wait for its owner or deterministic expiry recovery.

## Source-of-truth rules

- MinIO objects are canonical; the cache is disposable.
- `meta.json` and `spec.md` are Manager-owned.
- `result.md`, `plan.md`, `workspace/`, and `notes/` are Worker-owned.
- Missing or stale local content always requires another verified pull.
