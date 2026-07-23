---
name: agentteams-find-worker
description: Use when an admin wants to discover, recommend, or import a Worker template from the configured Nacos registry, including an explicit nacos package URI.
---

# Find or Import a Worker

Use Nacos only for template-backed Workers. Hand-authored Workers belong to
`worker-management`.

## Decision Flow

| Admin input | First tool | Next action |
|---|---|---|
| Requirement or template name | `find_worker` | Show up to three candidates; do not import |
| Full `nacos://` URI | none | Restate URI and proposed Worker name |
| Confirmed searched candidate | `import_worker` | Pass the returned discovery receipt, candidate name, and Worker name |
| Confirmed direct URI | `import_worker` | Pass only the package URI and Worker name |
| No useful candidate | none | Offer hand-authored creation before any import is confirmed |

`find_worker` returns typed candidates with runtime, pinned version, package URI,
and SHA-256 digest. Its discovery receipt is tamper-evident and must be passed
back unchanged.

`import_worker` is a mutating tool, so AgentScope asks the admin to confirm the
exact input. The workflow verifies the AgentSpec again, imports through the
Controller, waits for the Worker Room, and returns a typed receipt.

## Non-Negotiable Rules

- Never import as a side effect of `find_worker`.
- Never replace a failed Nacos import with `create_worker`.
- Report the redacted import failure and let the admin choose another candidate
  or explicitly request a hand-authored Worker.
- Never alter the candidate URI, version, digest, runtime, or discovery token.
- An explicit URI skips market search, not confirmation or integrity checks.

For exact request shapes and examples, read
`references/import-worker-template.md`.
