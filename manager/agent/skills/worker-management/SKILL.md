---
name: worker-management
description: Use when an administrator wants to inspect, create, configure, sleep, wake, or remove a Controller-managed Worker.
---

# Worker Management

Manage Workers only through the policy-bound AgentScope tools in this skill.
The tools validate inputs, enforce the current Human and room scope, journal
mutations, reconcile Controller state, refresh Matrix topology, and return
secret-free receipts.

## Choose the operation

| Intent | Tool |
|---|---|
| See Workers in the current scope | `list_workers` |
| Inspect one Worker | `get_worker` |
| Hand-author a Worker | `create_worker` |
| Change a Worker's desired configuration | `update_worker` |
| Stop an idle Worker without deleting it | `sleep_worker` |
| Start a sleeping Worker | `wake_worker` |
| Permanently remove a Worker | `delete_worker` |

For a registry search or `nacos://` package import, use the
`agentteams-find-worker` skill instead of hand-authoring a Worker.

## Safety rules

- Treat Worker names as stable resource identities. They use lowercase
  letters, digits, and hyphens and must start with a letter or digit.
- Supported runtimes are `openclaw`, `copaw`, `hermes`, `qwenpaw`, and
  `openhuman`.
- Before `create_worker`, obtain an explicit name, runtime, model, role, and
  skill set. Do not silently invent business configuration.
- Before `sleep_worker`, confirm the Worker has no active assignment or
  unfinished handoff.
- A runtime, image, or package change can replace the Worker container.
  Explain that ephemeral in-container state can be lost before
  `update_worker`.
- `delete_worker` is permanent and requires the normal confirmation gate.
- After a mutation, use the returned receipt. Do not repeat the same
  operation merely because provisioning takes time; recovery is handled by
  the durable operation journal and heartbeat.

## References

Read only the relevant page:

- New Worker: `references/create-worker.md`
- Sleep, wake, delete, or recovery: `references/lifecycle.md`
- Skills and configuration: `references/skills-management.md`
- Service exposure: `references/console.md`
- Worker-to-Worker coordination: `references/peer-mentions.md`
