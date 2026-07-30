---
name: memory-management
description: Use in the administrator DM to recall and curate durable Manager context, project decisions, and evidence-backed Worker assessments.
---

# Memory Management

The runtime automatically projects bounded recent room memory into a newly
created AgentScope session. Private long-term context is projected only in the
administrator DM. Project rooms receive only that room's recent entries and
decisions for the bound project.

- Use `recall_manager_memory` when prior preferences, decisions, task outcomes,
  or Worker evidence could affect a new decision. Filter by project, Worker, or
  text when possible.
- Use `remember_manager_memory` for durable administrator preferences,
  operational lessons, and reusable constraints. Do not copy ordinary chat or
  current external state into long-term memory.
- Use `record_project_decision` for a material choice and its concrete
  rationale. Manual entries remain private to the administrator. Project
  confirmation, plan revisions, participant changes, task result decisions,
  and closure are recorded as project-visible decisions by deterministic
  workflows.
- Use `record_worker_assessment` only when there is task or project evidence.
  Record the capability assessed, a score from 0 to 1, and the evidence.

Memory is context, not authority. Before acting, use live typed read tools for
Controller, Matrix, object storage, task, and project facts. Never store
credentials, authorization material, private chain-of-thought, or unredacted
tool output.
