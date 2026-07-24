---
name: worker-model-switch
description: Preflight and switch a Controller-managed Worker model through a policy-scoped typed tool.
---

# Worker Model Switch

Use `switch_worker_model`. Admin Rooms may target any Worker. Leader Rooms may
target only Workers allowed by the Controller-derived Team topology. The tool
requires confirmation.

The workflow:

1. preflights the requested model through the OpenAI-compatible gateway;
2. calls the typed `AgtClient` Worker update;
3. polls the Worker until its observed model matches;
4. fails if the Worker enters a failed phase;
5. reports the Controller-observed phase and effective capabilities.

Do not patch Worker runtime files, restart containers manually, or call the
Controller HTTP API directly. The Controller owns model resolution, runtime
configuration generation, storage publication, and Worker reconciliation.

Unknown models use explicit supplied capabilities or safe defaults. If
preflight fails, no Worker mutation occurs.
