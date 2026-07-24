---
name: model-switch
description: Preflight and switch the AgentScope Manager model through typed Controller desired state.
---

# Manager Model Switch

Use `switch_model` in the Admin Room. It requires confirmation.

The workflow:

1. strips only the managed `agentteams-gateway/` routing prefix;
2. performs a live OpenAI-compatible gateway completion preflight;
3. records secret-free model capabilities;
4. updates the Manager resource through `AgtClient`;
5. polls the Controller-generated runtime document;
6. activates the new model only between room turns.

An active `reply_stream` always finishes on its original model. The next turn
rebuilds the Agent while preserving `AgentState`.

For an unknown model, provide known context-window, maximum-output,
reasoning, and modality facts when available. Otherwise the workflow uses the
documented safe defaults. If preflight fails, no desired-state mutation occurs.
Provider or route repair belongs in the Higress console; never modify a
managed default provider from the model.
