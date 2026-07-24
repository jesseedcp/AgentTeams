---
name: mcporter
description: Compatibility guidance for discovering and calling configured MCP tools through AgentScope-native clients.
---

# AgentScope-Native MCP Discovery and Calls

The skill name is retained for behavioral compatibility only. There is no
`mcporter` executable, sidecar configuration, or command path in this Manager.

AgentScope `MCPClient` discovers each configured server and contributes its
tools to the room's `Toolkit`. Tool names are collision-proofed as:

```text
mcp__<server>__<tool>
```

## Discovery

Inspect the AgentScope Toolkit schemas available in the current room. An Admin
Room may use Manager-authorized MCPs. Worker, Leader, Team, Human, and Project
rooms receive only MCP descriptors allowed by immutable room policy.

## Calls

Call the namespaced AgentScope tool with structured JSON matching its schema.
Do not run a binary or read/write a sidecar JSON file. AgentScope supplies the
gateway authorization header from process secrets; runtime documents remain
secret-free.

Use read-only calls for configuration verification. For sustained work,
delegate to an authorized Worker so task ownership and artifacts remain
durable.
