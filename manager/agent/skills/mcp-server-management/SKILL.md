---
name: mcp-server-management
description: Configure REST-to-MCP or proxied MCP servers, rotate secret references, and grant or revoke Worker access through typed Manager tools.
---

# MCP Server Management

Use the Manager's typed tools. The model must never mutate Higress, Controller
resources, runtime files, or Worker files directly.

## Tools

| Intent | Tool |
|---|---|
| List secret-free server and consumer state | `list_mcp_servers` |
| Upsert a server, grant Workers, or revoke Workers | `configure_mcp` |
| Delete a server and remove its Controller descriptors | `remove_mcp` |

`configure_mcp` and `remove_mcp` require Admin Room confirmation.

## Secret Ingress

Never ask an administrator to paste an upstream token into Matrix. The
credential must already exist as an environment or mounted-secret reference
such as `AGENTTEAMS_MCP_GITHUB_TOKEN`; pass only that reference to
`configure_mcp`. The process resolves it after the AgentScope tool boundary.
The value may exist in memory while Higress is updated, but never in chat,
SQLite, MinIO journals, Controller resources, runtime documents, or Worker
manifests.

## Required Sequence

The workflow performs these steps; do not bypass them:

1. Reject local administration when `AGENTTEAMS_RUNTIME=aliyun`.
2. Validate the REST template or credential-free proxy URL.
3. Reconcile the Higress service source and MCP definition.
4. Read current consumers and replace the complete intended set.
5. Replace Manager and selected Worker descriptors through `AgtClient`.
6. Wait for a newer Controller runtime document when needed.
7. Rediscover tools with AgentScope `MCPClient`/`Toolkit`.
8. Invoke the configured read-only verification tool.
9. Notify only the selected Workers after verification succeeds.

Consumer updates are replacement operations. Always retain `manager` and every
unaffected Worker consumer.

## Configuration Rules

- REST templates contain exactly one empty `accessToken` slot.
- Proxies accept only `http` or `sse`; upstream URLs use HTTP(S), contain no
  userinfo, and carry credentials only through secret-referenced headers.
- `stdio` is not gateway-proxyable.
- Controller descriptors contain only name, gateway URL, and transport.
- A failed or ambiguous external effect is reconciled from Higress,
  Controller, runtime-document, and AgentScope facts before replay.

Read only the relevant reference:

| Situation | Reference |
|---|---|
| Upsert/rotate and verification | `references/create-update-server.md` |
| Custom REST template | `references/custom-yaml-guide.md` |
| List, grant, revoke, delete | `references/api-commands.md` |
| Proxy an existing MCP endpoint | `references/setup-mcp-proxy.md` |
| GitHub declarative tool definitions | `references/mcp-github.yaml` |
