# Create or Update a REST-to-MCP Server

Use `configure_mcp` with action `upsert` after Admin confirmation.

Provide:

- a DNS-label logical name without an `mcp-` prefix;
- a non-secret description;
- a YAML template containing exactly one `accessToken: ""` slot;
- a secret reference, never the secret value;
- a typed DNS service source with domain, port, and HTTP(S) protocol;
- selected Worker names;
- one read-only AgentScope tool name and safe verification arguments.

The workflow inserts the resolved secret only in its in-memory Higress request.
The generated Controller descriptor points at the Higress gateway and contains
no upstream credential.

Success is not the Console upsert response. Success requires:

1. complete consumer replacement;
2. Manager and Worker descriptor convergence;
3. runtime-document activation at a valid revision;
4. AgentScope tool discovery;
5. a real read-only tool call;
6. selected-Worker notifications after the call.

Credential rotation uses the same action and secret reference. If the gateway
effect is ambiguous, the recovery pass first lists Higress state. When no
definition exists and the secret is no longer available to the operation,
recovery reports needs-attention instead of inventing success.

The retained GitHub template is
`references/mcp-github.yaml`. Other integrations may supply a validated custom
template as described in `custom-yaml-guide.md`.
