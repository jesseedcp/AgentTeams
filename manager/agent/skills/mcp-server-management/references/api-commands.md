# Typed MCP Administration

Higress Console remains authoritative for MCP definitions and consumers, but
only the Manager's typed workflow may call it. Do not emit direct HTTP
administration instructions.

## List

Use `list_mcp_servers`. Results contain logical server names and complete
consumer sets, never raw configuration or credentials.

## Grant

Use `configure_mcp` with action `grant`, the logical server name, and selected
Worker names. The workflow reads the current consumers and sends one complete
replacement containing `manager`, existing unaffected Workers, and the newly
selected Workers. It then replaces those Workers' Controller descriptors.

## Revoke

Use `configure_mcp` with action `revoke`. The workflow removes only the
selected `worker-<name>` consumers, retains `manager` and unaffected
consumers, and removes the matching Worker descriptors.

## Delete

Use `remove_mcp`. It removes Manager and affected Worker descriptors through
`AgtClient`, then deletes the Higress definition. A retry reconciles observed
facts instead of assuming the prior response described the final state.

The Console adapter owns these routes internally:

- service-source registration;
- MCP definition list/upsert/delete;
- consumer list and complete replacement.

They are implementation boundaries, not model-callable tools.
