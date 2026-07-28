---
name: higress-gateway-management
description: Manage typed Higress LLM providers, AI routes, and consumers through audited AgentScope tools.
---

# Higress Gateway Management

Use this skill only in the Manager Admin DM. The Manager exposes a constrained
gateway surface; it does not expose arbitrary Console URLs, HTTP methods, curl,
or request bodies.

## Resource relationship

Configure resources in this order:

1. A **provider** stores an LLM endpoint configuration and one or more API
   tokens.
2. An **AI route** selects one or more providers by domain, path, and optional
   model predicates.
3. A **consumer** owns gateway credentials. A route's authentication policy
   lists the consumers that may call it.

Deleting should normally happen in reverse order. Before removing a provider,
use `list_gateway_resources` for routes and verify that no route still names
the provider.

## Typed tools

- `list_gateway_resources` lists `provider`, `route`, or `consumer` resources.
- `get_gateway_resource` reads one resource without returning credential
  values.
- `upsert_gateway_resource` creates or replaces one typed resource and
  requires Admin confirmation.
- `delete_gateway_resource` removes one resource and requires Admin
  confirmation.

Provider tokens and consumer credential values must be supplied as environment
references such as `env:DEEPSEEK_API_TOKEN`. Never place the secret value in a
chat message or tool argument. Returned provider state includes only
`token_count`; returned consumer state includes only credential type, source,
key name, and `value_count`.

Read [resources.md](references/resources.md) before creating or changing a
resource.

## Safety

- Keep route upstream weights at a total of 100.
- Use model predicates when more than one route serves the same domain.
- Keep authentication enabled and retain the `manager` consumer unless the
  Admin explicitly approves another design.
- Treat provider replacement, credential rotation, route replacement, and
  deletion as risky. Review the complete typed input before confirming.
- If recovery reports that a secret mutation needs attention, resubmit the
  desired resource with a new secret reference. The Manager deliberately does
  not persist credential values in SQLite or the operation journal.
