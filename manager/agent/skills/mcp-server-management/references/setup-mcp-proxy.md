# Proxy an Existing MCP Server

Use `configure_mcp` with action `upsert` and server kind `proxy`.

Required fields:

- logical DNS-label name;
- credential-free HTTP(S) backend URL;
- transport `http` or `sse`;
- optional structured headers whose values come from secret references;
- selected Workers;
- a discovered read-only verification tool and safe arguments.

Header schemes are explicit:

| Intended header | Name | Scheme |
|---|---|---|
| Bearer token | `Authorization` | `bearer` |
| Basic credential | `Authorization` | `basic` |
| API/custom key | exact header name | `raw` |

The process resolves references after the AgentScope tool boundary and renders
the known Higress security schema from structured data. It never concatenates
user values into YAML syntax.

Reject:

- `stdio`;
- URLs with username/password userinfo;
- credentials in query strings or paths;
- raw secret values in tool input;
- notification before AgentScope discovery and a real verification call.

Cloud runtime uses the cloud AI Gateway console; local mutation stops before
any effect.
