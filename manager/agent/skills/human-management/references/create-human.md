# Human Resource Contract

## Create

Call `create_human`:

```json
{
  "name": "john",
  "display_name": "John Doe",
  "email": "john@example.org",
  "permission_level": 2,
  "accessible_teams": ["alpha"],
  "accessible_workers": ["standalone-dev"],
  "note": "Product reviewer"
}
```

The resource name becomes the default Matrix localpart. The typed receipt is
returned only after a Matrix identity exists, the Human phase is active, and
topology has refreshed.

Use `get_human` for one resource or `list_humans` for the catalog. Receipts
include declared scope and current rooms but omit one-time credentials.

## Update

Call `update_human` with the name and only changed fields:

```json
{
  "name": "john",
  "permission_level": 3,
  "accessible_teams": [],
  "accessible_workers": ["standalone-dev"],
  "note": "External reviewer"
}
```

An empty list explicitly clears that scope. The workflow proves the new
Controller fields and refreshes Matrix policy before returning.

## Delete

Call `delete_human`:

```json
{"name":"john"}
```

Deletion removes AgentTeams access after Controller proves absence. Matrix
homeservers may retain the underlying account; do not claim that the account
itself was erased.
