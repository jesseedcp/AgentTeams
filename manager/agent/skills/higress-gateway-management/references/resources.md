# Supported Higress resources

## Provider

Required fields are `name`, `provider_type`, `protocol`, and `token_refs`.
Supported provider types follow the installed Higress Console contract.
`protocol` is `openai/v1` or `original`.

`raw_configs` is limited to JSON data and rejects credential-like keys.
Provider-specific non-secret values, for example a compatible API URL or Qwen
feature flag, may be placed there. API tokens belong only in `token_refs`.

Example typed resource:

```json
{
  "kind": "provider",
  "name": "deepseek",
  "provider_type": "deepseek",
  "protocol": "openai/v1",
  "token_refs": ["env:DEEPSEEK_API_TOKEN"]
}
```

## AI route

Required fields are `name`, `domains`, and `upstreams`. Predicates use
`EXACT`, `PRE`, or `REGEX`. Each upstream names an existing provider, and all
weights must total 100.

Example:

```json
{
  "kind": "route",
  "name": "deepseek-route",
  "domains": ["aigw-local.agentteams.io"],
  "upstreams": [
    {"provider": "deepseek", "weight": 100, "model_mapping": {}}
  ],
  "model_predicates": [
    {
      "match_type": "PRE",
      "match_value": "deepseek",
      "case_sensitive": false
    }
  ],
  "auth": {
    "enabled": true,
    "allowed_credential_types": ["key-auth"],
    "allowed_consumers": ["manager"]
  }
}
```

## Consumer

Only Higress `key-auth` credentials are supported. `BEARER` has no key name;
`HEADER` and `QUERY` require one. Values are environment references.

```json
{
  "kind": "consumer",
  "name": "worker-alice",
  "credentials": [
    {
      "source": "BEARER",
      "value_refs": ["env:WORKER_ALICE_GATEWAY_KEY"]
    }
  ]
}
```

The Manager tool uses these objects under the `resource` field of
`upsert_gateway_resource`.
