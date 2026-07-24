# Custom REST-to-MCP Template Guide

Generate declarative YAML only; deployment always goes through
`configure_mcp`.

## Required Shape

```yaml
server:
  name: weather-mcp-server
  config:
    accessToken: ""
tools:
  - name: get_weather
    description: Get current weather for a city
    args:
      - name: city
        type: string
        required: true
    requestTemplate:
      url: "https://api.weather.example/v1/weather?q={{.args.city}}"
      method: GET
      headers:
        - key: X-API-Key
          value: "{{.config.accessToken}}"
```

Rules:

- Exactly one empty `accessToken` slot is allowed.
- Never place a real credential in YAML.
- Tool names use snake_case and descriptions state observable behavior.
- Arguments use explicit types and mark only genuine requirements.
- Use the simplest supported request mapping.
- Literal upstream URLs must use HTTPS for external services.
- Optional response templates should reduce noisy output, not hide errors.

When requesting the upsert, pass the environment/mounted-secret reference,
service domain, port, protocol, Worker grants, and a safe read-only
verification call. The typed adapter safely quotes the resolved secret in
memory and rejects malformed templates before Higress mutation.
