# AgentScope Manager Image

`manager/Dockerfile` builds the single AgentTeams Manager image:
`agentteams/agentteams-manager`. It embeds AgentScope 2.0, the typed `agt`
client, Matrix E2EE support, the 16 retained Manager skills, and the four
image-owned prompt documents.

The image does not run infrastructure. Higress, Matrix, MinIO, Cinny, and
the Controller remain separate Controller-stack services. OpenClaw, CoPaw,
Hermes, QwenPaw, and OpenHuman remain supported Worker runtimes; none is a
Manager runtime.

## Build

Build the Controller image first because the Manager copies its `agt` binary:

```sh
docker build -t agentteams/agentteams-controller:latest \
  -f agentteams-controller/Dockerfile agentteams-controller
docker build \
  --build-arg AGENTTEAMS_CONTROLLER_IMAGE=agentteams/agentteams-controller:latest \
  -t agentteams/agentteams-manager:latest \
  -f manager/Dockerfile .
```

## Process contract

The default entrypoint validates required Controller-provided environment
names without printing their values, prepares private SQLite/media/E2EE
directories, and executes exactly one `agentteams-manager` daemon.

Operational HTTP is available on port 18799:

- `GET /healthz`: process liveness
- `GET /readyz`: dependency readiness
- `GET /metrics`: dependency-free Prometheus text metrics

These endpoints are operational only. Humans chat with the Manager through
Matrix, normally using Cinny.
