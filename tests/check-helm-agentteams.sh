#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${ROOT_DIR}/helm/agentteams"
COMMON_ARGS=(
    --set credentials.registrationToken=test
    --set credentials.adminPassword=test
    --set credentials.llmApiKey=test
    --set credentials.githubToken=github-test
    --set gateway.publicURL=http://localhost:18080
)

render="$(mktemp)"
kind_render="$(mktemp)"
trap 'rm -f "${render}" "${kind_render}"' EXIT

helm template agentteams "${CHART}" "${COMMON_ARGS[@]}" > "${render}"

grep -q 'name: agentteams-controller' "${render}"
grep -q 'app.kubernetes.io/name: agentteams' "${render}"
grep -q 'name: AGENTTEAMS_MANAGER_RUNTIME' "${render}"
grep -q 'value: "agentscope"' "${render}"
grep -q 'name: AGENTTEAMS_MANAGER_IMAGE' "${render}"
grep -q 'name: AGENTTEAMS_WORKER_IMAGE' "${render}"
grep -q 'name: AGENTTEAMS_COPAW_WORKER_IMAGE' "${render}"
grep -q 'name: AGENTTEAMS_HERMES_WORKER_IMAGE' "${render}"
grep -q 'name: AGENTTEAMS_QWENPAW_WORKER_IMAGE' "${render}"
grep -q 'name: AGENTTEAMS_OPENHUMAN_WORKER_IMAGE' "${render}"
grep -q 'agentteams/agentteams-qwenpaw-worker:' "${render}"
grep -q 'AGENTTEAMS_MCP_GITHUB_TOKEN: "github-test"' "${render}"
! grep -q 'agentteams-manager-copaw' "${render}"

for resource in managers teams workers; do
    cmp -s \
        "${ROOT_DIR}/agentteams-controller/config/crd/${resource}.agentteams.io.yaml" \
        "${CHART}/crds/${resource}.agentteams.io.yaml"
done
grep -q 'enum: \[agentscope\]' \
    "${CHART}/crds/managers.agentteams.io.yaml"
! grep -q 'enum: \[openclaw, copaw\]' \
    "${CHART}/crds/managers.agentteams.io.yaml"
grep -q 'enum: \[openclaw, copaw, hermes, qwenpaw, openhuman\]' \
    "${CHART}/crds/workers.agentteams.io.yaml"

if helm template agentteams "${CHART}" "${COMMON_ARGS[@]}" \
    --set manager.runtime=openclaw >/dev/null 2>&1; then
    echo "FAIL: Helm accepted a non-AgentScope Manager runtime" >&2
    exit 1
fi

helm template agentteams "${CHART}" "${COMMON_ARGS[@]}" \
    -f "${CHART}/values-kind.yaml" > "${kind_render}"
grep -q 'type: NodePort' "${kind_render}"
grep -q 'nodePort: 30080' "${kind_render}"
grep -Fq -- '-f "${CHART_DIR}/values-kind.yaml"' \
    "${ROOT_DIR}/hack/local-k8s-up.sh"
for image in console higress pilot gateway proxyv2; do
    grep -Fq "higress/${image}:2.2.1" "${ROOT_DIR}/hack/local-k8s-up.sh"
done

for doc in \
    docs/declarative-resource-management.md \
    docs/k8s-native-agent-orch.md \
    docs/zh-cn/declarative-resource-management.md \
    docs/zh-cn/k8s-native-agent-orch.md; do
    grep -Fq 'metadata.labels.agentteams.io/controller' "${ROOT_DIR}/${doc}"
done

echo "PASS: AgentTeams Helm release renders an AgentScope-only Manager"
