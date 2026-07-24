#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${ROOT_DIR}/helm/agentteams"
COMMON_ARGS=(
    --set credentials.registrationToken=test
    --set credentials.adminPassword=test
    --set credentials.llmApiKey=test
    --set gateway.publicURL=http://localhost:18080
)

render="$(mktemp)"
trap 'rm -f "${render}"' EXIT

helm template agentteams "${CHART}" "${COMMON_ARGS[@]}" > "${render}"

grep -q 'name: agentteams-controller' "${render}"
grep -q 'app.kubernetes.io/name: agentteams' "${render}"
grep -q 'name: AGENTTEAMS_MANAGER_RUNTIME' "${render}"
grep -q 'value: "agentscope"' "${render}"
grep -q 'name: AGENTTEAMS_MANAGER_IMAGE' "${render}"
! grep -q 'agentteams-manager-copaw' "${render}"

cmp -s \
    "${ROOT_DIR}/agentteams-controller/config/crd/managers.agentteams.io.yaml" \
    "${CHART}/crds/managers.agentteams.io.yaml"
grep -q 'enum: \[agentscope\]' \
    "${CHART}/crds/managers.agentteams.io.yaml"
! grep -q 'enum: \[openclaw, copaw\]' \
    "${CHART}/crds/managers.agentteams.io.yaml"

if helm template agentteams "${CHART}" "${COMMON_ARGS[@]}" \
    --set manager.runtime=openclaw >/dev/null 2>&1; then
    echo "FAIL: Helm accepted a non-AgentScope Manager runtime" >&2
    exit 1
fi

echo "PASS: AgentTeams Helm release renders an AgentScope-only Manager"
