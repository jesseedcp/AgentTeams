#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALLER="${ROOT_DIR}/install/agentteams-install.sh"
VERIFY="${ROOT_DIR}/install/agentteams-verify.sh"
MAKEFILE="${ROOT_DIR}/Makefile"

assert_contains() {
    local file="$1"
    local pattern="$2"
    grep -Fq -- "${pattern}" "${file}" || {
        echo "FAIL: ${file#${ROOT_DIR}/} is missing: ${pattern}" >&2
        exit 1
    }
}

assert_absent() {
    local file="$1"
    local pattern="$2"
    if grep -Fq -- "${pattern}" "${file}"; then
        echo "FAIL: ${file#${ROOT_DIR}/} still contains: ${pattern}" >&2
        exit 1
    fi
}

assert_contains "${INSTALLER}" 'AGENTTEAMS_MANAGER_RUNTIME=agentscope'
assert_contains "${INSTALLER}" 'http://127.0.0.1:18799/readyz'
assert_contains "${INSTALLER}" 'QWENPAW_WORKER_IMAGE'
assert_absent "${INSTALLER}" 'OPEN''HUMAN_WORKER_IMAGE'
assert_contains "${INSTALLER}" '${_ctrl_env_prefix}CINNY_PUBLIC_URL'
assert_contains "${VERIFY}" 'http://127.0.0.1:18799/readyz'

for legacy in \
    MANAGER_COPAW_IMAGE \
    AGENTTEAMS_INSTALL_MANAGER_COPAW_IMAGE \
    step_manager_runtime \
    AGENTTEAMS_FORCE_LEGACY \
    'openclaw gateway health'
do
    assert_absent "${INSTALLER}" "${legacy}"
done

for legacy in \
    MANAGER_COPAW_IMAGE \
    LOCAL_MANAGER_COPAW \
    build-manager-copaw \
    push-manager-copaw \
    push-native-manager-copaw
do
    assert_absent "${MAKEFILE}" "${legacy}"
done

assert_contains "${MAKEFILE}" \
    'build: build-manager build-worker build-copaw-worker build-hermes-worker build-qwenpaw-worker build-agentteams-controller'
assert_contains "${MAKEFILE}" \
    'push: push-manager push-worker push-copaw-worker push-hermes-worker push-qwenpaw-worker push-agentteams-controller push-embedded'

echo "PASS: Bash installer and Makefile expose one AgentScope Manager and four Worker runtimes"
