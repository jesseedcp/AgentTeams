#!/bin/bash
# run-all-tests.sh - Integration test orchestrator
# Builds images, starts Manager, runs all test cases, reports results.
#
# Usage:
#   ./tests/run-all-tests.sh                      # Build + run all tests
#   ./tests/run-all-tests.sh --skip-build          # Use existing images
#   ./tests/run-all-tests.sh --test-filter "01 02"  # Run specific tests only
#   ./tests/run-all-tests.sh --use-existing         # Run against already-installed Manager

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ============================================================
# Configuration
# ============================================================

SKIP_BUILD=false
USE_EXISTING=false
TEST_FILTER=""
AGENTTEAMS_VERSION="${AGENTTEAMS_VERSION:-latest}"

# Test environment variables
export TEST_ADMIN_USER="${TEST_ADMIN_USER:-admin}"
export TEST_ADMIN_PASSWORD="${TEST_ADMIN_PASSWORD:-testpassword123}"
export TEST_MINIO_USER="${TEST_MINIO_USER:-${TEST_ADMIN_USER}}"
export TEST_MINIO_PASSWORD="${TEST_MINIO_PASSWORD:-${TEST_ADMIN_PASSWORD}}"
export TEST_REGISTRATION_TOKEN="${TEST_REGISTRATION_TOKEN:-test-reg-token-$(openssl rand -hex 8)}"
export TEST_MATRIX_DOMAIN="${TEST_MATRIX_DOMAIN:-matrix-local.agentteams.io:18080}"
export TEST_MANAGER_HOST="${TEST_MANAGER_HOST:-127.0.0.1}"
export AGENTTEAMS_LLM_API_KEY="${AGENTTEAMS_LLM_API_KEY:-}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build) SKIP_BUILD=true; shift ;;
        --use-existing) USE_EXISTING=true; SKIP_BUILD=true; shift ;;
        --test-filter) TEST_FILTER="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Load credentials from agentteams-manager.env into TEST_* variables
load_env_file() {
    local env_file="${AGENTTEAMS_ENV_FILE:-${HOME}/agentteams-manager.env}"
    [ -f "${env_file}" ] || env_file="${PROJECT_ROOT}/agentteams-manager.env"
    [ -f "${env_file}" ] || env_file="${HOME}/agentteams-manager.env"
    [ -f "${env_file}" ] || env_file="${PROJECT_ROOT}/agentteams-manager.env"
    if [ -f "${env_file}" ]; then
        while IFS='=' read -r key value; do
            [[ "${key}" =~ ^#.*$ || -z "${key}" ]] && continue
            key=$(echo "${key}" | xargs)
            case "${key}" in
                AGENTTEAMS_ADMIN_USER)          export TEST_ADMIN_USER="${value}" ;;
                AGENTTEAMS_ADMIN_PASSWORD)      export TEST_ADMIN_PASSWORD="${value}" ;;
                AGENTTEAMS_MINIO_USER)          export TEST_MINIO_USER="${value}" ;;
                AGENTTEAMS_MINIO_PASSWORD)      export TEST_MINIO_PASSWORD="${value}" ;;
                AGENTTEAMS_REGISTRATION_TOKEN)  export TEST_REGISTRATION_TOKEN="${value}" ;;
                AGENTTEAMS_MATRIX_DOMAIN)       export TEST_MATRIX_DOMAIN="${value}" ;;
                AGENTTEAMS_LLM_API_KEY)         [ -z "${AGENTTEAMS_LLM_API_KEY}" ] && export AGENTTEAMS_LLM_API_KEY="${value}" ;;
                AGENTTEAMS_PORT_GATEWAY)        export TEST_GATEWAY_PORT="${value}" ;;
                AGENTTEAMS_PORT_CONSOLE)        export TEST_CONSOLE_PORT="${value}" ;;
            esac
        done < "${env_file}"
    fi
    export TEST_CONTROLLER_CONTAINER="${TEST_CONTROLLER_CONTAINER:-$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^agentteams-controller$' | head -1 || true)}"
    export TEST_CONTROLLER_CONTAINER="${TEST_CONTROLLER_CONTAINER:-agentteams-controller}"
    export TEST_AGENT_CONTAINER="${TEST_AGENT_CONTAINER:-$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^agentteams-manager(-|$)' | head -1 || true)}"
    export TEST_AGENT_CONTAINER="${TEST_AGENT_CONTAINER:-${TEST_CONTROLLER_CONTAINER}}"
}

if [ "${USE_EXISTING}" = true ]; then
    load_env_file
fi

# ============================================================
# Utilities
# ============================================================

log() {
    echo -e "\033[36m[ORCHESTRATOR]\033[0m $1"
}

error() {
    echo -e "\033[31m[ORCHESTRATOR ERROR]\033[0m $1" >&2
}

_filter_has_test() {
    local filter="$1"
    local test_num="$2"

    printf '%s\n' "${filter}" | grep -qw -- "${test_num}"
}

_expand_controller_cr_filter_for_ci() {
    local filter="$1"
    local expanded="${filter}"
    local required_old_shard="15 17 18 19 20 100"
    local new_controller_tests="22 23 24 25"
    local test_num

    for test_num in ${required_old_shard}; do
        if ! _filter_has_test "${filter}" "${test_num}"; then
            printf '%s\n' "${filter}"
            return 0
        fi
    done

    for test_num in ${new_controller_tests}; do
        if ls "${SCRIPT_DIR}"/test-"${test_num}"-*.sh >/dev/null 2>&1 \
            && ! _filter_has_test "${expanded}" "${test_num}"; then
            expanded="${expanded} ${test_num}"
        fi
    done

    printf '%s\n' "${expanded}"
}

# pull_request_target runs the workflow definition from the base branch, so a PR
# that only updates SHARD_C_TESTS would not exercise newly added tests until
# after merge. The checked-out test runner is from the PR HEAD, so expand the
# legacy controller shard here as a compatibility bridge for CI.
if [ "${GITHUB_ACTIONS:-}" = "true" ] && [ -n "${TEST_FILTER}" ]; then
    EXPANDED_TEST_FILTER="$(_expand_controller_cr_filter_for_ci "${TEST_FILTER}")"
    if [ "${EXPANDED_TEST_FILTER}" != "${TEST_FILTER}" ]; then
        log "Expanded controller-cr test filter: ${TEST_FILTER} -> ${EXPANDED_TEST_FILTER}"
        TEST_FILTER="${EXPANDED_TEST_FILTER}"
    fi
fi

cleanup() {
    if [ "${USE_EXISTING}" = true ]; then
        log "Using existing installation — skipping container cleanup"
        # Still clean up test worker containers
        for c in $(docker ps -a --filter "name=agentteams-test-worker-" --format '{{.Names}}' 2>/dev/null); do
            docker rm -f "$c" 2>/dev/null || true
        done
        return
    fi

    log "Cleaning up..."
    docker stop agentteams-controller 2>/dev/null || true
    docker rm agentteams-controller 2>/dev/null || true
    docker stop agentteams-manager 2>/dev/null || true
    docker rm agentteams-manager 2>/dev/null || true
    # Cleanup worker containers
    for c in $(docker ps -a --filter "name=agentteams-test-worker-" --format '{{.Names}}' 2>/dev/null); do
        docker rm -f "$c" 2>/dev/null || true
    done

    log "Cleanup complete"
}

trap cleanup EXIT

# ============================================================
# Step 1: Build images
# ============================================================

if [ "${SKIP_BUILD}" = false ]; then
    log "Building images via Makefile..."
    make -C "${PROJECT_ROOT}" build VERSION="${AGENTTEAMS_VERSION}"
    log "Images built successfully"
else
    log "Skipping image build (--skip-build)"
fi

# ============================================================
# Step 2: Start Manager container (skip if --use-existing)
# ============================================================

if [ "${USE_EXISTING}" = true ]; then
    log "Using existing Manager installation (--use-existing)"
    log "  Admin user: ${TEST_ADMIN_USER}"
    log "  Matrix domain: ${TEST_MATRIX_DOMAIN}"
    log "  Manager host: ${TEST_MANAGER_HOST}"

    # Verify the Manager is actually running (Matrix is not exposed; check via docker exec)
    if ! docker exec "${TEST_CONTROLLER_CONTAINER}" curl -sf "http://127.0.0.1:6167/_matrix/client/versions" > /dev/null 2>&1; then
        error "Manager does not appear to be running (container: ${TEST_CONTROLLER_CONTAINER}). Start it with 'make install' first."
    fi
    log "Manager is reachable"
else
    log "Installing Manager via install script..."

    # Clean up any existing installation, then install fresh using agentteams-install.sh.
    # This ensures ports, domains, and all initialization (Higress routes, Matrix users)
    # match exactly what users get in production.
    make -C "${PROJECT_ROOT}" uninstall 2>/dev/null || true
    AGENTTEAMS_NON_INTERACTIVE=1 AGENTTEAMS_YOLO=1 AGENTTEAMS_MOUNT_SOCKET=1 \
        AGENTTEAMS_INSTALL_MANAGER_IMAGE="agentteams/manager:${AGENTTEAMS_VERSION}" \
        AGENTTEAMS_INSTALL_WORKER_IMAGE="agentteams/worker-agent:${AGENTTEAMS_VERSION}" \
        make -C "${PROJECT_ROOT}" install SKIP_BUILD=1

    # ============================================================
    # Step 3: Wait for Manager to be healthy (via make wait-ready)
    # ============================================================

    make -C "${PROJECT_ROOT}" wait-ready

    # Load all configuration from the env file generated by the install script
    load_env_file
    log "  Admin user:     ${TEST_ADMIN_USER}"
    log "  Matrix domain:  ${TEST_MATRIX_DOMAIN}"
    log "  Gateway port:   ${TEST_GATEWAY_PORT}"
    log "  Console port:   ${TEST_CONSOLE_PORT}"
fi

# ============================================================
# Step 3.5: Verify the declared AgentScope Manager runtime
# ============================================================
source "${SCRIPT_DIR}/lib/matrix-client.sh"
source "${SCRIPT_DIR}/lib/agent-metrics.sh"

if ! docker exec "${TEST_AGENT_CONTAINER}" python -c \
    'import urllib.request; urllib.request.urlopen("http://127.0.0.1:18799/readyz", timeout=2).read()' \
    >/dev/null 2>&1; then
    error "AgentScope Manager readiness endpoint is unavailable in ${TEST_AGENT_CONTAINER}"
    exit 1
fi
log "AgentScope Manager declared configuration is active"

# ============================================================
# Step 4: Run test cases
# ============================================================

log "Running integration tests..."
echo ""

TOTAL_PASS=0
TOTAL_FAIL=0
RESULTS=()

# Determine which tests to run
TESTS=()
for test_file in "${SCRIPT_DIR}"/test-*.sh; do
    test_num=$(basename "${test_file}" | grep -o '[0-9]\+')
    if [ -n "${TEST_FILTER}" ]; then
        if echo "${TEST_FILTER}" | grep -qw "${test_num}"; then
            TESTS+=("${test_file}")
        fi
    else
        TESTS+=("${test_file}")
    fi
done

for test_file in "${TESTS[@]}"; do
    test_name=$(basename "${test_file}" .sh)
    log "Running: ${test_name}"

    # Wait for Manager to finish processing previous test before starting next
    wait_for_session_stable 10 120

    if bash "${test_file}"; then
        RESULTS+=("PASS: ${test_name}")
        TOTAL_PASS=$((TOTAL_PASS + 1))
    else
        RESULTS+=("FAIL: ${test_name}")
        TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi

    echo ""
done

# ============================================================
# Step 5: Report results
# ============================================================

echo ""
echo "========================================"
echo "  Integration Test Results"
echo "========================================"
echo "  Total:  $((TOTAL_PASS + TOTAL_FAIL))"
echo -e "  \033[32mPassed: ${TOTAL_PASS}\033[0m"
echo -e "  \033[31mFailed: ${TOTAL_FAIL}\033[0m"
echo "========================================"
echo ""

for result in "${RESULTS[@]}"; do
    if [[ "${result}" == PASS* ]]; then
        echo -e "  \033[32m${result}\033[0m"
    else
        echo -e "  \033[31m${result}\033[0m"
    fi
done

echo ""

if [ "${TOTAL_FAIL}" -gt 0 ]; then
    error "${TOTAL_FAIL} test(s) failed"
    exit 1
fi

log "All tests passed!"
exit 0
