#!/bin/bash
# test-23-runtime-switch.sh - Worker runtime switch and Manager hot reload
#
# Verifies the controller recreates a worker's container with the new
# runtime image when spec.runtime changes, while preserving identity
# (Matrix roomID, Higress consumer name) and user data in MinIO.
#
# The flow exercises:
#   1. create worker runtime=openclaw → container image is openclaw
#   2. write sentinel file to MinIO agents/<name>/
# 初学者提示：runtime 切换需要重建容器，但 Matrix 身份、房间和 MinIO 用户数据必须保持不变。
# 通过标准同时观察“应该变化的镜像”和“不应变化的身份/哨兵文件”，只验证新容器启动会漏掉数据丢失。
# 等待 Controller 完成重建属于最终一致性检查，不能在旧容器刚停止时就判定失败。
#   3. apply worker runtime=copaw → SpecChanged triggers recreate
#   4. new container image is copaw; sentinel preserved; consumer unchanged
#
# The final section also proves that a Manager model update is applied at a
# turn boundary without replacing the AgentScope Manager container.
# This is a controller-cr style test — no LLM required.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"
source "${SCRIPT_DIR}/lib/minio-client.sh"
source "${SCRIPT_DIR}/lib/higress-client.sh"

test_setup "23-runtime-switch"

TEST_WORKER="test-rt-$$"
STORAGE_PREFIX="${STORAGE_PREFIX:-${TEST_STORAGE_PREFIX:-agentteams/agentteams-storage}}"
ORIGINAL_MANAGER_MODEL=""
HOT_RELOAD_MODEL=""

_cleanup() {
    log_info "Cleaning up: ${TEST_WORKER}"
    exec_in_agent agt delete worker "${TEST_WORKER}" 2>/dev/null || true
    sleep 5
    remove_worker_container "${TEST_WORKER}"
    exec_in_manager mc rm -r --force "${STORAGE_PREFIX}/agents/${TEST_WORKER}/" 2>/dev/null || true
    exec_in_manager mc rm "${STORAGE_PREFIX}/agentteams-config/workers/${TEST_WORKER}.yaml" 2>/dev/null || true
    if [ -n "${ORIGINAL_MANAGER_MODEL}" ] && [ "${HOT_RELOAD_MODEL}" != "${ORIGINAL_MANAGER_MODEL}" ]; then
        exec_in_agent agt update manager --name default \
            --model "${ORIGINAL_MANAGER_MODEL}" >/dev/null 2>&1 || true
    fi
}
trap _cleanup EXIT

minio_setup

_get_higress_consumers_or_fail() {
    local label="$1"
    local consumers

    if ! higress_login "${TEST_ADMIN_USER}" "${TEST_ADMIN_PASSWORD}" > /dev/null 2>&1; then
        log_fail "Unable to log in to Higress before ${label}"
        return 1
    fi

    if ! consumers=$(higress_get_consumers 2>/dev/null); then
        log_fail "Unable to query Higress consumers during ${label}"
        return 1
    fi

    if ! echo "${consumers}" | jq -e '.data | type == "array"' >/dev/null 2>&1; then
        log_fail "Higress consumers response during ${label} is not valid JSON with a data array"
        return 1
    fi

    HIGRESS_CONSUMERS_JSON="${consumers}"
}

# ============================================================
# Section 1: Create worker with openclaw runtime
# ============================================================
log_section "Create Worker (runtime=openclaw)"

# apply (not create) so the second invocation can update in place
CREATE_OUTPUT=$(exec_in_agent agt apply worker --name "${TEST_WORKER}" --runtime openclaw 2>&1)
CREATE_EXIT=$?
if [ "${CREATE_EXIT}" -eq 0 ]; then
    log_pass "agt apply (openclaw) accepted"
else
    log_fail "agt apply (openclaw) failed: ${CREATE_OUTPUT}"
    test_teardown "23-runtime-switch"; test_summary; exit 1
fi

if wait_worker_provisioned "${TEST_WORKER}" 180; then
    log_pass "Worker provisioned"
else
    log_fail "Worker did not reach provisioned state"
    test_teardown "23-runtime-switch"; test_summary; exit 1
fi

if wait_for_worker_container "${TEST_WORKER}" 120; then
    log_pass "Container started under openclaw runtime"
else
    log_fail "Container did not start under openclaw"
fi

# ============================================================
# Section 2: Snapshot pre-switch state
# ============================================================
log_section "Snapshot Pre-Switch State"

OLD_CONTAINER="$(worker_container_name "${TEST_WORKER}")"
OLD_IMAGE=$(docker inspect --format '{{.Config.Image}}' "${OLD_CONTAINER}" 2>/dev/null || echo "")
OLD_CONTAINER_ID=$(docker inspect --format '{{.Id}}' "${OLD_CONTAINER}" 2>/dev/null | head -c 12 || echo "")
log_info "Pre-switch image: ${OLD_IMAGE}"
log_info "Pre-switch container ID (short): ${OLD_CONTAINER_ID}"

if echo "${OLD_IMAGE}" | grep -qi "openclaw\|worker-agent"; then
    log_pass "Pre-switch container is openclaw image"
else
    log_info "Pre-switch image label does not obviously identify openclaw (${OLD_IMAGE}); continuing"
fi

# Capture Matrix room ID and Higress consumer
OLD_ROOM_ID=$(get_worker_room_id "${TEST_WORKER}")
log_info "Pre-switch roomID: ${OLD_ROOM_ID}"

HIGRESS_CONSUMERS_JSON=""
if _get_higress_consumers_or_fail "pre-switch snapshot"; then
    OLD_CONSUMERS="${HIGRESS_CONSUMERS_JSON}"
    if echo "${OLD_CONSUMERS}" | jq -r '.data[]?.name // empty' 2>/dev/null | grep -Fxq "worker-${TEST_WORKER}"; then
        log_pass "Higress consumer present pre-switch"
    else
        log_fail "Higress consumer missing pre-switch"
    fi
fi

# Write sentinel file to MinIO (proxy for user data the controller must preserve)
exec_in_manager mc cp /etc/hostname \
    "${STORAGE_PREFIX}/agents/${TEST_WORKER}/runtime-switch-sentinel.txt" >/dev/null 2>&1 || true
if minio_file_exists "agents/${TEST_WORKER}/runtime-switch-sentinel.txt"; then
    log_pass "Sentinel file written to MinIO"
else
    log_fail "Sentinel file write failed"
fi

# ============================================================
# Section 3: Switch runtime to copaw
# ============================================================
log_section "Switch Runtime (openclaw → copaw)"

SWITCH_OUTPUT=$(exec_in_agent agt apply worker --name "${TEST_WORKER}" --runtime copaw 2>&1)
SWITCH_EXIT=$?
if [ "${SWITCH_EXIT}" -eq 0 ]; then
    log_pass "agt apply (copaw) accepted"
else
    log_fail "agt apply (copaw) failed: ${SWITCH_OUTPUT}"
fi

# Wait for the controller to recreate the container. We poll for either
# (a) the container ID changes, or (b) the image label contains "copaw".
log_info "Waiting for container recreation..."
DEADLINE=$(( $(date +%s) + 240 ))
NEW_CONTAINER_ID=""
NEW_IMAGE=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
    NEW_CONTAINER="$(worker_container_name "${TEST_WORKER}")"
    NEW_CONTAINER_ID=$(docker inspect --format '{{.Id}}' "${NEW_CONTAINER}" 2>/dev/null | head -c 12 || echo "")
    NEW_IMAGE=$(docker inspect --format '{{.Config.Image}}' "${NEW_CONTAINER}" 2>/dev/null || echo "")
    if [ -n "${NEW_CONTAINER_ID}" ] \
        && [ "${NEW_CONTAINER_ID}" != "${OLD_CONTAINER_ID}" ] \
        && [ -n "${NEW_IMAGE}" ]; then
        break
    fi
    sleep 5
done

# ============================================================
# Section 4: Verify post-switch state
# ============================================================
log_section "Verify Post-Switch State"

if [ -n "${NEW_CONTAINER_ID}" ] && [ "${NEW_CONTAINER_ID}" != "${OLD_CONTAINER_ID}" ]; then
    log_pass "Container recreated (id: ${OLD_CONTAINER_ID} → ${NEW_CONTAINER_ID})"
else
    log_fail "Container not recreated (id still ${OLD_CONTAINER_ID})"
fi

if echo "${NEW_IMAGE}" | grep -qi "copaw"; then
    log_pass "Post-switch image is copaw: ${NEW_IMAGE}"
else
    log_fail "Post-switch image does not look like copaw: ${NEW_IMAGE}"
fi

# Matrix room preserved
NEW_ROOM_ID=$(get_worker_room_id "${TEST_WORKER}")
if [ -n "${OLD_ROOM_ID}" ] && [ "${NEW_ROOM_ID}" = "${OLD_ROOM_ID}" ]; then
    log_pass "Matrix roomID preserved across runtime switch"
else
    log_fail "Matrix roomID changed (was: ${OLD_ROOM_ID}, now: ${NEW_ROOM_ID})"
fi

# Higress consumer preserved (same name)
HIGRESS_CONSUMERS_JSON=""
if _get_higress_consumers_or_fail "post-switch assertion"; then
    NEW_CONSUMERS="${HIGRESS_CONSUMERS_JSON}"
    if echo "${NEW_CONSUMERS}" | jq -r '.data[]?.name // empty' 2>/dev/null | grep -Fxq "worker-${TEST_WORKER}"; then
        log_pass "Higress consumer preserved across runtime switch"
    else
        log_fail "Higress consumer missing after runtime switch"
    fi
fi

# Sentinel preserved
if minio_file_exists "agents/${TEST_WORKER}/runtime-switch-sentinel.txt"; then
    log_pass "Sentinel file preserved across runtime switch"
else
    log_fail "Sentinel file lost during runtime switch"
fi

# openclaw.json should still exist (controller's source-of-truth config)
if minio_file_exists "agents/${TEST_WORKER}/openclaw.json"; then
    log_pass "openclaw.json present post-switch (controller-managed config)"
else
    log_fail "openclaw.json missing post-switch"
fi

# ============================================================
# Section 5: Hot-reload Manager model without container restart
# ============================================================
log_section "AgentScope Manager Model Hot Reload"

_MANAGER_CONTAINER="${TEST_AGENT_CONTAINER:-agentteams-manager}"
MANAGER_CONTAINER_ID_BEFORE=$(docker inspect --format '{{.Id}}' \
    "${_MANAGER_CONTAINER}" 2>/dev/null || true)
MANAGER_RESOURCE_BEFORE=$(exec_in_agent agt get managers default -o json 2>/dev/null || true)
ORIGINAL_MANAGER_MODEL=$(echo "${MANAGER_RESOURCE_BEFORE}" | jq -r '.model // empty')
assert_not_empty "${ORIGINAL_MANAGER_MODEL}" "Current Manager model is observable"
assert_eq "agentscope" "$(echo "${MANAGER_RESOURCE_BEFORE}" | jq -r '.runtime // empty')" \
    "Manager resource remains agentscope"

_manager_metric() {
    local metric_name="$1"
    docker exec "${_MANAGER_CONTAINER}" python -c \
        'import sys,urllib.request; n=sys.argv[1]; t=urllib.request.urlopen("http://127.0.0.1:18799/metrics",timeout=2).read().decode(); print(next((line.split()[1] for line in t.splitlines() if line.startswith(n+" ")),""))' \
        "${metric_name}" 2>/dev/null
}

RUNTIME_REVISION_BEFORE=$(_manager_metric agentteams_manager_runtime_revision)
assert_not_empty "${RUNTIME_REVISION_BEFORE}" "Manager runtime revision metric is exposed"

if [ -n "${AGENTTEAMS_HOT_RELOAD_MODEL:-}" ]; then
    HOT_RELOAD_MODEL="${AGENTTEAMS_HOT_RELOAD_MODEL}"
elif [ "${ORIGINAL_MANAGER_MODEL}" = "qwen3.6-plus" ]; then
    HOT_RELOAD_MODEL="qwen3.5-plus"
else
    HOT_RELOAD_MODEL="qwen3.6-plus"
fi

HOT_RELOAD_OUTPUT=$(exec_in_agent agt update manager --name default \
    --model "${HOT_RELOAD_MODEL}" 2>&1)
HOT_RELOAD_EXIT=$?
if [ "${HOT_RELOAD_EXIT}" -eq 0 ]; then
    log_pass "Manager model update accepted"
else
    log_fail "Manager model update failed: ${HOT_RELOAD_OUTPUT}"
fi

HOT_RELOAD_DEADLINE=$(( $(date +%s) + 180 ))
RUNTIME_REVISION_AFTER="${RUNTIME_REVISION_BEFORE}"
OBSERVED_MANAGER_MODEL=""
while [ "$(date +%s)" -lt "${HOT_RELOAD_DEADLINE}" ]; do
    OBSERVED_MANAGER_MODEL=$(exec_in_agent agt get managers default -o json 2>/dev/null | \
        jq -r '.model // empty')
    RUNTIME_REVISION_AFTER=$(_manager_metric agentteams_manager_runtime_revision)
    if [ "${OBSERVED_MANAGER_MODEL}" = "${HOT_RELOAD_MODEL}" ] && \
        awk "BEGIN {exit !(${RUNTIME_REVISION_AFTER:-0} > ${RUNTIME_REVISION_BEFORE:-0})}"; then
        break
    fi
    sleep 3
done

assert_eq "${HOT_RELOAD_MODEL}" "${OBSERVED_MANAGER_MODEL}" \
    "Controller and Manager converge on the updated model"
if awk "BEGIN {exit !(${RUNTIME_REVISION_AFTER:-0} > ${RUNTIME_REVISION_BEFORE:-0})}"; then
    log_pass "AgentScope runtime revision increased (${RUNTIME_REVISION_BEFORE} → ${RUNTIME_REVISION_AFTER})"
else
    log_fail "AgentScope runtime revision did not increase (${RUNTIME_REVISION_BEFORE} → ${RUNTIME_REVISION_AFTER})"
fi

MANAGER_CONTAINER_ID_AFTER=$(docker inspect --format '{{.Id}}' \
    "${_MANAGER_CONTAINER}" 2>/dev/null || true)
assert_eq "${MANAGER_CONTAINER_ID_BEFORE}" "${MANAGER_CONTAINER_ID_AFTER}" \
    "Manager model hot reload does not replace the container"

# ============================================================
# Summary
# ============================================================
test_teardown "23-runtime-switch"
test_summary
