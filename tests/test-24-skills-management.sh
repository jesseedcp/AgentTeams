#!/bin/bash
# test-24-skills-management.sh - Case 24: Worker skills round-trip via CLI
#
# Verifies the `--skills` flag on `agt create worker` and
# `agt update worker` flows through the controller and is reflected in
# the Controller Worker resource, and that the corresponding skill files
# land in agents/<name>/skills/.
#
# Built-in baseline skills (file-sync, etc.) are always pushed for every
# Worker regardless of --skills; the flag controls *on-demand* skills
# 测试意图：验证 CLI 的 Skill 声明经过 Controller 后，既反映在资源状态，也真正同步到 Worker 文件系统。
# 通过标准包括内置 Skill 始终保留、用户 Skill 可增删且两层状态一致，防止只更新 CR 或只写文件。
# 该测试不用 LLM，因为目标是确定性的配置传播，而不是模型是否会主动选择 Skill。
# pulled from manager/agent/worker-skills/. This test exercises the
# on-demand path with github-operations → git-delegation.
#
# This is a controller-cr style test — no LLM required.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"
source "${SCRIPT_DIR}/lib/minio-client.sh"

test_setup "24-skills-management"

TEST_WORKER="test-skl-$$"
STORAGE_PREFIX="${STORAGE_PREFIX:-${TEST_STORAGE_PREFIX:-agentteams/agentteams-storage}}"
TEST_WORKER_RUNTIME="${AGENTTEAMS_DEFAULT_WORKER_RUNTIME:-openclaw}"
if [ "${TEST_WORKER_RUNTIME}" = "copaw" ] || [ "${TEST_WORKER_RUNTIME}" = "qwenpaw" ]; then
    BASELINE_SKILL="file-sharing"
else
    BASELINE_SKILL="file-sync"
fi

_cleanup() {
    log_info "Cleaning up: ${TEST_WORKER}"
    exec_in_agent agt delete worker "${TEST_WORKER}" 2>/dev/null || true
    sleep 5
    remove_worker_container "${TEST_WORKER}"
    exec_in_manager mc rm -r --force "${STORAGE_PREFIX}/agents/${TEST_WORKER}/" 2>/dev/null || true
    exec_in_manager mc rm "${STORAGE_PREFIX}/agentteams-config/workers/${TEST_WORKER}.yaml" 2>/dev/null || true
}
trap _cleanup EXIT

minio_setup

# Helper: read desired skills from the Controller-owned Worker resource.
_worker_skills_from_controller() {
    local worker="$1"
    exec_in_agent agt get workers "${worker}" -o json 2>/dev/null \
        | jq -c '.skills // empty' 2>/dev/null
}

# ============================================================
# Section 1: Create worker with --skills github-operations
# ============================================================
log_section "Create Worker with --skills github-operations"

CREATE_OUTPUT=$(exec_in_agent agt create worker --name "${TEST_WORKER}" \
    --skills github-operations --no-wait 2>&1)
CREATE_EXIT=$?
if [ "${CREATE_EXIT}" -eq 0 ]; then
    log_pass "agt create worker --skills github-operations accepted"
else
    log_fail "agt create failed: ${CREATE_OUTPUT}"
    test_teardown "24-skills-management"; test_summary; exit 1
fi

if wait_worker_provisioned "${TEST_WORKER}" 180; then
    log_pass "Worker provisioned"
else
    log_fail "Worker did not reach provisioned state"
    test_teardown "24-skills-management"; test_summary; exit 1
fi

# ============================================================
# Section 2: Controller desired state and published skills
# ============================================================
log_section "Verify Controller State After Create"

DEADLINE=$(( $(date +%s) + 60 ))
INITIAL_SKILLS=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
    INITIAL_SKILLS=$(_worker_skills_from_controller "${TEST_WORKER}")
    [ -n "${INITIAL_SKILLS}" ] && [ "${INITIAL_SKILLS}" != "null" ] && break
    sleep 5
done

log_info "Initial skills in Controller: ${INITIAL_SKILLS}"
if echo "${INITIAL_SKILLS}" | jq -e 'index("github-operations")' >/dev/null 2>&1; then
    log_pass "Controller contains 'github-operations' for ${TEST_WORKER}"
else
    log_fail "Controller missing 'github-operations' (got: ${INITIAL_SKILLS})"
fi

if [ "${TEST_WORKER_RUNTIME}" = "qwenpaw" ]; then
    if wait_qwenpaw_api_matches "${TEST_WORKER}" /api/skills ".[] | select(.name == \"${BASELINE_SKILL}\" and .source == \"plugin:teamharness\")" 240; then
        log_pass "QwenPaw plugin skills reconciled"
    else
        log_fail "QwenPaw plugin skills did not reconcile"
    fi
fi

# Built-in baseline skill should be present in the runtime that consumes it.
if [ "${TEST_WORKER_RUNTIME}" = "qwenpaw" ]; then
    QWENPAW_SKILLS=$(read_qwenpaw_skills "${TEST_WORKER}")
    if echo "${QWENPAW_SKILLS}" | jq -e --arg skill "${BASELINE_SKILL}" \
        '.[] | select(.name == $skill and .source == "plugin:teamharness")' >/dev/null 2>&1; then
        log_pass "Built-in plugin skill '${BASELINE_SKILL}' visible through QwenPaw API"
    else
        log_fail "Built-in plugin skill '${BASELINE_SKILL}' missing from QwenPaw API"
    fi
elif minio_file_exists "agents/${TEST_WORKER}/skills/${BASELINE_SKILL}/SKILL.md"; then
    log_pass "Built-in skill '${BASELINE_SKILL}' present in MinIO for ${TEST_WORKER_RUNTIME} runtime"
else
    log_fail "Built-in skill '${BASELINE_SKILL}' missing in MinIO for ${TEST_WORKER_RUNTIME} runtime"
fi

if minio_file_exists "agents/${TEST_WORKER}/skills/github-operations/SKILL.md"; then
    log_pass "On-demand skill 'github-operations' present in MinIO"
else
    log_fail "On-demand skill 'github-operations' missing in MinIO"
fi

if wait_for_worker_container "${TEST_WORKER}" 180; then
    log_pass "Worker container running before skills update"
else
    log_fail "Worker container not running before skills update"
fi
PRE_UPDATE_CONTAINER_ID=$(docker inspect --format '{{.Id}}' "$(worker_container_name "${TEST_WORKER}")" 2>/dev/null | head -c 12 || echo "")

# ============================================================
# Section 3: Update skills via `agt update worker --skills`
# ============================================================
log_section "Update Skills (github-operations → git-delegation)"

UPDATE_OUTPUT=$(exec_in_agent agt update worker --name "${TEST_WORKER}" \
    --skills git-delegation 2>&1)
UPDATE_EXIT=$?
if [ "${UPDATE_EXIT}" -eq 0 ]; then
    log_pass "agt update worker --skills git-delegation accepted"
else
    log_fail "agt update failed (exit=${UPDATE_EXIT}): ${UPDATE_OUTPUT}"
fi

# Wait for the Controller to persist the replacement desired state.
log_info "Waiting for Controller state to reflect skill change..."
DEADLINE=$(( $(date +%s) + 120 ))
UPDATED_SKILLS=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
    UPDATED_SKILLS=$(_worker_skills_from_controller "${TEST_WORKER}")
    if echo "${UPDATED_SKILLS}" | jq -e 'index("git-delegation")' >/dev/null 2>&1 \
        && ! echo "${UPDATED_SKILLS}" | jq -e 'index("github-operations")' >/dev/null 2>&1; then
        break
    fi
    sleep 5
done

log_info "Updated skills in Controller: ${UPDATED_SKILLS}"

# Resource updates are visible before reconciliation necessarily finishes.
SKILL_RECONCILED=false
DEADLINE=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
    if minio_file_exists "agents/${TEST_WORKER}/skills/git-delegation/SKILL.md" \
        && ! minio_file_exists "agents/${TEST_WORKER}/skills/github-operations/SKILL.md"; then
        SKILL_RECONCILED=true
        break
    fi
    sleep 5
done

# ============================================================
# Section 4: Verify post-update state
# ============================================================
log_section "Verify Controller State After Update"

if echo "${UPDATED_SKILLS}" | jq -e 'index("git-delegation")' >/dev/null 2>&1; then
    log_pass "Controller contains 'git-delegation' after update"
else
    log_fail "Controller missing 'git-delegation' after update (got: ${UPDATED_SKILLS})"
fi

if echo "${UPDATED_SKILLS}" | jq -e 'index("github-operations")' >/dev/null 2>&1; then
    log_fail "Controller still contains 'github-operations' after replacement update"
else
    log_pass "Replaced skill 'github-operations' no longer in Controller state"
fi

if [ "${SKILL_RECONCILED}" = "true" ]; then
    log_pass "Controller replaced the published on-demand skill in MinIO"
else
    log_fail "Published skill state did not converge to only 'git-delegation'"
fi

# Worker container should still be running (skills update must not crash it).
# Wait here so a slow initial start is not mistaken for an update regression.
if wait_for_worker_container "${TEST_WORKER}" 120; then
    log_pass "Worker container still running after skills update"
    POST_UPDATE_CONTAINER_ID=$(docker inspect --format '{{.Id}}' "$(worker_container_name "${TEST_WORKER}")" 2>/dev/null | head -c 12 || echo "")
    if [ -n "${PRE_UPDATE_CONTAINER_ID}" ] && [ "${POST_UPDATE_CONTAINER_ID}" = "${PRE_UPDATE_CONTAINER_ID}" ]; then
        log_pass "Worker container survived skills update without recreation"
    else
        log_info "Worker container id changed during skills update (before: ${PRE_UPDATE_CONTAINER_ID}, after: ${POST_UPDATE_CONTAINER_ID})"
    fi
else
    log_fail "Worker container disappeared after skills update"
fi

# ============================================================
# Summary
# ============================================================
test_teardown "24-skills-management"
test_summary
