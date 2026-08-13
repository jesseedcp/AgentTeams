#!/bin/bash
# test-27-worker-runtime-matrix.sh - Release gate for all four Worker runtimes
#
# Each runtime is created, observed through the Controller, exercised through
# its real Matrix adapter, given a canonical MinIO task, required to publish a
# result artifact, and deleted before the next runtime starts.
# 测试意图：对 openclaw、copaw、hermes、qwenpaw 使用同一套黑盒标准，防止某个 runtime 只在单元测试中“看起来兼容”。
# 通过标准是每个 runtime 都能创建、Matrix 收发、消费 MinIO 任务、写结果并彻底删除；一个失败即阻断发布。
# 逐个运行和清理可避免同名房间、端口或缓存让后一个 runtime 得到假阳性结果。

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"
source "${SCRIPT_DIR}/lib/matrix-client.sh"
source "${SCRIPT_DIR}/lib/minio-client.sh"

test_setup "27-worker-runtime-matrix"

if ! require_llm_key; then
    log_fail "Four-runtime release matrix requires AGENTTEAMS_LLM_API_KEY"
    test_teardown "27-worker-runtime-matrix"
    test_summary
    exit 1
fi

ADMIN_LOGIN=$(matrix_login "${TEST_ADMIN_USER}" "${TEST_ADMIN_PASSWORD}")
ADMIN_TOKEN=$(echo "${ADMIN_LOGIN}" | jq -r '.access_token')
assert_not_empty "${ADMIN_TOKEN}" "Admin Matrix login succeeds"
minio_setup

STORAGE_PREFIX="${STORAGE_PREFIX:-${TEST_STORAGE_PREFIX:-agentteams/agentteams-storage}}"
WORKER_MODEL="${AGENTTEAMS_DEFAULT_MODEL:-qwen3.6-plus}"
RUN_SUFFIX="$(printf '%06d' $$)"
RUNTIMES=(openclaw copaw hermes qwenpaw)
CREATED_WORKERS=()
ACTIVE_TASK=""

_cleanup_runtime_matrix() {
    local worker
    for worker in "${CREATED_WORKERS[@]}"; do
        exec_in_agent agt delete worker "${worker}" >/dev/null 2>&1 || true
        remove_worker_container "${worker}"
    done
    if [ -n "${ACTIVE_TASK}" ]; then
        exec_in_manager mc rm -r --force \
            "${STORAGE_PREFIX}/shared/tasks/${ACTIVE_TASK}/" >/dev/null 2>&1 || true
    fi
}
trap _cleanup_runtime_matrix EXIT

_runtime_image_pattern() {
    case "$1" in
        openclaw) echo 'agentteams-worker|worker-agent' ;;
        copaw) echo 'copaw-worker' ;;
        hermes) echo 'hermes-worker' ;;
        qwenpaw) echo 'qwenpaw-worker' ;;
    esac
}

_task_suffix() {
    case "$1" in
        openclaw) echo opnclw ;;
        copaw) echo copawx ;;
        hermes) echo hermes ;;
        qwenpaw) echo qwenpw ;;
    esac
}

_write_task_fixture() {
    local task_id="$1"
    local worker="$2"
    local room_id="$3"
    local marker="$4"
    local created_at meta spec
    created_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    meta=$(jq -n \
        --arg task_id "${task_id}" \
        --arg worker "${worker}" \
        --arg room_id "${room_id}" \
        --arg created_at "${created_at}" \
        '{
          schema_version: 1,
          task_id: $task_id,
          task_type: "finite",
          status: "assigned",
          title: "Worker runtime matrix proof",
          assigned_to: $worker,
          room_id: $room_id,
          project_id: null,
          schedule: null,
          timezone: null,
          last_executed_at: null,
          next_scheduled_at: null,
          last_execution_event_id: null,
          created_at: $created_at,
          completed_at: null
        }')
    spec=$(cat <<EOF
# Worker runtime matrix proof

Create \`result.md\` in this task directory containing exactly this marker:

${marker}

Push the task directory to MinIO, then reply in the current room with:
\`TASK_COMPLETED: ${task_id} - ${marker}\`
EOF
)

    printf '%s\n' "${meta}" | docker exec -i \
        "${TEST_CONTROLLER_CONTAINER:-agentteams-controller}" \
        mc pipe "${STORAGE_PREFIX}/shared/tasks/${task_id}/meta.json" \
        >/dev/null
    printf '%s\n' "${spec}" | docker exec -i \
        "${TEST_CONTROLLER_CONTAINER:-agentteams-controller}" \
        mc pipe "${STORAGE_PREFIX}/shared/tasks/${task_id}/spec.md" \
        >/dev/null
}

_wait_for_completion_message() {
    local room_id="$1"
    local worker_user="$2"
    local task_id="$3"
    local timeout="${4:-180}"
    local elapsed=0
    while [ "${elapsed}" -lt "${timeout}" ]; do
        local bodies
        bodies=$(matrix_read_messages "${ADMIN_TOKEN}" "${room_id}" 50 2>/dev/null | \
            jq -r --arg user "${worker_user}" \
            '[.chunk[]
              | select(.sender == $user)
              | select(.type == "m.room.message")
              | .content.body // ""
            ] | join("\n")' 2>/dev/null)
        if echo "${bodies}" | grep -qi "${task_id}" && \
            echo "${bodies}" | grep -qiE 'TASK_COMPLETED|completed|complete|done'; then
            printf '%s\n' "${bodies}"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 1
}

for runtime in "${RUNTIMES[@]}"; do
    log_section "Worker Runtime: ${runtime}"

    worker="matrix-${runtime}-${RUN_SUFFIX}"
    CREATED_WORKERS+=("${worker}")
    ACTIVE_TASK=""

    CREATE_OUTPUT=$(exec_in_agent agt apply worker \
        --name "${worker}" \
        --runtime "${runtime}" \
        --model "${WORKER_MODEL}" \
        --identity "AgentTeams ${runtime} release-matrix worker" \
        --soul "Complete the requested task exactly and report its marker." \
        2>&1)
    CREATE_EXIT=$?
    if [ "${CREATE_EXIT}" -ne 0 ]; then
        log_fail "${runtime}: Worker creation accepted (${CREATE_OUTPUT})"
        continue
    fi
    log_pass "${runtime}: Worker creation accepted"

    if wait_worker_phase "${worker}" 300 "Running"; then
        log_pass "${runtime}: Worker phase is Running"
    else
        log_fail "${runtime}: Worker phase is Running"
        continue
    fi

    WORKER_JSON=$(exec_in_agent agt get workers "${worker}" -o json 2>/dev/null || true)
    assert_eq "${runtime}" "$(echo "${WORKER_JSON}" | jq -r '.runtime // empty')" \
        "${runtime}: Controller reports the requested runtime"

    ROOM_ID=$(echo "${WORKER_JSON}" | jq -r '.roomID // empty')
    WORKER_USER=$(echo "${WORKER_JSON}" | jq -r '.matrixUserID // empty')
    assert_not_empty "${ROOM_ID}" "${runtime}: Worker room is provisioned"
    assert_not_empty "${WORKER_USER}" "${runtime}: Matrix identity is provisioned"

    if ! wait_for_worker_container "${worker}" 180; then
        log_fail "${runtime}: Worker container is running"
        continue
    fi
    WORKER_CONTAINER=$(worker_container_name "${worker}")
    WORKER_IMAGE=$(docker inspect --format '{{.Config.Image}}' \
        "${WORKER_CONTAINER}" 2>/dev/null || true)
    if echo "${WORKER_IMAGE}" | grep -qiE "$(_runtime_image_pattern "${runtime}")"; then
        log_pass "${runtime}: Correct runtime image is active (${WORKER_IMAGE})"
    else
        log_fail "${runtime}: Incorrect runtime image (${WORKER_IMAGE})"
    fi

    if matrix_wait_for_user_joined "${ADMIN_TOKEN}" "${ROOM_ID}" \
        "${WORKER_USER}" 180; then
        log_pass "${runtime}: Worker joined its Matrix room"
    else
        log_fail "${runtime}: Worker joined its Matrix room"
        continue
    fi

    TASK_ID="task-$(date -u +%Y%m%d-%H%M%S)-$(_task_suffix "${runtime}")"
    ACTIVE_TASK="${TASK_ID}"
    MARKER="RUNTIME_MATRIX_$(echo "${runtime}" | tr '[:lower:]' '[:upper:]')"
    if _write_task_fixture "${TASK_ID}" "${worker}" "${ROOM_ID}" "${MARKER}"; then
        log_pass "${runtime}: Canonical task metadata and specification uploaded"
    else
        log_fail "${runtime}: Canonical task metadata and specification uploaded"
        continue
    fi

    ASSIGNMENT="${WORKER_USER} New task [${TASK_ID}]: Worker runtime matrix proof. Use your file-sync/task skill to pull shared/tasks/${TASK_ID}/spec.md, create and push result.md, then @mention the coordinator with TASK_COMPLETED."
    WORKER_REPLY=$(matrix_send_and_wait_for_reply \
        "${ADMIN_TOKEN}" "${ROOM_ID}" "${WORKER_USER}" \
        "${ASSIGNMENT}" 300 60 2>/dev/null || true)
    assert_not_empty "${WORKER_REPLY}" "${runtime}: Mention receives a Worker response"

    if minio_wait_for_file "shared/tasks/${TASK_ID}/result.md" 300; then
        log_pass "${runtime}: Worker published result.md"
        RESULT=$(minio_read_file "shared/tasks/${TASK_ID}/result.md" 2>/dev/null || true)
        assert_contains "${RESULT}" "${MARKER}" \
            "${runtime}: Task artifact contains the runtime marker"
    else
        log_fail "${runtime}: Worker published result.md"
    fi

    COMPLETION=$(_wait_for_completion_message \
        "${ROOM_ID}" "${WORKER_USER}" "${TASK_ID}" 180 2>/dev/null || true)
    assert_not_empty "${COMPLETION}" \
        "${runtime}: Worker reported task completion in Matrix"

    DELETE_OUTPUT=$(exec_in_agent agt delete worker "${worker}" 2>&1)
    DELETE_EXIT=$?
    if [ "${DELETE_EXIT}" -eq 0 ]; then
        log_pass "${runtime}: Worker deletion accepted"
    else
        log_fail "${runtime}: Worker deletion failed (${DELETE_OUTPUT})"
    fi

    DELETE_DEADLINE=$(( $(date +%s) + 180 ))
    while [ "$(date +%s)" -lt "${DELETE_DEADLINE}" ]; do
        if ! exec_in_agent agt get workers "${worker}" -o json \
            >/dev/null 2>&1 && \
            ! docker ps -a --format '{{.Names}}' 2>/dev/null | \
                grep -q "^$(worker_container_name "${worker}")$"; then
            break
        fi
        sleep 5
    done
    if ! exec_in_agent agt get workers "${worker}" -o json >/dev/null 2>&1; then
        log_pass "${runtime}: Controller resource is removed"
    else
        log_fail "${runtime}: Controller resource is removed"
    fi
    if ! docker ps -a --format '{{.Names}}' 2>/dev/null | \
        grep -q "^$(worker_container_name "${worker}")$"; then
        log_pass "${runtime}: Worker container is removed"
    else
        log_fail "${runtime}: Worker container is removed"
    fi

    exec_in_manager mc rm -r --force \
        "${STORAGE_PREFIX}/shared/tasks/${TASK_ID}/" >/dev/null 2>&1 || true
    ACTIVE_TASK=""
done

test_teardown "27-worker-runtime-matrix"
test_summary
