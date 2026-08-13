#!/bin/bash
# replay-task.sh - Send a task message to the Manager Agent via Matrix
#
# replay 走真实链路：以 admin 身份登录 Matrix，向 Manager 房间发送消息并等待新
# 事件。它适合复现对话/工具问题，但不会点击 Cinny，因此不能替代页面 UI 验收。
# env 文件含管理员密码；读取时只导入允许的键值，日志中不得打印 token/password。
#
# Acts as the "human admin" by sending a Matrix message to the Manager
# and optionally waiting for its reply.
#
# Usage:
#   ./scripts/replay-task.sh "Create a Worker named alice"   # CLI mode
#   ./scripts/replay-task.sh                                  # Interactive mode
#   echo "Create worker bob" | ./scripts/replay-task.sh       # Pipe mode
#
# Environment variables (or loaded from ./agentteams-manager.env):
#   AGENTTEAMS_ADMIN_USER          Admin username       (default: admin)
#   AGENTTEAMS_ADMIN_PASSWORD      Admin password       (required)
#   AGENTTEAMS_MATRIX_DOMAIN       Matrix domain        (default: matrix-local.agentteams.io:8080)
#   REPLAY_WAIT                Wait for reply        (default: 1, set 0 to skip)
#   REPLAY_TIMEOUT             Reply timeout secs    (default: 300)
#   REPLAY_READY_TIMEOUT       Manager readiness timeout (default: 300)
#   REPLAY_MANAGER_CONTAINER   Manager container name    (default: agentteams-manager)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ============================================================
# Load configuration
# ============================================================

# Load env file if present (variables already set in env take precedence)
# Search order: AGENTTEAMS_ENV_FILE > project root > HOME directory
ENV_FILE="${AGENTTEAMS_ENV_FILE:-${PROJECT_ROOT}/agentteams-manager.env}"
if [ ! -f "${ENV_FILE}" ] && [ -f "${HOME}/agentteams-manager.env" ]; then
    ENV_FILE="${HOME}/agentteams-manager.env"
fi
if [ -f "${ENV_FILE}" ]; then
    # Source the env file but don't override existing env vars
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "${key}" =~ ^#.*$ || -z "${key}" ]] && continue
        # Trim whitespace
        key=$(echo "${key}" | xargs)
        # Only set if not already in environment
        if [ -z "${!key}" ]; then
            export "${key}=${value}"
        fi
    done < "${ENV_FILE}"
fi

# Configuration with defaults
ADMIN_USER="${AGENTTEAMS_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${AGENTTEAMS_ADMIN_PASSWORD:-}"
MATRIX_DOMAIN="${AGENTTEAMS_MATRIX_DOMAIN:-matrix-local.agentteams.io:${AGENTTEAMS_PORT_GATEWAY:-8080}}"
WAIT_FOR_REPLY="${REPLAY_WAIT:-1}"
REPLY_TIMEOUT="${REPLAY_TIMEOUT:-300}"
MANAGER_USER="manager"
MANAGER_CONTAINER="${REPLAY_MANAGER_CONTAINER:-agentteams-manager}"

# When REPLAY_USE_DOCKER_EXEC=1, all Matrix API calls go through docker exec
# inside the container using the internal Tuwunel port. This avoids host proxy
# interference and works even when the gateway port isn't directly accessible.
USE_DOCKER_EXEC="${REPLAY_USE_DOCKER_EXEC:-0}"
if [ "${USE_DOCKER_EXEC}" = "1" ]; then
    MATRIX_URL="http://127.0.0.1:6167"
    CURL_PREFIX="docker exec ${MANAGER_CONTAINER}"
else
    MATRIX_URL="http://${MATRIX_DOMAIN}"
    CURL_PREFIX=""
fi

# ============================================================
# Utility functions
# ============================================================

# 逻辑说明：把 replay 阶段写到终端，stdout 不作为机器协议使用，因此可保留颜色前缀。
log() {
    echo -e "\033[36m[replay]\033[0m $1"
}

# 逻辑说明：报告无法继续的 Matrix 请求或参数错误并退出，避免在未知房间继续发送测试消息。
error() {
    echo -e "\033[31m[replay ERROR]\033[0m $1" >&2
    exit 1
}

# ============================================================
# Matrix API helpers (self-contained, no dependency on test libs)
# ============================================================

# 逻辑说明：集中组装 Matrix HTTP 方法、路径、JSON 和可选 Bearer；HTTP 失败原样返回给上层决定重试或终止。
matrix_api() {
    local method="$1"
    local path="$2"
    local data="$3"
    local token="$4"

    local auth_args=()
    if [ -n "${token}" ]; then
        auth_args+=(-H "Authorization: Bearer ${token}")
    fi

    if [ -n "${data}" ]; then
        ${CURL_PREFIX} curl -sf -X "${method}" \
            -H "Content-Type: application/json" \
            "${auth_args[@]}" \
            -d "${data}" \
            "${MATRIX_URL}${path}"
    else
        ${CURL_PREFIX} curl -sf -X "${method}" \
            "${auth_args[@]}" \
            "${MATRIX_URL}${path}"
    fi
}

# Login to Matrix, return access_token
# 逻辑说明：用管理员密码换取短期 access token，并只把 token 返回给调用者而不写 replay 日志。
do_login() {
    local resp
    resp=$(matrix_api POST "/_matrix/client/v3/login" \
        "{\"type\":\"m.login.password\",\"identifier\":{\"type\":\"m.id.user\",\"user\":\"${ADMIN_USER}\"},\"password\":\"${ADMIN_PASSWORD}\"}")
    echo "${resp}" | jq -r '.access_token // empty'
}

# Get joined rooms
# 逻辑说明：读取当前管理员已加入的 room_id 列表，供后续定位 Manager DM，函数不改变房间状态。
get_joined_rooms() {
    local token="$1"
    matrix_api GET "/_matrix/client/v3/joined_rooms" "" "${token}" | jq -r '.joined_rooms[]'
}

# Get room members
# 逻辑说明：编码 room_id 后查询成员，只返回 Matrix user id，避免调用方自行拼接不安全 URL。
get_room_members() {
    local token="$1"
    local room_id="$2"
    # URL-encode the room_id (! -> %21, : -> %3A)
    local encoded_room_id
    encoded_room_id=$(printf '%s' "${room_id}" | sed 's/!/%21/g; s/:/%3A/g')
    matrix_api GET "/_matrix/client/v3/rooms/${encoded_room_id}/members" "" "${token}" | jq -r '.chunk[].state_key' 2>/dev/null
}

# Find DM room with manager
# 逻辑说明：遍历已加入房间并核对 Manager 成员身份；找到现有 DM 就复用，避免每次 replay 新建房间。
find_manager_room() {
    local token="$1"
    local rooms
    rooms=$(get_joined_rooms "${token}")

    for room_id in ${rooms}; do
        local members
        members=$(get_room_members "${token}" "${room_id}" 2>/dev/null) || continue
        local member_count
        member_count=$(echo "${members}" | wc -l | xargs)

        # DM room: exactly 2 members, one is admin, one is manager
        if [ "${member_count}" = "2" ] && echo "${members}" | grep -q "@${MANAGER_USER}:"; then
            echo "${room_id}"
            return 0
        fi
    done

    return 1
}

# Create a DM room with the manager and return room_id
# 逻辑说明：仅在没有现成 Manager DM 时创建可信私聊并邀请准确的 Manager MXID，返回新 room_id。
create_dm_room() {
    local token="$1"

    local manager_full_id="@${MANAGER_USER}:${MATRIX_DOMAIN}"
    local resp
    resp=$(matrix_api POST "/_matrix/client/v3/createRoom" \
        "{\"is_direct\":true,\"invite\":[\"${manager_full_id}\"],\"preset\":\"trusted_private_chat\"}" "${token}")

    echo "${resp}" | jq -r '.room_id // empty' 2>/dev/null
}

# Send a message to a room
# 逻辑说明：为每条 replay 消息生成唯一 transaction id，Matrix 重试时据此去重，返回发送事件 ID。
send_message() {
    local token="$1"
    local room_id="$2"
    local body="$3"
    local txn_id
    txn_id="replay_$(date +%s%N)"
    local encoded_room_id
    encoded_room_id=$(printf '%s' "${room_id}" | sed 's/!/%21/g; s/:/%3A/g')

    # Escape the body for JSON
    local json_body
    json_body=$(printf '%s' "${body}" | jq -Rs .)

    matrix_api PUT "/_matrix/client/v3/rooms/${encoded_room_id}/send/m.room.message/${txn_id}" \
        "{\"msgtype\":\"m.text\",\"body\":${json_body}}" "${token}" > /dev/null
}

# Read recent messages from a room
# 逻辑说明：按上限逆向读取指定房间历史，结果保留完整 JSON 供等待和日志导出分别筛选。
read_messages() {
    local token="$1"
    local room_id="$2"
    local limit="${3:-10}"
    local encoded_room_id
    encoded_room_id=$(printf '%s' "${room_id}" | sed 's/!/%21/g; s/:/%3A/g')
    matrix_api GET "/_matrix/client/v3/rooms/${encoded_room_id}/messages?dir=b&limit=${limit}" "" "${token}"
}

# Wait for a reply from the manager
# Outputs ONLY the reply body to stdout (for capture).
# Progress messages go to stderr (visible in terminal but not captured).
# 逻辑说明：从发送事件之后轮询 Manager 回复并设置超时；进度写 stderr，stdout 只返回最终正文供调用者捕获。
wait_for_manager_reply() {
    local token="$1"
    local room_id="$2"
    local after_event="$3"
    local timeout="${4:-${REPLY_TIMEOUT}}"
    local elapsed=0

    local baseline_event="${after_event}"

    echo -e "\033[36m[replay]\033[0m Waiting for Manager reply (timeout: ${timeout}s)..." >&2

    while [ "${elapsed}" -lt "${timeout}" ]; do
        sleep 5
        elapsed=$((elapsed + 5))

        local messages
        messages=$(read_messages "${token}" "${room_id}" 10 2>/dev/null) || continue

        # Get latest manager message
        local latest_event latest_body
        latest_event=$(echo "${messages}" | jq -r --arg user "@${MANAGER_USER}:" \
            '[.chunk[] | select(.sender | contains($user)) | .event_id] | first // ""' 2>/dev/null)
        latest_body=$(echo "${messages}" | jq -r --arg user "@${MANAGER_USER}:" \
            '[.chunk[] | select(.sender | contains($user)) | .content.body] | first // empty' 2>/dev/null)

        # Only return if the event_id differs from baseline (new message)
        if [ -n "${latest_body}" ] && [ "${latest_event}" != "${baseline_event}" ]; then
            echo "" >&2
            echo -e "\033[32m[Manager]\033[0m" >&2
            echo "${latest_body}" >&2
            # Output ONLY the clean reply to stdout
            echo "${latest_body}"
            return 0
        fi

        printf "\r\033[36m[replay]\033[0m Waiting... (%ds/%ds)" "${elapsed}" "${timeout}" >&2
    done

    echo "" >&2
    echo -e "\033[36m[replay]\033[0m Timeout: no reply from Manager within ${timeout}s" >&2
    return 1
}

# ============================================================
# Main
# ============================================================

# Validate configuration
if [ -z "${ADMIN_PASSWORD}" ]; then
    error "AGENTTEAMS_ADMIN_PASSWORD is required. Set it via env var or ensure ./agentteams-manager.env exists."
fi

# Get task message from CLI arg, stdin, or interactive prompt
TASK_MSG=""

if [ $# -gt 0 ]; then
    # CLI mode
    TASK_MSG="$*"
elif [ ! -t 0 ]; then
    # Pipe mode (stdin is not a terminal)
    TASK_MSG=$(cat)
else
    # Interactive mode
    echo -e "\033[36m[replay]\033[0m Enter the task message to send to Manager:"
    echo -e "\033[36m[replay]\033[0m (Press Enter to send, Ctrl+C to cancel)"
    echo -n "> "
    read -r TASK_MSG
fi

if [ -z "${TASK_MSG}" ]; then
    error "Task message cannot be empty"
fi

log "Task: ${TASK_MSG}"
log ""

# ============================================================
# Conversation log setup
# ============================================================
LOG_DIR="${REPLAY_LOG_DIR:-${PROJECT_ROOT}/logs/replay}"
mkdir -p "${LOG_DIR}"
LOG_TS=$(date '+%Y%m%d-%H%M%S')
LOG_FILE="${LOG_DIR}/replay-${LOG_TS}.log"

# 逻辑说明：把已经选择并脱敏的 replay 内容追加到本轮专用日志，不负责输出认证 token。
write_log() {
    echo "$1" >> "${LOG_FILE}"
}

write_log "# AgentTeams Replay Log"
write_log "# Time: $(date '+%Y-%m-%d %H:%M:%S')"
write_log "# Task: ${TASK_MSG}"
write_log ""

# Step 1: Login
log "Logging in as '${ADMIN_USER}'..."
ACCESS_TOKEN=$(do_login)
if [ -z "${ACCESS_TOKEN}" ]; then
    error "Login failed. Check AGENTTEAMS_ADMIN_USER and AGENTTEAMS_ADMIN_PASSWORD."
fi
log "Login successful"

# Step 2: Find or create DM room with Manager
log "Finding DM room with Manager..."
ROOM_ID=$(find_manager_room "${ACCESS_TOKEN}" 2>/dev/null || true)
if [ -z "${ROOM_ID}" ]; then
    log "No existing DM room found, creating one..."
    ROOM_ID=$(create_dm_room "${ACCESS_TOKEN}")
    if [ -z "${ROOM_ID}" ]; then
        error "Failed to create DM room with @${MANAGER_USER}. Is the Manager Agent running?"
    fi
    log "Created DM room: ${ROOM_ID}"
else
    log "Found existing room: ${ROOM_ID}"
fi

# Step 3: Wait for the AgentScope Manager to report ready, then verify its
# Matrix session has joined the DM room.
READY_TIMEOUT="${REPLAY_READY_TIMEOUT:-300}"
READY_ELAPSED=0
MANAGER_FULL_ID="@${MANAGER_USER}:${MATRIX_DOMAIN}"

log "Waiting for Manager agent to be ready..."

# Phase 1: wait for AgentScope recovery, runtime configuration, and Matrix.
MANAGER_READY=false
while [ "${READY_ELAPSED}" -lt "${READY_TIMEOUT}" ]; do
    if docker exec "${MANAGER_CONTAINER}" python -c \
        'import urllib.request; urllib.request.urlopen("http://127.0.0.1:18799/readyz", timeout=2).read()' \
        >/dev/null 2>&1; then
        MANAGER_READY=true
        log "AgentScope Manager is ready"
        break
    fi
    sleep 5
    READY_ELAPSED=$((READY_ELAPSED + 5))
    printf "\r\033[36m[replay]\033[0m Waiting for AgentScope Manager... (%ds/%ds)" "${READY_ELAPSED}" "${READY_TIMEOUT}"
done

if [ "${MANAGER_READY}" != "true" ]; then
    error "AgentScope Manager did not become ready within ${READY_TIMEOUT}s. Check: docker logs ${MANAGER_CONTAINER}"
fi

# Phase 2: Wait for Manager to join the DM room (confirms Matrix channel is active)
# Reset elapsed counter for phase 2 (phase 1 may have consumed the budget)
READY_ELAPSED=0
while [ "${READY_ELAPSED}" -lt "${READY_TIMEOUT}" ]; do
    MEMBERS=$(get_room_members "${ACCESS_TOKEN}" "${ROOM_ID}" 2>/dev/null) || true
    if echo "${MEMBERS}" | grep -q "${MANAGER_FULL_ID}"; then
        log "Manager has joined the room"
        break
    fi
    sleep 3
    READY_ELAPSED=$((READY_ELAPSED + 3))
    printf "\r\033[36m[replay]\033[0m Waiting for Manager to join room... (%ds/%ds)" "${READY_ELAPSED}" "${READY_TIMEOUT}"
done

if ! echo "${MEMBERS}" | grep -q "${MANAGER_FULL_ID}" 2>/dev/null; then
    error "Manager did not join the room within ${READY_TIMEOUT}s. /readyz succeeded but the Matrix room membership is missing."
fi

# Step 4: Send message
BASELINE_MANAGER_EVENT=""
if [ "${WAIT_FOR_REPLY}" = "1" ]; then
    # Capture the boundary before sending so an immediate reply cannot become the baseline.
    if ! BASELINE_MANAGER_EVENT=$(read_messages "${ACCESS_TOKEN}" "${ROOM_ID}" 5 2>/dev/null | \
        jq -r --arg user "@${MANAGER_USER}:" \
        '[.chunk[] | select(.sender | contains($user)) | .event_id] | first // ""' 2>/dev/null); then
        error "Failed to read the Manager message boundary before sending the task."
    fi
fi

log "Sending task message..."
send_message "${ACCESS_TOKEN}" "${ROOM_ID}" "${TASK_MSG}"
log "Message sent"

# Step 5: Wait for reply
if [ "${WAIT_FOR_REPLY}" = "1" ]; then
    REPLY=$(wait_for_manager_reply "${ACCESS_TOKEN}" "${ROOM_ID}" "${BASELINE_MANAGER_EVENT}" "${REPLY_TIMEOUT}")

    # ============================================================
    # Collect room messages into the log file
    # ============================================================
    log ""
    log "--- Collecting room messages ---"

    # Helper: dump messages of a room into the log
    # 逻辑说明：读取一个相关房间的消息并格式化进报告；查询失败只跳过该房间，不破坏已收集结果。
    dump_room_messages() {
        local token="$1"
        local rid="$2"
        local limit="${3:-100}"
        read_messages "${token}" "${rid}" "${limit}" 2>/dev/null | jq -r '
            .chunk | reverse | .[] |
            select(.content.body != null and .content.body != "") |
            "**[\(.origin_server_ts / 1000 | strftime("%H:%M:%S"))] \(.sender | split(":")[0] | ltrimstr("@"))**\n\n\(.content.body)\n"
        ' 2>/dev/null
    }

    # DM Room
    write_log "## DM (admin <-> manager)"
    write_log ""
    dump_room_messages "${ACCESS_TOKEN}" "${ROOM_ID}" 100 >> "${LOG_FILE}"

    # Worker / other rooms that include @manager
    ALL_ROOMS=$(get_joined_rooms "${ACCESS_TOKEN}" 2>/dev/null)
    for rid in ${ALL_ROOMS}; do
        [ "${rid}" = "${ROOM_ID}" ] && continue
        ROOM_MEMBERS=$(get_room_members "${ACCESS_TOKEN}" "${rid}" 2>/dev/null) || continue
        echo "${ROOM_MEMBERS}" | grep -q "@${MANAGER_USER}:" || continue

        MEMBER_NAMES=$(echo "${ROOM_MEMBERS}" | sed 's/@//g; s/:.*//g' | tr '\n' ', ' | sed 's/,$//')
        write_log "---"
        write_log ""
        write_log "## Room: ${MEMBER_NAMES}"
        write_log ""
        dump_room_messages "${ACCESS_TOKEN}" "${rid}" 100 >> "${LOG_FILE}"
    done

    log "Conversation log saved to: ${LOG_FILE}"
else
    log "Skipping reply wait (REPLAY_WAIT=0)"
fi
