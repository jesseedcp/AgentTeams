#!/bin/bash
# MinIO 测试客户端：检查任务简报、Worker 配置、记忆、快照和结果是否真正写入持久化对象存储。
# 测试读取最终对象，而不是只信任 Manager 的聊天回复；这能发现“回复成功但落盘失败”的回归。
# 测试账户只允许访问测试 bucket/前缀，辅助函数不得回显 Secret Key。
# minio-client.sh - MinIO verification helpers for integration tests
#
# All mc commands run via exec_in_manager() (docker exec into the Manager container)
# so that MinIO (port 9000/9001) does not need to be exposed to the host.

_MINIO_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${_MINIO_LIB_DIR}/test-helpers.sh" 2>/dev/null || true

# Configure mc alias for test MinIO (runs inside Manager container)
# Usage: minio_setup
minio_setup() {
    exec_in_manager mc alias set agentteams "${TEST_MINIO_URL}" \
        "${TEST_MINIO_USER}" "${TEST_MINIO_PASSWORD}" 2>/dev/null
}

minio_storage_prefix() {
    printf '%s\n' "${STORAGE_PREFIX:-${TEST_STORAGE_PREFIX:-agentteams/agentteams-storage}}"
}

# ============================================================
# File verification
# ============================================================

# Check if a file exists in MinIO
# Usage: minio_file_exists <path>
# Example: minio_file_exists "agents/manager/SOUL.md"
minio_file_exists() {
    local path="$1"
    exec_in_manager mc stat "$(minio_storage_prefix)/${path}" > /dev/null 2>&1
}

# Read file content from MinIO
# Usage: minio_read_file <path>
minio_read_file() {
    local path="$1"
    exec_in_manager mc cat "$(minio_storage_prefix)/${path}" 2>/dev/null
}

# List directory contents in MinIO
# Usage: minio_list_dir <path>
minio_list_dir() {
    local path="$1"
    exec_in_manager mc ls "$(minio_storage_prefix)/${path}" 2>/dev/null
}

# Wait for a file to appear in MinIO
# Usage: minio_wait_for_file <path> [timeout_seconds]
minio_wait_for_file() {
    local path="$1"
    local timeout="${2:-120}"
    local elapsed=0

    while ! minio_file_exists "${path}"; do
        sleep 5
        elapsed=$((elapsed + 5))
        if [ "${elapsed}" -ge "${timeout}" ]; then
            return 1
        fi
    done
    return 0
}
