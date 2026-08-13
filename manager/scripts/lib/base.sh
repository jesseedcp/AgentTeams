#!/bin/bash
# base.sh - Shared utilities for AgentTeams startup scripts
# Source this file: source /opt/agentteams/scripts/lib/base.sh

set -e

# Wait for a TCP service to become available
# Usage: waitForService "ServiceName" "host" port [timeout_seconds]
# 逻辑说明：在启动下游进程前轮询 TCP 端口并设置上限；超时返回失败，避免依赖未就绪时继续初始化。
waitForService() {
    local name="$1"
    local host="$2"
    local port="$3"
    local timeout="${4:-120}"
    local elapsed=0

    echo "[agentteams] Waiting for ${name} at ${host}:${port}..."
    while ! curl -sf "http://${host}:${port}/" > /dev/null 2>&1 && \
          ! nc -z "${host}" "${port}" 2>/dev/null; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ "${elapsed}" -ge "${timeout}" ]; then
            echo "[agentteams] ERROR: ${name} did not become available within ${timeout}s"
            exit 1
        fi
    done
    echo "[agentteams] ${name} is ready (took ${elapsed}s)"
}

# Wait for an HTTP endpoint to return 200
# Usage: waitForHTTP "ServiceName" "url" [timeout_seconds]
# 逻辑说明：轮询真正的 HTTP 200 而不只检查端口打开，保证服务已经能处理应用请求。
waitForHTTP() {
    local name="$1"
    local url="$2"
    local timeout="${3:-120}"
    local elapsed=0

    echo "[agentteams] Waiting for ${name} HTTP at ${url}..."
    while [ "$(curl -sf -o /dev/null -w '%{http_code}' "${url}" 2>/dev/null)" != "200" ]; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ "${elapsed}" -ge "${timeout}" ]; then
            echo "[agentteams] ERROR: ${name} HTTP not ready within ${timeout}s"
            exit 1
        fi
    done
    echo "[agentteams] ${name} HTTP is ready (took ${elapsed}s)"
}

# Generate a cryptographically secure random key
# Usage: generateKey [length_bytes]
# 逻辑说明：使用 OpenSSL 的密码学随机源生成十六进制秘密，调用方负责安全写入而不能打印到日志。
generateKey() {
    local bytes="${1:-32}"
    openssl rand -hex "${bytes}"
}

# Log with timestamp
# Usage: log "message"
# 逻辑说明：统一初始化子进程的时间戳，便于按多进程启动顺序排查依赖问题。
log() {
    echo "[agentteams $(date '+%Y-%m-%d %H:%M:%S')] $1"
}
