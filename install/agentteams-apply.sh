#!/bin/bash
# agentteams-apply.sh - Unified entry point for declarative resource management
#
# Thin shell that forwards to `agt apply` inside the Manager container.
# Supports Worker, Team, and Human resources in YAML format.
#
# 宿主机脚本只负责把文件安全复制到容器并转发参数；解析资源、计算差异和调用
# Controller 的权威逻辑都在 typed `agt` CLI 中。`--prune` 会删除清单之外的受管
# 资源，属于破坏性全量同步；初学者应先用 `--dry-run` 查看差异。
#
# Usage:
#   ./agentteams-apply.sh -f resource.yaml              # incremental apply
#   ./agentteams-apply.sh -f resource.yaml --prune      # full sync (delete extras)
#   ./agentteams-apply.sh -f resource.yaml --dry-run    # show diff only
#   ./agentteams-apply.sh -f resource.yaml --watch      # watch file changes
#
# Environment:
#   AGENTTEAMS_CONTAINER_CMD   Override container runtime (docker/podman)

set -e

# 逻辑说明：为声明式 apply 的检测和提交阶段统一添加可识别的终端前缀。
log() {
    echo -e "\033[36m[AgentTeams Apply]\033[0m $1"
}

# 逻辑说明：把可恢复错误写到 stderr，是否退出由调用者决定。
error() {
    echo -e "\033[31m[AgentTeams Apply ERROR]\033[0m $1" >&2
}

# 逻辑说明：用于无法继续的输入或环境错误，先复用统一错误格式再终止脚本。
die() {
    error "$1"
    exit 1
}

# ============================================================
# Detect container runtime
# ============================================================
CONTAINER_CMD="${AGENTTEAMS_CONTAINER_CMD:-}"
if [ -z "${CONTAINER_CMD}" ]; then
    if command -v docker > /dev/null 2>&1; then
        CONTAINER_CMD="docker"
    elif command -v podman > /dev/null 2>&1; then
        CONTAINER_CMD="podman"
    else
        die "Neither docker nor podman found"
    fi
fi

# ============================================================
# Verify Manager container is running
# ============================================================
if ! ${CONTAINER_CMD} ps --filter name=agentteams-manager --format '{{.Names}}' 2>/dev/null | grep -q 'agentteams-manager'; then
    die "agentteams-manager container is not running"
fi

# /tmp/import 是一次性中转区，不是持久数据目录；权威状态最终在 Controller/CR 中。
# Ensure /tmp/import exists before copying files into container
${CONTAINER_CMD} exec agentteams-manager mkdir -p /tmp/import 2>/dev/null || true

# ============================================================
# Copy YAML files and referenced packages into container
# ============================================================
ARGS=()
NEXT_IS_FILE=false

for arg in "$@"; do
    if [ "${NEXT_IS_FILE}" = true ]; then
        NEXT_IS_FILE=false
        if [ -f "${arg}" ]; then
            BASENAME=$(basename "${arg}")
            ${CONTAINER_CMD} cp "${arg}" "agentteams-manager:/tmp/import/${BASENAME}"
            ARGS+=("/tmp/import/${BASENAME}")
            log "Copied ${arg} → container:/tmp/import/${BASENAME}"
        else
            die "File not found: ${arg}"
        fi
        continue
    fi

    if [ "${arg}" = "-f" ] || [ "${arg}" = "--file" ]; then
        NEXT_IS_FILE=true
        ARGS+=("-f")
        continue
    fi

    ARGS+=("${arg}")
done

# ============================================================
# Forward to AgentTeams CLI inside container
# ============================================================
log "Forwarding to agt apply..."
${CONTAINER_CMD} exec agentteams-manager agt apply "${ARGS[@]}"
