#!/bin/sh
# AgentScope Manager 容器入口。Controller 已把 CR 中的期望配置投射为环境变量；
# 这里先做 fail-fast 校验，再启动 Python 进程。若放过空变量，错误会延迟成 Matrix、
# Gateway 或对象存储连接失败，难以判断根因。`exec` 让 Python 接管 PID 1，从而正确
# 接收 Kubernetes 的终止信号并完成 SQLite/会话收尾。
set -eu

if [ "$#" -gt 0 ]; then
    # 显式命令用于调试/测试镜像；正常部署没有参数，走下方标准启动链路。
    exec "$@"
fi

# 这些值来自 Helm Secret → Controller → Manager Pod，而不是镜像内置配置。
required_environment="
AGENTTEAMS_MANAGER_NAME
AGENTTEAMS_MANAGER_MATRIX_USER_ID
AGENTTEAMS_MANAGER_MATRIX_TOKEN
AGENTTEAMS_MANAGER_ADMIN_ROOM_ID
AGENTTEAMS_MATRIX_URL
AGENTTEAMS_MATRIX_DOMAIN
AGENTTEAMS_CONTROLLER_URL
AGENTTEAMS_AI_GATEWAY_URL
AGENTTEAMS_MANAGER_GATEWAY_KEY
AGENTTEAMS_FS_ENDPOINT
AGENTTEAMS_FS_BUCKET
AGENTTEAMS_FS_ACCESS_KEY
AGENTTEAMS_FS_SECRET_KEY
AGENTTEAMS_DEFAULT_MODEL
AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY
"

for name in ${required_environment}; do
    if ! printenv "${name}" >/dev/null 2>&1; then
        echo "missing required environment variable: ${name}" >&2
        exit 64
    fi
    if [ -z "$(printenv "${name}")" ]; then
        echo "required environment variable is empty: ${name}" >&2
        exit 64
    fi
done

for path in \
    /opt/agentteams/manager/SOUL.md \
    /opt/agentteams/manager/AGENTS.md \
    /opt/agentteams/manager/TOOLS.md \
    /opt/agentteams/manager/HEARTBEAT.md \
    /opt/agentteams/manager/skills \
    /opt/agentteams/config/known-models.json; do
    if [ ! -e "${path}" ]; then
        echo "required image path is missing: ${path}" >&2
        exit 70
    fi
done

workspace="${AGENTTEAMS_MANAGER_WORKSPACE:-/var/lib/agentteams-manager}"
case "${workspace}" in
    /*) ;;
    *)
        echo "AGENTTEAMS_MANAGER_WORKSPACE must be absolute" >&2
        exit 64
        ;;
esac

umask 077
mkdir -p "${workspace}/state"
mkdir -p "${workspace}/media"
mkdir -p "${workspace}/matrix-e2ee"
mkdir -p "${workspace}/tmp"
chmod 700 \
    "${workspace}" \
    "${workspace}/state" \
    "${workspace}/media" \
    "${workspace}/matrix-e2ee" \
    "${workspace}/tmp"

exec agentteams-manager
