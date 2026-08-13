#!/usr/bin/env bash
# TeamHarness 顶层卸载分派器：对当前可用的宿主执行清理，并记录审计事件。
# 卸载只移除插件注册/内容，不删除 AgentTeams Worker 工作区或共享任务产物。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
export AGENTTEAMS_PLUGIN_DIR="${AGENTTEAMS_PLUGIN_DIR:-$PLUGIN_DIR}"

if command -v qwenpaw >/dev/null 2>&1; then
  bash "${PLUGIN_DIR}/adapters/qwenpaw/uninstall.sh"
fi

if command -v claude >/dev/null 2>&1; then
  bash "${PLUGIN_DIR}/adapters/claude-code/uninstall.sh"
fi

log_file="${TEAMHARNESS_INSTALL_LOG:-}"
if [ -n "$log_file" ]; then
  mkdir -p "$(dirname "$log_file")"
  printf '{"event":"uninstall","runtime":"teamharness","pluginDir":"%s"}\n' "$AGENTTEAMS_PLUGIN_DIR" >> "$log_file"
fi
