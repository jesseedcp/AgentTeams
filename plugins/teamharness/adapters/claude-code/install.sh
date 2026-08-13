#!/usr/bin/env bash
# Claude Code adapter 当前只记录安装契约，供本地插件生命周期与测试核对。
# 它不会启动 Worker、写 Matrix token 或改变 AgentScope Manager 配置。
set -euo pipefail

log_file="${TEAMHARNESS_INSTALL_LOG:-}"
if [ -n "$log_file" ]; then
  mkdir -p "$(dirname "$log_file")"
  printf '{"event":"install","runtime":"claude-code","pluginDir":"%s"}\n' "${AGENTTEAMS_PLUGIN_DIR:-${PWD}}" >> "$log_file"
fi
