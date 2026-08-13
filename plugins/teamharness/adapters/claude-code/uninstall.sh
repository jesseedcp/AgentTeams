#!/usr/bin/env bash
# 与 install 对称地记录 Claude Code adapter 卸载事件，不删除项目工作文件。
set -euo pipefail

log_file="${TEAMHARNESS_INSTALL_LOG:-}"
if [ -n "$log_file" ]; then
  mkdir -p "$(dirname "$log_file")"
  printf '{"event":"uninstall","runtime":"claude-code","pluginDir":"%s"}\n' "${AGENTTEAMS_PLUGIN_DIR:-${PWD}}" >> "$log_file"
fi
