#!/usr/bin/env bash
# TeamHarness 顶层安装分派器：探测本机支持的 runtime，再调用对应 adapter。
# 初学者注意：同一个插件可以有多种宿主适配器；这里不复制实现，只选择 QwenPaw
# 或 Claude Code 安装路径。没有受支持 runtime 时失败，避免写入虚假的安装状态。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
export AGENTTEAMS_PLUGIN_DIR="${AGENTTEAMS_PLUGIN_DIR:-$PLUGIN_DIR}"

ran=0

if command -v qwenpaw >/dev/null 2>&1; then
  bash "${PLUGIN_DIR}/adapters/qwenpaw/install.sh"
  ran=1
fi

if command -v claude >/dev/null 2>&1; then
  bash "${PLUGIN_DIR}/adapters/claude-code/install.sh"
  ran=1
fi

if [ "$ran" -eq 0 ]; then
  echo "ERROR: no supported TeamHarness local runtime found; expected qwenpaw or claude" >&2
  exit 1
fi
