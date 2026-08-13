#!/usr/bin/env bash
# WorkerFlow 顶层卸载入口；宿主不存在时保持幂等，不清除用户的普通工作区。
set -euo pipefail

if command -v qwenpaw >/dev/null 2>&1; then
  bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/adapters/qwenpaw/uninstall.sh"
fi
