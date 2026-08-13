#!/usr/bin/env bash
# 从 QwenPaw 注册表移除 WorkerFlow；“未安装”视为已达到目标，便于重复执行。
set -euo pipefail

if command -v qwenpaw >/dev/null 2>&1; then
  printf 'y\n' | qwenpaw plugin uninstall workerflow >/dev/null 2>&1 || true
fi
