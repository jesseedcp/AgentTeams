#!/usr/bin/env bash
# 构建、校验并通过 QwenPaw CLI 安装 WorkerFlow 插件包。临时 staging 由 trap 清理，
# 所以中断不会留下被下一次启动误认成有效插件的半成品。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKERFLOW_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_SCRIPT="${SCRIPT_DIR}/scripts/build-qwenpaw-plugin.rb"

if ! command -v qwenpaw >/dev/null 2>&1; then
  echo "ERROR: qwenpaw command not found" >&2
  exit 1
fi

if ! command -v ruby >/dev/null 2>&1; then
  echo "ERROR: ruby is required to build the QwenPaw plugin package" >&2
  exit 1
fi

stage_dir="$(mktemp -d "${TMPDIR:-/tmp}/workerflow-qwenpaw-install.XXXXXX")"
cleanup() {
  rm -rf "$stage_dir"
}
trap cleanup EXIT

package_zip="$(OUT_DIR="${stage_dir}/dist" ruby "${BUILD_SCRIPT}" "${WORKERFLOW_DIR}/plugin.yaml" | tail -n 1)"
unpack_dir="${stage_dir}/unpacked"
mkdir -p "$unpack_dir"

python3 - "$package_zip" "$unpack_dir" <<'PY'
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as archive:
    archive.extractall(sys.argv[2])
PY

package_dir="$(find "$unpack_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [ -z "$package_dir" ] || [ ! -f "$package_dir/plugin.json" ]; then
  echo "ERROR: generated QwenPaw plugin package is invalid" >&2
  exit 1
fi

qwenpaw plugin install "$package_dir" --force
