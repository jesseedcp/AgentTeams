#!/bin/bash
# mc-wrapper.sh - Transparent STS credential refresh for mc (MinIO Client)
# 初学者导读：其他脚本以为自己在调用普通 ``mc``，实际先经过本 wrapper 刷新短期
# 凭据，再用 ``exec`` 交给真实 mc.bin。exec 保留退出码和信号，调用方能正确判断
# 同步是否失败；刷新失败不在此泄露 token，具体存储命令仍由上层处理。
#
# Installed as /usr/local/bin/mc (symlink), with the real binary at /usr/local/bin/mc.bin.
# In cloud mode (RRSA/OIDC), refreshes STS credentials before every mc invocation.
# In local mode, ensure_mc_credentials is a no-op — near-zero overhead.

# Source credential management (provides ensure_mc_credentials)
. /opt/agentteams/scripts/lib/oss-credentials.sh 2>/dev/null

# Refresh STS credentials if needed (no-op in local mode)
ensure_mc_credentials 2>/dev/null || true

# Delegate to the real mc binary
exec /usr/local/bin/mc.bin "$@"
