#!/bin/bash
# merge-openclaw-config.sh - Merge remote (MinIO) and local (Worker) openclaw.json
# 初学者导读：重启恢复时既有 Controller 新配置，也可能有 Worker 运行中保存的本地
# 自定义项，不能简单让整份文件互相覆盖。这里按字段声明“谁是权威”：模型/网关
# 服从远端 Controller，登录后刷新的 Matrix token 与用户插件细节保留本地。规则
# 集中在这里才能让各 Worker runtime 获得同样结果。
#
# Design principle (local-first):
#   Local (Worker disk) is the authoritative base. Periodic pulls from MinIO only
#   overlay Controller-managed slices so the Worker keeps its own customizations.
#   Remote (MinIO/Controller) overwrites only: models, gateway, and channels (deep
#   merge where remote wins on conflicting keys).
#   All other top-level fields (tools, agents, mcp, etc.) stay from local.
#   Merge rules:
#     - plugins.entries: deep merge — remote provides base/defaults, local wins
#       on shared keys so user customizations (e.g. memory-core dreaming schedule)
#       survive periodic syncs
#     - plugins.load.paths: union of both sides
#     - channels: deep merge (remote wins shared keys, local-only keys preserved)
#     - channels.matrix.accessToken: local wins (Worker re-login)
#
# Usage (as sourced function):
#   source /opt/agentteams/scripts/lib/merge-openclaw-config.sh
#   merge_openclaw_config <remote_path> <local_path> [<output_path>]
#
# If output_path is omitted, writes merged result to local_path.

# 逻辑说明：把远端 Controller 管理字段合入本地配置，同时保留 Worker 本地拥有的字段；输出采用临时文件替换，避免写出半份 JSON。
merge_openclaw_config() {
    local remote_path="$1"
    local local_path="$2"
    local output_path="${3:-$local_path}"

    if [ ! -f "${remote_path}" ]; then
        # No remote version, keep local as-is
        return 0
    fi

    if [ ! -f "${local_path}" ]; then
        # No local version, use remote directly
        mv "${remote_path}" "${output_path}"
        return 0
    fi

    local merged
    if ! merged=$(jq -n --slurpfile remote_file "${remote_path}" --slurpfile local_file "${local_path}" '
        ($remote_file[0]) as $remote
        | ($local_file[0]) as $local
        |
        $local
        | if ($remote.models // null) != null then .models = $remote.models else . end
        | if ($remote.gateway // null) != null then .gateway = $remote.gateway else . end
        | if ($remote.channels // null) != null or ($local.channels // null) != null then
            .channels = (($local.channels // {}) * ($remote.channels // {}))
          else . end
        | if ($local.channels.matrix.accessToken // null) != null then
            .channels.matrix.accessToken = $local.channels.matrix.accessToken
          else . end
        | if ($remote.plugins // null) != null or ($local.plugins // null) != null then
            .plugins = (
              ($local.plugins // {})
              | if ($remote.plugins.entries // null) != null or ($local.plugins.entries // null) != null then
                  .entries = (($remote.plugins.entries // {}) * ($local.plugins.entries // {}))
                else . end
              | if ($remote.plugins.load.paths // null) != null or ($local.plugins.load.paths // null) != null then
                  .load = ((.load // {}) | .paths = ([($remote.plugins.load.paths // [])[], ($local.plugins.load.paths // [])[]] | unique))
                else . end
            )
          else . end
    ' 2>/dev/null); then
        # Keep local unchanged and let the caller decide whether to retry.
        return 1
    fi

    [ -n "${merged}" ] || return 1
    printf '%s\n' "${merged}" > "${output_path}"
}
