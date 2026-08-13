#!/bin/bash
# Select the storage paths mirrored by the embedded Controller or legacy Manager.
# 初学者导读：Controller 只需恢复自己的配置前缀，而旧 runtime 可能需要完整存储树。
# 把 scope 判断集中在这里，能避免 Controller 无意下载 Worker 会话/产物，也保留旧版
# 恢复路径的兼容性；新的 AgentScope Manager 不会因此重新变成旧 Manager runtime。

agentteams_mirror_initial() {
    local scope="$1"
    local storage_prefix="$2"
    local local_root="$3"

    if [ "${scope}" = "controller" ]; then
        mc mirror \
            "${storage_prefix}/agentteams-config/" \
            "${local_root}/agentteams-config/" \
            --overwrite
        return
    fi

    mc mirror "${storage_prefix}/" "${local_root}/" --overwrite
}

agentteams_mirror_fallback() {
    local scope="$1"
    local storage_prefix="$2"
    local local_root="$3"

    [ "${scope}" != "controller" ] || return 0
    mc mirror "${storage_prefix}/" "${local_root}/" --overwrite --newer-than "5m"
}
