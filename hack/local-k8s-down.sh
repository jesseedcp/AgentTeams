#!/bin/bash
# local-k8s-down.sh — Tear down the local AgentTeams kind cluster.
#
# 这是破坏性清理：先触发 Helm uninstall hook 清理 Controller 动态资源，再删除整个
# kind 集群。集群内 PVC/Secret/房间数据通常随节点容器一起消失；它不会自动备份，
# 也不应把 CLUSTER_NAME 的匹配放宽成删除所有 kind 集群。
#
# Usage:
#   ./hack/local-k8s-down.sh

set -euo pipefail

CLUSTER_NAME="${AGENTTEAMS_CLUSTER_NAME:-agentteams}"
NAMESPACE="${AGENTTEAMS_NAMESPACE:-agentteams}"

log() { echo -e "\033[36m[AgentTeams K8s]\033[0m $1"; }

# Uninstall Helm release (if exists)
if helm list -n "$NAMESPACE" 2>/dev/null | grep -q agentteams; then
    log "Uninstalling Helm release 'agentteams'..."
    helm uninstall agentteams -n "$NAMESPACE" 2>/dev/null || true
fi

# Delete kind cluster
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    log "Deleting kind cluster '${CLUSTER_NAME}'..."
    kind delete cluster --name "$CLUSTER_NAME"
    log "Cluster deleted."
else
    log "kind cluster '${CLUSTER_NAME}' not found, nothing to delete."
fi
