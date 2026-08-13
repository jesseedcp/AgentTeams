// Package apiserver 在 embedded 部署模式中启动一个本地 Kubernetes-compatible
// API server。它让 Controller 在没有外部 Kubernetes 集群时，仍能用同一套
// CR、client-go 和 reconcile 逻辑管理资源。其底层状态通过 kine 保存到
// SQLite，而 incluster 模式则直接使用真实 Kubernetes API。
package apiserver
