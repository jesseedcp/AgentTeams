// Package remoteclient 缓存远程 Kubernetes 集群的 client 与 informer。
// 它避免每次 reconcile 都重新建立连接，同时监视 kubeconfig/凭据变化并
// 替换过期 client。缓存寿命受 context 管理：旧实例被替换或 Controller 退出时，
// 对应 watch 必须停止，否则会泄漏 goroutine 和连接。
package remoteclient
