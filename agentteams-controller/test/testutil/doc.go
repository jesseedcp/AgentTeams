// Package testutil 提供 Controller 集成测试的共用环境搭建工具。
// 它使用 controller-runtime envtest 启动轻量 Kubernetes API server/etcd，
// 让 reconciler 在真实 API 语义下测试 generation、status、finalizer 和 watch，
// 但不需要一个完整 Kubernetes 集群。它不会自动提供 Matrix、Higress
// 或真实 Pod，这些外部边界仍由 mock 代替。
package testutil
