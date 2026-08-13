// Package sandbox 定义可插拔的隔离运行环境，并选择 OpenKruise 等具体
// 实现。sandbox 和普通 Pod 都承载 Worker，但 sandbox 可以把生命周期、
// 卷和远程调度委托给专门的运行时。注册表把名称映射到实现，
// 让 Controller 不需要在 reconcile 中写大量分支。
package sandbox
