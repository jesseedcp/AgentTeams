// Package controller 实现 AgentTeams 的声明式控制循环。
//
// 以 Worker 为例：用户把想要的模型、runtime 和成员关系写入 spec；
// Reconcile 每次被调用时都重新读取当前资源，比较期望状态与 Matrix、
// Higress、存储和容器的实际状态，只补齐缺失的部分，最后把观察
// 结果写入 status。
//
// Reconcile 不是只执行一次的“创建流程”：事件重复、进程重启、定时
// 检查和 status 更新都可能再次触发它。因此每个外部操作都必须幂等，
// 即重复执行不会创建第二个账号、房间或 Pod。删除使用 finalizer，
// 先清理 Kubernetes 之外的资源，再允许 CR 真正消失。
package controller
