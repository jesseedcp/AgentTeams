// Package gateway 封装 Controller 与 Higress/AI Gateway 之间的交互。它创建
// consumer、授权 AI route、解析 model provider 并管理对外服务路由。
//
// Controller 只保存“某个 Agent 应访问哪个 provider”的期望关系；真正
// 的模型路由和认证配置由 Higress 维护。网关配置传播可能是异步的，
// 所以“API 写入成功”不一定意味数据面已立即可用，reconcile 需要通过
// 查询或稍后重试来确认实际状态。
package gateway
