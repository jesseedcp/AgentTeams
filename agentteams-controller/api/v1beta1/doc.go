// Package v1beta1 定义 AgentTeams 在 Kubernetes 中的四类声明式资源：
// Worker、Team、Human 和 Manager。
//
// 可以把这些类型理解成 Controller 的“订单格式”。用户写入 spec
// 表达想要的状态，Controller 观察 Matrix、Higress、Pod 等系统的实际
// 状态，再把处理结果写入 status。spec 和 status 必须分开：如果把
// Controller 观察到的结果反写进 spec，就会把用户的期望与系统现状
// 混在一起，重试和故障恢复也将无法正确判断。
//
// 本包是 API 合约，字段名和 JSON tag 会被 CRD、REST API、agt CLI 以及
// Manager 共同使用。修改这里不只是修改 Go struct，还意味着修改持久化
// 格式和对外接口，因此需要同步生成物并考虑旧数据兼容性。
package v1beta1
