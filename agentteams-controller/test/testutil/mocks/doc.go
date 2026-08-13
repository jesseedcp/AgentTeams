// Package mocks 提供 service/backend 接口的可观察测试替身。
// Reconciler 测试用它们记录“调用了什么”并预置成功或错误，从而验证
// 重试、幂等和状态写回，而不访问真实 Matrix、Higress、OSS 或容器。
// mock 能证明 Controller 在某个回答下做出正确决策，但不能取代真实
// 协议的集成测试。
package mocks
