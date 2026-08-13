// Package backend 抽象“在哪里运行 Agent 容器”，并提供 Docker、Kubernetes
// Pod 和 sandbox 等实现。
//
// Controller 将已经解析好的 CreateRequest 交给后端，后端负责创建、
// 查询、启停和删除实际运行载体。这些方法可能因网络超时在“实际
// 已成功，但未收到回答”的情况下返回错误，所以调用方必须在下一次
// reconcile 根据实际状态验证，而不能盲目重复创建。
package backend
