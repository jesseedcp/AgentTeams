// Package metrics 定义 AgentTeams Controller 的 Prometheus 指标。它记录各类
// reconcile 的数量、耗时和错误，以及当前 CR 数量，用于回答“系统
// 是真的停了，还是只在等待外部系统”。指标 label 必须保持低基数，
// 不应把 Worker 名、room ID 等无界值直接作为 label，否则会造成时序数爆炸。
package metrics
