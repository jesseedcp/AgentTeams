// Package httputil 提供 Controller HTTP handler 共用的 JSON 回答工具。它统一
// Content-Type、状态码和错误结构，使 agt CLI 与 AgentScope Manager 不需要
// 为每个端点猜测不同回答格式。错误回答不应包含 token、密码或
// 上游系统返回的敏感原文。
package httputil
