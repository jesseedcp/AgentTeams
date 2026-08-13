// Package main 实现 agt 命令行客户端。
//
// 以“agt create worker demo”为例：Cobra 先解析参数，客户端再把结构化
// JSON 发给 Controller REST API。REST API 只写入 Worker CR，真正创建 Matrix
// 账号、配置和容器的是后续 reconcile。因此 CLI 返回“已创建资源”
// 不等于 Worker 已经 Ready，要通过 get/status 继续观察 status。
//
// AgentScope Manager 也通过这个类型化 CLI 调用 Controller。参数必须作为
// argv 原样传递，不应拼成 shell 命令字符串，否则用户输入会带来
// 注入风险，也会丢失明确的类型与错误语义。
package main
