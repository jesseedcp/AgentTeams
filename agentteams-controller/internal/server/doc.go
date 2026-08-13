// Package server 对外提供 Controller REST API，是 agt CLI 和 AgentScope Manager
// 进入声明式资源层的边界。
//
// handler 负责认证、校验 JSON、读写 CR 与转换 HTTP 错误，不负责在
// 一次请求里同步创建所有外部资源。例如 POST /workers 成功只表示
// Worker CR 已写入，后续由 controller 逐步把实际状态收敛到 spec。可以
// 取消的 request context 会向下传播，但已提交的 CR 不因客户端断开而回滚。
package server
