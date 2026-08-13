// Package proxy 在 embedded 模式中提供受限的 Docker API 转发。请求先经过
// 身份验证和 SecurityValidator，只允许 Controller 工作流需要的路径与方法。
// Docker socket 等价于主机级权限，不能当成普通 HTTP 代理公开，也不能仅依赖
// 前端隐藏按钮来限制访问。
package proxy
