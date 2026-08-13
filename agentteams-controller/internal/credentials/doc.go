// Package credentials 签发 Controller 内置 STS 临时凭据。临时凭据把权限
// 限定到已验证的 Worker/Manager 身份和较短有效期，避免向 Agent 分发
// Controller 或管理员的长期密钥。context 取消会中断上游请求，防止
// 客户端已经离开后继续做无意义的外部调用。
package credentials
