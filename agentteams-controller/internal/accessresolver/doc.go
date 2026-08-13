// Package accessresolver 把 CR 中面向人的权限声明解析成凭据服务能执行
// 的精确授权。例如 ${self.name} 先替换为当前 Worker 名，再把“workspace”
// 这类逻辑名称解析为真实 bucket 或 gateway。这个边界不返回长期
// 密钥，只生成可用于申请最小权限临时凭据的条目。
package accessresolver
