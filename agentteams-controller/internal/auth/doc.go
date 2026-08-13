// Package auth 为 Controller REST API 完成身份验证与权限判断。
//
// 请求中的 Bearer token 先通过 Kubernetes TokenReview 确认真伪，再将
// ServiceAccount 或管理员身份转换为 CallerIdentity，最后根据操作类型
// 和资源归属授权。验证回答会短暂缓存，但凭据轮换或资源删除时
// 必须使缓存失效，否则旧 token 可能在缓存期内继续获得权限。
package auth
