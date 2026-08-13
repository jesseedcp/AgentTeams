// Package initializer 执行安装后只需准备一次或可安全重试的基础初始化，
// 例如 Matrix AppService、Cinny 配置和 MCP 初始资源。初始化与日常
// reconcile 不同，但仍必须幂等：Controller 重启时再执行不应生成第二份
// 相同配置或覆盖用户已经更改的状态。
package initializer
