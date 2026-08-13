// Package credprovider 封装外部 agentteams-credential-provider API，将已解析的
// AccessEntry 换成有限期、最小权限的数据面凭据。这里是 Controller 与
// 云厂商授权系统的边界：错误要带回足够语义供 reconcile 重试，
// 但日志和 CR status 不得包含真实凭据。
package credprovider
