// Package externalsso 注册外部 SSO 形式的 Human 身份源。这类用户的密码
// 与会话由外部身份系统管理，Controller 只根据稳定 subject 得到 Matrix
// 身份并同步房间成员关系，不应生成或持久化本地密码。
package externalsso
