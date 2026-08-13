// Package mail 提供 SMTP 邮件发送能力。连接、认证和发送都是外部
// 副作用，必须受 context 超时约束，并在错误中避免回显 SMTP 密码。
// 上层应把发送失败视为可诊断的外部错误，而不是修改 CR 期望状态。
package mail
