// Package humanidentity 把 Human CR 的身份来源解析为统一的 Matrix 身份。
// 调和器只关心最终 user ID、登录方式和是否管理初始密码，不需要为
// 传统密码与外部 SSO 在主流程中堆叠分支。身份一旦已经建立就是
// 房间成员关系的稳定键，不能在原 CR 上无声切换为另一个账号。
package humanidentity
