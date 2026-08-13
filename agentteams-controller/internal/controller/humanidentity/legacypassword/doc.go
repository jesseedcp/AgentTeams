// Package legacypassword 注册传统用户名/密码形式的 Human 身份源。
// 该实现为现有部署保留兼容路径：初次创建可设置密码，稳态 reconcile
// 不能重置用户在 Cinny 中主动修改过的密码。
package legacypassword
