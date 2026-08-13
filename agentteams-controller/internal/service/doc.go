// Package service 编排 Matrix、Higress、OSS、凭据与配置部署等外部副作。
// controller 负责决定“期望与实际差什么”，service 负责把某一个差异
// 变成可执行的外部调用。
//
// 服务方法要么自身幂等，要么让调用者能在下一次 reconcile 查证结果。
// 网络超时只说明“没收到确定答案”，并不证明外部操作没发生。这就是
// 创建房间、注册账号和上传配置时需要稳定名称、查询与重试语义的原因。
package service
