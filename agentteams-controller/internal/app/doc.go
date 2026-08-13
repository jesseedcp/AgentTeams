// Package app 是 Controller 的“组装根”：它根据配置创建存储、Kubernetes
// client、Matrix/Higress/OSS 客户端、reconciler 和 REST server，然后按正确
// 顺序启动与停止它们。
//
// 业务规则不应塞进这个包：app 只决定“用哪个实现、如何连接”，
// 具体状态收敛在 controller，外部副作用在 service。这种分工使单元测试
// 能替换依赖，也避免启动代码成为不可测试的巨大函数。
package app
