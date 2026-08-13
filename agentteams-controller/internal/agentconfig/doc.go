// Package agentconfig 把 Controller 中统一的 Worker/Manager 设定生成不同 Agent
// runtime 可读取的配置文件。
//
// 生成器不创建 Pod，也不直接调用 Matrix 或 Higress；它只进行可预测的
// 纯数据转换。这样同一份 spec 在重复 reconcile 时会得到稳定输出，
// 避免配置每次都变化并触发无意义的容器重启。配置中只写无密文
// 或可以安全持久化的引用，真实凭据由运行时环境提供。
package agentconfig
