// Package executor 处理需要与 Nacos 或本地进程交互的包和服务执行。
// 它把远程协议、身份凭据和命令执行细节隔离在 reconcile 之外。
// 所有输入都应作为结构化参数传递，尤其不得把用户值直接拼入 shell；
// context 的超时与取消用于防止外部系统挂起整个 Controller。
package executor
