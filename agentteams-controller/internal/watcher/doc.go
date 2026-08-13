// Package watcher 监听配置或凭据文件变化，把外部轮换转换成进程内
// 的重新加载事件。文件系统通知可能重复、合并，也可能因“写临时文件
// 再重命名”表现为多个事件，因此上层回调必须允许重复执行。context
// 被取消时 watcher 必须关闭，防止泄漏文件句柄和 goroutine。
package watcher
