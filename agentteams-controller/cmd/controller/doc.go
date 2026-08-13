// Package main 是 agentteams-controller 进程的启动入口。
//
// 它只负责读取配置、创建 App 并传递可取消的 context；Controller、
// REST API、存储和外部客户端的组装位于 internal/app。context 被取消
// 时，各后台任务应停止接收新工作并释放资源，避免进程退出时
// 留下半完成的写入。
package main
