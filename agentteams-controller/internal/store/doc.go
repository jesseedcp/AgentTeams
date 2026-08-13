// Package store 为 embedded 部署模式提供持久化的 Kubernetes-compatible 存储。
// kine 将 client-go 使用的 etcd 协议映射到标准 SQLite 文件，因此本地
// 部署不需要额外 Redis 或独立 etcd 进程也能保存 CR 和 watch 修订号。
//
// SQLite 使用 WAL（write-ahead log）让读取不必在每次写入时停顿，
// busy timeout 则给短暂写锁冲突一个等待窗口。context 结束时必须让 kine
// 停止，否则可能在进程关闭期间仍保持数据库文件和端口。
package store
