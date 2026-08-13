package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/k3s-io/kine/pkg/endpoint"
)

// Config holds kine/store configuration.
type Config struct {
	// DataDir is the directory for SQLite database.
	DataDir string
	// ListenAddress for the kine etcd-compatible endpoint.
	ListenAddress string
	// KubeMode: "embedded" (default, kine+SQLite) or "incluster" (real K8s API).
	KubeMode string
}

// KineServer wraps a running kine instance.
type KineServer struct {
	ETCDConfig endpoint.ETCDConfig
}

// StartKine starts an embedded kine server backed by SQLite.
// Returns ETCDConfig that can be used to connect via client-go.
// StartKine 启动一个由 SQLite 持久化的 embedded kine server，并返回
// client-go 可使用的 etcd-compatible 连接信息。
//
// DSN 中的 WAL 让读取在写入期间仍可继续，busy_timeout 则让短暂的写锁
// 冲突等待最多 30 秒，而不是立即把正常并发当作失败。这个数据库保存
// embedded Kubernetes API 的 CR 状态，与 AgentScope Manager 用于会话/操作的 SQLite
// 是不同职责，不应混用或互相直接读表。
func StartKine(ctx context.Context, cfg Config) (*KineServer, error) {
	// 逻辑说明：补齐数据目录和监听地址，确保目录存在后构造启用 WAL/shared cache/busy timeout 的 SQLite DSN；随后让 kine 绑定 context 启动 etcd 兼容端点，只有监听成功才返回连接配置。
	if cfg.DataDir == "" {
		cfg.DataDir = "/data/agentteams-controller"
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:2379"
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir %s: %w", cfg.DataDir, err)
	}

	dbPath := filepath.Join(cfg.DataDir, "agentteams.db")
	dsn := fmt.Sprintf("sqlite://%s?_journal=WAL&cache=shared&_busy_timeout=30000", dbPath)

	etcdCfg, err := endpoint.Listen(ctx, endpoint.Config{
		Listener:       cfg.ListenAddress,
		Endpoint:       dsn,
		NotifyInterval: time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start kine: %w", err)
	}

	return &KineServer{ETCDConfig: etcdCfg}, nil
}
