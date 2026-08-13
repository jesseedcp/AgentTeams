package watcher

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/fsnotify/fsnotify"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

// FileWatcher monitors a local directory for YAML changes and syncs them to kine.
type FileWatcher struct {
	// WatchDir is the local directory mirrored from MinIO (e.g. /root/agentteams-fs/agentteams-config/)
	WatchDir string
	Client   client.Client

	// Track file content hashes to detect actual changes vs status-only writes
	mu     sync.Mutex
	hashes map[string]string // filepath → sha256 of spec portion
}

func New(watchDir string, c client.Client) *FileWatcher {
	// 逻辑说明：保存镜像目录与 Kubernetes client，并创建空的路径→spec 摘要表；摘要将在同步时防止 status 回写再次触发 reconcile。
	return &FileWatcher{
		WatchDir: watchDir,
		Client:   c,
		hashes:   make(map[string]string),
	}
}

// InitialSync performs a full scan of the watch directory and syncs all YAML files to kine.
func (w *FileWatcher) InitialSync(ctx context.Context) error {
	// 逻辑说明：递归扫描监控目录，只处理 yaml/yml 普通文件；单文件同步失败记日志后继续统计其余资源，目录遍历本身的最终错误返回给启动方。
	logger := log.FromContext(ctx).WithName("file-watcher")
	logger.Info("starting initial sync", "dir", w.WatchDir)

	count := 0
	err := filepath.WalkDir(w.WatchDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if d.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
			return nil
		}

		if syncErr := w.syncFile(ctx, path); syncErr != nil {
			logger.Error(syncErr, "failed to sync file during initial scan", "path", path)
		} else {
			count++
		}
		return nil
	})

	logger.Info("initial sync complete", "files", count)
	return err
}

// Watch starts the fsnotify watcher loop. Blocks until context is cancelled.
func (w *FileWatcher) Watch(ctx context.Context) error {
	// 逻辑说明：创建 fsnotify 并确保标准子目录存在，注册当前全部目录后进入单 goroutine 事件循环；YAML 事件按路径合并并延迟 500ms 批处理，context 取消或通道关闭时释放 watcher。
	logger := log.FromContext(ctx).WithName("file-watcher")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}
	defer watcher.Close()

	// Watch the base dir and all subdirectories
	// Ensure base dir and standard subdirectories exist (mc-mirror may not have created them yet)
	for _, subdir := range []string{"", "workers", "teams", "humans"} {
		dir := filepath.Join(w.WatchDir, subdir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create dir %s: %w", dir, err)
		}
	}

	dirs := []string{}
	filepath.WalkDir(w.WatchDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	for _, dir := range dirs {
		if err := watcher.Add(dir); err != nil {
			logger.Error(err, "failed to watch directory", "dir", dir)
		} else {
			logger.Info("watching directory", "dir", dir)
		}
	}

	logger.Info("watching for changes", "dir", w.WatchDir)

	// Debounce: collect events for 500ms before processing.
	// All access to pending happens in this single goroutine (no race).
	pending := make(map[string]fsnotify.Op)
	var debounceC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if !isYAMLFile(event.Name) {
				continue
			}

			pending[event.Name] = event.Op
			// Reset debounce timer
			debounceC = time.After(500 * time.Millisecond)

		case <-debounceC:
			w.processPending(ctx, pending)
			pending = make(map[string]fsnotify.Op)
			debounceC = nil

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			logger.Error(err, "fsnotify error")
		}
	}
}

func (w *FileWatcher) processPending(ctx context.Context, pending map[string]fsnotify.Op) {
	// 逻辑说明：逐个合并后的路径判断最终事件类型，删除/重命名走资源清理，创建/写入走 upsert；单个文件失败只记录，不阻断同批其他文件。
	logger := log.FromContext(ctx).WithName("file-watcher")

	for path, op := range pending {
		if op&(fsnotify.Remove|fsnotify.Rename) != 0 {
			if err := w.handleDelete(ctx, path); err != nil {
				logger.Error(err, "failed to handle delete", "path", path)
			}
		} else if op&(fsnotify.Create|fsnotify.Write) != 0 {
			if err := w.syncFile(ctx, path); err != nil {
				logger.Error(err, "failed to sync file", "path", path)
			}
		}
	}
}

// syncFile reads a YAML file, parses it, and upserts the resource into kine.
func (w *FileWatcher) syncFile(ctx context.Context, path string) error {
	// 逻辑说明：读取 YAML 并计算排除 status 的摘要，在锁内与旧摘要比较以切断 controller 状态回写循环；spec 有变化时按相对目录解析 kind/name，再分派对应 CR upsert。
	logger := log.FromContext(ctx).WithName("file-watcher")

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	// Check if spec content actually changed (ignore status-only updates)
	specHash := hashSpecPortion(data)
	w.mu.Lock()
	prevHash := w.hashes[path]
	if prevHash == specHash {
		w.mu.Unlock()
		return nil // no spec change, skip to avoid reconcile loop
	}
	w.hashes[path] = specHash
	w.mu.Unlock()

	// Determine kind from directory structure: agentteams-config/{kind}s/{name}.yaml
	kind, name := parsePathKindName(path, w.WatchDir)
	if kind == "" {
		return fmt.Errorf("cannot determine resource kind from path: %s", path)
	}

	logger.Info("syncing resource", "kind", kind, "name", name, "path", path)

	switch kind {
	case "worker":
		return w.upsertWorker(ctx, name, data)
	case "team":
		return w.upsertTeam(ctx, name, data)
	case "human":
		return w.upsertHuman(ctx, name, data)
	default:
		return fmt.Errorf("unknown resource kind: %s", kind)
	}
}

func (w *FileWatcher) upsertWorker(ctx context.Context, name string, data []byte) error {
	// 逻辑说明：解析 Worker YAML，以文件名作为权威名称并补默认 namespace；查询现存 CR，未取到时创建，取到时只替换 Spec 后更新，从而保留 resourceVersion 与 Status。
	var worker v1beta1.Worker
	if err := yaml.Unmarshal(data, &worker); err != nil {
		return fmt.Errorf("parse worker YAML: %w", err)
	}
	worker.Name = name
	if worker.Namespace == "" {
		worker.Namespace = "default"
	}

	existing := &v1beta1.Worker{}
	err := w.Client.Get(ctx, client.ObjectKeyFromObject(&worker), existing)
	if err != nil {
		// Not found → create
		return w.Client.Create(ctx, &worker)
	}
	// Found → update spec
	existing.Spec = worker.Spec
	return w.Client.Update(ctx, existing)
}

func (w *FileWatcher) upsertTeam(ctx context.Context, name string, data []byte) error {
	// 逻辑说明：解析 Team YAML，以文件路径纠正名称并补 default namespace；查询失败按新对象创建，存在时仅同步 Spec，避免文件内容覆盖服务端 Status/元数据。
	var team v1beta1.Team
	if err := yaml.Unmarshal(data, &team); err != nil {
		return fmt.Errorf("parse team YAML: %w", err)
	}
	team.Name = name
	if team.Namespace == "" {
		team.Namespace = "default"
	}

	existing := &v1beta1.Team{}
	err := w.Client.Get(ctx, client.ObjectKeyFromObject(&team), existing)
	if err != nil {
		return w.Client.Create(ctx, &team)
	}
	existing.Spec = team.Spec
	return w.Client.Update(ctx, existing)
}

func (w *FileWatcher) upsertHuman(ctx context.Context, name string, data []byte) error {
	// 逻辑说明：解析 Human YAML，使用文件名/default namespace 定位对象；不存在则创建，存在则把期望 Spec 写入原对象并更新，以保留并发控制字段。
	var human v1beta1.Human
	if err := yaml.Unmarshal(data, &human); err != nil {
		return fmt.Errorf("parse human YAML: %w", err)
	}
	human.Name = name
	if human.Namespace == "" {
		human.Namespace = "default"
	}

	existing := &v1beta1.Human{}
	err := w.Client.Get(ctx, client.ObjectKeyFromObject(&human), existing)
	if err != nil {
		return w.Client.Create(ctx, &human)
	}
	existing.Spec = human.Spec
	return w.Client.Update(ctx, existing)
}

// handleDelete removes a resource from kine when its YAML file is deleted.
func (w *FileWatcher) handleDelete(ctx context.Context, path string) error {
	// 逻辑说明：先在锁内移除文件摘要，再从路径推导 CR 类型与名称；对已知类型删除 default namespace 对象并忽略 NotFound，使重复文件事件保持幂等。
	logger := log.FromContext(ctx).WithName("file-watcher")

	w.mu.Lock()
	delete(w.hashes, path)
	w.mu.Unlock()

	kind, name := parsePathKindName(path, w.WatchDir)
	if kind == "" {
		return nil
	}

	logger.Info("deleting resource", "kind", kind, "name", name)

	switch kind {
	case "worker":
		obj := &v1beta1.Worker{}
		obj.Name = name
		obj.Namespace = "default"
		return client.IgnoreNotFound(w.Client.Delete(ctx, obj))
	case "team":
		obj := &v1beta1.Team{}
		obj.Name = name
		obj.Namespace = "default"
		return client.IgnoreNotFound(w.Client.Delete(ctx, obj))
	case "human":
		obj := &v1beta1.Human{}
		obj.Name = name
		obj.Namespace = "default"
		return client.IgnoreNotFound(w.Client.Delete(ctx, obj))
	}
	return nil
}

// parsePathKindName extracts kind and name from path like:
// /root/agentteams-fs/agentteams-config/workers/alice.yaml → ("worker", "alice")
func parsePathKindName(path, watchDir string) (kind, name string) {
	// 逻辑说明：把绝对路径转成监控根目录下的两段相对路径，仅接受 workers/teams/humans 目录，并去掉文件扩展名生成对应单数 kind 与资源名。
	rel, err := filepath.Rel(watchDir, path)
	if err != nil {
		return "", ""
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 2 {
		return "", ""
	}

	// Directory name is plural (workers, teams, humans) → singular kind
	dirName := parts[0]
	fileName := strings.TrimSuffix(parts[1], filepath.Ext(parts[1]))

	switch dirName {
	case "workers":
		return "worker", fileName
	case "teams":
		return "team", fileName
	case "humans":
		return "human", fileName
	}
	return "", ""
}

// hashSpecPortion computes a hash of the YAML content excluding status fields,
// so that status-only writes by the reconciler don't trigger re-sync.
func hashSpecPortion(data []byte) string {
	// 逻辑说明：逐行排除顶层 status 块，遇到下一顶层字段恢复收集，再对其余 YAML 计算截短 SHA-256；因此纯状态更新不会被误判为期望 Spec 变化。
	// Simple approach: hash everything before "status:" line
	lines := strings.Split(string(data), "\n")
	var specLines []string
	inStatus := false
	for _, line := range lines {
		if strings.HasPrefix(line, "status:") {
			inStatus = true
			continue
		}
		if inStatus && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			inStatus = false // exited status block
		}
		if !inStatus {
			specLines = append(specLines, line)
		}
	}
	h := sha256.Sum256([]byte(strings.Join(specLines, "\n")))
	return fmt.Sprintf("%x", h[:8])
}

func isYAMLFile(path string) bool {
	return strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")
}
