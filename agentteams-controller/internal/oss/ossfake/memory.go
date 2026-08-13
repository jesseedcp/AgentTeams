// Package ossfake provides in-memory fakes of the oss.StorageClient interface
// for use in unit and integration tests that exercise code paths dependent on
// object storage (package handler uploads, runtime configuration, etc.).
//
// The Memory client stores objects in a map keyed by their full object path.
// Paths are treated as opaque strings — there is no bucket/prefix logic, so
// tests see the exact keys that the production code passes in.
//
// 这是测试专用的内存实现，不是生产存储。它保留与真实 OSS 客户端
// 相同的接口，让配置部署和上传测试不需要 MinIO 进程。它不模拟网络、
// 认证或最终一致性，因此通过这些测试不等于真实 OSS 集成一定正常。
package ossfake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
)

// Memory is an in-memory implementation of oss.StorageClient suitable for
// tests. All methods are safe for concurrent use.
type Memory struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemory constructs an empty in-memory storage client.
func NewMemory() *Memory {
	// 逻辑说明：为测试构造独立的空对象 map；每个实例互不共享状态，后续并发访问由 Memory 自身的读写锁保护。
	return &Memory{objects: make(map[string][]byte)}
}

// PutObject stores data under key.
func (m *Memory) PutObject(_ context.Context, key string, data []byte) error {
	// 逻辑说明：写锁保护 map，并复制输入字节再保存，防止调用方随后修改原切片绕过锁改变“已写入”的对象内容。
	m.mu.Lock()
	defer m.mu.Unlock()
	buf := make([]byte, len(data))
	copy(buf, data)
	m.objects[key] = buf
	return nil
}

// PutFile reads a local file and stores its contents under key.
func (m *Memory) PutFile(ctx context.Context, localPath, key string) error {
	// 逻辑说明：完整读取本地文件后复用 PutObject 的加锁与复制语义；读取失败保留路径上下文且不创建半个对象。
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", localPath, err)
	}
	return m.PutObject(ctx, key, data)
}

// GetObject returns the bytes stored under key. Returns os.ErrNotExist when
// the key is missing to match the production MinIO client's behavior.
func (m *Memory) GetObject(_ context.Context, key string) ([]byte, error) {
	// 逻辑说明：读锁下查找对象，缺失时与生产实现一致返回 os.ErrNotExist；命中后复制字节再返回，避免外部修改内部存储。
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

// Stat returns nil when key exists, os.ErrNotExist otherwise. ManagerConfigStore
// relies on errors.Is(err, os.ErrNotExist) to detect first-time writes.
func (m *Memory) Stat(_ context.Context, key string) error {
	// 逻辑说明：读锁下只检查键是否存在，不复制内容；缺失统一返回 os.ErrNotExist，模拟上层首次写入判断所依赖的生产契约。
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.objects[key]; !ok {
		return os.ErrNotExist
	}
	return nil
}

// DeleteObject removes the object stored under key. Deleting a missing key
// is a no-op.
func (m *Memory) DeleteObject(_ context.Context, key string) error {
	// 逻辑说明：写锁下直接删除键；Go map 对缺失键删除无副作用，因此自然保持幂等语义。
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

// Mirror copies every object under src to dst by swapping the src prefix for
// dst. Local filesystem sources/destinations are not supported by the fake —
// both src and dst must be in-memory prefixes. MirrorOptions.Exclude is
// currently ignored; Overwrite=true is implicit (existing destination keys
// are replaced).
func (m *Memory) Mirror(_ context.Context, src, dst string, _ oss.MirrorOptions) error {
	// 逻辑说明：写锁下规范源/目标前缀，扫描源树并把相对后缀映射到目标；每份内容都复制后写入，避免源目标对象共享可变字节切片。
	m.mu.Lock()
	defer m.mu.Unlock()
	src = strings.TrimSuffix(src, "/")
	dst = strings.TrimSuffix(dst, "/")
	for key, data := range m.objects {
		if key != src && !strings.HasPrefix(key, src+"/") {
			continue
		}
		rel := strings.TrimPrefix(key, src)
		newKey := dst + rel
		buf := make([]byte, len(data))
		copy(buf, data)
		m.objects[newKey] = buf
	}
	return nil
}

// ListObjects returns all keys whose names start with prefix, sorted.
func (m *Memory) ListObjects(_ context.Context, prefix string) ([]string, error) {
	// 逻辑说明：读锁下收集所有匹配前缀的完整键，并排序后返回，使并发安全的 fake 也为测试提供确定顺序。
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0)
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

// DeletePrefix removes every object whose key starts with prefix.
func (m *Memory) DeletePrefix(_ context.Context, prefix string) error {
	// 逻辑说明：写锁下扫描并删除所有匹配键；没有匹配项时仍返回成功，复现生产清理可安全重试的行为。
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			delete(m.objects, key)
		}
	}
	return nil
}

// EnsureBucket is a no-op for the in-memory fake.
func (m *Memory) EnsureBucket(_ context.Context) error { return nil }

// Ensure Memory satisfies the interface at compile time.
var _ oss.StorageClient = (*Memory)(nil)
var _ oss.BucketManager = (*Memory)(nil)

// ErrNotExist is re-exported for tests that want to match against the exact
// sentinel returned by GetObject/Stat without importing "os".
var ErrNotExist = errors.New("object does not exist")
