package backend

import (
	"context"
	"fmt"
)

// DefaultContainerPrefix is the baked-in worker container/pod prefix.
// New constructors no longer force this fallback when prefix is empty;
// LoadConfig controls defaulting through AGENTTEAMS_RESOURCE_AUTOPREFIX and
// AGENTTEAMS_RESOURCE_PREFIX.
const DefaultContainerPrefix = "agentteams-worker-"

// Registry holds all available worker backends and provides auto-detection.
//
// Historically the registry also tracked a GatewayBackend slice, but
// gateway selection moved to a dedicated gateway.Client implementation
// (HigressClient / AIGatewayClient) wired directly in app/app.go.
type Registry struct {
	workerBackends []WorkerBackend
}

// NewRegistry creates a Registry with the given worker backends.
func NewRegistry(workers []WorkerBackend) *Registry {
	return &Registry{workerBackends: workers}
}

// DetectWorkerBackend returns the first available worker backend.
// Priority is determined by registration order (set in buildBackends):
//  1. Docker backend (socket available)
//  2. K8s backend (incluster mode)
//  3. nil
func (r *Registry) DetectWorkerBackend(ctx context.Context) WorkerBackend {
	// 逻辑说明：按注册顺序调用 Available 并返回第一个可用 Worker backend；全部不可用返回 nil，因此顺序同时表达 Docker/K8s/Sandbox 优先级。
	for _, b := range r.workerBackends {
		if b.Available(ctx) {
			return b
		}
	}
	return nil
}

// FindServiceBackend returns the first available backend that implements
// ServiceBackend, or nil if none qualifies.
func (r *Registry) FindServiceBackend(ctx context.Context) ServiceBackend {
	// 逻辑说明：遍历 backend，筛出同时实现 ServiceBackend 且当前可用的第一个实例；没有能够管理 Kubernetes Service 的后端时返回 nil。
	for _, b := range r.workerBackends {
		if sb, ok := b.(ServiceBackend); ok && b.Available(ctx) {
			return sb
		}
	}
	return nil
}

// GetWorkerBackend returns a specific worker backend by name, or auto-detects if name is empty.
func (r *Registry) GetWorkerBackend(ctx context.Context, name string) (WorkerBackend, error) {
	// 逻辑说明：名称为空时走自动探测，否则按精确 backend 名称查找；无可用或未知名称返回明确错误，不静默换用其他运行载体。
	if name == "" {
		b := r.DetectWorkerBackend(ctx)
		if b == nil {
			return nil, fmt.Errorf("no worker backend available")
		}
		return b, nil
	}
	for _, b := range r.workerBackends {
		if b.Name() == name {
			return b, nil
		}
	}
	return nil, fmt.Errorf("unknown worker backend: %q", name)
}

// GetBackendForType returns the backend for the given backendRuntime type.
// "pod" maps to the "k8s" backend; "sandbox" maps to the "sandbox" backend.
// Returns nil, error if the requested backend is not registered/available.
func (r *Registry) GetBackendForType(ctx context.Context, backendRuntime string) (WorkerBackend, error) {
	// 逻辑说明：把 CR 中 `pod` 类型规范成实现名 `k8s`，再要求名称匹配且 Available；找不到时同时报告规范名与原始类型，避免错误回退。
	targetName := backendRuntime
	if backendRuntime == "pod" {
		targetName = "k8s"
	}
	for _, b := range r.workerBackends {
		if b.Name() == targetName && b.Available(ctx) {
			return b, nil
		}
	}
	return nil, fmt.Errorf("backend %q (backendRuntime=%q) not available", targetName, backendRuntime)
}
