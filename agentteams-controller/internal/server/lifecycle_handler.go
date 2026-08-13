package server

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LifecycleHandler handles imperative worker lifecycle operations.
type LifecycleHandler struct {
	k8s       client.Client
	registry  *backend.Registry
	namespace string

	readyMu sync.RWMutex
	ready   map[string]bool
}

func NewLifecycleHandler(k8s client.Client, registry *backend.Registry, namespace string) *LifecycleHandler {
	// 逻辑说明：保存 CR client、运行时后端注册表与 namespace，并初始化进程内 readiness 表；该表仅作即时信号，权威期望状态仍在 Worker Spec。
	return &LifecycleHandler{
		k8s:       k8s,
		registry:  registry,
		namespace: namespace,
		ready:     make(map[string]bool),
	}
}

// Wake handles POST /api/v1/workers/{name}/wake
func (h *LifecycleHandler) Wake(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：读取 Worker 后先把 Spec 期望状态持久化为 Running 触发 reconcile，再尽力直接启动后端以降低延迟；清空 readiness 并尽力刷新 Status，最后返回已接受的生命周期目标。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	// Set desired state in spec (declarative, triggers reconciler)
	running := "Running"
	worker.Spec.State = &running
	if err := h.k8s.Update(r.Context(), &worker); err != nil {
		writeK8sError(w, "update worker spec.state", err)
		return
	}

	// Directly operate on backend for immediate response
	b := h.registry.DetectWorkerBackend(r.Context())
	if b != nil {
		_ = b.Start(r.Context(), name)
	}

	h.setReady(name, false)

	// Refresh and update status
	_ = h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
	worker.Status.Phase = "Running"
	worker.Status.Message = ""
	_ = h.k8s.Status().Update(r.Context(), &worker)

	httputil.WriteJSON(w, http.StatusOK, WorkerLifecycleResponse{Name: name, Phase: "Running"})
}

// Sleep handles POST /api/v1/workers/{name}/sleep
func (h *LifecycleHandler) Sleep(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：先将 Worker Spec 写为 Sleeping，再尽力立即停止后端；无论后端即时调用是否成功都由 reconcile 继续收敛，同时清除 readiness 并尽力更新展示状态。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	// Set desired state in spec (declarative, triggers reconciler)
	sleeping := "Sleeping"
	worker.Spec.State = &sleeping
	if err := h.k8s.Update(r.Context(), &worker); err != nil {
		writeK8sError(w, "update worker spec.state", err)
		return
	}

	// Directly operate on backend for immediate response
	b := h.registry.DetectWorkerBackend(r.Context())
	if b != nil {
		_ = b.Stop(r.Context(), name)
	}

	h.setReady(name, false)

	// Refresh and update status
	_ = h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
	worker.Status.Phase = "Sleeping"
	worker.Status.Message = ""
	_ = h.k8s.Status().Update(r.Context(), &worker)

	httputil.WriteJSON(w, http.StatusOK, WorkerLifecycleResponse{Name: name, Phase: "Sleeping"})
}

// EnsureReady handles POST /api/v1/workers/{name}/ensure-ready
func (h *LifecycleHandler) EnsureReady(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：读取 CR 后只对 Stopped/Sleeping worker 改写 Running 并尝试即时启动，启动失败留给 reconciler 重建；响应阶段结合 CR phase 与进程内 ready 信号区分 Running/Ready。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	if worker.Status.Phase == "Stopped" || worker.Status.Phase == "Sleeping" {
		// Set desired state in spec (declarative)
		running := "Running"
		worker.Spec.State = &running
		if err := h.k8s.Update(r.Context(), &worker); err != nil {
			writeK8sError(w, "update worker spec.state", err)
			return
		}

		// Directly operate on backend for immediate response
		b := h.registry.DetectWorkerBackend(r.Context())
		if b != nil {
			if err := b.Start(r.Context(), name); err != nil {
				// Start may fail if container/pod was removed (Stopped state on K8s).
				// The reconciler will handle recreation.
				log.Printf("[WARN] ensure-ready start worker %s: %v (reconciler will retry)", name, err)
			}
		}

		h.setReady(name, false)

		// Refresh and update status
		_ = h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
		worker.Status.Phase = "Running"
		worker.Status.Message = ""
		_ = h.k8s.Status().Update(r.Context(), &worker)
	}

	phase := worker.Status.Phase
	if phase == "Running" && h.isReady(name) {
		phase = "Ready"
	}

	httputil.WriteJSON(w, http.StatusOK, WorkerLifecycleResponse{Name: name, Phase: phase})
}

// Ready handles POST /api/v1/workers/{name}/ready — worker self-reports readiness.
func (h *LifecycleHandler) Ready(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：名称通过后把已由授权中间件确认的 worker 标为进程内 ready，并以 204 应答；不直接篡改 CR 期望状态。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	// Authorization (self-only for workers) is enforced by RequireAuthz middleware.
	h.setReady(name, true)
	log.Printf("[READY] Worker %s reported ready", name)
	w.WriteHeader(http.StatusNoContent)
}

// Heartbeat handles POST /api/v1/workers/{name}/heartbeat. A successful
// heartbeat refreshes liveness and may advance (but never move backward)
// the runtime-reported business activity timestamp.
func (h *LifecycleHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：允许空正文或校验可选 RFC3339 活动时间，用 RetryOnConflict 重读并更新权威 heartbeat；业务活动时间只前进不回退，成功后恢复内存 readiness。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var payload struct {
		LastActiveAt string `json:"lastActiveAt,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		httputil.WriteError(w, http.StatusBadRequest, "invalid heartbeat request")
		return
	}
	if payload.LastActiveAt != "" {
		if _, err := time.Parse(time.RFC3339, payload.LastActiveAt); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "lastActiveAt must be RFC3339")
			return
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var worker v1beta1.Worker
		if err := h.k8s.Get(
			r.Context(),
			client.ObjectKey{Name: name, Namespace: h.namespace},
			&worker,
		); err != nil {
			return err
		}
		worker.Status.LastHeartbeat = now
		if isLastActiveNewer(payload.LastActiveAt, worker.Status.LastActiveAt) {
			worker.Status.LastActiveAt = payload.LastActiveAt
		}
		return h.k8s.Status().Update(r.Context(), &worker)
	})
	if err != nil {
		writeK8sError(w, "update worker heartbeat", err)
		return
	}

	// A heartbeat is stronger evidence of readiness than the one-shot ready
	// event and restores the in-memory ready flag after a controller restart.
	h.setReady(name, true)
	w.WriteHeader(http.StatusNoContent)
}

// GetWorkerRuntimeStatus handles GET /api/v1/workers/{name}/status — aggregates CR + backend state.
func (h *LifecycleHandler) GetWorkerRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：先从 CR 合成声明式状态，再向检测到的后端查询实际容器/Pod 状态补充消息；只有后端 Running 且收到 ready/heartbeat 时才把响应 phase 提升为 Ready。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	resp := workerToResponse(&worker)

	b := h.registry.DetectWorkerBackend(r.Context())
	if b != nil {
		result, err := b.Status(r.Context(), name)
		if err == nil && result != nil {
			resp.Message = "backend=" + result.Backend + " status=" + string(result.Status)
			if result.Message != "" {
				resp.Message += " message=" + result.Message
			}
			resp.ContainerState = string(result.Status)
			if result.Status == backend.StatusRunning && h.isReady(name) {
				resp.Phase = "Ready"
			}
		}
	}

	httputil.WriteJSON(w, http.StatusOK, resp)
}

// --- readiness helpers ---

func (h *LifecycleHandler) setReady(name string, ready bool) {
	// 逻辑说明：写锁下仅保存 ready=true 的名称，false 时删除键；这样 map 不积累长期离线 worker，读写可并发安全。
	h.readyMu.Lock()
	defer h.readyMu.Unlock()
	if ready {
		h.ready[name] = true
	} else {
		delete(h.ready, name)
	}
}

func (h *LifecycleHandler) isReady(name string) bool {
	// 逻辑说明：读锁下查询进程内 readiness，允许并发状态请求与 heartbeat；缺失键自然返回 false，Controller 重启后会等待下一次 ready/heartbeat 重建信号。
	h.readyMu.RLock()
	defer h.readyMu.RUnlock()
	return h.ready[name]
}

func writeBackendError(w http.ResponseWriter, err error) {
	// 逻辑说明：用 errors.Is 识别可包装的后端 NotFound/Conflict 并映射 404/409，其余内部故障返回 500，保持所有生命周期 handler 的错误语义一致。
	switch {
	case errors.Is(err, backend.ErrNotFound):
		httputil.WriteError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, backend.ErrConflict):
		httputil.WriteError(w, http.StatusConflict, err.Error())
	default:
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
	}
}
