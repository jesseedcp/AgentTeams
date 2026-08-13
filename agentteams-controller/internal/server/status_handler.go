package server

import (
	"fmt"
	"net/http"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusHandler handles /healthz, /api/v1/status, /api/v1/version.
type StatusHandler struct {
	k8s       client.Client
	namespace string
	kubeMode  string
}

func NewStatusHandler(k8s client.Client, namespace, kubeMode string) *StatusHandler {
	// 逻辑说明：保存状态查询所需的 Kubernetes client、作用域和部署模式；实际资源 List 延迟到受认证的 ClusterStatus 请求。
	return &StatusHandler{k8s: k8s, namespace: namespace, kubeMode: kubeMode}
}

func (h *StatusHandler) Healthz(w http.ResponseWriter, _ *http.Request) {
	// 逻辑说明：只证明 HTTP 进程能响应，不访问 Kubernetes 或外部依赖；调用方需要完整依赖状态时应使用受认证的 ClusterStatus。
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

type ClusterStatusResponse struct {
	KubeMode     string `json:"kubeMode"`
	TotalWorkers int    `json:"totalWorkers"`
	TotalTeams   int    `json:"totalTeams"`
	TotalHumans  int    `json:"totalHumans"`
}

func (h *StatusHandler) ClusterStatus(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：在同一 namespace 依次列出 Worker、Team、Human；任一种查询失败立即返回 500，全部成功才用权威列表长度和 kube mode 组成集群摘要。
	ctx := r.Context()

	var workers v1beta1.WorkerList
	if err := h.k8s.List(ctx, &workers, client.InNamespace(h.namespace)); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to list workers: "+err.Error())
		return
	}

	var teams v1beta1.TeamList
	if err := h.k8s.List(ctx, &teams, client.InNamespace(h.namespace)); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to list teams: "+err.Error())
		return
	}

	var humans v1beta1.HumanList
	if err := h.k8s.List(ctx, &humans, client.InNamespace(h.namespace)); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to list humans: "+err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusOK, ClusterStatusResponse{
		KubeMode:     h.kubeMode,
		TotalWorkers: len(workers.Items),
		TotalTeams:   len(teams.Items),
		TotalHumans:  len(humans.Items),
	})
}

type VersionResponse struct {
	Controller string `json:"controller"`
	KubeMode   string `json:"kubeMode"`
}

func (h *StatusHandler) Version(w http.ResponseWriter, _ *http.Request) {
	// 逻辑说明：返回当前构建标识与运行的 Kubernetes 模式，不发外部请求；响应可用于客户端判断部署形态而不泄露资源内容。
	httputil.WriteJSON(w, http.StatusOK, VersionResponse{
		Controller: "dev",
		KubeMode:   h.kubeMode,
	})
}
