package controller

import (
	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type managerScope struct {
	manager           *v1beta1.Manager
	provResult        *service.ManagerProvisionResult
	patchBase         client.Patch
	modelProviderInfo *gateway.ModelProviderInfo
}

// computeManagerPhase determines the Manager status phase based on reconcile outcome.
// When reconcile succeeds, phase reflects the desired lifecycle state.
// When reconcile fails, phase depends on whether infrastructure was provisioned.
func computeManagerPhase(m *v1beta1.Manager, reconcileErr error) string {
	// 逻辑说明：computeManagerPhase 接收 m(*v1beta1.Manager)、reconcileErr(error)，依次借助 DesiredState计算Manager的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if reconcileErr != nil {
		if m.Status.MatrixUserID == "" {
			return "Failed"
		}
		if m.Status.Phase == "" {
			return "Pending"
		}
		return m.Status.Phase
	}
	return m.Spec.DesiredState()
}
