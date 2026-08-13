package service

import (
	"context"
	"fmt"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
)

// --- Port Exposure ---

// domainForExpose generates the auto domain name for a worker's exposed port.
func domainForExpose(workerName string, port int) string {
	return fmt.Sprintf("worker-%s-%d-local.agentteams.io", workerName, port)
}

// ContainerDNSName returns the FQDN for a worker container that Higress can resolve.
func ContainerDNSName(workerName string) string {
	return fmt.Sprintf("%s.local", workerName)
}

// ReconcileExpose compares desired expose ports with current status, creates new
// gateway resources for added ports, and removes resources for deleted ports.
func (p *Provisioner) ReconcileExpose(ctx context.Context, workerName string, desired []v1beta1.ExposePort, current []v1beta1.ExposedPortStatus) ([]v1beta1.ExposedPortStatus, error) {
	// 逻辑说明：ReconcileExpose 接收 ctx(context.Context)、workerName(string)、desired([]v1beta1.ExposePort)、current([]v1beta1.ExposedPortStatus)，依次借助 domainForExpose、ExposePort、ContainerDNSName、UnexposePort调谐Worker 端口路由的期望结果。
	// 返回/状态：返回 []v1beta1.ExposedPortStatus、error；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	if p.gateway == nil {
		return current, nil
	}

	desiredSet := make(map[int]v1beta1.ExposePort)
	for _, ep := range desired {
		desiredSet[ep.Port] = ep
	}
	currentSet := make(map[int]v1beta1.ExposedPortStatus)
	for _, ep := range current {
		currentSet[ep.Port] = ep
	}

	var result []v1beta1.ExposedPortStatus
	var firstErr error

	for _, ep := range desired {
		if _, exists := currentSet[ep.Port]; exists {
			result = append(result, currentSet[ep.Port])
			continue
		}

		domain := domainForExpose(workerName, ep.Port)
		err := p.gateway.ExposePort(ctx, gateway.PortExposeRequest{
			WorkerName:  workerName,
			ServiceHost: ContainerDNSName(workerName),
			Port:        ep.Port,
			Domain:      domain,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("expose port %d: %w", ep.Port, err)
			}
			continue
		}

		result = append(result, v1beta1.ExposedPortStatus{
			Port:   ep.Port,
			Domain: domain,
		})
	}

	for _, ep := range current {
		if _, stillDesired := desiredSet[ep.Port]; stillDesired {
			continue
		}

		err := p.gateway.UnexposePort(ctx, gateway.PortExposeRequest{
			WorkerName: workerName,
			Port:       ep.Port,
			Domain:     ep.Domain,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpose port %d: %w", ep.Port, err)
			}
		}
	}

	return result, firstErr
}
