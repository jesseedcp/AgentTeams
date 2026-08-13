package controller

import (
	"context"
	"fmt"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reconcileManagerInfrastructure ensures Matrix account, Gateway consumer, MinIO user,
// Matrix room, and credentials are provisioned for the Manager. Idempotent: if already
// provisioned (MatrixUserID set), it refreshes credentials and restores gateway auth.
func (r *ManagerReconciler) reconcileManagerInfrastructure(ctx context.Context, s *managerScope) (reconcile.Result, error) {
	// 逻辑说明：reconcileManagerInfrastructure 接收 ctx(context.Context)、s(*managerScope)，依次借助 RefreshManagerCredentials、EnsureManagerGatewayAuth、ProvisionManager调谐Manager的期望结果。
	// 返回/状态：返回 reconcile.Result、error；会更新 Manager的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	m := s.manager

	if m.Status.MatrixUserID != "" {
		refreshResult, err := r.Provisioner.RefreshManagerCredentials(ctx, m.Name)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("refresh credentials: %w", err)
		}

		// Gateway auth errors must propagate so controller-runtime re-queues
		// the reconcile with backoff. Previously this was swallowed as
		// "non-fatal", which masked real failures (e.g. Higress Console PUT
		// returning non-200) and left the data plane stuck with an empty
		// allowedConsumers list until a subsequent event happened to retry.
		if err := r.Provisioner.EnsureManagerGatewayAuth(ctx, m.Name, refreshResult.GatewayKey); err != nil {
			return reconcile.Result{}, fmt.Errorf("restore manager gateway auth: %w", err)
		}

		s.provResult = &service.ManagerProvisionResult{
			MatrixUserID:   m.Status.MatrixUserID,
			MatrixToken:    refreshResult.MatrixToken,
			RoomID:         m.Status.RoomID,
			GatewayKey:     refreshResult.GatewayKey,
			MinIOPassword:  refreshResult.MinIOPassword,
			MatrixPassword: refreshResult.MatrixPassword,
		}
		return reconcile.Result{}, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("provisioning manager infrastructure", "name", m.Name)

	provResult, err := r.Provisioner.ProvisionManager(ctx, service.ManagerProvisionRequest{
		Name: m.Name,
	})
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("provision manager: %w", err)
	}

	m.Status.MatrixUserID = provResult.MatrixUserID
	m.Status.RoomID = provResult.RoomID
	s.provResult = provResult

	return reconcile.Result{}, nil
}
