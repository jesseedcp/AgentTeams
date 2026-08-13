package controller

import (
	"context"
	"errors"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func (r *ManagerReconciler) reconcileManagerDelete(ctx context.Context, s *managerScope) (reconcile.Result, error) {
	// 逻辑说明：reconcileManagerDelete 接收 ctx(context.Context)、s(*managerScope)，依次借助 LeaveAllManagerRooms、DeleteManagerRoom、DeprovisionManager、managerBackend调谐Manager的期望结果。
	// 返回/状态：返回 reconcile.Result、error；会更新 Manager的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	logger := log.FromContext(ctx)
	m := s.manager
	logger.Info("deleting manager", "name", m.Name)

	managerName := m.Name

	if err := r.Provisioner.LeaveAllManagerRooms(ctx, managerName); err != nil {
		logger.Error(err, "manager leave-all-rooms failed (non-fatal)")
	}
	if m.Status.RoomID != "" {
		if err := r.Provisioner.DeleteManagerRoom(ctx, m.Status.RoomID); err != nil {
			logger.Error(err, "manager room delete command failed (non-fatal)",
				"roomID", m.Status.RoomID)
		}
	}

	if err := r.Provisioner.DeprovisionManager(ctx, managerName); err != nil {
		logger.Error(err, "deprovision failed (non-fatal)")
	}

	if wb := r.managerBackend(ctx); wb != nil {
		containerName := r.managerContainerName(managerName)
		if err := wb.Delete(ctx, containerName); err != nil && !errors.Is(err, backend.ErrNotFound) {
			logger.Error(err, "failed to delete manager container (may already be removed)")
		}
	}

	if err := r.Deployer.CleanupOSSData(ctx, managerName); err != nil {
		logger.Error(err, "failed to clean up OSS agent data (non-fatal)")
	}
	if err := r.Provisioner.DeleteCredentials(ctx, managerName); err != nil {
		logger.Error(err, "failed to delete credentials (non-fatal)")
	}
	if err := r.Provisioner.DeleteManagerServiceAccount(ctx, managerName); err != nil {
		logger.Error(err, "failed to delete ServiceAccount (non-fatal)")
	}

	// Release the Matrix alias that tied this Manager to its Admin DM room.
	// The room is preserved; only the controller's stable identifier is
	// released so a future Manager CR with the same name can reclaim it.
	if err := r.Provisioner.DeleteManagerRoomAlias(ctx, managerName); err != nil {
		logger.Error(err, "failed to delete manager room alias (non-fatal)")
	}

	controllerutil.RemoveFinalizer(m, finalizerName)
	if err := r.Update(ctx, m); err != nil {
		return reconcile.Result{}, err
	}

	logger.Info("manager deleted", "name", managerName)
	return reconcile.Result{}, nil
}
