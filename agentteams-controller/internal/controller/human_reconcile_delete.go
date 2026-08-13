package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reconcileHumanDelete cleans up best-effort external state before
// removing the finalizer. The human has no container, no gateway
// consumer, and no MinIO account — only Matrix room memberships. We can't log in as the
// human to /leave (password may be stale), so we rely on the Tuwunel
// admin bot's force-leave-room command instead.
//
// Every external call here is non-fatal: a transient Matrix or OSS
// failure must not wedge finalizer removal, and the homeserver's
// delete_rooms_after_leave / forget_forced_upon_leave flags provide a
// safety net if any force-leave never lands.
func (r *HumanReconciler) reconcileHumanDelete(ctx context.Context, s *humanScope) (reconcile.Result, error) {
	// 逻辑说明：reconcileHumanDelete 接收 ctx(context.Context)、s(*humanScope)，依次借助 ForceLeaveRoom、EnsureDeactivated、DeepCopy、RemoveFinalizer调谐Human的期望结果。
	// 返回/状态：返回 reconcile.Result、error；会更新 Human的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	logger := log.FromContext(ctx)
	h := s.human
	logger.Info("deleting human", "name", h.Name)

	humanUserID := h.Status.MatrixUserID
	if humanUserID == "" {
		humanUserID = s.identity.MatrixUserID
	}
	for _, roomID := range h.Status.Rooms {
		if err := r.Provisioner.ForceLeaveRoom(ctx, humanUserID, roomID); err != nil {
			logger.Error(err, "force-leave-room failed (non-fatal)",
				"user", humanUserID, "roomID", roomID)
		}
	}

	if s.identity.Source != nil {
		if err := s.identity.Source.EnsureDeactivated(ctx, &h.Spec, &h.Status); err != nil {
			return reconcile.Result{RequeueAfter: reconcileInterval}, err
		}
	}

	base := h.DeepCopy()
	controllerutil.RemoveFinalizer(h, finalizerName)
	if err := r.Patch(ctx, h, client.MergeFrom(base)); err != nil {
		return reconcile.Result{}, err
	}

	logger.Info("human deleted", "name", h.Name)
	return reconcile.Result{}, nil
}
