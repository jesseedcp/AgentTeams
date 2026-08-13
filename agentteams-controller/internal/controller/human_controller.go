package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/controller/humanidentity"
	_ "github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/controller/humanidentity/externalsso"
	_ "github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/controller/humanidentity/legacypassword"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/matrix"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/metrics"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// HumanReconciler reconciles Human resources using Service-layer orchestration.
//
// Unlike Worker/Manager, a Human has no backend container and no gateway
// consumer: the reconciler's entire job is to keep a Matrix user plus a
// set of room memberships in sync with Spec.AccessibleWorkers/Teams.
type HumanReconciler struct {
	client.Client

	Provisioner service.HumanProvisioner
}

// Reconcile 将 Human spec 中的身份与可访问资源同步到 Matrix。
// 它不创建 Pod；一轮典型处理是解析用户身份、确保账号存在，再将
// 用户加入 desired rooms 并移出不再允许的 rooms。重复触发时结果必须相同。
func (r *HumanReconciler) Reconcile(ctx context.Context, req reconcile.Request) (retres reconcile.Result, reterr error) {
	// 逻辑说明：Reconcile 接收 ctx(context.Context)、req(reconcile.Request)，依次借助 Now、Observe、Get、IgnoreNotFound调谐Human的期望结果。
	// 返回/状态：返回 retres、reterr；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	start := time.Now()
	defer func() { metrics.Observe("human", start, reterr) }()

	logger := log.FromContext(ctx)

	var human v1beta1.Human
	if err := r.Get(ctx, req.NamespacedName, &human); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	patchBase := client.MergeFrom(human.DeepCopy())

	s := &humanScope{
		human:     &human,
		username:  human.Spec.EffectiveUsername(human.Name),
		patchBase: patchBase,
	}

	// Defer status patch so every phase writes through a single merge-patch
	// at the end of the reconcile loop. We skip the patch when the object
	// is being deleted — the finalizer cleanup path calls r.Update itself
	// and the CR may no longer exist by the time the defer runs.
	defer func() {
		if !human.DeletionTimestamp.IsZero() {
			return
		}

		if reterr == nil {
			if human.Status.Phase != "Degraded" {
				human.Status.Message = ""
			}
		} else {
			human.Status.Message = reterr.Error()
		}
		human.Status.Phase = computeHumanPhase(&human, reterr)

		if err := r.Status().Patch(ctx, &human, patchBase); err != nil {
			logger.Error(err, "failed to patch human status; CR will appear to have no status",
				"name", human.Name, "phase", human.Status.Phase, "matrixUserID", human.Status.MatrixUserID)
			reterr = kerrors.NewAggregate([]error{reterr, err})
			return
		}
		logger.Info("human status patched",
			"name", human.Name, "phase", human.Status.Phase,
			"matrixUserID", human.Status.MatrixUserID, "reconcileFailed", reterr != nil)
	}()

	if !human.DeletionTimestamp.IsZero() {
		// Matrix 账号和房间成员关系不属于 Kubernetes，ownerReference
		// 无法帮忙删除它们，因此需要 finalizer 执行外部清理。
		if controllerutil.ContainsFinalizer(&human, finalizerName) {
			if err := r.resolveHumanScope(s); err != nil && human.Status.MatrixUserID == "" {
				logger.Error(err, "failed to resolve deleting human identity; continuing best-effort cleanup", "name", human.Name)
			}
			return r.reconcileHumanDelete(ctx, s)
		}
		return reconcile.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&human, finalizerName) {
		base := human.DeepCopy()
		controllerutil.AddFinalizer(&human, finalizerName)
		if err := r.Patch(ctx, &human, client.MergeFrom(base)); err != nil {
			return reconcile.Result{}, err
		}
	}

	return r.reconcileHumanNormal(ctx, s)
}

// reconcileHumanNormal runs the declarative convergence loop. Phases in
// order: infrastructure (Matrix account), then rooms (membership). Only
// infrastructure is fatal; room reconciliation logs errors but never returns
// them, so a transient Matrix hiccup
// on room invite/kick does not block the next reconcile.
func (r *HumanReconciler) reconcileHumanNormal(ctx context.Context, s *humanScope) (reconcile.Result, error) {
	// 逻辑说明：reconcileHumanNormal 接收 ctx(context.Context)、s(*humanScope)，依次借助 resolveHumanScope、reconcileHumanInfra、Is、reconcileHumanRooms调谐Human的期望结果。
	// 返回/状态：返回 reconcile.Result、error；会更新 Human的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if err := r.resolveHumanScope(s); err != nil {
		s.human.Status.Phase = "Degraded"
		s.human.Status.Message = err.Error()
		return reconcile.Result{RequeueAfter: reconcileInterval}, nil
	}
	if err := r.reconcileHumanInfra(ctx, s); err != nil {
		if errors.Is(err, matrix.ErrAppServiceNotReady) {
			log.FromContext(ctx).Info("Matrix AppService not active yet; requeueing human provisioning",
				"name", s.human.Name)
			return reconcile.Result{RequeueAfter: appServiceNotReadyRequeue}, nil
		}
		return reconcile.Result{RequeueAfter: reconcileInterval}, err
	}
	r.reconcileHumanRooms(ctx, s)

	return reconcile.Result{RequeueAfter: reconcileInterval}, nil
}

func (r *HumanReconciler) resolveHumanScope(s *humanScope) error {
	// 逻辑说明：resolveHumanScope 接收 s(*humanScope)，依次借助 ResolveHuman解析Human的期望结果。
	// 返回/状态：返回 error；会更新 Human的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	resolved, err := humanidentity.ResolveHuman(&s.human.Spec, s.human.Name, humanidentity.Deps{
		Provisioner: r.Provisioner,
	})
	if err != nil {
		return err
	}
	// Once a Matrix account exists, the derived MXID is the human's stable
	// identity. Any change to it — switching to/from SSO, editing
	// identitySource.subject, or renaming the legacy username — means a
	// different account. Re-provisioning in place would leave Status.Rooms
	// pointing at the previous user's memberships, so the rooms phase would
	// treat them as already observed and never invite/join the new user,
	// leaving a Human that looks Active but whose new identity is in no
	// rooms. Block the switch and require recreating the CR instead.
	if s.human.Status.MatrixUserID != "" && s.human.Status.MatrixUserID != resolved.MatrixUserID {
		return fmt.Errorf("identitySource changed; recreate CR to switch identity")
	}
	s.identity = resolved
	s.username = resolved.MatrixLocalpart
	if !resolved.ManagesInitialPassword {
		s.human.Status.InitialPassword = ""
	}
	return nil
}

func (r *HumanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// 逻辑说明：SetupWithManager 接收 mgr(ctrl.Manager)，依次借助 Complete、For、NewControllerManagedBy设置Manager的期望结果。
	// 返回/状态：返回 error；会注册 watch、事件过滤器或对象到调谐请求的映射，不直接创建业务资源。
	// 失败/重试：注册失败会阻止 Controller 正常启动；事件处理本身由 controller-runtime 持续驱动。
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.Human{}).
		Complete(r)
}
