package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/auth"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/metrics"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	finalizerName         = "agentteams.io/cleanup"
	reconcileInterval     = 5 * time.Minute
	edgeReconcileInterval = 1 * time.Minute
	edgeHeartbeatTimeout  = 2 * time.Minute
	reconcileRetryDelay   = 30 * time.Second
	// appServiceNotReadyRequeue is the short backoff used while the
	// controller's Matrix AppService token has not been registered/verified
	// with the homeserver yet (transient startup race, M_UNKNOWN_TOKEN).
	appServiceNotReadyRequeue = 5 * time.Second
)

// WorkerReconciler reconciles standalone Worker resources. Team members are
// owned by Team CRs and are reconciled by TeamReconciler through the shared
// member_reconcile helpers, not by WorkerReconciler.
type WorkerReconciler struct {
	client.Client

	Provisioner                 service.WorkerProvisioner
	Deployer                    service.WorkerDeployer
	Backend                     *backend.Registry
	EnvBuilder                  service.WorkerEnvBuilderI
	ResourcePrefix              auth.ResourcePrefix         // tenant prefix used to derive SA names
	ManagerConfig               *service.ManagerConfigStore // nil in incluster mode
	GatewayClient               gateway.Client              // gateway client for modelProvider resolution
	DynamicClient               dynamic.Interface
	RemoteDynamicClientProvider backend.RemoteDynamicClientProvider
	AuthTokenExpirationSeconds  int64

	// DefaultRuntime is the value passed to backend.CreateRequest.RuntimeFallback
	// when a Worker CR omits spec.runtime. Sourced from
	// AGENTTEAMS_DEFAULT_WORKER_RUNTIME (Config.DefaultWorkerRuntime). Empty means
	// "no operator preference" — backend.ResolveRuntime will fall back to
	// "openclaw".
	DefaultRuntime string

	// DefaultBackendRuntime is the cluster-level default backendRuntime ("pod" or "sandbox").
	// Used when Worker CR's spec.backendRuntime is not set.
	// Sourced from AGENTTEAMS_WORKER_BACKEND_RUNTIME env var.
	DefaultBackendRuntime string

	// ControllerName identifies this controller instance. Stamped on every
	// Pod/SA/Secret created under this reconciler via the
	// agentteams.io/controller label so multiple controller instances sharing a
	// namespace do not cross-watch each other's resources.
	ControllerName string

	// AuthCache is cleared after deleting a rotated Edge Worker's
	// ServiceAccount so old SA tokens cannot pass via cached TokenReview.
	AuthCache interface{ InvalidateCache() }

	// WorkerDepsStorageBucket/Endpoint identify the main workspace OSS bucket
	// used for the built-in sandbox token/env/data mounts.
	WorkerDepsStorageBucket   string
	WorkerDepsStorageEndpoint string
	MountAuthType             string
	MountRoleName             string
}

// Reconcile 将一个独立 Worker 的实际状态逐步收敛到 spec。
//
// Kubernetes 可能因 CR 更新、Pod 事件、定时重排或 Controller 重启而重复
// 调用本函数。所以这里不假定“上一步一定没做过”，而是每次都
// 从 CR 和外部系统重新获取现状。ctx 在该轮 reconcile 被取消或超时时
// 传递给下游，防止旧轮次在新轮次开始后继续修改外部状态。
func (r *WorkerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (retres reconcile.Result, reterr error) {
	// 逻辑说明：Reconcile 接收 ctx(context.Context)、req(reconcile.Request)，依次借助 Now、Observe、Get、IgnoreNotFound调谐Worker 成员的期望结果。
	// 返回/状态：返回 retres、reterr；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	start := time.Now()
	defer func() { metrics.Observe("worker", start, reterr) }()

	logger := log.FromContext(ctx)

	var worker v1beta1.Worker
	if err := r.Get(ctx, req.NamespacedName, &worker); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	patchBase := client.MergeFrom(worker.DeepCopy())

	// Shared MemberState captured by the defer so phase computation can
	// observe the actual container state recorded during reconcile.
	state := &MemberState{}

	// Unified status patch at the end of every reconcile. ObservedGeneration
	// is only written when reconcile succeeds, preventing the infinite-loop
	// bug where a failed status write triggered re-reconcile with
	// Generation != ObservedGeneration.
	defer func() {
		if !worker.DeletionTimestamp.IsZero() {
			return
		}
		if isEdgeWorker(&worker) && reterr == nil {
			if edgeHeartbeatStale(worker.Status.LastHeartbeat, edgeHeartbeatTimeout) {
				worker.Status.Phase = "Pending"
			} else if worker.Status.Phase == "" {
				worker.Status.Phase = "Pending"
			}
		} else {
			worker.Status.Phase = computeWorkerPhase(&worker, state.ContainerState, reterr)
		}
		// generation 每次修改 spec 都会增加。只有整轮无错误时才更新
		// ObservedGeneration，否则前端会误以为失败的新 spec 已生效。
		if reterr == nil {
			worker.Status.ObservedGeneration = worker.Generation
			worker.Status.Message = state.Message
		} else {
			worker.Status.Message = reterr.Error()
		}
		if err := r.Status().Patch(ctx, &worker, patchBase); err != nil {
			logger.Error(err, "failed to patch worker status")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	if !worker.DeletionTimestamp.IsZero() {
		// Kubernetes 删除带 finalizer 的 CR 时会先设置 DeletionTimestamp，
		// 但保留对象。这给 Controller 机会先删 Matrix 别名、凭据和
		// 容器；清理成功后移除 finalizer，CR 才会真正消失。
		if controllerutil.ContainsFinalizer(&worker, finalizerName) {
			return r.reconcileDelete(ctx, &worker)
		}
		return reconcile.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&worker, finalizerName) {
		base := worker.DeepCopy()
		controllerutil.AddFinalizer(&worker, finalizerName)
		if err := r.Patch(ctx, &worker, client.MergeFrom(base)); err != nil {
			return reconcile.Result{}, err
		}
	}

	return r.reconcileNormal(ctx, &worker, state)
}

func isEdgeWorker(w *v1beta1.Worker) bool {
	return w != nil && w.Spec.DeployMode != nil && *w.Spec.DeployMode == v1beta1.DeployModeEdge
}

func edgeHeartbeatStale(lastHeartbeat string, timeout time.Duration) bool {
	// 逻辑说明：edgeHeartbeatStale 接收 lastHeartbeat(string)、timeout(time.Duration)，依次借助 Parse、Since处理Worker 成员的期望结果。
	// 返回/状态：返回 bool；会更新 Worker 成员的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if lastHeartbeat == "" {
		return true
	}
	ts, err := time.Parse(time.RFC3339, lastHeartbeat)
	if err != nil {
		return true
	}
	return time.Since(ts) > timeout
}

// reconcileNormal builds a MemberContext from the Worker CR, runs the shared
// member reconcile phases, and writes runtime state back to Worker.Status.
func (r *WorkerReconciler) reconcileNormal(ctx context.Context, w *v1beta1.Worker, state *MemberState) (reconcile.Result, error) {
	// 逻辑说明：reconcileNormal 接收 ctx(context.Context)、w(*v1beta1.Worker)、state(*MemberState)，依次借助 effectiveWorkerSpec、validateWorkerDeploymentTargetImmutable、workerMemberContextWithSpec、workerTeamName调谐Worker 成员的期望结果。
	// 返回/状态：返回 reconcile.Result、error；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	logger := log.FromContext(ctx)

	deps := MemberDeps{
		Provisioner:                 r.Provisioner,
		Deployer:                    r.Deployer,
		Backend:                     r.Backend,
		EnvBuilder:                  r.EnvBuilder,
		ResourcePrefix:              r.ResourcePrefix,
		DefaultRuntime:              r.DefaultRuntime,
		GatewayClient:               r.GatewayClient,
		DynamicClient:               r.DynamicClient,
		RemoteDynamicClientProvider: r.RemoteDynamicClientProvider,
		AuthTokenExpirationSeconds:  r.AuthTokenExpirationSeconds,
		ControllerName:              r.ControllerName,
		WorkerDepsStorageBucket:     r.WorkerDepsStorageBucket,
		WorkerDepsStorageEndpoint:   r.WorkerDepsStorageEndpoint,
		MountAuthType:               r.MountAuthType,
		MountRoleName:               r.MountRoleName,
	}
	effectiveSpec, resourceSpec, updateStrategy, err := r.effectiveWorkerSpec(ctx, w, false)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := validateWorkerDeploymentTargetImmutable(w, effectiveSpec); err != nil {
		return reconcile.Result{}, err
	}
	mctx := r.workerMemberContextWithSpec(w, effectiveSpec, resourceSpec, updateStrategy)
	mctx.TeamName, err = r.workerTeamName(ctx, w)
	if err != nil {
		return reconcile.Result{}, err
	}
	teamRole, inTeam, err := r.teamRoleForWorker(ctx, w.Namespace, w.Name)
	if err != nil {
		return reconcile.Result{}, err
	}
	configOwnedByTeam := inTeam && backend.ResolveRuntime(effectiveSpec.Runtime, r.DefaultRuntime) == backend.RuntimeQwenPaw

	if effectiveSpec.ModelProvider != "" && r.GatewayClient != nil {
		info, err := r.GatewayClient.ResolveModelProvider(ctx, effectiveSpec.ModelProvider)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("resolve model provider %q: %w", effectiveSpec.ModelProvider, err)
		}
		mctx.ModelProviderInfo = info
	}
	configContext := mctx
	if inTeam && teamRole == RoleTeamLeader {
		configContext.Role = RoleTeamLeader
	}

	if mctx.DeployMode == v1beta1.DeployModeEdge {
		// Edge UUID rotation: when the UUID label changes, delete the SA so any
		// previously issued long-lived tokens are invalidated. The next call to
		// EdgeHandler.ExchangeToken will recreate the SA and mint a fresh token
		// bound to the new UUID. Skipped on first issuance (appliedUUID empty).
		currentUUID := w.Labels[v1beta1.LabelWorkerEdgeUUID]
		appliedUUID := w.Annotations[v1beta1.AnnotationEdgeAppliedUUID]
		if currentUUID != "" && appliedUUID != "" && currentUUID != appliedUUID {
			if err := r.Provisioner.DeleteServiceAccount(ctx, w.Name); err != nil {
				logger.Error(err, "failed to delete SA during edge UUID rotation")
				return reconcile.Result{}, err
			}
			if r.AuthCache != nil {
				r.AuthCache.InvalidateCache()
			}
			if w.Annotations == nil {
				w.Annotations = make(map[string]string)
			}
			w.Annotations[v1beta1.AnnotationEdgeAppliedUUID] = currentUUID
			if err := r.Update(ctx, w); err != nil {
				return reconcile.Result{}, err
			}
			logger.Info("edge UUID rotated, SA deleted", "oldUUID", appliedUUID, "newUUID", currentUUID)
		}
		// Edge workers run off-cluster: the controller does not manage Pods,
		// Services, or Expose for them. SA lifecycle is driven on demand by
		// EdgeHandler.ExchangeToken. The lightweight controller path still
		// provisions Matrix/gateway credentials and writes runtime.yaml for the
		// remote-managed local worker.
		if res, err := ReconcileMemberInfra(ctx, deps, mctx, state); err != nil || res.RequeueAfter > 0 {
			applyMemberStateToWorker(w, state)
			return res, err
		}
		if err := EnsureModelProviderAuth(ctx, deps, mctx, state); err != nil {
			applyMemberStateToWorker(w, state)
			return reconcile.Result{}, err
		}
		if !configOwnedByTeam {
			if err := ReconcileMemberConfig(ctx, deps, configContext, state); err != nil {
				applyMemberStateToWorker(w, state)
				return reconcile.Result{}, err
			}
		} else {
			logger.Info("worker runtime config owned by TeamReconciler, skipping standalone config reconcile", "worker", w.Name, "team", mctx.TeamName)
		}
		applyMemberStateToWorker(w, state)
		w.Status.SpecHash = mctx.AppliedSpecHash
		return reconcile.Result{RequeueAfter: edgeReconcileInterval}, nil
	}

	// Validate cross-cluster deployment fields before entering phases.
	if err := ValidateMemberDeployment(mctx); err != nil {
		return reconcile.Result{}, err
	}

	if res, err := ReconcileMemberInfra(ctx, deps, mctx, state); err != nil || res.RequeueAfter > 0 {
		applyMemberStateToWorker(w, state)
		return res, err
	}
	if err := EnsureModelProviderAuth(ctx, deps, mctx, state); err != nil {
		applyMemberStateToWorker(w, state)
		return reconcile.Result{}, err
	}
	if err := EnsureMemberServiceAccount(ctx, deps, mctx); err != nil {
		applyMemberStateToWorker(w, state)
		return reconcile.Result{}, err
	}
	if !configOwnedByTeam {
		if err := ReconcileMemberConfig(ctx, deps, configContext, state); err != nil {
			applyMemberStateToWorker(w, state)
			return reconcile.Result{}, err
		}
	} else {
		logger.Info("worker runtime config owned by TeamReconciler, skipping standalone config reconcile", "worker", w.Name, "team", mctx.TeamName)
	}
	if res, err := ReconcileMemberContainer(ctx, deps, mctx, state); err != nil || res.RequeueAfter > 0 {
		applyMemberStateToWorker(w, state)
		return res, err
	}
	applyDeploymentTargetStatus(w, mctx)
	svcName, err := ReconcileMemberService(ctx, &mctx, &deps)
	if err != nil {
		applyMemberStateToWorker(w, state)
		return reconcile.Result{}, err
	}
	// Stamp or remove the service-name label on the Worker CR.
	// IMPORTANT: snapshot base BEFORE mutating w so MergeFrom produces
	// a non-empty patch — capturing base after the mutation makes the
	// diff identical and the label change never lands.
	base := w.DeepCopy()
	if labelChanged := reconcileWorkerSvcLabel(w, svcName); labelChanged {
		if err := r.Patch(ctx, w, client.MergeFrom(base)); err != nil {
			return reconcile.Result{}, fmt.Errorf("patch worker svc label: %w", err)
		}
	}
	_ = ReconcileMemberExpose(ctx, deps, mctx, state)
	applyMemberStateToWorker(w, state)
	w.Status.SpecHash = mctx.AppliedSpecHash
	applyDeploymentTargetStatus(w, mctx)

	r.reconcileManagerAccess(ctx, w, mctx, state)

	if w.Status.ObservedGeneration == 0 {
		logger.Info("worker created", "name", w.Name, "roomID", w.Status.RoomID)
	} else if w.Generation != w.Status.ObservedGeneration {
		logger.Info("worker updated", "name", w.Name)
	}

	requeueAfter := minPositiveDuration(reconcileInterval, state.RequeueAfter)
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

func (r *WorkerReconciler) workerTeamName(ctx context.Context, w *v1beta1.Worker) (string, error) {
	// 逻辑说明：workerTeamName 接收 ctx(context.Context)、w(*v1beta1.Worker)，依次借助 List、InNamespace、Slice、EffectiveTeamName处理Team的期望结果。
	// 返回/状态：返回 Worker 当前所属的唯一 Team 名称；只读取 Team 列表，发现重复归属时返回错误且不改状态。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if teamName := w.Annotations[v1beta1.AnnotationWorkerTeamName]; teamName != "" {
		return teamName, nil
	}
	var teams v1beta1.TeamList
	if err := r.List(ctx, &teams, client.InNamespace(w.Namespace)); err != nil {
		return "", fmt.Errorf("list teams for worker %q: %w", w.Name, err)
	}
	sort.Slice(teams.Items, func(i, j int) bool {
		return teams.Items[i].Name < teams.Items[j].Name
	})
	for _, team := range teams.Items {
		for _, member := range team.Spec.WorkerMembers {
			if member.Name == w.Name {
				return team.Spec.EffectiveTeamName(team.Name), nil
			}
		}
	}
	return "", nil
}

// reconcileDelete cleans up all infrastructure for the Worker and then removes
// the finalizer.
func (r *WorkerReconciler) reconcileDelete(ctx context.Context, w *v1beta1.Worker) (reconcile.Result, error) {
	// 逻辑说明：reconcileDelete 接收 ctx(context.Context)、w(*v1beta1.Worker)，依次借助 teamRoleForWorker、effectiveWorkerSpec、workerSpecWithAppliedDeploymentTarget、workerMemberContextWithSpec调谐Worker 成员的期望结果。
	// 返回/状态：返回 reconcile.Result、error；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	logger := log.FromContext(ctx)
	logger.Info("deleting worker", "name", w.Name)

	if _, inTeam, err := r.teamRoleForWorker(ctx, w.Namespace, w.Name); err != nil {
		return reconcile.Result{}, err
	} else if inTeam {
		logger.Info("worker deletion blocked while referenced by Team", "name", w.Name)
		return reconcile.Result{RequeueAfter: reconcileInterval}, nil
	}

	deps := MemberDeps{
		Provisioner:                 r.Provisioner,
		Deployer:                    r.Deployer,
		Backend:                     r.Backend,
		EnvBuilder:                  r.EnvBuilder,
		ResourcePrefix:              r.ResourcePrefix,
		DefaultRuntime:              r.DefaultRuntime,
		GatewayClient:               r.GatewayClient,
		DynamicClient:               r.DynamicClient,
		RemoteDynamicClientProvider: r.RemoteDynamicClientProvider,
		AuthTokenExpirationSeconds:  r.AuthTokenExpirationSeconds,
		ControllerName:              r.ControllerName,
		WorkerDepsStorageBucket:     r.WorkerDepsStorageBucket,
		WorkerDepsStorageEndpoint:   r.WorkerDepsStorageEndpoint,
		MountAuthType:               r.MountAuthType,
		MountRoleName:               r.MountRoleName,
	}
	effectiveSpec, resourceSpec, updateStrategy, err := r.effectiveWorkerSpec(ctx, w, true)
	if err != nil {
		return reconcile.Result{}, err
	}
	effectiveSpec = workerSpecWithAppliedDeploymentTarget(effectiveSpec, w.Status)
	mctx := r.workerMemberContextWithSpec(w, effectiveSpec, resourceSpec, updateStrategy)

	_ = ReconcileMemberDelete(ctx, deps, mctx)

	if r.ManagerConfig != nil && r.ManagerConfig.Enabled() {
		workerMatrixID := r.Provisioner.MatrixUserID(w.Name)
		if err := r.ManagerConfig.UpdateManagerGroupAllowFrom(workerMatrixID, false); err != nil {
			logger.Error(err, "failed to update Manager groupAllowFrom (non-fatal)")
		}
	}

	base := w.DeepCopy()
	controllerutil.RemoveFinalizer(w, finalizerName)
	if err := r.Patch(ctx, w, client.MergeFrom(base)); err != nil {
		return reconcile.Result{}, err
	}

	logger.Info("worker deleted", "name", w.Name)
	return reconcile.Result{}, nil
}

// reconcileManagerAccess grants standalone Workers publish rights into the
// Manager's group DM room and removes that right for non-leader Team members.
func (r *WorkerReconciler) reconcileManagerAccess(ctx context.Context, w *v1beta1.Worker, mctx MemberContext, state *MemberState) {
	// 逻辑说明：reconcileManagerAccess 接收 ctx(context.Context)、w(*v1beta1.Worker)、mctx(MemberContext)、state(*MemberState)，依次借助 Enabled、teamRoleForWorker、ForceLeaveRoom、MatrixUserID调谐Manager的期望结果。
	// 返回/状态：返回 无；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	if r.ManagerConfig == nil || !r.ManagerConfig.Enabled() {
		return
	}
	logger := log.FromContext(ctx)
	runtimeName := mctx.RuntimeName

	role, inTeam, err := r.teamRoleForWorker(ctx, w.Namespace, w.Name)
	if err != nil {
		logger.Error(err, "failed to check Team membership before Manager access update (non-fatal)", "worker", w.Name)
	}
	if inTeam {
		// TeamReconciler owns Team-scoped Manager access and channel policies.
		if role != RoleTeamLeader {
			if w.Status.RoomID != "" {
				if err := r.Provisioner.ForceLeaveRoom(
					ctx,
					r.ManagerConfig.MatrixUserID("manager"),
					w.Status.RoomID,
				); err != nil {
					logger.Error(err, "failed to remove Manager from Team worker personal room (non-fatal)", "worker", w.Name, "roomID", w.Status.RoomID)
				}
			}
			if err := r.ManagerConfig.UpdateManagerGroupAllowFrom(r.ManagerConfig.MatrixUserID(runtimeName), false); err != nil {
				logger.Error(err, "failed to revoke standalone Manager groupAllowFrom for Team worker (non-fatal)", "worker", w.Name, "runtimeName", runtimeName)
			}
		}
		return
	}

	// WorkerReconciler only handles standalone workers. Grant group-DM
	// publish rights for the standalone worker.
	if state.ProvResult != nil {
		if err := r.ManagerConfig.UpdateManagerGroupAllowFrom(state.ProvResult.MatrixUserID, true); err != nil {
			logger.Error(err, "failed to update Manager groupAllowFrom (non-fatal)")
		}
	}

}

func (r *WorkerReconciler) teamRoleForWorker(ctx context.Context, namespace, workerName string) (MemberRole, bool, error) {
	// 逻辑说明：teamRoleForWorker 接收 ctx(context.Context)、namespace/workerName(string)，依次借助 teamMembershipForWorker处理Team的期望结果。
	// 返回/状态：返回 Worker 在 Team 中的角色、是否属于团队及查询错误；这里只读取关系，不修改 Worker 或 Team。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	role, _, inTeam, err := r.teamMembershipForWorker(ctx, namespace, workerName)
	return role, inTeam, err
}

func (r *WorkerReconciler) teamMembershipForWorker(
	ctx context.Context,
	namespace string,
	workerName string,
) (MemberRole, string, bool, error) {
	// 逻辑说明：teamMembershipForWorker 接收 ctx(context.Context)、namespace(string)、workerName(string)，依次借助 List、InNamespace、EffectiveTeamName处理Team的期望结果。
	// 返回/状态：返回 MemberRole、string、bool、error；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	if r.Client == nil || workerName == "" {
		return "", "", false, nil
	}

	var teams v1beta1.TeamList
	if err := r.List(ctx, &teams, client.InNamespace(namespace)); err != nil {
		return "", "", false, fmt.Errorf("list teams: %w", err)
	}

	for _, team := range teams.Items {
		for _, ref := range team.Spec.WorkerMembers {
			if ref.Name != workerName {
				continue
			}
			teamName := team.Spec.EffectiveTeamName(team.Name)
			if ref.Role == RoleTeamLeader.String() {
				return RoleTeamLeader, teamName, true, nil
			}
			return RoleTeamWorker, teamName, true, nil
		}
	}
	return "", "", false, nil
}

func (r *WorkerReconciler) effectiveWorkerSpec(_ context.Context, w *v1beta1.Worker, _ bool) (v1beta1.WorkerSpec, *v1beta1.AgentResourceRequirements, string, error) {
	// 逻辑说明：effectiveWorkerSpec 接收 _(context.Context)、w(*v1beta1.Worker)、_(bool)，依次借助 DeepCopy处理Worker 成员的期望结果。
	// 返回/状态：返回 v1beta1.WorkerSpec、*v1beta1.AgentResourceRequirements、string、error；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	spec := *w.Spec.DeepCopy()
	return spec, spec.Resources, "", nil
}

func validateWorkerDeploymentTargetImmutable(w *v1beta1.Worker, desired v1beta1.WorkerSpec) error {
	// 逻辑说明：validateWorkerDeploymentTargetImmutable 接收 w(*v1beta1.Worker)、desired(v1beta1.WorkerSpec)，依次借助 workerSpecDeploymentMode校验Worker 成员的期望结果。
	// 返回/状态：返回 error；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if w.Status.DeployMode == "" {
		return nil
	}
	currentMode := w.Status.DeployMode
	if currentMode == "" {
		currentMode = v1beta1.DeployModeLocal
	}
	desiredMode := workerSpecDeploymentMode(desired)
	if currentMode != desiredMode {
		return fmt.Errorf("spec.deployMode cannot be changed after the Worker runtime has been provisioned; delete and recreate the Worker to move it (current=%s, desired=%s)",
			currentMode,
			desiredMode)
	}
	return nil
}

func workerSpecWithAppliedDeploymentTarget(spec v1beta1.WorkerSpec, status v1beta1.WorkerStatus) v1beta1.WorkerSpec {
	// 逻辑说明：workerSpecWithAppliedDeploymentTarget 接收 spec(v1beta1.WorkerSpec)、status(v1beta1.WorkerStatus)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 v1beta1.WorkerSpec；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if status.DeployMode == "" {
		return spec
	}
	mode := status.DeployMode
	if mode == "" {
		mode = v1beta1.DeployModeLocal
	}
	spec.DeployMode = &mode
	return spec
}

func applyDeploymentTargetStatus(w *v1beta1.Worker, m MemberContext) {
	// 逻辑说明：applyDeploymentTargetStatus 接收 w(*v1beta1.Worker)、m(MemberContext)，按本函数中的条件与转换步骤应用Worker 成员的期望结果。
	// 返回/状态：返回 无；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	w.Status.DeployMode = m.DeployMode
}

func workerSpecDeploymentMode(spec v1beta1.WorkerSpec) string {
	// 逻辑说明：workerSpecDeploymentMode 接收 spec(v1beta1.WorkerSpec)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 string；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	mode := v1beta1.DeployModeLocal
	if spec.DeployMode != nil && *spec.DeployMode != "" {
		mode = *spec.DeployMode
	}
	return mode
}

func agentResourcesToBackend(resources *v1beta1.AgentResourceRequirements) *backend.ResourceRequirements {
	// 逻辑说明：agentResourcesToBackend 接收 resources(*v1beta1.AgentResourceRequirements)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 *backend.ResourceRequirements；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	if resources == nil ||
		(resources.Requests.CPU == "" &&
			resources.Requests.Memory == "" &&
			resources.Limits.CPU == "" &&
			resources.Limits.Memory == "") {
		return nil
	}
	return &backend.ResourceRequirements{
		CPURequest:    resources.Requests.CPU,
		CPULimit:      resources.Limits.CPU,
		MemoryRequest: resources.Requests.Memory,
		MemoryLimit:   resources.Limits.Memory,
	}
}

func mergeBackendResourceRequirements(defaults, override *backend.ResourceRequirements) *backend.ResourceRequirements {
	// 逻辑说明：mergeBackendResourceRequirements 接收 defaults/override(*backend.ResourceRequirements)，按本函数中的条件与转换步骤合并Worker 成员的期望结果。
	// 返回/状态：返回 *backend.ResourceRequirements；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if override == nil {
		return defaults
	}
	if defaults == nil {
		return override
	}
	merged := *defaults
	if override.CPURequest != "" {
		merged.CPURequest = override.CPURequest
	}
	if override.CPULimit != "" {
		merged.CPULimit = override.CPULimit
	}
	if override.MemoryRequest != "" {
		merged.MemoryRequest = override.MemoryRequest
	}
	if override.MemoryLimit != "" {
		merged.MemoryLimit = override.MemoryLimit
	}
	return &merged
}

// workerMemberContext translates a Worker CR into a MemberContext for the
// shared member reconcile helpers. WorkerReconciler always produces a
// standalone context — team semantics are injected externally by
// TeamReconciler via Matrix Room invite and MinIO AGENTS.MD, never via
// Worker CR annotations.
//
// PodLabels are built by layering four sources low-to-high: ConfigMap-based
// pod template (added downstream by ApplyPodTemplate), the CR's
// metadata.labels, the CR's spec.labels, and the controller-forced system
// labels (controller name and member role). Controller-forced keys
// deliberately come last so anything the user writes that collides (e.g.
// `agentteams.io/controller`) is silently overridden rather than rejected.
func (r *WorkerReconciler) workerMemberContext(w *v1beta1.Worker) MemberContext {
	return r.workerMemberContextWithSpec(w, w.Spec, nil, "")
}

func (r *WorkerReconciler) workerMemberContextWithSpec(w *v1beta1.Worker, spec v1beta1.WorkerSpec, resourceSpec *v1beta1.AgentResourceRequirements, updateStrategy string) MemberContext {
	// 逻辑说明：workerMemberContextWithSpec 接收 w(*v1beta1.Worker)、spec(v1beta1.WorkerSpec)、resourceSpec(*v1beta1.AgentResourceRequirements)、updateStrategy(string)，依次借助 EffectiveWorkerName、ResolveRuntime、GetBackendRuntime、workerSpecWithEffectiveBackendRuntimeForHash处理Worker 成员的期望结果。
	// 返回/状态：返回 MemberContext；会更新 Worker 成员的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	runtimeName := spec.EffectiveWorkerName(w.Name)
	effectiveRuntime := backend.ResolveRuntime(spec.Runtime, r.DefaultRuntime)
	backendRuntime := spec.GetBackendRuntime()
	if backendRuntime == "" {
		backendRuntime = r.DefaultBackendRuntime
	}
	hashSpec := workerSpecWithEffectiveBackendRuntimeForHash(spec, backendRuntime)
	appliedSpecHash := hashAppliedWorkerSpecForRuntimeAndResources(hashSpec, effectiveRuntime, resourceSpec)
	// Only pod-affecting fields trigger recreation. A Worker without a recorded
	// spec hash follows the normal create path and records it after success.
	specChanged := w.Status.SpecHash != "" && w.Status.SpecHash != appliedSpecHash

	// Cross-cluster deployment fields.
	deployMode := v1beta1.DeployModeLocal
	if spec.DeployMode != nil {
		deployMode = *spec.DeployMode
	}
	var serviceEnabled bool
	if spec.ServiceEnabled != nil {
		serviceEnabled = *spec.ServiceEnabled
	}

	systemLabels := map[string]string{
		v1beta1.LabelController: r.ControllerName,
		v1beta1.LabelRole:       RoleStandalone.String(),
	}
	return MemberContext{
		Name:               w.Name,
		RuntimeName:        runtimeName,
		Namespace:          w.Namespace,
		Role:               RoleStandalone,
		Spec:               spec,
		Generation:         w.Generation,
		ObservedGeneration: w.Status.ObservedGeneration,
		PodLabels: mergeLabels(
			w.ObjectMeta.Labels,
			spec.Labels,
			systemLabels,
		),
		// SpecChanged is gated on ObservedGeneration > 0 so brand-new
		// Workers go through StatusNotFound create instead of a transient
		// spec-change delete. CurrentSpecHash lets sandbox read managerConfig live
		// annotations only when Worker.status.specHash is empty.
		SpecChanged:          specChanged,
		AppliedSpecHash:      appliedSpecHash,
		CurrentSpecHash:      w.Status.SpecHash,
		IsUpdate:             w.Status.Phase != "" && w.Status.Phase != "Pending" && w.Status.Phase != "Failed",
		ExistingMatrixUserID: w.Status.MatrixUserID,
		ExistingRoomID:       w.Status.RoomID,
		CurrentExposedPorts:  w.Status.ExposedPorts,
		Owner:                w,
		DeployMode:           deployMode,
		ServiceEnabled:       serviceEnabled,
		Resources:            agentResourcesToBackend(resourceSpec),
		BackendRuntime:       backendRuntime,
		StatusBackendRuntime: w.Status.BackendRuntime,
	}
}

// applyMemberStateToWorker copies runtime state into Worker.Status fields.
// Phase, ObservedGeneration, Message are owned by the deferred patch in
// Reconcile; this helper only touches infra/runtime fields.
func applyMemberStateToWorker(w *v1beta1.Worker, state *MemberState) {
	// 逻辑说明：applyMemberStateToWorker 接收 w(*v1beta1.Worker)、state(*MemberState)，按本函数中的条件与转换步骤应用Worker 成员的期望结果。
	// 返回/状态：返回 无；会更新 Worker 成员的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if state == nil {
		return
	}
	if state.MatrixUserID != "" {
		w.Status.MatrixUserID = state.MatrixUserID
	}
	if state.RoomID != "" {
		w.Status.RoomID = state.RoomID
	}
	if state.ContainerState != "" {
		w.Status.ContainerState = state.ContainerState
	}
	if state.ExposedPorts != nil || len(w.Spec.Expose) == 0 {
		w.Status.ExposedPorts = state.ExposedPorts
	}
	if state.BackendRuntime != "" {
		w.Status.BackendRuntime = state.BackendRuntime
	}
}

// reconcileWorkerSvcLabel adds or removes the worker Service name
// label on the Worker CR. Returns true if the label set was modified.
func reconcileWorkerSvcLabel(w *v1beta1.Worker, svcName string) bool {
	// 逻辑说明：reconcileWorkerSvcLabel 接收 w(*v1beta1.Worker)、svcName(string)，依次借助 delete调谐Worker 成员的期望结果。
	// 返回/状态：返回 bool；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	if svcName != "" {
		if w.Labels == nil {
			w.Labels = make(map[string]string)
		}
		if w.Labels[v1beta1.LabelWorkerSvcName] == svcName {
			return false
		}
		w.Labels[v1beta1.LabelWorkerSvcName] = svcName
		return true
	}
	// Service disabled/removed — delete label if present.
	if _, exists := w.Labels[v1beta1.LabelWorkerSvcName]; !exists {
		return false
	}
	delete(w.Labels, v1beta1.LabelWorkerSvcName)
	return true
}

// computeWorkerPhase determines the Worker status phase from the reconcile
// outcome. Delegates to the shared computeMemberPhase function.
func computeWorkerPhase(w *v1beta1.Worker, containerState string, reconcileErr error) string {
	// 逻辑说明：computeWorkerPhase 接收 w(*v1beta1.Worker)、containerState(string)、reconcileErr(error)，依次借助 computeMemberPhase、DesiredState计算Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	return computeMemberPhase(w.Status.Phase, w.Status.MatrixUserID, w.Spec.DesiredState(), containerState, reconcileErr)
}

// SetupWithManager 向 controller-runtime 注册 Worker 主资源及其 Pod/Sandbox
// 观察关系。子资源变化也要触发 Worker reconcile，因为 Pod 的实际
// 状态不会自动写回 Worker.Status。
func (r *WorkerReconciler) SetupWithManager(mgr ctrl.Manager) (controller.Controller, error) {
	// 逻辑说明：SetupWithManager 接收 mgr(ctrl.Manager)，依次借助 For、NewControllerManagedBy、Background、GetBackendForType设置Manager的期望结果。
	// 返回/状态：返回 controller.Controller、error；会注册 watch、事件过滤器或对象到调谐请求的映射，不直接创建业务资源。
	// 失败/重试：注册失败会阻止 Controller 正常启动；事件处理本身由 controller-runtime 持续驱动。
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.Worker{})

	if r.Backend != nil {
		ctx := context.Background()
		// Watch Pods when the K8s ("pod") backend is registered & available.
		if wb, _ := r.Backend.GetBackendForType(ctx, v1beta1.BackendRuntimePod); wb != nil {
			bldr = bldr.Watches(
				&corev1.Pod{},
				handler.EnqueueRequestsFromMapFunc(WorkerPodMapFunc("")),
				builder.WithPredicates(PodLifecyclePredicates(v1beta1.LabelWorker, r.ControllerName)),
			)
		}
		// Watch Sandbox CRs and transient SandboxClaim CRs when the sandbox
		// backend is registered & available.
		if wb, _ := r.Backend.GetBackendForType(ctx, v1beta1.BackendRuntimeSandbox); wb != nil {
			if sb, ok := wb.(*backend.SandboxBackend); ok {
				bldr = bldr.Watches(
					sb.WatchObject(),
					handler.EnqueueRequestsFromMapFunc(WorkerPodMapFunc("")),
					builder.WithPredicates(SandboxLifecyclePredicates(v1beta1.LabelWorker, r.ControllerName)),
				)
				bldr = bldr.Watches(
					sb.ClaimWatchObject(),
					handler.EnqueueRequestsFromMapFunc(WorkerPodMapFunc("")),
					builder.WithPredicates(SandboxLifecyclePredicates(v1beta1.LabelWorker, r.ControllerName)),
				)
			}
		}
		// Docker / embedded mode has no watch source; reconciles for those
		// deployments are time-driven by RequeueAfter.
	}

	return bldr.Build(r)
}

// WorkerPodMapFunc returns a MapFunc for routing Pod events to Worker reconcile
// requests. If namespace is non-empty, it overrides obj.GetNamespace() — used
// for remote clusters where Pod namespace != CR namespace.
func WorkerPodMapFunc(namespace string) handler.MapFunc {
	// 逻辑说明：WorkerPodMapFunc 接收 namespace(string)，依次借助 GetLabels、GetNamespace处理Worker 成员的期望结果。
	// 返回/状态：返回 handler.MapFunc；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	return func(_ context.Context, obj client.Object) []reconcile.Request {
		workerName := obj.GetLabels()[v1beta1.LabelWorker]
		if workerName == "" {
			return nil
		}
		ns := namespace
		if ns == "" {
			ns = obj.GetNamespace()
		}
		return []reconcile.Request{
			{NamespacedName: client.ObjectKey{
				Name:      workerName,
				Namespace: ns,
			}},
		}
	}
}

// hashAppliedWorkerSpec computes a fnv64a hash of the WorkerSpec with selected
// config-only, lifecycle/policy-only, and service-only fields zeroed out. This
// captures only spec fields that should trigger container recreation when
// changed.
//
// Current standard-runtime coverage (fnv64a over json.Marshal with excluded
// fields zeroed):
//
//	ModelProvider, Runtime, Image, WorkerName, Identity, Soul,
//	Agents, Skills, RemoteSkills, Package, Console, ChannelPolicy, ContainerManaged,
//	DeployMode, BackendRuntime, Labels, Env, Volumes, Mounts.
//
// Excluded (do not trigger pod recreation):
//
//	Model, McpServers — config-only (consumed by ReconcileMemberConfig)
//	AccessEntries — permission-only (resolved by credential issuance)
//	AgentIdentity, CredentialBindings — runtime credential config
//	State, IdleTimeout — lifecycle/policy
//	ServiceEnabled, Expose — service-only (consumed by ReconcileMemberService)
//
// Consumed by workerMemberContext to populate MemberContext.AppliedSpecHash,
// which owning reconcilers write to status.specHash after a successful
// reconcile. Sandbox resources no longer store this hash.
func hashAppliedWorkerSpec(spec v1beta1.WorkerSpec) string {
	// 逻辑说明：hashAppliedWorkerSpec 接收 spec(v1beta1.WorkerSpec)，依次借助 workerDepsLayoutHashVersion、Marshal、New64a、Write计算哈希Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	spec.Model = ""          // config-only: written to openclaw.json/runtime.yaml
	spec.McpServers = nil    // config-only: written to mcporter/runtime config
	spec.AccessEntries = nil // permission-only: resolved when credentials are issued
	spec.AgentIdentity = nil // config-only: written to runtime.yaml
	spec.CredentialBindings = nil
	spec.State = nil          // exclude lifecycle state from hash
	spec.IdleTimeout = ""     // exclude controller-side autosleep policy from hash
	spec.ServiceEnabled = nil // service-only: does not affect pod
	spec.Expose = nil         // service-only: does not affect pod
	layoutVersion := workerDepsLayoutHashVersion(spec)
	if layoutVersion == "" {
		buf, err := json.Marshal(spec)
		if err != nil {
			return ""
		}
		h := fnv.New64a()
		_, _ = h.Write(buf)
		return fmt.Sprintf("%x", h.Sum64())
	}
	payload := struct {
		Spec             v1beta1.WorkerSpec `json:"spec"`
		WorkerDepsLayout string             `json:"workerDepsLayout,omitempty"`
	}{
		Spec:             spec,
		WorkerDepsLayout: layoutVersion,
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(buf)
	return fmt.Sprintf("%x", h.Sum64())
}

func hashAppliedWorkerSpecForRuntime(spec v1beta1.WorkerSpec, runtime string) string {
	// 逻辑说明：hashAppliedWorkerSpecForRuntime 接收 spec(v1beta1.WorkerSpec)、runtime(string)，依次借助 hashQwenPawPodSpec、hashAppliedWorkerSpec计算哈希Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if runtime == backend.RuntimeQwenPaw {
		if spec.Runtime == "" {
			spec.Runtime = runtime
		}
		return hashQwenPawPodSpec(spec)
	}
	return hashAppliedWorkerSpec(spec)
}

func hashAppliedWorkerSpecForRuntimeAndResources(spec v1beta1.WorkerSpec, runtime string, resources *v1beta1.AgentResourceRequirements) string {
	// 逻辑说明：hashAppliedWorkerSpecForRuntimeAndResources 接收 spec(v1beta1.WorkerSpec)、runtime(string)、resources(*v1beta1.AgentResourceRequirements)，依次借助 hashQwenPawPodSpecWithResources、hashAppliedWorkerSpec、workerDepsLayoutHashVersion、Marshal计算哈希Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if runtime == backend.RuntimeQwenPaw {
		if spec.Runtime == "" {
			spec.Runtime = runtime
		}
		return hashQwenPawPodSpecWithResources(spec, resources)
	}
	if resources == nil {
		return hashAppliedWorkerSpec(spec)
	}
	spec.Model = ""           // config-only: written to openclaw.json/runtime.yaml
	spec.McpServers = nil     // config-only: written to mcporter/runtime config
	spec.AccessEntries = nil  // permission-only: resolved when credentials are issued
	spec.State = nil          // exclude lifecycle state from hash
	spec.IdleTimeout = ""     // exclude controller-side autosleep policy
	spec.ServiceEnabled = nil // service-only: does not affect pod
	spec.Expose = nil         // service-only: does not affect pod
	spec.Resources = nil
	payload := struct {
		Spec             v1beta1.WorkerSpec                 `json:"spec"`
		Resources        *v1beta1.AgentResourceRequirements `json:"resources,omitempty"`
		WorkerDepsLayout string                             `json:"workerDepsLayout,omitempty"`
	}{
		Spec:             spec,
		Resources:        resources,
		WorkerDepsLayout: workerDepsLayoutHashVersion(spec),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(buf)
	return fmt.Sprintf("%x", h.Sum64())
}

func workerSpecWithEffectiveBackendRuntimeForHash(spec v1beta1.WorkerSpec, backendRuntime string) v1beta1.WorkerSpec {
	// 逻辑说明：workerSpecWithEffectiveBackendRuntimeForHash 接收 spec(v1beta1.WorkerSpec)、backendRuntime(string)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 v1beta1.WorkerSpec；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	if spec.BackendRuntime == nil && backendRuntime != "" {
		spec.BackendRuntime = &backendRuntime
	}
	return spec
}

func hashQwenPawPodSpec(spec v1beta1.WorkerSpec) string {
	return hashQwenPawPodSpecWithResources(spec, nil)
}

func hashQwenPawPodSpecWithResources(spec v1beta1.WorkerSpec, resources *v1beta1.AgentResourceRequirements) string {
	// 逻辑说明：hashQwenPawPodSpecWithResources 接收 spec(v1beta1.WorkerSpec)、resources(*v1beta1.AgentResourceRequirements)，依次借助 workerDepsLayoutHashVersion、Marshal、New64a、Write计算哈希Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	type qwenPawPodSpec struct {
		Runtime          string                             `json:"runtime,omitempty"`
		Image            string                             `json:"image,omitempty"`
		WorkerName       string                             `json:"workerName,omitempty"`
		ContainerManaged *bool                              `json:"containerManaged,omitempty"`
		DeployMode       *string                            `json:"deployMode,omitempty"`
		BackendRuntime   *string                            `json:"backendRuntime,omitempty"`
		Resources        *v1beta1.AgentResourceRequirements `json:"resources,omitempty"`
		Console          *v1beta1.WorkerConsoleSpec         `json:"console,omitempty"`
		Env              map[string]string                  `json:"env,omitempty"`
		Labels           map[string]string                  `json:"labels,omitempty"`
		Volumes          []v1beta1.WorkerVolumeSpec         `json:"volumes,omitempty"`
		Mounts           []v1beta1.WorkerMountSpec          `json:"mounts,omitempty"`
		WorkerDepsLayout string                             `json:"workerDepsLayout,omitempty"`
	}
	payload := qwenPawPodSpec{
		Runtime:          spec.Runtime,
		Image:            spec.Image,
		WorkerName:       spec.WorkerName,
		ContainerManaged: spec.ContainerManaged,
		DeployMode:       spec.DeployMode,
		BackendRuntime:   spec.BackendRuntime,
		Resources:        resources,
		Console:          spec.Console,
		Env:              spec.Env,
		Labels:           spec.Labels,
		Volumes:          spec.Volumes,
		Mounts:           spec.Mounts,
		WorkerDepsLayout: workerDepsLayoutHashVersion(spec),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(buf)
	return fmt.Sprintf("%x", h.Sum64())
}

func workerDepsLayoutHashVersion(spec v1beta1.WorkerSpec) string {
	// 逻辑说明：workerDepsLayoutHashVersion 接收 spec(v1beta1.WorkerSpec)，依次借助 workerDepsLayoutVersionForBackendRuntime、GetBackendRuntime处理Worker 成员的期望结果。
	// 返回/状态：返回 string；会更新 Worker 成员的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if len(spec.Volumes) > 0 || len(spec.Mounts) > 0 {
		return workerDepsLayoutVersion
	}
	return workerDepsLayoutVersionForBackendRuntime(spec.GetBackendRuntime())
}

func workerDepsLayoutVersionForBackendRuntime(backendRuntime string) string {
	// 逻辑说明：workerDepsLayoutVersionForBackendRuntime 接收 backendRuntime(string)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 string；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	switch backendRuntime {
	case v1beta1.BackendRuntimeSandbox:
		return workerDepsLayoutVersion
	default:
		return ""
	}
}

// PodLifecyclePredicates filters Pod events to only trigger reconciliation on
// create, delete, or relevant status transitions. A pod is considered "ours" only when
// it carries both:
//
//   - labelKey (one of the AgentTeams identity labels) with a non-empty
//     value — identifying which CR
//     kind owns the pod.
//   - agentteams.io/controller == controllerName — identifying which controller
//     instance owns the pod.
//
// The controller filter is defense-in-depth against the informer cache label
// selector configured in app.startInCluster (opts.Cache.ByObject for Pods).
// If a future watch source is wired without that cache filter, this predicate
// still prevents cross-instance reconcile when two agentteams-controller
// releases share a namespace.
func PodLifecyclePredicates(labelKey, controllerName string) predicate.Predicate {
	// 逻辑说明：PodLifecyclePredicates 接收 labelKey/controllerName(string)，依次借助 GetLabels、matches、podLifecycleSignal处理Worker 成员的期望结果。
	// 返回/状态：返回 predicate.Predicate；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	matches := func(obj client.Object) bool {
		l := obj.GetLabels()
		return l[labelKey] != "" && l[v1beta1.LabelController] == controllerName
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return matches(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return matches(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !matches(e.ObjectNew) {
				return false
			}
			oldPod, ok1 := e.ObjectOld.(*corev1.Pod)
			newPod, ok2 := e.ObjectNew.(*corev1.Pod)
			if !ok1 || !ok2 {
				return true
			}
			return podLifecycleSignal(oldPod) != podLifecycleSignal(newPod)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

func podLifecycleSignal(pod *corev1.Pod) string {
	// 逻辑说明：podLifecycleSignal 接收 pod(*corev1.Pod)，依次借助 Join、podReadyConditionSignal、podContainerStatusesSignal处理Worker 成员的期望结果。
	// 返回/状态：返回 string；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	if pod == nil {
		return ""
	}
	return strings.Join([]string{
		string(pod.Status.Phase),
		podReadyConditionSignal(pod.Status.Conditions),
		podContainerStatusesSignal(pod.Status.InitContainerStatuses),
		podContainerStatusesSignal(pod.Status.ContainerStatuses),
	}, "\n")
}

func podReadyConditionSignal(conditions []corev1.PodCondition) string {
	// 逻辑说明：podReadyConditionSignal 接收 conditions([]corev1.PodCondition)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 string；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	for i := range conditions {
		cond := conditions[i]
		if cond.Type == corev1.PodReady {
			return fmt.Sprintf("%s|%s|%s", cond.Status, cond.Reason, cond.Message)
		}
	}
	return ""
}

func podContainerStatusesSignal(statuses []corev1.ContainerStatus) string {
	// 逻辑说明：podContainerStatusesSignal 接收 statuses([]corev1.ContainerStatus)，依次借助 Strings、Join处理Worker 成员的期望结果。
	// 返回/状态：返回 string；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	if len(statuses) == 0 {
		return ""
	}
	parts := make([]string, 0, len(statuses))
	for i := range statuses {
		cs := statuses[i]
		state, reason, message := "unknown", "", ""
		switch {
		case cs.State.Waiting != nil:
			state = "waiting"
			reason = cs.State.Waiting.Reason
			message = cs.State.Waiting.Message
		case cs.State.Running != nil:
			state = "running"
		case cs.State.Terminated != nil:
			state = "terminated"
			reason = cs.State.Terminated.Reason
			message = cs.State.Terminated.Message
		}
		parts = append(parts, fmt.Sprintf("%s|%s|%s|%s|%t", cs.Name, state, reason, message, cs.Ready))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

// SandboxLifecyclePredicates filters Sandbox CR events to only trigger
// reconciliation on create, delete, or .status.phase transitions.
// A sandbox is considered "ours" only when it carries both the given labelKey
// with a non-empty value and agentteams.io/controller == controllerName.
func SandboxLifecyclePredicates(labelKey, controllerName string) predicate.Predicate {
	// 逻辑说明：SandboxLifecyclePredicates 接收 labelKey/controllerName(string)，依次借助 GetLabels、matches、extractUnstructuredPhase、extractUnstructuredReadyCondition处理Worker 成员的期望结果。
	// 返回/状态：返回 predicate.Predicate；可能查询、创建、停止、更新或删除 Pod/沙箱实例，并同步内存中的观测状态。
	// 失败/重试：后端失败会返回错误或重排时间；下一轮先重新读取实例，避免重复创建或删除。
	matches := func(obj client.Object) bool {
		l := obj.GetLabels()
		return l[labelKey] != "" && l[v1beta1.LabelController] == controllerName
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return matches(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return matches(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !matches(e.ObjectNew) {
				return false
			}
			// For unstructured objects, compare .status.phase string.
			oldPhase := extractUnstructuredPhase(e.ObjectOld)
			newPhase := extractUnstructuredPhase(e.ObjectNew)
			if oldPhase != newPhase {
				return true
			}
			// Also reconcile when the Ready condition status flips, since
			// remote sandbox backends may surface pod failures via
			// .status.conditions[type=Ready] without changing .status.phase.
			oldReady := extractUnstructuredReadyCondition(e.ObjectOld)
			newReady := extractUnstructuredReadyCondition(e.ObjectNew)
			return oldReady != newReady
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// extractUnstructuredPhase reads .status.phase from an unstructured object.
func extractUnstructuredPhase(obj client.Object) string {
	// 逻辑说明：extractUnstructuredPhase 接收 obj(client.Object)，依次借助 UnstructuredContent提取Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	u, ok := obj.(interface {
		UnstructuredContent() map[string]interface{}
	})
	if !ok {
		return ""
	}
	content := u.UnstructuredContent()
	status, ok := content["status"].(map[string]interface{})
	if !ok {
		return ""
	}
	phase, _ := status["phase"].(string)
	return phase
}

// extractUnstructuredReadyCondition reads the status value of the
// .status.conditions[type=Ready] entry from an unstructured object.
// Returns an empty string when the object is not unstructured, has no
// conditions, or has no Ready condition. This keeps old/new comparisons
// stable so missing conditions do not falsely trigger reconciliation.
func extractUnstructuredReadyCondition(obj client.Object) string {
	// 逻辑说明：extractUnstructuredReadyCondition 接收 obj(client.Object)，依次借助 UnstructuredContent提取Worker 成员的期望结果。
	// 返回/状态：返回 string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	u, ok := obj.(interface {
		UnstructuredContent() map[string]interface{}
	})
	if !ok {
		return ""
	}
	content := u.UnstructuredContent()
	status, ok := content["status"].(map[string]interface{})
	if !ok {
		return ""
	}
	conditions, ok := status["conditions"].([]interface{})
	if !ok {
		return ""
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _ := cond["type"].(string)
		if condType != "Ready" {
			continue
		}
		condStatus, _ := cond["status"].(string)
		return condStatus
	}
	return ""
}

// --- Package-level helpers ---

func nilIfEmpty(s string) *string {
	// 逻辑说明：nilIfEmpty 接收 s(string)，按本函数中的条件与转换步骤处理Worker 成员的期望结果。
	// 返回/状态：返回 *string；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if s == "" {
		return nil
	}
	return &s
}
