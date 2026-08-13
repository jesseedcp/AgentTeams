package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	authpkg "github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/auth"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// k8sUpdateMaxRetries is the max attempts for Get→patch spec→Update against
// optimistic locking conflicts when the controller updates status between Get and Update.
const k8sUpdateMaxRetries = 3

// ResourceHandler handles declarative CRUD operations on CRs.
//
// Team CRs reference independently managed Worker CRs. Worker CRUD always
// operates on Worker CRs; Team CRUD only owns membership and coordination.
type ResourceHandler struct {
	client    client.Client
	namespace string
	backend   *backend.Registry

	defaultWorkerRuntime string

	// controllerName is stamped as agentteams.io/controller on every CR this
	// handler creates, overwriting any value supplied by the client. This
	// enforces that HTTP-created resources always belong to the serving
	// controller instance, regardless of what the caller attempts to set.
	// Empty string means no enforcement (embedded mode).
	controllerName string
}

// NewResourceHandler creates a handler. backend may be nil, in which case
// runtime status is omitted from synthetic team member responses.
// controllerName, when non-empty, is force-stamped as agentteams.io/controller
// on every CR this handler creates so HTTP-created resources cannot escape
// the serving controller instance's cache scope.
func NewResourceHandler(c client.Client, namespace string, b *backend.Registry, controllerName string) *ResourceHandler {
	// 逻辑说明：绑定 Kubernetes client、默认 namespace、可选运行后端和 controller ownership 名称；构造本身不读取或修改任何 CR。
	return &ResourceHandler{
		client:         c,
		namespace:      namespace,
		backend:        b,
		controllerName: controllerName,
	}
}

// stampControllerLabel force-writes the controller ownership label on meta.
// Callers invoke this on every Create path so the HTTP API cannot be used
// to produce CRs that escape the owning controller's cache scope.
func (h *ResourceHandler) stampControllerLabel(meta *metav1.ObjectMeta) {
	// 逻辑说明：embedded 模式没有 controllerName 时不干预；云模式确保 labels map 存在并强制覆盖 ownership 标签，阻止请求者把新 CR 逃逸到其他 controller 作用域。
	if h.controllerName == "" {
		return
	}
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	meta.Labels[v1beta1.LabelController] = h.controllerName
}

// --- Workers ---

// CreateWorker 校验请求并创建 Worker CR。HTTP 201 表示期望状态已被
// Kubernetes API 接受，不表示 Matrix 账号和容器已 Ready；完成程度要继续
// 查看 Worker.status。
func (h *ResourceHandler) CreateWorker(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：解析并校验名称/runtime/console，补齐容器托管与默认 runtime 后构造无状态凭据的 Worker Spec；再检查 Team Leader 权限边界、强制 controller 标签并创建 CR，201 仅表示期望状态已接受。
	var req CreateWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	if !backend.ValidRuntime(req.Runtime) {
		httputil.WriteError(
			w,
			http.StatusBadRequest,
			"unsupported Worker runtime: "+req.Runtime,
		)
		return
	}

	// containerManaged default is true (controller manages container).
	containerManaged := true
	if req.ContainerManaged != nil {
		containerManaged = *req.ContainerManaged
	}
	runtime := backend.ResolveRuntime(req.Runtime, h.defaultWorkerRuntime)
	console, err := normalizedWorkerConsole(req.Console, runtime)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: h.namespace,
		},
		Spec: v1beta1.WorkerSpec{
			Model:            req.Model,
			ModelProvider:    req.ModelProvider,
			WorkerName:       req.WorkerName,
			Runtime:          runtime,
			Image:            req.Image,
			Identity:         req.Identity,
			Soul:             req.Soul,
			Agents:           req.Agents,
			Skills:           req.Skills,
			McpServers:       req.McpServers,
			Package:          req.Package,
			Expose:           req.Expose,
			Console:          console,
			ChannelPolicy:    req.ChannelPolicy,
			Resources:        req.Resources,
			ContainerManaged: &containerManaged,
			State:            req.State,
		},
	}

	// Team leaders cannot create infrastructure resources; Manager/Admin owns
	// Worker creation and Team leaders only coordinate assigned members.
	caller := authpkg.CallerFromContext(r.Context())
	if caller != nil && caller.Role == authpkg.RoleTeamLeader {
		httputil.WriteError(w, http.StatusConflict, "team leaders cannot create Worker resources")
		return
	}

	h.stampControllerLabel(&worker.ObjectMeta)

	if err := h.client.Create(r.Context(), worker); err != nil {
		writeK8sError(w, "create worker", err)
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, workerToResponse(worker))
}

func (h *ResourceHandler) GetWorker(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：按路径名称读取独立 Worker CR，并查询 Team 引用补充 role/room 身份；只有 CR 与成员查询都成功才返回合成视图，未找到和其他 K8s 错误分别映射。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	err := h.client.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
	switch {
	case err == nil:
		resp := workerToResponse(&worker)
		if team, member, ok, terr := h.findTeamMember(r.Context(), name); terr != nil {
			writeK8sError(w, "get worker", terr)
			return
		} else if ok {
			h.applyTeamMember(&resp, team, member)
		}
		httputil.WriteJSON(w, http.StatusOK, resp)
		return
	case !apierrors.IsNotFound(err):
		writeK8sError(w, "get worker", err)
		return
	}

	httputil.WriteError(w, http.StatusNotFound, "get worker: not found")
}

func (h *ResourceHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：列出 namespace 内全部 Worker，为每个对象查询并合并 Team 成员信息，再应用可选 team query 过滤；任一成员查询失败不返回不完整集合。
	teamFilter := r.URL.Query().Get("team")

	workers := make([]WorkerResponse, 0)

	var list v1beta1.WorkerList
	if err := h.client.List(r.Context(), &list, client.InNamespace(h.namespace)); err != nil {
		writeK8sError(w, "list workers", err)
		return
	}
	for i := range list.Items {
		resp := workerToResponse(&list.Items[i])
		if team, member, ok, terr := h.findTeamMember(r.Context(), list.Items[i].Name); terr != nil {
			writeK8sError(w, "list workers: lookup team member", terr)
			return
		} else if ok {
			h.applyTeamMember(&resp, team, member)
		}
		if teamFilter != "" && resp.Team != teamFilter {
			continue
		}
		workers = append(workers, resp)
	}

	httputil.WriteJSON(w, http.StatusOK, WorkerListResponse{Workers: workers, Total: len(workers)})
}

// UpdateWorker 用冲突重试更新 Worker spec，而不直接操作容器。
// Kubernetes resourceVersion 用于乐观并发控制：如果另一个请求先修改了
// 对象，本请求重新读取并合并，避免用旧副本覆盖新状态。
func (h *ResourceHandler) UpdateWorker(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：校验部分更新请求后，在每次乐观锁尝试中重新读取最新 Worker、只覆盖显式字段并复制切片，复验 console/runtime 组合；resourceVersion 冲突退避重试，成功只更新 Spec 交由 reconcile 执行。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var req UpdateWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Runtime != nil && !backend.ValidRuntime(*req.Runtime) {
		httputil.WriteError(
			w,
			http.StatusBadRequest,
			"unsupported Worker runtime: "+*req.Runtime,
		)
		return
	}

	ctx := r.Context()
	for attempt := 0; attempt < k8sUpdateMaxRetries; attempt++ {
		var worker v1beta1.Worker
		if err := h.client.Get(ctx, client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
			writeK8sError(w, "get worker for update", err)
			return
		}

		if req.Model != nil {
			worker.Spec.Model = *req.Model
		}
		if req.ModelProvider != nil {
			worker.Spec.ModelProvider = *req.ModelProvider
		}
		if req.WorkerName != nil {
			worker.Spec.WorkerName = *req.WorkerName
		}
		if req.Runtime != nil {
			worker.Spec.Runtime = *req.Runtime
		}
		if req.Image != nil {
			worker.Spec.Image = *req.Image
		}
		if req.Identity != nil {
			worker.Spec.Identity = *req.Identity
		}
		if req.Soul != nil {
			worker.Spec.Soul = *req.Soul
		}
		if req.Agents != nil {
			worker.Spec.Agents = *req.Agents
		}
		if req.Skills != nil {
			worker.Spec.Skills = append(
				[]string(nil),
				(*req.Skills)...,
			)
		}
		if req.McpServers != nil {
			worker.Spec.McpServers = append(
				[]v1beta1.MCPServer(nil),
				(*req.McpServers)...,
			)
		}
		if req.Package != nil {
			worker.Spec.Package = *req.Package
		}
		if req.Expose != nil {
			worker.Spec.Expose = append(
				[]v1beta1.ExposePort(nil),
				(*req.Expose)...,
			)
		}
		if req.Console != nil {
			console, err := normalizedWorkerConsole(
				req.Console,
				backend.ResolveRuntime(worker.Spec.Runtime, backend.RuntimeOpenClaw),
			)
			if err != nil {
				httputil.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			worker.Spec.Console = console
		}
		if req.ChannelPolicy != nil {
			worker.Spec.ChannelPolicy = req.ChannelPolicy
		}
		if req.Resources != nil {
			worker.Spec.Resources = req.Resources
		}
		if req.ContainerManaged != nil {
			worker.Spec.ContainerManaged = req.ContainerManaged
		}
		if req.State != nil {
			worker.Spec.State = req.State
		}
		if worker.Spec.Console != nil {
			effectiveRuntime := backend.ResolveRuntime(
				worker.Spec.Runtime,
				backend.RuntimeOpenClaw,
			)
			if err := worker.Spec.Console.Validate(effectiveRuntime); err != nil {
				httputil.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
		}

		if err := h.client.Update(ctx, &worker); err != nil {
			if apierrors.IsConflict(err) && attempt+1 < k8sUpdateMaxRetries {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			writeK8sError(w, "update worker", err)
			return
		}

		httputil.WriteJSON(w, http.StatusOK, workerToResponse(&worker))
		return
	}
}

// DeleteWorker 请求删除 Worker CR。204 之后对象可能仍处于 Terminating，
// 因为 finalizer 需要完成 Matrix、Gateway、凭据和容器清理后才允许其消失。
func (h *ResourceHandler) DeleteWorker(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：先查询是否仍被任一 Team 引用，有引用则以 409 要求先改 Team；否则删除 CR 并返回 204，实际容器/Matrix 等清理由 finalizer 异步完成。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	if team, ok, err := h.findTeamForMember(r.Context(), name); err != nil {
		writeK8sError(w, "delete worker", err)
		return
	} else if ok {
		httputil.WriteError(w, http.StatusConflict,
			"worker is a member of team "+team+"; remove via PUT/DELETE /api/v1/teams/"+team)
		return
	}

	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.namespace},
	}
	if err := h.client.Delete(r.Context(), worker); err != nil {
		writeK8sError(w, "delete worker", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Teams ---

// CreateTeam 创建一个引用现有 Worker CR 的 Team CR。成功回答仅确认
// 团队期望状态已持久化，房间创建和成员配置注入由 TeamReconciler 异步完成。
func (h *ResourceHandler) CreateTeam(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：解析名称与非空成员，验证每个 Worker 存在、角色唯一且未加入其他 Team；随后构造 Team Spec、强制归属标签并创建，房间与成员配置由 reconciler 后续收敛。
	var req CreateTeamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	if len(req.WorkerMembers) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "workerMembers is required")
		return
	}
	if err := h.validateTeamWorkerMembers(r.Context(), req.Name, req.WorkerMembers); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: h.namespace,
		},
		Spec: v1beta1.TeamSpec{
			Description:    req.Description,
			TeamName:       req.TeamName,
			Admin:          req.Admin,
			HumanMembers:   req.HumanMembers,
			WorkerMembers:  req.WorkerMembers,
			HeartbeatEvery: req.HeartbeatEvery,
			PeerMentions:   req.PeerMentions,
			ChannelPolicy:  req.ChannelPolicy,
		},
	}

	h.stampControllerLabel(&team.ObjectMeta)

	if err := h.client.Create(r.Context(), team); err != nil {
		writeK8sError(w, "create team", err)
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, teamToResponse(team))
}

func (h *ResourceHandler) GetTeam(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：校验路径名后从固定 namespace 读取 Team，并把 Spec/Status 转换成稳定 API 响应；Kubernetes 错误由统一映射处理。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "team name is required")
		return
	}

	var team v1beta1.Team
	if err := h.client.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &team); err != nil {
		writeK8sError(w, "get team", err)
		return
	}

	httputil.WriteJSON(w, http.StatusOK, teamToResponse(&team))
}

func (h *ResourceHandler) ListTeams(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：列出 namespace 内 Team，逐个合并期望成员和状态成员为响应，并以实际返回数量填 total；列表失败时不输出部分结果。
	var list v1beta1.TeamList
	if err := h.client.List(r.Context(), &list, client.InNamespace(h.namespace)); err != nil {
		writeK8sError(w, "list teams", err)
		return
	}

	teams := make([]TeamResponse, 0, len(list.Items))
	for i := range list.Items {
		teams = append(teams, teamToResponse(&list.Items[i]))
	}

	httputil.WriteJSON(w, http.StatusOK, TeamListResponse{Teams: teams, Total: len(teams)})
}

// UpdateTeam 只修改 Team 的期望成员与策略，不直接发送 Matrix invite/kick。
// 这样 REST 请求中断时期望状态仍可被后续 reconcile 恢复完成。
func (h *ResourceHandler) UpdateTeam(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：解析部分字段并在成员变更时先验证跨 Team 唯一性；每次冲突重试都重读 Team、合并显式期望字段后 Update，Matrix invite/kick 由后续 reconcile 依据最终 Spec 执行。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "team name is required")
		return
	}

	var req UpdateTeamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.WorkerMembers != nil {
		if err := h.validateTeamWorkerMembers(r.Context(), name, req.WorkerMembers); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	ctx := r.Context()
	for attempt := 0; attempt < k8sUpdateMaxRetries; attempt++ {
		var team v1beta1.Team
		if err := h.client.Get(ctx, client.ObjectKey{Name: name, Namespace: h.namespace}, &team); err != nil {
			writeK8sError(w, "get team for update", err)
			return
		}

		if req.Description != "" {
			team.Spec.Description = req.Description
		}
		if req.TeamName != "" {
			team.Spec.TeamName = req.TeamName
		}
		if req.Admin != nil {
			team.Spec.Admin = req.Admin
		}
		if req.PeerMentions != nil {
			team.Spec.PeerMentions = req.PeerMentions
		}
		if req.ChannelPolicy != nil {
			team.Spec.ChannelPolicy = req.ChannelPolicy
		}
		if req.WorkerMembers != nil {
			team.Spec.WorkerMembers = req.WorkerMembers
		}
		if req.HeartbeatEvery != nil {
			team.Spec.HeartbeatEvery = *req.HeartbeatEvery
		}

		if err := h.client.Update(ctx, &team); err != nil {
			if apierrors.IsConflict(err) && attempt+1 < k8sUpdateMaxRetries {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			writeK8sError(w, "update team", err)
			return
		}

		httputil.WriteJSON(w, http.StatusOK, teamToResponse(&team))
		return
	}
}

func (h *ResourceHandler) DeleteTeam(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：用路径名称构造 namespaced Team 引用并请求删除；成功 204 只代表 deletion 已接受，finalizer/reconciler 仍可继续清理协作资源。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "team name is required")
		return
	}

	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.namespace},
	}
	if err := h.client.Delete(r.Context(), team); err != nil {
		writeK8sError(w, "delete team", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Humans ---

// CreateHuman 创建 Human CR。Matrix 账号和房间权限由 HumanReconciler 之后收敛，
// 因此 API 不在同一 HTTP 请求中暴露或长期保留登录 token。
func (h *ResourceHandler) CreateHuman(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：解析身份与访问范围，构造不含 Matrix 密钥的 Human Spec，强制 controller ownership 后持久化；账号/房间权限由 HumanReconciler 异步创建。
	var req CreateHumanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}

	human := &v1beta1.Human{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: h.namespace,
		},
		Spec: v1beta1.HumanSpec{
			DisplayName:       req.DisplayName,
			Email:             req.Email,
			PermissionLevel:   req.PermissionLevel,
			AccessibleTeams:   req.AccessibleTeams,
			AccessibleWorkers: req.AccessibleWorkers,
			Note:              req.Note,
		},
	}

	h.stampControllerLabel(&human.ObjectMeta)

	if err := h.client.Create(r.Context(), human); err != nil {
		writeK8sError(w, "create human", err)
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, humanToResponse(human))
}

func (h *ResourceHandler) GetHuman(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：校验路径名并读取固定 namespace 的 Human CR，将期望权限与已生成 Matrix 状态合成响应；读取失败统一映射为对应 HTTP 状态。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "human name is required")
		return
	}

	var human v1beta1.Human
	if err := h.client.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &human); err != nil {
		writeK8sError(w, "get human", err)
		return
	}

	httputil.WriteJSON(w, http.StatusOK, humanToResponse(&human))
}

func (h *ResourceHandler) ListHumans(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：列出当前 namespace 的 Human CR 并逐一转换，total 与真实响应切片一致；Kubernetes List 失败时不返回缓存或部分数据。
	var list v1beta1.HumanList
	if err := h.client.List(r.Context(), &list, client.InNamespace(h.namespace)); err != nil {
		writeK8sError(w, "list humans", err)
		return
	}

	humans := make([]HumanResponse, 0, len(list.Items))
	for i := range list.Items {
		humans = append(humans, humanToResponse(&list.Items[i]))
	}

	httputil.WriteJSON(w, http.StatusOK, HumanListResponse{Humans: humans, Total: len(humans)})
}

func (h *ResourceHandler) UpdateHuman(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：拒绝越界 permissionLevel 和空补丁；在最多三次乐观锁尝试中重读并合并显式字段，复制访问范围切片避免别名，冲突退避后重试。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "human name is required")
		return
	}

	var req UpdateHumanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.PermissionLevel != nil && (*req.PermissionLevel < 1 || *req.PermissionLevel > 3) {
		httputil.WriteError(w, http.StatusBadRequest, "permissionLevel must be between 1 and 3")
		return
	}
	if req.DisplayName == nil &&
		req.Email == nil &&
		req.PermissionLevel == nil &&
		req.AccessibleTeams == nil &&
		req.AccessibleWorkers == nil &&
		req.Note == nil {
		httputil.WriteError(w, http.StatusBadRequest, "at least one field must be specified for update")
		return
	}

	ctx := r.Context()
	for attempt := 0; attempt < k8sUpdateMaxRetries; attempt++ {
		var human v1beta1.Human
		if err := h.client.Get(ctx, client.ObjectKey{Name: name, Namespace: h.namespace}, &human); err != nil {
			writeK8sError(w, "get human for update", err)
			return
		}
		if req.DisplayName != nil {
			human.Spec.DisplayName = *req.DisplayName
		}
		if req.Email != nil {
			human.Spec.Email = *req.Email
		}
		if req.PermissionLevel != nil {
			human.Spec.PermissionLevel = *req.PermissionLevel
		}
		if req.AccessibleTeams != nil {
			human.Spec.AccessibleTeams = append([]string(nil), (*req.AccessibleTeams)...)
		}
		if req.AccessibleWorkers != nil {
			human.Spec.AccessibleWorkers = append([]string(nil), (*req.AccessibleWorkers)...)
		}
		if req.Note != nil {
			human.Spec.Note = *req.Note
		}
		if err := h.client.Update(ctx, &human); err != nil {
			if apierrors.IsConflict(err) && attempt+1 < k8sUpdateMaxRetries {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			writeK8sError(w, "update human", err)
			return
		}
		httputil.WriteJSON(w, http.StatusOK, humanToResponse(&human))
		return
	}
}

func (h *ResourceHandler) DeleteHuman(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：按路径名称请求删除 Human CR；只有 API 接受删除才返回 204，Matrix 账号与房间回收仍由对象 finalizer 完成。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "human name is required")
		return
	}

	human := &v1beta1.Human{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.namespace},
	}
	if err := h.client.Delete(r.Context(), human); err != nil {
		writeK8sError(w, "delete human", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Managers ---

// CreateManager 创建 AgentScope Manager CR。它只记录无密文期望状态，
// Manager 的 Matrix token、gateway key 和存储凭据由 Controller 在部署阶段生成并注入。
func (h *ResourceHandler) CreateManager(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：校验名称、必填模型、Manager runtime 和 Coding CLI 配置，解析默认 runtime 后构造不含密文的 Manager Spec；强制 ownership 标签并创建，凭据与运行实例由 controller 后置供应。
	var req CreateManagerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Model == "" {
		httputil.WriteError(w, http.StatusBadRequest, "model is required")
		return
	}
	if !backend.ValidManagerRuntime(req.Runtime) {
		httputil.WriteError(
			w,
			http.StatusBadRequest,
			"unsupported Manager runtime: "+req.Runtime,
		)
		return
	}
	if req.CodingCLI != nil {
		if err := req.CodingCLI.Validate(); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	mgr := &v1beta1.Manager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: h.namespace,
		},
		Spec: v1beta1.ManagerSpec{
			Model:         req.Model,
			ModelProvider: req.ModelProvider,
			Runtime:       backend.ResolveManagerRuntime(req.Runtime),
			Image:         req.Image,
			Soul:          req.Soul,
			Identity:      req.Identity,
			Agents:        req.Agents,
			Skills:        req.Skills,
			McpServers:    req.McpServers,
			Package:       req.Package,
			State:         req.State,
			Resources:     req.Resources,
			CodingCLI:     req.CodingCLI,
		},
	}
	if req.Config != nil {
		mgr.Spec.Config = *req.Config
	}

	h.stampControllerLabel(&mgr.ObjectMeta)

	if err := h.client.Create(r.Context(), mgr); err != nil {
		writeK8sError(w, "create manager", err)
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, managerToResponse(mgr))
}

func (h *ResourceHandler) GetManager(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：校验路径名并读取 Manager CR，把运行状态、Matrix 身份和期望配置转换为响应；Kubernetes 错误不被包装成空 Manager。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "manager name is required")
		return
	}

	var mgr v1beta1.Manager
	if err := h.client.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &mgr); err != nil {
		writeK8sError(w, "get manager", err)
		return
	}

	httputil.WriteJSON(w, http.StatusOK, managerToResponse(&mgr))
}

func (h *ResourceHandler) ListManagers(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：列出 namespace 内全部 Manager 并转换为无密钥响应，total 取最终切片长度；列表失败立即返回统一 K8s 错误。
	var list v1beta1.ManagerList
	if err := h.client.List(r.Context(), &list, client.InNamespace(h.namespace)); err != nil {
		writeK8sError(w, "list managers", err)
		return
	}

	managers := make([]ManagerResponse, 0, len(list.Items))
	for i := range list.Items {
		managers = append(managers, managerToResponse(&list.Items[i]))
	}

	httputil.WriteJSON(w, http.StatusOK, ManagerListResponse{Managers: managers, Total: len(managers)})
}

// UpdateManager 更新 Manager 的期望配置。已在处理的 AgentScope turn 不由
// HTTP handler 直接中断；Controller 发布新 revision，运行时在安全边界激活它。
func (h *ResourceHandler) UpdateManager(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：解析并校验 runtime/Coding CLI 后，在乐观锁循环中重读最新 Manager、合并非空或非 nil 字段并深拷贝 CodingCLI；冲突退避重试，成功交由 controller 发布新 revision。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "manager name is required")
		return
	}

	var req UpdateManagerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if !backend.ValidManagerRuntime(req.Runtime) {
		httputil.WriteError(
			w,
			http.StatusBadRequest,
			"unsupported Manager runtime: "+req.Runtime,
		)
		return
	}
	if req.CodingCLI != nil {
		if err := req.CodingCLI.Validate(); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	ctx := r.Context()
	for attempt := 0; attempt < k8sUpdateMaxRetries; attempt++ {
		var mgr v1beta1.Manager
		if err := h.client.Get(ctx, client.ObjectKey{Name: name, Namespace: h.namespace}, &mgr); err != nil {
			writeK8sError(w, "get manager for update", err)
			return
		}

		if req.Model != "" {
			mgr.Spec.Model = req.Model
		}
		if req.ModelProvider != "" {
			mgr.Spec.ModelProvider = req.ModelProvider
		}
		if req.Runtime != "" {
			mgr.Spec.Runtime = req.Runtime
		}
		if req.Image != "" {
			mgr.Spec.Image = req.Image
		}
		if req.Soul != "" {
			mgr.Spec.Soul = req.Soul
		}
		if req.Identity != "" {
			mgr.Spec.Identity = req.Identity
		}
		if req.Agents != "" {
			mgr.Spec.Agents = req.Agents
		}
		if req.Skills != nil {
			mgr.Spec.Skills = req.Skills
		}
		if req.McpServers != nil {
			mgr.Spec.McpServers = req.McpServers
		}
		if req.Package != "" {
			mgr.Spec.Package = req.Package
		}
		if req.Config != nil {
			mgr.Spec.Config = *req.Config
		}
		if req.State != nil {
			mgr.Spec.State = req.State
		}
		if req.Resources != nil {
			mgr.Spec.Resources = req.Resources
		}
		if req.CodingCLI != nil {
			mgr.Spec.CodingCLI = req.CodingCLI.DeepCopy()
		}

		if err := h.client.Update(ctx, &mgr); err != nil {
			if apierrors.IsConflict(err) && attempt+1 < k8sUpdateMaxRetries {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			writeK8sError(w, "update manager", err)
			return
		}

		httputil.WriteJSON(w, http.StatusOK, managerToResponse(&mgr))
		return
	}
}

func (h *ResourceHandler) DeleteManager(w http.ResponseWriter, r *http.Request) {
	// 逻辑说明：构造固定 namespace 的 Manager 引用并提交删除；只有 Kubernetes 接受请求才返回 204，运行时与外部身份清理由 finalizer 继续完成。
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "manager name is required")
		return
	}

	mgr := &v1beta1.Manager{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.namespace},
	}
	if err := h.client.Delete(r.Context(), mgr); err != nil {
		writeK8sError(w, "delete manager", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Conversion helpers ---

func workerToResponse(w *v1beta1.Worker) WorkerResponse {
	// 逻辑说明：从 Spec 复制期望配置、从 Status 复制实际身份/容器状态，并复制所有切片避免响应修改 CR；空 phase 规范为 Pending，再展开已暴露端口。
	resp := WorkerResponse{
		Name:             w.Name,
		WorkerName:       w.Spec.WorkerName,
		Phase:            w.Status.Phase,
		State:            w.Spec.DesiredState(),
		Model:            w.Spec.Model,
		Runtime:          w.Spec.Runtime,
		Image:            w.Spec.Image,
		Identity:         w.Spec.Identity,
		Soul:             w.Spec.Soul,
		Agents:           w.Spec.Agents,
		Skills:           append([]string(nil), w.Spec.Skills...),
		McpServers:       append([]v1beta1.MCPServer(nil), w.Spec.McpServers...),
		Package:          w.Spec.Package,
		Expose:           append([]v1beta1.ExposePort(nil), w.Spec.Expose...),
		Console:          responseWorkerConsole(w.Spec.Console),
		BackendRuntime:   w.Spec.GetBackendRuntime(),
		ContainerManaged: w.Spec.DesiredContainerMan(),
		ChannelPolicy:    w.Spec.ChannelPolicy,
		ContainerState:   w.Status.ContainerState,
		MatrixUserID:     w.Status.MatrixUserID,
		RoomID:           w.Status.RoomID,
		Message:          w.Status.Message,
	}
	if resp.Phase == "" {
		resp.Phase = "Pending"
	}
	for _, ep := range w.Status.ExposedPorts {
		resp.ExposedPorts = append(resp.ExposedPorts, ExposedPortInfo{Port: ep.Port, Domain: ep.Domain})
	}
	return resp
}

func normalizedWorkerConsole(
	console *v1beta1.WorkerConsoleSpec,
	runtime string,
) (*v1beta1.WorkerConsoleSpec, error) {
	// 逻辑说明：nil 表示未配置；否则复制后按有效 runtime 校验，并仅在启用且未指定端口时补默认端口，避免修改请求对象本身。
	if console == nil {
		return nil, nil
	}
	normalized := *console
	if err := normalized.Validate(runtime); err != nil {
		return nil, err
	}
	if normalized.Enabled && normalized.Port == 0 {
		normalized.Port = v1beta1.DefaultWorkerConsolePort
	}
	return &normalized, nil
}

func responseWorkerConsole(
	console *v1beta1.WorkerConsoleSpec,
) *v1beta1.WorkerConsoleSpec {
	// 逻辑说明：为响应复制 console 配置并规范启用时的零端口；不回写 CR，保持序列化视图与存储对象解耦。
	if console == nil {
		return nil
	}
	response := *console
	if response.Enabled && response.Port == 0 {
		response.Port = v1beta1.DefaultWorkerConsolePort
	}
	return &response
}

func teamToResponse(t *v1beta1.Team) TeamResponse {
	// 逻辑说明：合并 Team Spec 的期望成员与 Status 的房间/就绪信息，空 phase 规范为 Pending；按角色拆出 leader/worker 名单，并按状态成员汇总暴露端口。
	resp := TeamResponse{
		Name:           t.Name,
		TeamName:       t.Spec.EffectiveTeamName(t.Name),
		Phase:          t.Status.Phase,
		Description:    t.Spec.Description,
		Admin:          t.Spec.Admin,
		HumanMembers:   t.Spec.HumanMembers,
		WorkerMembers:  t.Spec.WorkerMembers,
		HeartbeatEvery: t.Spec.HeartbeatEvery,
		PeerMentions:   t.Spec.PeerMentions,
		TeamRoomID:     t.Status.TeamRoomID,
		LeaderDMRoomID: t.Status.LeaderDMRoomID,
		LeaderReady:    t.Status.LeaderReady,
		ReadyWorkers:   t.Status.ReadyWorkers,
		TotalWorkers:   t.Status.TotalWorkers,
		Message:        t.Status.Message,
	}
	if resp.Phase == "" {
		resp.Phase = "Pending"
	}
	for _, ref := range t.Spec.WorkerMembers {
		if ref.Role == "team_leader" {
			resp.LeaderName = ref.Name
			continue
		}
		resp.WorkerNames = append(resp.WorkerNames, ref.Name)
	}
	for _, ms := range t.Status.Members {
		if len(ms.ExposedPorts) == 0 {
			continue
		}
		if resp.WorkerExposedPorts == nil {
			resp.WorkerExposedPorts = make(map[string][]ExposedPortInfo)
		}
		for _, p := range ms.ExposedPorts {
			resp.WorkerExposedPorts[ms.Name] = append(resp.WorkerExposedPorts[ms.Name], ExposedPortInfo{Port: p.Port, Domain: p.Domain})
		}
	}
	return resp
}

func managerToResponse(m *v1beta1.Manager) ManagerResponse {
	// 逻辑说明：合并 Manager 的期望配置与实际 Matrix/版本状态，对可变切片和 CodingCLI 做复制；未进入 reconcile phase 时以 Pending 呈现。
	resp := ManagerResponse{
		Name:         m.Name,
		Phase:        m.Status.Phase,
		State:        m.Spec.DesiredState(),
		Model:        m.Spec.Model,
		Runtime:      m.Spec.Runtime,
		Image:        m.Spec.Image,
		Identity:     m.Spec.Identity,
		MatrixUserID: m.Status.MatrixUserID,
		RoomID:       m.Status.RoomID,
		Version:      m.Status.Version,
		Message:      m.Status.Message,
		McpServers:   append([]v1beta1.MCPServer(nil), m.Spec.McpServers...),
		CodingCLI:    m.Spec.CodingCLI.DeepCopy(),
		WelcomeSent:  m.Status.WelcomeSent,
	}
	if resp.Phase == "" {
		resp.Phase = "Pending"
	}
	return resp
}

func humanToResponse(h *v1beta1.Human) HumanResponse {
	// 逻辑说明：把 Human Spec 权限范围与 Status 中 Matrix 身份、初始密码和房间结果组成 API 视图，并将尚无 phase 的新对象规范为 Pending。
	resp := HumanResponse{
		Name:              h.Name,
		Phase:             h.Status.Phase,
		DisplayName:       h.Spec.DisplayName,
		Email:             h.Spec.Email,
		PermissionLevel:   h.Spec.PermissionLevel,
		AccessibleTeams:   h.Spec.AccessibleTeams,
		AccessibleWorkers: h.Spec.AccessibleWorkers,
		Note:              h.Spec.Note,
		MatrixUserID:      h.Status.MatrixUserID,
		InitialPassword:   h.Status.InitialPassword,
		Rooms:             h.Status.Rooms,
		Message:           h.Status.Message,
	}
	if resp.Phase == "" {
		resp.Phase = "Pending"
	}
	return resp
}

// findTeamForMember reports whether the given worker name is a member
// (leader or worker) of any Team in the current namespace.
func (h *ResourceHandler) findTeamForMember(ctx context.Context, name string) (string, bool, error) {
	// 逻辑说明：复用完整成员查找，只向删除校验暴露 Team 名称与是否命中；列表错误原样返回，未命中不伪造团队。
	team, _, ok, err := h.findTeamMember(ctx, name)
	if err != nil || !ok {
		return "", false, err
	}
	return team.Name, true, nil
}

func (h *ResourceHandler) validateTeamWorkerMembers(ctx context.Context, teamName string, members []v1beta1.TeamWorkerRef) error {
	// 逻辑说明：检查名称非空、同 Team 不重复、角色合法且恰有一个 leader，并逐一确认 Worker CR 存在；再扫描其他 Team 阻止同一 Worker 同时加入多个团队。
	seen := make(map[string]struct{}, len(members))
	leaders := 0
	for _, ref := range members {
		if ref.Name == "" {
			return fmt.Errorf("workerMembers.name is required")
		}
		if _, ok := seen[ref.Name]; ok {
			return fmt.Errorf("Worker %s is listed more than once", ref.Name)
		}
		seen[ref.Name] = struct{}{}
		switch ref.Role {
		case "team_leader":
			leaders++
		case "worker":
		default:
			return fmt.Errorf("Worker %s has invalid role %q", ref.Name, ref.Role)
		}

		var worker v1beta1.Worker
		if err := h.client.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: h.namespace}, &worker); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("referenced Worker %s does not exist", ref.Name)
			}
			return fmt.Errorf("get referenced Worker %s: %w", ref.Name, err)
		}
	}
	if leaders != 1 {
		return fmt.Errorf("workerMembers must contain exactly one team_leader")
	}

	var teams v1beta1.TeamList
	if err := h.client.List(ctx, &teams, client.InNamespace(h.namespace)); err != nil {
		return fmt.Errorf("list Teams: %w", err)
	}
	for i := range teams.Items {
		team := &teams.Items[i]
		if team.Name == teamName {
			continue
		}
		for _, ref := range team.Spec.WorkerMembers {
			if _, ok := seen[ref.Name]; ok {
				return fmt.Errorf("Worker %s is already a member of Team %s", ref.Name, team.Name)
			}
		}
	}
	return nil
}

// findTeamMember does the same as findTeamForMember but also returns the
// resolved Team CR and the member's name (for response synthesis).
func (h *ResourceHandler) findTeamMember(ctx context.Context, name string) (*v1beta1.Team, string, bool, error) {
	// 逻辑说明：列出当前 namespace 的 Team 并扫描期望 WorkerMembers，命中时返回实际 Team 指针与成员名供响应补全；列表失败与未找到明确区分。
	var list v1beta1.TeamList
	if err := h.client.List(ctx, &list, client.InNamespace(h.namespace)); err != nil {
		return nil, "", false, err
	}
	for i := range list.Items {
		t := &list.Items[i]
		for _, ref := range t.Spec.WorkerMembers {
			if ref.Name == name {
				return t, ref.Name, true, nil
			}
		}
	}
	return nil, "", false, nil
}

func (h *ResourceHandler) applyTeamMember(resp *WorkerResponse, t *v1beta1.Team, memberName string) {
	// 逻辑说明：先从 Team Spec 写入团队和角色，再仅在 Worker 自身响应缺少房间/Matrix user 时用 Team Status 成员快照补齐，避免覆盖更权威的 Worker Status。
	resp.Team = t.Name
	resp.Role = teamMemberRole(t, memberName)
	if ms := t.Status.MemberByName(memberName); ms != nil {
		if resp.RoomID == "" {
			resp.RoomID = ms.RoomID
		}
		if resp.MatrixUserID == "" {
			resp.MatrixUserID = ms.MatrixUserID
		}
	}
}

func teamMemberRole(t *v1beta1.Team, memberName string) string {
	// 逻辑说明：在期望成员中匹配名称并把 team_leader 原样返回，其余合法角色统一为 worker；未命中也回退 worker 以保持旧响应兼容。
	for _, ref := range t.Spec.WorkerMembers {
		if ref.Name != memberName {
			continue
		}
		if ref.Role == "team_leader" {
			return "team_leader"
		}
		return "worker"
	}
	return "worker"
}

// writeK8sError maps K8s API errors to HTTP status codes.
func writeK8sError(w http.ResponseWriter, op string, err error) {
	// 逻辑说明：识别 Kubernetes NotFound、AlreadyExists 和 resourceVersion Conflict，分别映射 404/409 与可重试提示；其他 API 故障返回带操作上下文的 500。
	switch {
	case apierrors.IsNotFound(err):
		httputil.WriteError(w, http.StatusNotFound, op+": not found")
	case apierrors.IsAlreadyExists(err):
		httputil.WriteError(w, http.StatusConflict, op+": already exists")
	case apierrors.IsConflict(err):
		httputil.WriteError(w, http.StatusConflict, op+": conflict (object modified, retry)")
	default:
		httputil.WriteError(w, http.StatusInternalServerError, op+": "+err.Error())
	}
}
