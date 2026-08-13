package auth

import "fmt"

// Action represents an API operation.
type Action string

const (
	ActionCreate             Action = "create"
	ActionUpdate             Action = "update"
	ActionDelete             Action = "delete"
	ActionGet                Action = "get"
	ActionList               Action = "list"
	ActionWake               Action = "wake"
	ActionSleep              Action = "sleep"
	ActionEnsureReady        Action = "ensure-ready"
	ActionReady              Action = "ready"
	ActionHeartbeat          Action = "heartbeat"
	ActionSTS                Action = "sts"
	ActionStatus             Action = "status"
	ActionRefreshMatrixToken Action = "refresh-matrix-token"
	ActionGateway            Action = "gateway"
)

// AuthzRequest describes the resource being accessed.
type AuthzRequest struct {
	Action       Action
	ResourceKind string // "worker" | "team" | "human" | "manager" | "gateway" | "status" | "credentials"
	ResourceName string // target resource name (empty for list operations)
	ResourceTeam string // target resource's team (resolved by handler/middleware)
}

// Authorizer enforces the Role + Team permission matrix.
type Authorizer struct{}

func NewAuthorizer() *Authorizer {
	// 逻辑说明：Authorizer 无可变状态，构造独立值供中间件注入并统一执行角色矩阵。
	return &Authorizer{}
}

// Authorize checks whether caller is allowed to perform the requested action.
// Returns nil if allowed, an error describing the denial otherwise.
func (a *Authorizer) Authorize(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：先拒绝空身份，再按角色分派；Admin/Manager 全权，其余角色必须进入更细资源规则。
	if caller == nil {
		return fmt.Errorf("authorization denied: no caller identity")
	}

	switch caller.Role {
	case RoleAdmin, RoleManager:
		return nil // full access

	case RoleTeamLeader:
		return a.authorizeTeamLeader(caller, req)

	case RoleWorker:
		return a.authorizeWorker(caller, req)

	default:
		return fmt.Errorf("authorization denied: unknown role %q", caller.Role)
	}
}

func (a *Authorizer) authorizeTeamLeader(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：Team Leader 可读集群状态、操作本团队 Worker、读取团队并换取自绑定凭据，其余默认拒绝。
	switch req.ResourceKind {
	case "status":
		return nil // read-only cluster info

	case "worker":
		return a.authorizeTeamLeaderWorkerAction(caller, req)

	case "team":
		if req.Action == ActionGet || req.Action == ActionList {
			return nil
		}
		return deny(caller, req)

	case "credentials":
		// Credential endpoints (STS + Matrix token refresh) are always
		// self-scoped: the issued token / refreshed credential is bound to the
		// calling identity, and these routes never embed a target ResourceName
		// (the handler uses caller.Username), so no requireSelf check is needed.
		if req.Action == ActionSTS || req.Action == ActionRefreshMatrixToken {
			return nil
		}
		return deny(caller, req)

	default:
		return deny(caller, req)
	}
}

func (a *Authorizer) authorizeTeamLeaderWorkerAction(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：列表由 handler 后续按团队过滤；所有指定 Worker 的读写/生命周期动作都要求同团队。
	switch req.Action {
	case ActionGet:
		return a.requireSameTeam(caller, req)
	case ActionList:
		return nil // handler filters by team
	case ActionCreate, ActionUpdate:
		return a.requireSameTeam(caller, req)
	case ActionWake, ActionSleep, ActionEnsureReady, ActionReady, ActionHeartbeat, ActionStatus:
		return a.requireSameTeam(caller, req)
	default:
		return deny(caller, req)
	}
}

func (a *Authorizer) authorizeWorker(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：普通 Worker 只可读状态、操作自己或领取调用者绑定凭据，其他资源类型全部拒绝。
	switch req.ResourceKind {
	case "status":
		return nil

	case "worker":
		return a.authorizeWorkerSelfAction(caller, req)

	case "credentials":
		// Credential endpoints (STS + Matrix token refresh) are always
		// self-scoped: the issued token / refreshed credential is bound to the
		// calling worker, and these routes never embed a target ResourceName
		// (the handler uses caller.Username), so no requireSelf check is needed.
		if req.Action == ActionSTS || req.Action == ActionRefreshMatrixToken {
			return nil
		}
		return deny(caller, req)

	default:
		return deny(caller, req)
	}
}

func (a *Authorizer) authorizeWorkerSelfAction(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：仅白名单自助动作进入 requireSelf，未列出的创建、修改、删除和他人操作一律拒绝。
	switch req.Action {
	case ActionReady, ActionHeartbeat:
		return a.requireSelf(caller, req)
	case ActionSTS:
		return a.requireSelf(caller, req)
	case ActionGet:
		return a.requireSelf(caller, req)
	case ActionStatus:
		return a.requireSelf(caller, req)
	default:
		return deny(caller, req)
	}
}

func (a *Authorizer) requireSameTeam(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：Leader 必须已解析出团队；目标有团队时必须相等，空目标团队留给创建等尚未归属场景。
	if caller.Team == "" {
		return fmt.Errorf("authorization denied: team-leader %q has no team", caller.Username)
	}
	if req.ResourceTeam != "" && req.ResourceTeam != caller.Team {
		return fmt.Errorf("authorization denied: team-leader %q (team %s) cannot access resource in team %s",
			caller.Username, caller.Team, req.ResourceTeam)
	}
	return nil
}

func (a *Authorizer) requireSelf(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：目标名存在时必须等于调用者稳定用户名；无目标名用于调用者隐式绑定的端点。
	if req.ResourceName != "" && req.ResourceName != caller.Username {
		return fmt.Errorf("authorization denied: %s %q cannot access resource %q",
			caller.Role, caller.Username, req.ResourceName)
	}
	return nil
}

func deny(caller *CallerIdentity, req AuthzRequest) error {
	// 逻辑说明：集中生成不含 token 的稳定拒绝原因，供 HTTP 层返回 403 和审计日志定位规则。
	return fmt.Errorf("authorization denied: %s %q cannot %s %s",
		caller.Role, caller.Username, req.Action, req.ResourceKind)
}
