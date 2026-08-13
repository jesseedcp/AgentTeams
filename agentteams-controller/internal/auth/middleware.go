package auth

import (
	"context"
	"log"
	"net/http"
	"strings"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type contextKey string

const callerKey contextKey = "caller"

// CallerFromContext extracts the CallerIdentity from the request context.
func CallerFromContext(ctx context.Context) *CallerIdentity {
	// 逻辑说明：读取鉴权中间件写入的私有 context key；不存在表示请求尚未通过认证链。
	if v := ctx.Value(callerKey); v != nil {
		return v.(*CallerIdentity)
	}
	return nil
}

// CallerKeyForTest returns the context key for injecting CallerIdentity in tests.
func CallerKeyForTest() contextKey {
	return callerKey
}

// Middleware provides HTTP authentication and authorization middleware.
type Middleware struct {
	authenticator Authenticator
	enricher      IdentityEnricher
	authorizer    *Authorizer
	k8s           client.Client
	namespace     string
}

// NewMiddleware creates an auth Middleware with the full auth chain.
func NewMiddleware(auth Authenticator, enricher IdentityEnricher, authz *Authorizer, k8s client.Client, namespace string) *Middleware {
	// 逻辑说明：组合认证、身份补全、授权和资源团队查询依赖，具体 handler 只声明动作与资源类型。
	return &Middleware{
		authenticator: auth,
		enricher:      enricher,
		authorizer:    authz,
		k8s:           k8s,
		namespace:     namespace,
	}
}

// Authenticate returns middleware that authenticates the caller and places
// the CallerIdentity in the request context. No authorization is performed.
func (m *Middleware) Authenticate(next http.Handler) http.Handler {
	// 逻辑说明：认证被禁用时透传；否则验证并补全调用者，失败返回 401，成功把身份副本写入请求 context。
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.authenticator == nil {
			next.ServeHTTP(w, r)
			return
		}

		identity, ok := m.authenticateAndEnrich(r)
		if !ok {
			httputil.WriteError(w, http.StatusUnauthorized, "invalid or missing bearer token")
			return
		}

		ctx := context.WithValue(r.Context(), callerKey, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ResourceNameFunc extracts the target resource name from an HTTP request.
type ResourceNameFunc func(r *http.Request) string

// NameFromPath returns a ResourceNameFunc that reads the "name" path parameter.
func NameFromPath(r *http.Request) string {
	return r.PathValue("name")
}

// RequireAuthz returns middleware that authenticates, enriches, resolves the
// target resource's team, and checks authorization against the permission matrix.
func (m *Middleware) RequireAuthz(action Action, kind string, nameFn ResourceNameFunc) func(http.Handler) http.Handler {
	// 逻辑说明：返回按路由参数化的中间件，依次认证、解析目标名/团队、执行角色矩阵，再把身份传给 handler。
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if m.authenticator == nil {
				next.ServeHTTP(w, r)
				return
			}

			identity, ok := m.authenticateAndEnrich(r)
			if !ok {
				httputil.WriteError(w, http.StatusUnauthorized, "invalid or missing bearer token")
				return
			}

			resourceName := ""
			if nameFn != nil {
				resourceName = nameFn(r)
			}

			resourceTeam := m.resolveResourceTeam(r.Context(), kind, resourceName)

			authzReq := AuthzRequest{
				Action:       action,
				ResourceKind: kind,
				ResourceName: resourceName,
				ResourceTeam: resourceTeam,
			}

			if err := m.authorizer.Authorize(identity, authzReq); err != nil {
				httputil.WriteError(w, http.StatusForbidden, err.Error())
				return
			}

			ctx := context.WithValue(r.Context(), callerKey, identity)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// resolveResourceTeam resolves Worker membership from Team.spec.workerMembers.
func (m *Middleware) resolveResourceTeam(ctx context.Context, kind, name string) string {
	// 逻辑说明：仅 Worker 明细请求需要查询团队；读取失败或无成员关系时返回空值，由授权器采取保守规则。
	if name == "" || m.k8s == nil {
		return ""
	}
	if kind != "worker" {
		return ""
	}

	var teams v1beta1.TeamList
	if err := m.k8s.List(ctx, &teams, client.InNamespace(m.namespace)); err != nil {
		return ""
	}
	for i := range teams.Items {
		for _, member := range teams.Items[i].Spec.WorkerMembers {
			if member.Name == name {
				return teams.Items[i].Name
			}
		}
	}
	return ""
}

func (m *Middleware) authenticateAndEnrich(r *http.Request) (*CallerIdentity, bool) {
	// 逻辑说明：严格提取 Bearer token 后认证；身份补全失败只记录并保留基础 SA 身份，让授权器继续限制权限。
	token := extractBearerToken(r)
	if token == "" {
		return nil, false
	}

	identity, err := m.authenticator.Authenticate(r.Context(), token)
	if err != nil {
		log.Printf("[AUTH] authentication failed: %v", err)
		return nil, false
	}

	if m.enricher != nil {
		if err := m.enricher.EnrichIdentity(r.Context(), identity); err != nil {
			log.Printf("[AUTH] identity enrichment failed for %s: %v", identity.Username, err)
		}
	}

	return identity, true
}

func extractBearerToken(r *http.Request) string {
	// 逻辑说明：只接受精确 Bearer 前缀并返回其余 token；缺失或其他认证方案都视为未认证。
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == authHeader {
		return ""
	}
	return token
}
