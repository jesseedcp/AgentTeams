package credprovider

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TokenManager caches an STS token issued for a fixed IssueRequest
// (session name + resolved access entries) and refreshes it transparently
// before expiry. It is safe for concurrent use and is intended for
// long-lived controller-internal callers (OSS mc alias setup, APIG SDK
// client) that repeatedly ask for fresh credentials.
//
// Typical usage:
//
//	tm := credprovider.NewTokenManager(client, credprovider.IssueRequest{
//	    SessionName: "agentteams-controller",
//	    Entries:     accessresolver.ControllerDefaults(bucket, gatewayID),
//	})
//	tok, err := tm.Token(ctx)  // blocks only on cache-miss / expiry
type TokenManager struct {
	client Client
	req    IssueRequest
	margin time.Duration // refresh when remaining lifetime < margin
	now    func() time.Time

	mu        sync.Mutex
	cached    *IssueResponse
	expiresAt time.Time
}

// NewTokenManager creates a TokenManager that asks client for tokens
// matching req. The refresh margin defaults to 10 minutes.
func NewTokenManager(client Client, req IssueRequest) *TokenManager {
	return &TokenManager{
		client: client,
		req:    req,
		margin: 10 * time.Minute,
		now:    time.Now,
	}
}

// WithRefreshMargin sets a custom margin: the token is refreshed when
// less than margin is left before expiry. Useful in tests.
func (m *TokenManager) WithRefreshMargin(d time.Duration) *TokenManager {
	// 逻辑说明：覆盖 token 提前刷新的安全余量并返回同一管理器，供构造阶段链式配置；调用方应在并发取 token 前完成设置。
	m.margin = d
	return m
}

// Token returns a valid STS token, refreshing it if the cached one is
// missing or about to expire.
func (m *TokenManager) Token(ctx context.Context) (*IssueResponse, error) {
	// 逻辑说明：用互斥锁串行化缓存检查与刷新；缓存离过期时间仍超过安全余量时直接复用，否则向侧车签发新 token，并按返回寿命更新过期点，失败时不覆盖旧状态。
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cached != nil && m.now().Add(m.margin).Before(m.expiresAt) {
		return m.cached, nil
	}

	tok, err := m.client.Issue(ctx, m.req)
	if err != nil {
		return nil, fmt.Errorf("refresh %s token: %w", m.req.SessionName, err)
	}
	m.cached = tok
	m.expiresAt = m.now().Add(time.Duration(tok.ExpiresInSec) * time.Second)
	return tok, nil
}

// Invalidate forces the next Token call to refresh.
func (m *TokenManager) Invalidate() {
	// 逻辑说明：在互斥锁保护下同时清空缓存对象和过期时间，使下一次 Token 必须重新签发；它不立即访问凭据侧车。
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cached = nil
	m.expiresAt = time.Time{}
}
