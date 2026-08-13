package executor

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/credprovider"
)

const (
	nacosAuthTypeNone  = "none"
	nacosAuthTypeNacos = "nacos"
	nacosAuthTypeSTS   = "sts-agentteams"
)

var defaultNacosSTSResources = []string{"agentSpec/*", "skill/*"}

// nacosCredential abstracts all authentication logic for NacosAIClient.
// NacosAIClient never touches credential state directly — it only calls
// Refresh before sending a request and Apply to inject headers.
type nacosCredential interface {
	// Refresh ensures the credential is valid (re-login or STS refresh as needed).
	Refresh(ctx context.Context) error
	// Apply injects authentication headers into an outbound request.
	Apply(req *http.Request)
}

// ── nacosNoneCredential ───────────────────────────────────────────────────

type nacosNoneCredential struct{}

func (nacosNoneCredential) Refresh(_ context.Context) error { return nil }
func (nacosNoneCredential) Apply(_ *http.Request)           {}

// ── nacosUserPassCredential ───────────────────────────────────────────────

type nacosUserPassCredential struct {
	serverAddr       string
	username         string
	password         string
	httpClient       *http.Client
	mu               sync.RWMutex
	accessToken      string
	tokenExpireAt    time.Time
	authLoginVersion string
}

func (c *nacosUserPassCredential) Refresh(ctx context.Context) error {
	// 逻辑说明：用写锁串行化 token 检查/登录；token 永不过期或距离过期超过五秒时复用，否则在锁内重新登录，避免并发请求重复刷新。
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.accessToken != "" {
		if c.tokenExpireAt.IsZero() || time.Now().Add(5*time.Second).Before(c.tokenExpireAt) {
			return nil
		}
	}
	return c.login(ctx)
}

func (c *nacosUserPassCredential) Apply(req *http.Request) {
	// 逻辑说明：在读锁下复制当前 access token，非空时写入 Bearer Authorization；不持锁执行网络，也不会把用户名密码放进请求。
	c.mu.RLock()
	tok := c.accessToken
	c.mu.RUnlock()
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
}

func (c *nacosUserPassCredential) login(ctx context.Context) error {
	// 逻辑说明：构造表单后优先尝试已知/默认 v3 登录，失败再兼容 v1；成功记住版本供下次优化，两者都未接受时返回明确错误。
	form := url.Values{}
	form.Set("username", c.username)
	form.Set("password", c.password)

	tryV3 := c.authLoginVersion == "" || c.authLoginVersion == "v3"
	if tryV3 {
		ok, err := c.tryLogin(ctx, fmt.Sprintf("http://%s/nacos/v3/auth/user/login", c.serverAddr), form)
		if err == nil && ok {
			c.authLoginVersion = "v3"
			return nil
		}
	}

	ok, err := c.tryLogin(ctx, fmt.Sprintf("http://%s/nacos/v1/auth/login", c.serverAddr), form)
	if err != nil {
		return err
	}
	if ok {
		c.authLoginVersion = "v1"
		return nil
	}
	return fmt.Errorf("login failed with both v3 and v1 auth endpoints")
}

func (c *nacosUserPassCredential) tryLogin(ctx context.Context, loginURL string, form url.Values) (bool, error) {
	// 逻辑说明：POST URL-encoded 凭据并读取响应；传输/I/O 错误返回 error，非 200 返回 `false,nil` 允许尝试另一版本，200 交给 JSON 解析决定是否有效。
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, strings.NewReader(form.Encode()))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	return c.applyLoginResponse(body), nil
}

func (c *nacosUserPassCredential) applyLoginResponse(body []byte) bool {
	// 逻辑说明：解析登录 JSON，兼容 token 位于顶层或 `data` 下两种 Nacos 版本格式；无效 JSON 返回 false 且不修改现有凭据。
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return false
	}
	if data, ok := result["data"].(map[string]interface{}); ok {
		return c.applyLoginMap(data)
	}
	return c.applyLoginMap(result)
}

func (c *nacosUserPassCredential) applyLoginMap(data map[string]interface{}) bool {
	// 逻辑说明：要求非空 accessToken 后写入缓存，并兼容多种数值类型解析 tokenTtl；正 TTL 计算绝对过期点，缺 TTL 表示长期有效。
	token, ok := data["accessToken"].(string)
	if !ok || token == "" {
		return false
	}
	c.accessToken = token

	var ttlSeconds int64
	switch value := data["tokenTtl"].(type) {
	case float64:
		ttlSeconds = int64(value)
	case int64:
		ttlSeconds = value
	case int:
		ttlSeconds = int64(value)
	}
	if ttlSeconds > 0 {
		c.tokenExpireAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	} else {
		c.tokenExpireAt = time.Time{}
	}
	return true
}

// ── nacosSTSCredential ────────────────────────────────────────────────────

type nacosSTSCredential struct {
	namespace   string
	sessionName string
	tm          *credprovider.TokenManager
	mu          sync.RWMutex
	cached      *credprovider.IssueResponse
}

func newNacosSTSCredential(namespace string, client credprovider.Client, resources []string) *nacosSTSCredential {
	// 逻辑说明：资源为空时采用只读 AgentSpec/Skill 默认范围，生成独立会话名并创建固定 namespace 的 TokenManager；资源切片再次复制，防止权限边界被外部突变。
	if len(resources) == 0 {
		resources = defaultNacosSTSResources
	}
	sessionName := uuid.NewString()
	tm := credprovider.NewTokenManager(client, credprovider.IssueRequest{
		SessionName: sessionName,
		Entries: []credprovider.AccessEntry{
			{
				Service:     credprovider.ServiceAIRegistry,
				Permissions: []string{"read", "list"},
				Scope: credprovider.AccessScope{
					NamespaceID: namespace,
					Resources:   append([]string(nil), resources...),
				},
			},
		},
	})
	return &nacosSTSCredential{namespace: namespace, sessionName: sessionName, tm: tm}
}

func (c *nacosSTSCredential) Refresh(ctx context.Context) error {
	// 逻辑说明：记录不含秘密的会话上下文，通过 TokenManager 复用或刷新 STS，再在写锁下原子替换缓存；签发失败保留现有缓存但向调用方返回错误。
	log.FromContext(ctx).Info("refreshing Nacos STS token",
		"sessionName", c.sessionName,
		"callerSessionName", "agentteams-nacos-"+c.namespace,
		"namespace", c.namespace,
	)
	tok, err := c.tm.Token(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.cached = tok
	c.mu.Unlock()
	return nil
}

func (c *nacosSTSCredential) Apply(req *http.Request) {
	// 逻辑说明：读取同一批缓存 STS，以 namespace/group/毫秒时间戳计算 HMAC-SHA1 签名并写入 Nacos Spas headers；无缓存时不注入，秘密本身不记录日志。
	c.mu.RLock()
	tok := c.cached
	c.mu.RUnlock()
	if tok == nil {
		return
	}

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	group := "DEFAULT_GROUP"

	signData := c.namespace + "+" + group + "+" + timestamp
	if c.namespace == "" {
		signData = timestamp
	}

	mac := hmac.New(sha1.New, []byte(tok.AccessKeySecret))
	mac.Write([]byte(signData))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req.Header.Set("Spas-AccessKey", tok.AccessKeyID)
	req.Header.Set("Spas-SecurityToken", tok.SecurityToken)
	req.Header.Set("Timestamp", timestamp)
	req.Header.Set("Spas-Signature", signature)
}
