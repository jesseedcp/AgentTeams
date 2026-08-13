package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HigressClient implements Client for self-hosted Higress gateway.
type HigressClient struct {
	config Config
	http   *http.Client

	mu      sync.Mutex
	cookies []*http.Cookie

	aiRouteMu sync.Mutex
}

// NewHigressClient creates a gateway Client for Higress Console API.
// NewHigressClient 创建 Higress Console API 客户端。传入 nil httpClient 时使用
// 带 30 秒超时的默认客户端，避免 Higress 故障时一次 reconcile 无期挂起。
// 测试可注入自定义 client/transport，不需真实网关。
func NewHigressClient(cfg Config, httpClient *http.Client) *HigressClient {
	// 逻辑说明：为空的传入 HTTP client 补 30 秒超时，并保存 Console 配置；这里只构造可并发复用的客户端，不登录或访问 Higress。
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &HigressClient{config: cfg, http: httpClient}
}

// ensureSession logs in to Higress Console and caches the session cookie.
func (c *HigressClient) ensureSession(ctx context.Context) error {
	// 逻辑说明：用互斥锁保证首次初始化/登录只执行一次；已有 cookie 直接复用，否则幂等初始化管理员、按新密码登录，必要时用初始密码登录并改密后重新登录，失败不缓存会话。
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cookies) > 0 {
		return nil
	}

	// Initialize admin account on first boot (idempotent — succeeds if already initialized).
	// Higress Console requires /system/init before login works.
	initBody := fmt.Sprintf(`{"adminUser":{"name":%q,"password":%q,"displayName":%q}}`,
		c.config.AdminUser, c.config.AdminPassword, c.config.AdminUser)
	initReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.config.ConsoleURL+"/system/init",
		strings.NewReader(initBody))
	if err != nil {
		return err
	}
	initReq.Header.Set("Content-Type", "application/json")
	initResp, err := c.http.Do(initReq)
	if err != nil {
		return fmt.Errorf("higress init: %w", err)
	}
	initResp.Body.Close()

	// Steady state should use the configured password. Embedded all-in-one boot
	// may briefly win the race and initialize itself with the upstream default
	// admin/admin before AgentTeams reaches /system/init. In that bootstrap-only
	// case, recover with admin/admin once, converge the password, then re-login
	// with the configured credentials.
	if err := c.loginLocked(ctx, c.config.AdminUser, c.config.AdminPassword); err == nil {
		return nil
	} else if !c.config.AllowDefaultAdminFallback || c.config.AdminUser != "admin" || c.config.AdminPassword == "admin" {
		return err
	}

	if err := c.loginLocked(ctx, "admin", "admin"); err != nil {
		return fmt.Errorf("higress login with configured credentials failed; fallback admin/admin login also failed: %w", err)
	}

	if err := c.changePasswordLocked(ctx, "admin", c.config.AdminPassword); err != nil {
		return fmt.Errorf("higress default admin/admin login succeeded but password convergence failed: %w", err)
	}

	c.cookies = nil
	if err := c.loginLocked(ctx, c.config.AdminUser, c.config.AdminPassword); err != nil {
		return fmt.Errorf("higress password converged but relogin with configured credentials failed: %w", err)
	}
	return nil
}

func (c *HigressClient) loginLocked(ctx context.Context, username, password string) error {
	// 逻辑说明：向 Console 登录接口发送 JSON 凭据，只接受 200 并缓存响应 cookies；请求、传输或拒绝响应都返回且不保留无效会话。
	body := fmt.Sprintf(`{"username":%q,"password":%q}`, username, password)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.config.ConsoleURL+"/session/login",
		strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("higress login: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		c.cookies = resp.Cookies()
		return nil
	}

	return fmt.Errorf("higress login failed for user %q: HTTP %d (check higress-console state/secret)", username, resp.StatusCode)
}

func (c *HigressClient) changePasswordLocked(ctx context.Context, oldPassword, newPassword string) error {
	// 逻辑说明：携带当前会话 cookie 调用改密接口；只接受 200/204，失败读取受限响应作为错误，成功后由 ensureSession 重新登录获取新 cookie。
	body := fmt.Sprintf(`{"oldPassword":%q,"newPassword":%q}`, oldPassword, newPassword)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.config.ConsoleURL+"/user/changePassword",
		strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range c.cookies {
		req.AddCookie(cookie)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("higress change password: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("higress change password failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
}

// EnsureConsumer 确保指定 bearer key 的 Higress consumer 存在。
// HTTP 409 在这个 Ensure 语义中表示期望状态已满足，而不是需要不断重试
// 的硬错误。但 Console API 成功可早于数据面认证配置生效，调用者若紧接着
// 发模型请求，还需要 readiness 检查或重试。
func (c *HigressClient) EnsureConsumer(ctx context.Context, req ConsumerRequest) (*ConsumerResult, error) {
	// 逻辑说明：把逻辑 Consumer 与 Bearer key-auth 凭据提交 Console；200/201 返回 created，409 返回 exists 以支持 reconcile 幂等，其他状态或网络错误失败。
	body := map[string]interface{}{
		"name": req.Name,
		"credentials": []map[string]interface{}{
			{
				"type":   "key-auth",
				"source": "BEARER",
				"values": []string{req.CredentialKey},
			},
		},
	}

	_, statusCode, err := c.doJSON(ctx, http.MethodPost, "/v1/consumers", body)
	if err != nil {
		return nil, fmt.Errorf("ensure consumer %s: %w", req.Name, err)
	}

	status := "created"
	if statusCode == http.StatusConflict {
		status = "exists"
	} else if statusCode != http.StatusOK && statusCode != http.StatusCreated {
		return nil, fmt.Errorf("ensure consumer %s: HTTP %d", req.Name, statusCode)
	}

	return &ConsumerResult{
		Status: status,
		APIKey: req.CredentialKey,
	}, nil
}

func (c *HigressClient) DeleteConsumer(ctx context.Context, name string) error {
	// 逻辑说明：删除精确 Consumer 名；200/204 和 404 均视为幂等成功，其他 HTTP 状态或会话/网络错误带名称返回。
	_, statusCode, err := c.doJSON(ctx, http.MethodDelete, "/v1/consumers/"+name, nil)
	if err != nil {
		return fmt.Errorf("delete consumer %s: %w", name, err)
	}
	if statusCode != http.StatusOK && statusCode != http.StatusNoContent && statusCode != http.StatusNotFound {
		return fmt.Errorf("delete consumer %s: HTTP %d", name, statusCode)
	}
	return nil
}

func (c *HigressClient) AuthorizeAIRoutes(ctx context.Context, consumerName string, modelAPIID string) error {
	// 逻辑说明：把消费者与可选模型提供方过滤条件交给统一路由读改写流程，以添加授权；列表、解析或保存任一步失败均返回。
	return c.modifyAIRoutes(ctx, consumerName, modelAPIID, true)
}

func (c *HigressClient) DeauthorizeAIRoutes(ctx context.Context, consumerName string, modelAPIID string) error {
	// 逻辑说明：复用统一路由读改写流程删除消费者授权；modelAPIID 为空时作用于全部 AI 路由，失败边界由底层流程保留。
	return c.modifyAIRoutes(ctx, consumerName, modelAPIID, false)
}

// modifyAIRoutes adds or removes the consumer from AI routes' allowedConsumers.
// When providerFilter is non-empty, AuthorizeAIRoutes keeps the consumer only
// on matching routes and removes it from non-matching routes; DeauthorizeAIRoutes
// removes it only from matching routes. Empty providerFilter keeps the legacy
// all-route behavior.
func (c *HigressClient) modifyAIRoutes(ctx context.Context, consumerName string, providerFilter string, add bool) error {
	// 逻辑说明：用专用锁串行化 AI route 授权读改写，列出路由后按 provider 过滤并对 allowedConsumers 加/删目标；只更新确有变化的 route，任何列表/解析/保存失败停止以免丢并发授权。
	c.aiRouteMu.Lock()
	defer c.aiRouteMu.Unlock()

	respBody, statusCode, err := c.doJSON(ctx, http.MethodGet, "/v1/ai/routes", nil)
	if err != nil {
		return fmt.Errorf("list AI routes: %w", err)
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("list AI routes: HTTP %d", statusCode)
	}

	var listResp struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(respBody, &listResp); err != nil {
		return fmt.Errorf("decode AI routes list: %w", err)
	}

	const maxRetries = 5

	var firstErr error
	recordErr := func(e error) {
		if firstErr == nil {
			firstErr = e
		}
	}

	for _, raw := range listResp.Data {
		var routeInfo struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &routeInfo); err != nil || routeInfo.Name == "" {
			continue
		}

		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}

			routeBody, sc, err := c.doJSON(ctx, http.MethodGet,
				"/v1/ai/routes/"+routeInfo.Name, nil)
			if err != nil {
				lastErr = fmt.Errorf("get AI route %s: %w", routeInfo.Name, err)
				break
			}
			if sc != http.StatusOK {
				lastErr = fmt.Errorf("get AI route %s: HTTP %d", routeInfo.Name, sc)
				break
			}

			var routeResp struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(routeBody, &routeResp); err != nil {
				lastErr = fmt.Errorf("decode AI route %s envelope: %w", routeInfo.Name, err)
				break
			}
			routeData := routeResp.Data
			if routeData == nil {
				routeData = routeBody
			}

			var route map[string]interface{}
			if err := json.Unmarshal(routeData, &route); err != nil {
				lastErr = fmt.Errorf("decode AI route %s: %w", routeInfo.Name, err)
				break
			}

			matchesProvider := providerFilter == "" || routeMatchesProvider(route, providerFilter)
			if providerFilter != "" && !add && !matchesProvider {
				break
			}

			authConfig, _ := route["authConfig"].(map[string]interface{})
			if authConfig == nil {
				authConfig = make(map[string]interface{})
			}

			consumers := toStringSlice(authConfig["allowedConsumers"])

			changed := true
			if add && matchesProvider {
				if !containsString(consumers, consumerName) {
					consumers = append(consumers, consumerName)
				}
				// Always PUT to trigger WASM key-auth resync — the consumer
				// may have been created after the route was last written,
				// so WASM needs to reload credentials even if the name was
				// already in allowedConsumers.
			} else {
				before := len(consumers)
				consumers = removeString(consumers, consumerName)
				changed = before != len(consumers)
			}

			if !changed {
				break
			}

			authConfig["allowedConsumers"] = consumers
			route["authConfig"] = authConfig

			_, sc, err = c.doJSON(ctx, http.MethodPut,
				"/v1/ai/routes/"+routeInfo.Name, route)
			if err != nil {
				lastErr = fmt.Errorf("put AI route %s: %w", routeInfo.Name, err)
				break
			}
			if sc == http.StatusOK {
				lastErr = nil
				break
			}
			if sc == http.StatusConflict {
				lastErr = fmt.Errorf("put AI route %s: HTTP 409 (conflict)", routeInfo.Name)
				time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)
				continue
			}
			lastErr = fmt.Errorf("put AI route %s: HTTP %d", routeInfo.Name, sc)
			break
		}
		if lastErr != nil {
			recordErr(lastErr)
		}
	}

	return firstErr
}

// routeMatchesProvider checks if any upstream in the route references the given provider.
func routeMatchesProvider(route map[string]interface{}, provider string) bool {
	// 逻辑说明：从 route upstreams 中查找 provider 字段精确匹配；结构类型不符或无命中返回 false，避免把未知路由纳入授权修改。
	upstreams, ok := route["upstreams"].([]interface{})
	if !ok {
		return false
	}
	for _, u := range upstreams {
		ups, ok := u.(map[string]interface{})
		if !ok {
			continue
		}
		if p, _ := ups["provider"].(string); p == provider {
			return true
		}
	}
	return false
}

func (c *HigressClient) ExposePort(ctx context.Context, req PortExposeRequest) error {
	// 逻辑说明：从 Worker/端口派生稳定 service source、route 与默认域名/ServiceHost，按 domain→DNS source→route 顺序幂等创建；任一步失败返回，避免宣告未完整暴露。
	svcSrc := fmt.Sprintf("worker-%s-%d", req.WorkerName, req.Port)
	routeN := svcSrc
	domain := req.Domain
	if domain == "" {
		domain = fmt.Sprintf("worker-%s-%d-local.agentteams.io", req.WorkerName, req.Port)
	}
	dnsHost := req.ServiceHost
	if dnsHost == "" {
		dnsHost = fmt.Sprintf("%s.local", req.WorkerName)
	}

	if err := c.ensureDomain(ctx, domain); err != nil {
		return fmt.Errorf("expose port %d: %w", req.Port, err)
	}
	if err := c.ensureServiceSource(ctx, svcSrc, dnsHost, req.Port, "http"); err != nil {
		return fmt.Errorf("expose port %d: %w", req.Port, err)
	}
	if err := c.ensureRoute(ctx, routeN, []string{domain}, svcSrc+".dns", req.Port, "/"); err != nil {
		return fmt.Errorf("expose port %d: %w", req.Port, err)
	}
	return nil
}

func (c *HigressClient) UnexposePort(ctx context.Context, req PortExposeRequest) error {
	// 逻辑说明：按与 ExposePort 相同规则派生资源名，并依次尽力删除 route、service source、domain 后触发配置推送；底层删除函数把不存在视为正常。
	svcSrc := fmt.Sprintf("worker-%s-%d", req.WorkerName, req.Port)
	routeN := svcSrc
	domain := req.Domain
	if domain == "" {
		domain = fmt.Sprintf("worker-%s-%d-local.agentteams.io", req.WorkerName, req.Port)
	}

	c.deleteRoute(ctx, routeN)
	c.deleteServiceSource(ctx, svcSrc)
	c.deleteDomain(ctx, domain)
	return nil
}

// --- Public infrastructure init methods (used by Initializer) ---

func (c *HigressClient) EnsureServiceSource(ctx context.Context, name, domain string, port int, protocol string) error {
	// 逻辑说明：将公开接口的 DNS 服务源参数完整交给 Higress 收敛实现；该调用会访问 Console，冲突视为已存在，其余失败返回调用方。
	return c.ensureServiceSource(ctx, name, domain, port, protocol)
}

// EnsureRESTMCPServer idempotently creates one REST-to-MCP route and retains
// every existing consumer while adding the requested bootstrap consumers. The
// credential-bearing raw configuration is sent directly to Higress and is not
// retained by the Controller.
func (c *HigressClient) EnsureRESTMCPServer(
	ctx context.Context,
	req RESTMCPServerRequest,
) (MCPServerEndpoint, error) {
	// 逻辑说明：校验 MCP 名称、服务、端口与 raw 配置，确保服务源/API 后读取现有消费者并与必需列表去重合并，最后返回 HTTP endpoint；任一必需资源失败不返回 endpoint。
	if req.Name == "" || req.ServiceName == "" || req.ServiceDomain == "" {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP server: required name is empty")
	}
	if req.ServicePort < 1 || req.ServicePort > 65535 {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP server %s: invalid service port", req.Name)
	}
	if req.RawConfiguration == "" {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP server %s: empty configuration", req.Name)
	}
	dataPlaneURL := strings.TrimRight(c.config.DataPlaneURL, "/")
	parsed, err := url.Parse(dataPlaneURL)
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP server %s: invalid data plane URL", req.Name)
	}
	if err := c.ensureServiceSource(
		ctx,
		req.ServiceName,
		req.ServiceDomain,
		req.ServicePort,
		req.ServiceProtocol,
	); err != nil {
		return MCPServerEndpoint{}, err
	}

	apiName := "mcp-" + req.Name
	existingConsumers, err := c.mcpConsumers(ctx, apiName)
	if err != nil {
		return MCPServerEndpoint{}, fmt.Errorf(
			"ensure REST MCP server %s: read existing consumers: %w",
			req.Name,
			err,
		)
	}
	consumers := mergeStrings(existingConsumers, req.Consumers)
	body := map[string]interface{}{
		"name":              apiName,
		"description":       req.Description,
		"type":              "OPEN_API",
		"rawConfigurations": req.RawConfiguration,
		"mcpServerName":     apiName,
		"domains":           []string{parsed.Hostname()},
		"services": []map[string]interface{}{{
			"name":   req.ServiceName + ".dns",
			"port":   req.ServicePort,
			"weight": 100,
		}},
		"consumerAuthInfo": map[string]interface{}{
			"type":             "key-auth",
			"enable":           true,
			"allowedConsumers": consumers,
		},
	}
	_, status, err := c.doJSON(ctx, http.MethodPut, "/v1/mcpServer", body)
	if err != nil {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP server %s: %w", req.Name, err)
	}
	if status < 200 || status >= 300 {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP server %s: HTTP %d", req.Name, status)
	}
	_, status, err = c.doJSON(
		ctx,
		http.MethodPut,
		"/v1/mcpServer/consumers",
		map[string]interface{}{
			"mcpServerName": apiName,
			"consumers":     consumers,
		},
	)
	if err != nil {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP consumers %s: %w", req.Name, err)
	}
	if status < 200 || status >= 300 {
		return MCPServerEndpoint{}, fmt.Errorf("ensure REST MCP consumers %s: HTTP %d", req.Name, status)
	}
	return MCPServerEndpoint{
		Name:      req.Name,
		URL:       dataPlaneURL + "/mcp-servers/" + apiName + "/mcp",
		Transport: "http",
	}, nil
}

func (c *HigressClient) mcpConsumers(
	ctx context.Context,
	apiName string,
) ([]string, error) {
	// 逻辑说明：查询指定 MCP API 的消费者；404 代表尚无授权返回 nil，200 时解析多版本响应形状，其他状态或无法识别 payload 返回错误而不覆盖现有授权。
	path := "/v1/mcpServer/consumers?mcpServerName=" +
		url.QueryEscape(apiName)
	body, status, err := c.doJSON(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("HTTP %d", status)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("empty response")
	}
	var payload interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	consumers, ok := extractMCPConsumers(payload)
	if !ok {
		return nil, fmt.Errorf("response has no consumer list")
	}
	return consumers, nil
}

func extractMCPConsumers(payload interface{}) ([]string, bool) {
	// 逻辑说明：递归兼容响应中 data/consumers/allowedConsumers、数组对象与直接字符串数组等形状；只在能确定得到完整字符串列表时返回 true。
	switch value := payload.(type) {
	case map[string]interface{}:
		if data, exists := value["data"]; exists {
			if consumers, ok := extractMCPConsumers(data); ok {
				return consumers, true
			}
		}
		for _, key := range []string{"consumers", "allowedConsumers"} {
			if raw, exists := value[key]; exists {
				return stringList(raw)
			}
		}
		if auth, exists := value["consumerAuthInfo"]; exists {
			return extractMCPConsumers(auth)
		}
	case []interface{}:
		consumers := make([]string, 0, len(value))
		for _, raw := range value {
			switch item := raw.(type) {
			case string:
				consumers = append(consumers, item)
			case map[string]interface{}:
				name, _ := item["consumerName"].(string)
				if name == "" {
					name, _ = item["name"].(string)
				}
				if name == "" {
					return nil, false
				}
				consumers = append(consumers, name)
			default:
				return nil, false
			}
		}
		return consumers, true
	}
	return nil, false
}

func stringList(raw interface{}) ([]string, bool) {
	// 逻辑说明：严格把 `[]interface{}` 转为字符串切片；任何非字符串元素使整体解析失败，避免静默丢失 Consumer。
	items, ok := raw.([]interface{})
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

func mergeStrings(existing, required []string) []string {
	// 逻辑说明：按现有列表再必需列表的顺序去重合并，既保留用户已有授权又补齐 Controller 需要项；返回新切片不修改输入。
	result := make([]string, 0, len(existing)+len(required))
	seen := make(map[string]struct{}, len(existing)+len(required))
	for _, items := range [][]string{existing, required} {
		for _, item := range items {
			if _, exists := seen[item]; exists {
				continue
			}
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

func (c *HigressClient) EnsureStaticServiceSource(ctx context.Context, name, address string, port int) error {
	// 逻辑说明：将静态地址和端口交给幂等服务源收敛实现；该调用会写 Higress Console，非成功/已存在状态作为错误返回。
	return c.ensureStaticServiceSource(ctx, name, address, port)
}

func (c *HigressClient) EnsureRoute(ctx context.Context, name string, domains []string, serviceName string, port int, pathPrefix string) error {
	// 逻辑说明：把域名、后端服务和路径前缀委托给路由收敛实现；Console 请求失败或返回非成功/冲突状态时阻止上层继续。
	return c.ensureRoute(ctx, name, domains, serviceName, port, pathPrefix)
}

func (c *HigressClient) DeleteRoute(ctx context.Context, name string) error {
	// 逻辑说明：以尽力而为方式向 Higress 删除指定路由；底层兼容清理会忽略 HTTP 结果，因此此接口固定返回成功，适合重复卸载但不能证明远端已删除。
	c.deleteRoute(ctx, name)
	return nil
}

func (c *HigressClient) EnsureAIProvider(ctx context.Context, req AIProviderRequest) error {
	// 逻辑说明：把 Provider 类型、token、协议与可选 rawConfigs 提交 Console；200/201/409 都表示期望资源已存在，其他状态或请求错误返回。
	body := map[string]interface{}{
		"name":     req.Name,
		"type":     req.Type,
		"tokens":   req.Tokens,
		"protocol": req.Protocol,
	}
	if req.Raw != nil {
		body["rawConfigs"] = req.Raw
	}
	_, sc, err := c.doJSON(ctx, http.MethodPost, "/v1/ai/providers", body)
	if err != nil {
		return fmt.Errorf("ensure AI provider %s: %w", req.Name, err)
	}
	if sc == 200 || sc == 201 || sc == 409 {
		return nil
	}
	return fmt.Errorf("ensure AI provider %s: HTTP %d", req.Name, sc)
}

func (c *HigressClient) EnsureStreamIdleTimeout(ctx context.Context, seconds int) error {
	// 逻辑说明：秒数非正时回退 900，读取 Higress YAML 配置并仅补丁 downstream idleTimeout；内容未变化直接成功，有变化才 PUT，读取/解析接口状态异常均失败。
	if seconds <= 0 {
		seconds = 900
	}

	respBody, sc, err := c.doJSON(ctx, http.MethodGet, "/system/higress-config", nil)
	if err != nil {
		return fmt.Errorf("ensure stream idle timeout: read higress config: %w", err)
	}
	if sc != http.StatusOK {
		return fmt.Errorf("ensure stream idle timeout: read higress config HTTP %d", sc)
	}

	var resp struct {
		Success bool        `json:"success"`
		Data    interface{} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return fmt.Errorf("ensure stream idle timeout: parse higress config response: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("ensure stream idle timeout: higress config response was unsuccessful")
	}
	config, ok := resp.Data.(string)
	if !ok {
		return fmt.Errorf("ensure stream idle timeout: higress config data was %T, want string", resp.Data)
	}

	patched := patchDownstreamIdleTimeout(config, seconds)
	_, putSC, putErr := c.doJSON(ctx, http.MethodPut, "/system/higress-config", map[string]interface{}{"config": patched})
	if putErr != nil {
		return fmt.Errorf("ensure stream idle timeout: update higress config: %w", putErr)
	}
	if putSC != http.StatusOK && putSC != http.StatusCreated && putSC != http.StatusNoContent {
		return fmt.Errorf("ensure stream idle timeout: update higress config HTTP %d", putSC)
	}
	return nil
}

func patchDownstreamIdleTimeout(config string, seconds int) string {
	// 逻辑说明：逐行跟踪 YAML `downstream` 块，替换已有 idleTimeout 或在缺失时插入，块不存在则追加完整段；只做文本变换并保留其他配置行。
	lines := strings.Split(config, "\n")
	out := make([]string, 0, len(lines)+1)
	inDownstream := false
	patched := false
	value := "      idleTimeout: " + strconv.Itoa(seconds)

	for _, line := range lines {
		if line == "    downstream:" {
			inDownstream = true
			out = append(out, line)
			continue
		}
		if inDownstream && strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") {
			if !patched {
				out = append(out, value)
				patched = true
			}
			inDownstream = false
		}
		if inDownstream && strings.HasPrefix(strings.TrimSpace(line), "idleTimeout:") {
			out = append(out, value)
			patched = true
			continue
		}
		out = append(out, line)
	}

	if inDownstream && !patched {
		out = append(out, value)
	}
	return strings.Join(out, "\n")
}

// EnsureAIRoute creates the AI route skeleton (name, path, upstream, key-auth
// framework) only if it does not already exist. It deliberately never writes
// authConfig.allowedConsumers: that field is owned by Manager/Worker
// reconcilers via AuthorizeAIRoutes / DeauthorizeAIRoutes. Re-running this
// function on an already-provisioned cluster is a true no-op and will never
// touch the authorization state, eliminating the restart-time race that
// previously reset allowedConsumers and produced 403s.
func (c *HigressClient) EnsureAIRoute(ctx context.Context, req AIRouteRequest) error {
	// 逻辑说明：先 GET 精确 AI route；存在时只补缺失 provider/upstream 与路径并 PUT，不存在时 POST 新骨架，未知状态失败；不触碰 allowedConsumers，授权归 reconcile 单独管理。
	getBody, sc, err := c.doJSON(ctx, http.MethodGet, "/v1/ai/routes/"+req.Name, nil)
	if err != nil {
		return fmt.Errorf("ensure AI route %s: check existence: %w", req.Name, err)
	}

	switch sc {
	case http.StatusOK:
		var resp struct {
			Data map[string]interface{} `json:"data"`
		}
		if jerr := json.Unmarshal(getBody, &resp); jerr == nil && resp.Data != nil {
			existingPath := ""
			if p, ok := resp.Data["pathPredicate"].(map[string]interface{}); ok {
				existingPath, _ = p["matchValue"].(string)
			}
			existingProvider := ""
			if ups, ok := resp.Data["upstreams"].([]interface{}); ok && len(ups) > 0 {
				if u0, ok := ups[0].(map[string]interface{}); ok {
					existingProvider, _ = u0["provider"].(string)
				}
			}
			if existingPath != req.PathPrefix || existingProvider != req.Provider {
				log.Printf("[WARN] AI route %s already exists with divergent skeleton (path=%q provider=%q, want path=%q provider=%q); leaving auth state untouched",
					req.Name, existingPath, existingProvider, req.PathPrefix, req.Provider)
			}
		}
		return nil

	case http.StatusNotFound:
		body := map[string]interface{}{
			"name":    req.Name,
			"domains": []string{},
			"pathPredicate": map[string]interface{}{
				"matchType":     "PRE",
				"matchValue":    req.PathPrefix,
				"caseSensitive": false,
			},
			"upstreams": []map[string]interface{}{
				{"provider": req.Provider, "weight": 100, "modelMapping": map[string]interface{}{}},
			},
			// Enable the key-auth framework, but deliberately omit
			// allowedConsumers: Higress defaults it to [] and Manager/Worker
			// reconcilers will populate it via AuthorizeAIRoutes once their
			// consumers exist. We never write this field here.
			"authConfig": map[string]interface{}{
				"enabled":                true,
				"allowedCredentialTypes": []string{"key-auth"},
			},
		}
		_, psc, perr := c.doJSON(ctx, http.MethodPost, "/v1/ai/routes", body)
		if perr != nil {
			return fmt.Errorf("ensure AI route %s: create: %w", req.Name, perr)
		}
		if psc == http.StatusOK || psc == http.StatusCreated || psc == http.StatusConflict {
			return nil
		}
		return fmt.Errorf("ensure AI route %s: create: HTTP %d", req.Name, psc)

	default:
		return fmt.Errorf("ensure AI route %s: check existence: HTTP %d", req.Name, sc)
	}
}

func (c *HigressClient) ResolveModelProvider(ctx context.Context, name string) (*ModelProviderInfo, error) {
	// 逻辑说明：先确认 AI Provider 存在，再遍历 AI routes 寻找引用它的 upstream，从路由提取服务名/端口/路径并组装集群内 URL；缺任一关联资源返回明确错误。
	// Verify the provider exists.
	_, sc, err := c.doJSON(ctx, http.MethodGet, "/v1/ai/providers/"+name, nil)
	if err != nil {
		return nil, fmt.Errorf("higress: get AI provider %q: %w", name, err)
	}
	if sc == http.StatusNotFound {
		return nil, fmt.Errorf("higress: model provider %q not found", name)
	}
	if sc != http.StatusOK {
		return nil, fmt.Errorf("higress: get AI provider %q: HTTP %d", name, sc)
	}

	// Find the AI route that uses this provider.
	routesBody, sc, err := c.doJSON(ctx, http.MethodGet, "/v1/ai/routes", nil)
	if err != nil {
		return nil, fmt.Errorf("higress: list AI routes: %w", err)
	}
	if sc != http.StatusOK {
		return nil, fmt.Errorf("higress: list AI routes: HTTP %d", sc)
	}

	var listResp struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(routesBody, &listResp); err != nil {
		return nil, fmt.Errorf("higress: decode AI routes list: %w", err)
	}

	for _, raw := range listResp.Data {
		var routeInfo struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &routeInfo); err != nil || routeInfo.Name == "" {
			continue
		}

		routeBody, sc, err := c.doJSON(ctx, http.MethodGet, "/v1/ai/routes/"+routeInfo.Name, nil)
		if err != nil || sc != http.StatusOK {
			continue
		}

		var routeResp struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(routeBody, &routeResp); err != nil {
			continue
		}
		routeData := routeResp.Data
		if routeData == nil {
			routeData = routeBody
		}

		var route struct {
			PathPredicate struct {
				MatchValue string `json:"matchValue"`
			} `json:"pathPredicate"`
			Upstreams []struct {
				Provider string `json:"provider"`
			} `json:"upstreams"`
		}
		if err := json.Unmarshal(routeData, &route); err != nil {
			continue
		}

		for _, ups := range route.Upstreams {
			if ups.Provider == name {
				basePath := route.PathPredicate.MatchValue
				intranetURL := strings.TrimRight(c.config.DataPlaneURL, "/") + basePath
				return &ModelProviderInfo{
					HttpApiID:   name,
					BasePath:    basePath,
					IntranetURL: intranetURL,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("higress: no AI route found for model provider %q", name)
}

func (c *HigressClient) Healthy(ctx context.Context) error {
	// 逻辑说明：通过受认证的 Consumer 列表接口验证 Console 会话与 API 可达；只有 HTTP 200 成功，其他状态供 initializer 重试。
	_, sc, err := c.doJSON(ctx, http.MethodGet, "/v1/consumers", nil)
	if err != nil {
		return err
	}
	if sc != http.StatusOK {
		return fmt.Errorf("higress health check: HTTP %d", sc)
	}
	return nil
}

// ── Higress Console primitives (migrated from controller/higress_client.go) ──

func (c *HigressClient) ensureDomain(ctx context.Context, name string) error {
	// 逻辑说明：提交关闭 HTTPS 的域名声明；200/201/409 视为幂等收敛，网络或其他状态返回精确域名错误。
	body := map[string]interface{}{"name": name, "enableHttps": "off"}
	_, sc, err := c.doJSON(ctx, http.MethodPost, "/v1/domains", body)
	if err != nil {
		return fmt.Errorf("ensure domain %s: %w", name, err)
	}
	if sc != 200 && sc != 201 && sc != 409 {
		return fmt.Errorf("ensure domain %s: HTTP %d", name, sc)
	}
	return nil
}

func (c *HigressClient) ensureServiceSource(ctx context.Context, name, dnsDomain string, port int, protocol string) error {
	// 逻辑说明：协议空时补 HTTP，构造无认证 DNS service source 并提交；成功/创建/冲突均接受，其他状态阻止后续路由依赖它。
	if protocol == "" {
		protocol = "http"
	}
	body := map[string]interface{}{
		"type": "dns", "name": name, "domain": dnsDomain,
		"port": port, "protocol": protocol,
		"properties": map[string]interface{}{},
		"authN":      map[string]interface{}{"enabled": false},
	}
	_, sc, err := c.doJSON(ctx, http.MethodPost, "/v1/service-sources", body)
	if err != nil {
		return fmt.Errorf("ensure service source %s: %w", name, err)
	}
	if sc != 200 && sc != 201 && sc != 409 {
		return fmt.Errorf("ensure service source %s: HTTP %d", name, sc)
	}
	return nil
}

func (c *HigressClient) ensureStaticServiceSource(ctx context.Context, name, address string, port int) error {
	// 逻辑说明：为内嵌部署构造 address:port 的静态 HTTP service source 且禁用 authN；200/201/409 幂等成功，其他错误返回。
	body := map[string]interface{}{
		"type": "static", "name": name, "domain": fmt.Sprintf("%s:%d", address, port),
		"port": port, "protocol": "http",
		"properties": map[string]interface{}{},
		"authN":      map[string]interface{}{"enabled": false},
	}
	_, sc, err := c.doJSON(ctx, http.MethodPost, "/v1/service-sources", body)
	if err != nil {
		return fmt.Errorf("ensure static service source %s: %w", name, err)
	}
	if sc != 200 && sc != 201 && sc != 409 {
		return fmt.Errorf("ensure static service source %s: HTTP %d", name, sc)
	}
	return nil
}

func (c *HigressClient) ensureRoute(ctx context.Context, name string, domains []string, serviceName string, port int, pathPrefix string) error {
	// 逻辑说明：为空路径补 `/`，构造前缀匹配、单后端 100 权重路由并提交；200/201/409 表示已收敛，其他状态带路由名失败。
	if pathPrefix == "" {
		pathPrefix = "/"
	}
	body := map[string]interface{}{
		"name":    name,
		"domains": domains,
		"path":    map[string]interface{}{"matchType": "PRE", "matchValue": pathPrefix, "caseSensitive": false},
		"services": []map[string]interface{}{
			{"name": serviceName, "port": port, "weight": 100},
		},
	}
	_, sc, err := c.doJSON(ctx, http.MethodPost, "/v1/routes", body)
	if err != nil {
		return fmt.Errorf("ensure route %s: %w", name, err)
	}
	if sc == 200 || sc == 201 || sc == 409 {
		return nil
	}
	return fmt.Errorf("ensure route %s: HTTP %d", name, sc)
}

func (c *HigressClient) deleteRoute(ctx context.Context, name string) {
	// 逻辑说明：向 Console 发出路由删除请求并故意丢弃响应，保证资源缺失或清理失败不会中断兼容卸载链。
	c.doJSON(ctx, http.MethodDelete, "/v1/routes/"+name, nil)
}

func (c *HigressClient) deleteServiceSource(ctx context.Context, name string) {
	// 逻辑说明：尽力删除指定服务源并忽略远端响应；调用方用它做幂等清理，因而此处不把资源缺失或网络错误向上传播。
	c.doJSON(ctx, http.MethodDelete, "/v1/service-sources/"+name, nil)
}

func (c *HigressClient) deleteDomain(ctx context.Context, name string) {
	// 逻辑说明：尽力向 Console 删除域名绑定并丢弃结果，使重复卸载可以继续清理其余资源；该函数不保证远端删除成功。
	c.doJSON(ctx, http.MethodDelete, "/v1/domains/"+name, nil)
}

// doJSON performs an HTTP request with session cookies.
func (c *HigressClient) doJSON(ctx context.Context, method, path string, reqBody interface{}) ([]byte, int, error) {
	// 逻辑说明：确保登录会话后编码可选 JSON、附 cookies 发起 Console 请求并读取完整响应；401 时清空 cookie、重新登录且只重试一次，返回正文与状态供调用方判定业务语义。
	if err := c.ensureSession(ctx); err != nil {
		return nil, 0, err
	}

	var bodyReader io.Reader
	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return nil, 0, fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	url := strings.TrimRight(c.config.ConsoleURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	c.mu.Lock()
	for _, cookie := range c.cookies {
		req.AddCookie(cookie)
	}
	c.mu.Unlock()

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		c.mu.Lock()
		c.cookies = nil
		c.mu.Unlock()
	}

	respBody, _ := io.ReadAll(resp.Body)
	return respBody, resp.StatusCode, nil
}

// ── helpers ──

func toStringSlice(v interface{}) []string {
	// 逻辑说明：兼容 `[]interface{}` 与 `[]string` 两种 JSON/内部表示，前者仅收集字符串元素；nil 或其他类型返回 nil。
	if v == nil {
		return nil
	}
	switch arr := v.(type) {
	case []interface{}:
		var result []string
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return arr
	}
	return nil
}

func containsString(slice []string, s string) bool {
	// 逻辑说明：线性查找字符串精确匹配并首次命中返回 true；用于小型 Consumer 列表的幂等判断。
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	// 逻辑说明：构造新切片并保留所有不等于目标的项，因此会移除重复目标且不修改输入底层顺序语义。
	var result []string
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}
