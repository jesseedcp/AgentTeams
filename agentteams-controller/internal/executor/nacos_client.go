package executor

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/credprovider"
)

const (
	nacosPreflightTimeout = 5 * time.Second
)

// NacosAIClient is the unified entry point for Nacos AI capabilities.
// Authentication is fully delegated to cred; the struct holds no auth state.
type NacosAIClient struct {
	serverAddr string
	namespace  string
	httpClient *http.Client
	cred       nacosCredential
}

type nacosAIClientOptions struct {
	stsResources []string
}

type NacosAIClientOption func(*nacosAIClientOptions)

func WithNacosSTSResources(resources []string) NacosAIClientOption {
	// 逻辑说明：返回一个 option 闭包，在构造时复制资源切片到私有配置；调用方后续修改原切片不会扩大或缩小 STS 权限。
	return func(opts *nacosAIClientOptions) {
		opts.stsResources = append([]string(nil), resources...)
	}
}

// NewNacosAIClient constructs and connects a NacosAIClient.
//
//   - rawAddr: host:port, optionally prefixed with user:pass@ or nacos://
//   - namespace: Nacos namespace ID (empty → "public")
//   - authType: "nacos" | "sts-agentteams" | "none" | "" (auto-detect from addr)
//   - credClient: required only for "sts-agentteams"; pass nil otherwise
func NewNacosAIClient(
	ctx context.Context,
	rawAddr string,
	namespace string,
	authType string,
	credClient credprovider.Client,
	options ...NacosAIClientOption,
) (*NacosAIClient, error) {
	// 逻辑说明：应用选项、解析地址/凭据、补 public namespace，根据显式或自动认证类型构造 credential，再先 TCP 预检后首次刷新；配置、连通性或认证失败均不返回 client。
	opts := nacosAIClientOptions{}
	for _, apply := range options {
		apply(&opts)
	}

	host, port, username, password, err := parseNacosAddr(rawAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid nacos address %q: %w", rawAddr, err)
	}

	if namespace == "" {
		namespace = "public"
	}

	serverAddr := net.JoinHostPort(host, port)
	httpCl := &http.Client{Timeout: 60 * time.Second}

	// Auto-detect auth type when not explicitly set.
	if authType == "" {
		if username != "" && password != "" {
			authType = nacosAuthTypeNacos
		} else if username != "" || password != "" {
			return nil, fmt.Errorf("both username and password are required in nacos URL or env (use nacos://user:pass@host:port or set AGENTTEAMS_NACOS_USERNAME/AGENTTEAMS_NACOS_PASSWORD)")
		} else {
			authType = nacosAuthTypeNone
		}
	}

	var cred nacosCredential
	switch authType {
	case nacosAuthTypeNacos:
		if username == "" || password == "" {
			return nil, fmt.Errorf("nacos auth requires username and password")
		}
		cred = &nacosUserPassCredential{
			serverAddr: serverAddr,
			username:   username,
			password:   password,
			httpClient: httpCl,
		}
	case nacosAuthTypeSTS:
		if credClient == nil {
			return nil, fmt.Errorf("sts-agentteams auth requires a credprovider.Client")
		}
		cred = newNacosSTSCredential(namespace, credClient, opts.stsResources)
	case nacosAuthTypeNone, "":
		cred = nacosNoneCredential{}
	default:
		return nil, fmt.Errorf("unsupported nacos auth type %q", authType)
	}

	client := &NacosAIClient{
		serverAddr: serverAddr,
		namespace:  namespace,
		httpClient: httpCl,
		cred:       cred,
	}

	if err := client.preflightConnect(ctx); err != nil {
		return nil, err
	}

	if err := cred.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("credential refresh failed: %w", err)
	}

	return client, nil
}

func (c *NacosAIClient) preflightConnect(ctx context.Context) error {
	// 逻辑说明：调用方没有 deadline 时补五秒超时，然后只建立并立即关闭目标 TCP 连接；用来快速区分地址不可达，不发送 Nacos 业务或凭据。
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, nacosPreflightTimeout)
		defer cancel()
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.serverAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.serverAddr, err)
	}
	return conn.Close()
}

// prepareRequest refreshes credentials if needed and applies auth headers.
// Must be called before every outbound HTTP request.
func (c *NacosAIClient) prepareRequest(ctx context.Context, req *http.Request) error {
	// 逻辑说明：每次外发 HTTP 前先保证 credential 有效，再把对应 header/signature 注入请求；刷新失败时不发送未认证请求。
	if err := c.cred.Refresh(ctx); err != nil {
		return fmt.Errorf("credential refresh: %w", err)
	}
	c.cred.Apply(req)
	return nil
}

// ── parseNacosAddr ────────────────────────────────────────────────────────

func parseNacosAddr(raw string) (host, port, username, password string, err error) {
	// 逻辑说明：为无 scheme 地址补 HTTP 后解析 host/port/userinfo，端口默认 8848；URL 未带成对凭据时回退进程环境，缺 host 返回错误。
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", "", "", err
	}
	if parsed.Hostname() == "" {
		return "", "", "", "", fmt.Errorf("missing host")
	}

	port = parsed.Port()
	if port == "" {
		port = "8848"
	}

	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}

	if username == "" && password == "" {
		username = os.Getenv("AGENTTEAMS_NACOS_USERNAME")
		password = os.Getenv("AGENTTEAMS_NACOS_PASSWORD")
	}

	return parsed.Hostname(), port, username, password, nil
}
