package initializer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/matrix"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

// Config holds parameters for cluster initialization.
type Config struct {
	ManagerEnabled   bool
	ManagerModel     string
	ManagerRuntime   string
	ManagerImage     string
	ManagerResources *v1beta1.AgentResourceRequirements
	ManagerCodingCLI *v1beta1.ManagerCodingCLISpec
	AdminUser        string
	AdminPassword    string
	Namespace        string
	IsEmbedded       bool   // embedded mode: use static service sources for local services
	AgentFSDir       string // local filesystem root for agent workspaces (embedded mode)
	ControllerName   string // AGENTTEAMS_CONTROLLER_NAME; stamped as agentteams.io/controller label on created CRs in incluster mode

	// Matrix AppService mode
	AppServiceEnabled         bool
	AppServiceID              string
	AppServiceToken           string
	AppServiceHSToken         string
	AppServiceSenderLocalpart string
	AppServicePushURL         string
	MatrixDomain              string // needed for AS registration YAML

	// Provider selection — drives which initialization steps run.
	GatewayProvider string // "higress" | "ai-gateway"
	StorageProvider string // "minio"   | "oss"

	// Gateway initialization (only consulted when GatewayProvider == "higress")
	LLMProvider                string // e.g. "qwen", "openai"
	LLMAPIKey                  string
	LLMApiURL                  string // provider-specific base URL (optional)
	OpenAIBaseURL              string // custom base URL for openai-compat providers
	AIStreamIdleTimeoutSeconds int
	TuwunelURL                 string // internal Tuwunel URL, e.g. http://tuwunel:6167
	CinnyURL                   string // internal Cinny URL (optional)
	ManagerAdminURL            string // internal Manager Admin URL (optional)
	GitHubToken                string
	SkillsDir                  string
}

func (c Config) managesGatewayRoutes() bool {
	return c.GatewayProvider == "" || c.GatewayProvider == "higress"
}

func (c Config) managesStorage() bool {
	return c.StorageProvider == "" || c.StorageProvider == "minio"
}

// Initializer performs one-time cluster bootstrap: waits for infrastructure,
// initializes storage structure, registers the admin account, sets up gateway
// routes, and optionally creates the Manager CR.
type Initializer struct {
	OSS     oss.StorageClient
	Matrix  matrix.Client
	Gateway gateway.Client
	MCP     gateway.MCPBootstrapper
	Dynamic dynamic.Interface
	RestCfg *rest.Config
	Config  Config
}

func (i *Initializer) Run(ctx context.Context) error {
	// 逻辑说明：按存储、Matrix、AppService、网关、GitHub MCP、Manager CR 的依赖顺序执行一次性引导；必需基础设施失败立即停止，可选集成按配置跳过，成功后保证声明式资源已创建或合并。
	logger := ctrl.Log.WithName("initializer")
	logger.Info("starting cluster initialization")

	if err := i.waitForOSS(ctx); err != nil {
		return fmt.Errorf("OSS not ready: %w", err)
	}
	logger.Info("OSS is ready")

	if err := i.ensureOSSStructure(ctx); err != nil {
		return fmt.Errorf("OSS structure init failed: %w", err)
	}
	logger.Info("OSS directory structure initialized")

	if err := i.waitForMatrix(ctx); err != nil {
		return fmt.Errorf("Matrix not ready: %w", err)
	}
	logger.Info("Matrix is ready")

	if err := i.registerAdmin(ctx); err != nil {
		return fmt.Errorf("admin registration failed: %w", err)
	}
	logger.Info("admin account ready", "user", i.Config.AdminUser)

	// Register Matrix AppService if enabled (must happen after admin is ready)
	if i.Config.AppServiceEnabled {
		if err := i.registerAppService(ctx); err != nil {
			return fmt.Errorf("Matrix AppService registration failed: %w", err)
		}
		logger.Info("Matrix AppService registered and verified",
			"id", i.Config.AppServiceID,
			"sender", i.Config.AppServiceSenderLocalpart)
	}

	if i.Gateway != nil {
		if err := i.waitForGateway(ctx); err != nil {
			return fmt.Errorf("Gateway not ready: %w", err)
		}
		logger.Info("Gateway is ready")

		if i.Config.managesGatewayRoutes() {
			if err := i.initGatewayRoutes(ctx); err != nil {
				return fmt.Errorf("Gateway route init failed: %w", err)
			}
			logger.Info("Gateway routes initialized")
		} else {
			logger.Info("skipping gateway route initialization",
				"provider", i.Config.GatewayProvider,
				"reason", "routes are managed out-of-band by the cloud platform")
		}
	}

	managerMCPServers := []v1beta1.MCPServer(nil)
	if i.Config.GitHubToken != "" && i.Config.managesGatewayRoutes() {
		if i.MCP == nil {
			logger.Info(
				"skipping GitHub MCP bootstrap",
				"reason",
				"gateway does not expose local MCP administration",
			)
		} else {
			endpoint, err := i.bootstrapGitHubMCP(ctx)
			if err != nil {
				logger.Error(
					err,
					"GitHub MCP bootstrap failed (non-fatal)",
				)
			} else {
				managerMCPServers = append(
					managerMCPServers,
					v1beta1.MCPServer{
						Name:      endpoint.Name,
						URL:       endpoint.URL,
						Transport: endpoint.Transport,
					},
				)
				logger.Info("GitHub MCP bootstrap complete")
			}
		}
	}

	if i.Config.ManagerEnabled {
		if err := i.ensureManagerCR(ctx, managerMCPServers); err != nil {
			return fmt.Errorf("Manager CR creation failed: %w", err)
		}
		logger.Info("Manager CR ensured", "name", "default")
	}

	logger.Info("cluster initialization complete")
	return nil
}

// waitForOSS polls MinIO/OSS until the bucket is accessible.
//
// For the embedded MinIO (storage.provider == "minio") the bucket is
// created on demand through BucketManager.EnsureBucket. For an
// externally-managed OSS bucket the initializer does not try to create
// or mutate anything — it just polls ListObjects to confirm that the
// controller's credentials grant access to the configured bucket.
func (i *Initializer) waitForOSS(ctx context.Context) error {
	// 逻辑说明：自管 MinIO 且支持 BucketManager 时重试创建/确认桶，否则只通过列对象验证外部 OSS 权限；两条路径都受五分钟超时与 context 取消控制。
	if i.Config.managesStorage() {
		if bm, ok := i.OSS.(oss.BucketManager); ok {
			return retry(ctx, 3*time.Second, 5*time.Minute, func() error {
				return bm.EnsureBucket(ctx)
			})
		}
	}
	return retry(ctx, 3*time.Second, 5*time.Minute, func() error {
		_, err := i.OSS.ListObjects(ctx, "")
		return err
	})
}

func (i *Initializer) ensureOSSStructure(ctx context.Context) error {
	// 逻辑说明：依次向约定的共享、Worker、Team、Human 和 Agent 前缀写入空 `.gitkeep`，建立对象存储目录骨架；任一写入失败立即返回并指出具体前缀，重复调用保持幂等。
	dirs := []string{
		"shared/knowledge/",
		"shared/tasks/",
		"workers/",
		"agentteams-config/workers/",
		"agentteams-config/teams/",
		"agentteams-config/humans/",
		"agents/",
	}
	for _, dir := range dirs {
		if err := i.OSS.PutObject(ctx, dir+".gitkeep", []byte("")); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	return nil
}

// waitForMatrix polls the Matrix server until it responds.
func (i *Initializer) waitForMatrix(ctx context.Context) error {
	// 逻辑说明：用故意无效的健康检查账号反复登录；连接类错误表示服务未就绪需重试，而 401/403 等 HTTP 响应证明 Matrix 已可达并视为成功。
	return retry(ctx, 3*time.Second, 5*time.Minute, func() error {
		_, err := i.Matrix.Login(ctx, "__healthcheck__", "invalid")
		if err != nil && isMatrixConnError(err) {
			return err
		}
		// Any non-connection error (403, 401, etc.) means Matrix is up.
		return nil
	})
}

func (i *Initializer) registerAdmin(ctx context.Context) error {
	// 逻辑说明：用初始化配置中的账号密码幂等确保 Matrix 管理员存在；忽略成功时的用户详情，只把创建或更新失败交给启动流程中止。
	_, err := i.Matrix.EnsureUser(ctx, matrix.EnsureUserRequest{
		Username: i.Config.AdminUser,
		Password: i.Config.AdminPassword,
	})
	return err
}

// registerAppService registers the AgentTeams controller as a Matrix Application
// Service via the Tuwunel admin bot, then verifies with a smoke test.
func (i *Initializer) registerAppService(ctx context.Context) error {
	// 逻辑说明：从初始化配置渲染 Tuwunel AppService 注册内容，先提交注册再执行冒烟测试；两步任一失败都带阶段上下文返回，避免宣告一个不可用桥接。
	cfg := matrix.Config{
		Domain:                    i.Config.MatrixDomain,
		AppServiceID:              i.Config.AppServiceID,
		AppServiceToken:           i.Config.AppServiceToken,
		AppServiceHSToken:         i.Config.AppServiceHSToken,
		AppServiceSenderLocalpart: i.Config.AppServiceSenderLocalpart,
		AppServicePushURL:         i.Config.AppServicePushURL,
	}
	reg := matrix.RenderAppServiceRegistration(cfg)
	if err := i.Matrix.RegisterAppService(ctx, reg); err != nil {
		return fmt.Errorf("register appservice: %w", err)
	}
	if err := i.Matrix.AppServiceSmokeTest(ctx); err != nil {
		return fmt.Errorf("appservice smoke test: %w", err)
	}
	return nil
}

// waitForGateway polls the Higress Console until it responds.
func (i *Initializer) waitForGateway(ctx context.Context) error {
	// 逻辑说明：每三秒调用网关健康检查，最多等待五分钟；成功立即返回，超时或 context 取消由统一 retry 保留最后错误。
	return retry(ctx, 3*time.Second, 5*time.Minute, func() error {
		return i.Gateway.Healthy(ctx)
	})
}

// initGatewayRoutes registers service sources, LLM provider, AI route, and
// infrastructure routes (Matrix, Cinny) in Higress. All calls are
// idempotent — safe to re-run on controller restart.
func (i *Initializer) initGatewayRoutes(ctx context.Context) error {
	// 逻辑说明：按部署模式幂等注册 Tuwunel、Cinny、Manager Admin、LLM Provider 与 AI 路由，并清理旧 Element/默认路由；单项网关配置失败记录为非致命错误，让其余独立路由仍可收敛。
	logger := ctrl.Log.WithName("initializer")
	cfg := i.Config

	// 1. Tuwunel service source
	if cfg.TuwunelURL != "" {
		host, port, err := parseHostPort(cfg.TuwunelURL)
		if err != nil {
			return fmt.Errorf("parse Tuwunel URL: %w", err)
		}

		var svcSuffix string
		if cfg.IsEmbedded {
			if err := i.Gateway.EnsureStaticServiceSource(ctx, "tuwunel", host, port); err != nil {
				logger.Error(err, "failed to register Tuwunel static service source (non-fatal)")
			}
			svcSuffix = "static"
		} else {
			if err := i.Gateway.EnsureServiceSource(ctx, "tuwunel", host, port, "http"); err != nil {
				logger.Error(err, "failed to register Tuwunel service source (non-fatal)")
			}
			svcSuffix = "dns"
		}

		// Matrix Homeserver routes (/_matrix/*, /_tuwunel/* → Tuwunel)
		if err := i.Gateway.EnsureRoute(ctx, "matrix-homeserver", nil, "tuwunel."+svcSuffix, port, "/_matrix"); err != nil {
			logger.Error(err, "failed to create Matrix route (non-fatal)")
		}
	}

	// 2. Cinny service source + route. Remove the old Element route first so
	// upgrades do not leave two root routes competing for the same hostname.
	if cfg.CinnyURL != "" {
		if err := i.Gateway.DeleteRoute(ctx, "element-web"); err != nil {
			logger.Error(err, "failed to remove legacy Element route (non-fatal)")
		}

		host, port, err := parseHostPort(cfg.CinnyURL)
		if err != nil {
			logger.Error(err, "failed to parse Cinny URL (non-fatal)")
		} else {
			var svcSuffix string
			if cfg.IsEmbedded {
				if err := i.Gateway.EnsureStaticServiceSource(ctx, "cinny", host, port); err != nil {
					logger.Error(err, "failed to register Cinny static service source (non-fatal)")
				}
				svcSuffix = "static"
			} else {
				if err := i.Gateway.EnsureServiceSource(ctx, "cinny", host, port, "http"); err != nil {
					logger.Error(err, "failed to register Cinny service source (non-fatal)")
				}
				svcSuffix = "dns"
			}
			if err := i.Gateway.EnsureRoute(ctx, "cinny", nil, "cinny."+svcSuffix, port, "/"); err != nil {
				logger.Error(err, "failed to create Cinny route (non-fatal)")
			}
		}
	}

	if cfg.ManagerAdminURL != "" {
		host, port, err := parseHostPort(cfg.ManagerAdminURL)
		if err != nil {
			logger.Error(err, "failed to parse Manager Admin URL (non-fatal)")
		} else {
			svcSuffix := "dns"
			if cfg.IsEmbedded {
				if err := i.Gateway.EnsureStaticServiceSource(ctx, "manager-admin", host, port); err != nil {
					logger.Error(err, "failed to register Manager Admin static service source (non-fatal)")
				}
				svcSuffix = "static"
			} else if err := i.Gateway.EnsureServiceSource(ctx, "manager-admin", host, port, "http"); err != nil {
				logger.Error(err, "failed to register Manager Admin service source (non-fatal)")
			}
			if err := i.Gateway.EnsureRoute(
				ctx,
				"manager-admin",
				nil,
				"manager-admin."+svcSuffix,
				port,
				"/manager-admin",
			); err != nil {
				logger.Error(err, "failed to create Manager Admin route (non-fatal)")
			}
		}
	}

	// 3. LLM Provider
	if cfg.LLMAPIKey != "" {
		streamIdleTimeout := cfg.AIStreamIdleTimeoutSeconds
		if streamIdleTimeout <= 0 {
			streamIdleTimeout = 900
		}
		if err := i.Gateway.EnsureStreamIdleTimeout(ctx, streamIdleTimeout); err != nil {
			logger.Error(err, "failed to update Higress stream idle timeout (non-fatal)")
		}

		provider := cfg.LLMProvider
		if provider == "" {
			provider = "qwen"
		}

		switch provider {
		case "qwen":
			raw := map[string]interface{}{
				"agentteamsMode":       true,
				"qwenEnableSearch":     false,
				"qwenEnableCompatible": true,
				"qwenFileIds":          []interface{}{},
			}
			if err := i.Gateway.EnsureAIProvider(ctx, gateway.AIProviderRequest{
				Name:     "qwen",
				Type:     "qwen",
				Tokens:   []string{cfg.LLMAPIKey},
				Protocol: "openai/v1",
				Raw:      raw,
			}); err != nil {
				logger.Error(err, "failed to create LLM provider (non-fatal)")
			}

		case "openai-compat":
			if cfg.OpenAIBaseURL == "" {
				// No custom base URL — fall back to official OpenAI endpoint
				logger.Info("AGENTTEAMS_OPENAI_BASE_URL not set, using official OpenAI endpoint")
				raw := map[string]interface{}{"agentteamsMode": true}
				if err := i.Gateway.EnsureAIProvider(ctx, gateway.AIProviderRequest{
					Name:     "openai-compat",
					Type:     "openai",
					Tokens:   []string{cfg.LLMAPIKey},
					Protocol: "openai/v1",
					Raw:      raw,
				}); err != nil {
					logger.Error(err, "failed to create LLM provider (non-fatal)")
				}
			} else {
				// Parse URL to create DNS service source
				host, port, err := parseHostPort(cfg.OpenAIBaseURL)
				if err != nil {
					logger.Error(err, "failed to parse AGENTTEAMS_OPENAI_BASE_URL (non-fatal)")
				} else {
					proto := "https"
					if strings.HasPrefix(cfg.OpenAIBaseURL, "http://") {
						proto = "http"
					}
					if err := i.Gateway.EnsureServiceSource(ctx, "openai-compat", host, port, proto); err != nil {
						logger.Error(err, "failed to register openai-compat service source (non-fatal)")
					}
					// Wait for DNS service source to propagate before creating provider
					time.Sleep(2 * time.Second)
					raw := map[string]interface{}{
						"agentteamsMode":          true,
						"openaiCustomUrl":         cfg.OpenAIBaseURL,
						"openaiCustomServiceName": "openai-compat.dns",
						"openaiCustomServicePort": port,
					}
					if err := i.Gateway.EnsureAIProvider(ctx, gateway.AIProviderRequest{
						Name:     "openai-compat",
						Type:     "openai",
						Tokens:   []string{cfg.LLMAPIKey},
						Protocol: "openai/v1",
						Raw:      raw,
					}); err != nil {
						logger.Error(err, "failed to create LLM provider (non-fatal)")
					}
				}
			}

		default:
			if cfg.OpenAIBaseURL != "" {
				// Provider name is unrecognized but a custom base URL is provided —
				// set up an openai-compatible provider with the custom endpoint.
				host, port, err := parseHostPort(cfg.OpenAIBaseURL)
				if err != nil {
					logger.Error(err, "failed to parse AGENTTEAMS_OPENAI_BASE_URL (non-fatal)")
				} else {
					proto := "https"
					if strings.HasPrefix(cfg.OpenAIBaseURL, "http://") {
						proto = "http"
					}
					if err := i.Gateway.EnsureServiceSource(ctx, provider, host, port, proto); err != nil {
						logger.Error(err, "failed to register service source for provider (non-fatal)")
					}
					time.Sleep(2 * time.Second)
					raw := map[string]interface{}{
						"agentteamsMode":          true,
						"openaiCustomUrl":         cfg.OpenAIBaseURL,
						"openaiCustomServiceName": provider + ".dns",
						"openaiCustomServicePort": port,
					}
					if err := i.Gateway.EnsureAIProvider(ctx, gateway.AIProviderRequest{
						Name:     provider,
						Type:     "openai",
						Tokens:   []string{cfg.LLMAPIKey},
						Protocol: "openai/v1",
						Raw:      raw,
					}); err != nil {
						logger.Error(err, "failed to create LLM provider (non-fatal)")
					}
				}
			} else {
				raw := map[string]interface{}{"agentteamsMode": true}
				if err := i.Gateway.EnsureAIProvider(ctx, gateway.AIProviderRequest{
					Name:     provider,
					Type:     "openai",
					Tokens:   []string{cfg.LLMAPIKey},
					Protocol: "openai/v1",
					Raw:      raw,
				}); err != nil {
					logger.Error(err, "failed to create LLM provider (non-fatal)")
				}
			}
		}

		// 4. AI Route skeleton — only creates the route if it does not yet
		// exist. We intentionally do NOT pass any authorization data here:
		// authConfig.allowedConsumers is owned exclusively by Manager/Worker
		// Reconcilers (via AuthorizeAIRoutes). This separation of ownership
		// avoids the restart-time race where the Initializer would otherwise
		// re-declare an empty allowedConsumers list and transiently lock out
		// the Manager/Workers.
		if err := i.Gateway.EnsureAIRoute(ctx, gateway.AIRouteRequest{
			Name:       "default-ai-route",
			PathPrefix: "/v1",
			Provider:   provider,
		}); err != nil {
			logger.Error(err, "failed to create AI route (non-fatal)")
		}
	}

	// 5. Remove Higress default landing page
	if err := i.Gateway.DeleteRoute(ctx, "default"); err != nil {
		logger.Error(err, "failed to remove default route (non-fatal)")
	}

	return nil
}

// parseHostPort extracts host and port from a URL like "http://host:port".
func parseHostPort(rawURL string) (string, int, error) {
	// 逻辑说明：解析服务 URL 并返回主机与整数端口；未显式写端口时按 HTTPS=443、其他=80 回退，非法端口保留原文本包装错误。
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, err
	}
	host := u.Hostname()
	portStr := u.Port()
	if portStr == "" {
		if u.Scheme == "https" {
			return host, 443, nil
		}
		return host, 80, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	return host, port, nil
}

func (i *Initializer) bootstrapGitHubMCP(
	ctx context.Context,
) (gateway.MCPServerEndpoint, error) {
	// 逻辑说明：从 Skills 目录读取 GitHub MCP 模板，确认恰有一个空凭据槽后注入转义 token，再调用网关创建仅 Manager 可用的 REST MCP；模板异常时不发送任何配置。
	skillsDir := i.Config.SkillsDir
	if skillsDir == "" {
		skillsDir = "/opt/agentteams/agent/skills"
	}
	path := filepath.Join(
		skillsDir,
		"mcp-server-management",
		"references",
		"mcp-github.yaml",
	)
	template, err := os.ReadFile(path)
	if err != nil {
		return gateway.MCPServerEndpoint{}, fmt.Errorf(
			"read GitHub MCP template: %w",
			err,
		)
	}
	const credentialSlot = `accessToken: ""`
	if strings.Count(string(template), credentialSlot) != 1 {
		return gateway.MCPServerEndpoint{}, fmt.Errorf(
			"GitHub MCP template must contain exactly one credential slot",
		)
	}
	rawConfiguration := strings.Replace(
		string(template),
		credentialSlot,
		"accessToken: "+strconv.Quote(i.Config.GitHubToken),
		1,
	)
	return i.MCP.EnsureRESTMCPServer(
		ctx,
		gateway.RESTMCPServerRequest{
			Name:             "github",
			Description:      "GitHub MCP Server",
			RawConfiguration: rawConfiguration,
			ServiceName:      "github-api",
			ServiceDomain:    "api.github.com",
			ServicePort:      443,
			ServiceProtocol:  "https",
			Consumers:        []string{"manager"},
		},
	)
}

func (i *Initializer) ensureManagerCR(
	ctx context.Context,
	desiredMCPServers []v1beta1.MCPServer,
) error {
	// 逻辑说明：获取默认 Manager CR；已存在时只合并 GitHub MCP 与 Coding CLI 差异并按需 Update，不存在时组装完整 spec/控制器标签后 Create，其他 API 错误不被误判成缺失。
	logger := ctrl.Log.WithName("initializer")

	dynClient := i.Dynamic
	if dynClient == nil {
		var err error
		dynClient, err = dynamic.NewForConfig(i.RestCfg)
		if err != nil {
			return fmt.Errorf("create dynamic client: %w", err)
		}
	}

	gvr := schema.GroupVersionResource{
		Group:    v1beta1.GroupName,
		Version:  v1beta1.Version,
		Resource: "managers",
	}

	ns := i.Config.Namespace
	name := "default"

	current, err := dynClient.Resource(gvr).Namespace(ns).Get(
		ctx,
		name,
		metav1.GetOptions{},
	)
	if err == nil {
		changed := false
		if len(desiredMCPServers) > 0 {
			existing, _, nestedErr := unstructured.NestedSlice(
				current.Object,
				"spec",
				"mcpServers",
			)
			if nestedErr != nil {
				return fmt.Errorf("read Manager MCP servers: %w", nestedErr)
			}
			merged := mergeManagerMCPServers(
				existing,
				desiredMCPServers,
			)
			if !reflect.DeepEqual(existing, merged) {
				if err := unstructured.SetNestedSlice(
					current.Object,
					merged,
					"spec",
					"mcpServers",
				); err != nil {
					return fmt.Errorf("set Manager MCP servers: %w", err)
				}
				changed = true
			}
		}
		if i.Config.ManagerCodingCLI != nil {
			desired, convertErr := managerCodingCLIMap(
				i.Config.ManagerCodingCLI,
			)
			if convertErr != nil {
				return convertErr
			}
			existing, _, nestedErr := unstructured.NestedMap(
				current.Object,
				"spec",
				"codingCLI",
			)
			if nestedErr != nil {
				return fmt.Errorf("read Manager coding CLI state: %w", nestedErr)
			}
			if !reflect.DeepEqual(existing, desired) {
				if err := unstructured.SetNestedMap(
					current.Object,
					desired,
					"spec",
					"codingCLI",
				); err != nil {
					return fmt.Errorf("set Manager coding CLI state: %w", err)
				}
				changed = true
			}
		}
		if !changed {
			logger.Info("Manager CR already contains bootstrap desired state")
			return nil
		}
		if _, err := dynClient.Resource(gvr).Namespace(ns).Update(
			ctx,
			current,
			metav1.UpdateOptions{},
		); err != nil {
			return fmt.Errorf("update Manager bootstrap desired state: %w", err)
		}
		logger.Info("Manager CR bootstrap desired state updated")
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get Manager CR: %w", err)
	}

	spec := map[string]interface{}{
		"model":   i.Config.ManagerModel,
		"runtime": i.Config.ManagerRuntime,
	}
	if i.Config.ManagerImage != "" {
		spec["image"] = i.Config.ManagerImage
	}
	if i.Config.ManagerResources != nil {
		spec["resources"] = i.Config.ManagerResources
	}
	if len(desiredMCPServers) > 0 {
		spec["mcpServers"] = mergeManagerMCPServers(
			nil,
			desiredMCPServers,
		)
	}
	if i.Config.ManagerCodingCLI != nil {
		codingCLI, convertErr := managerCodingCLIMap(
			i.Config.ManagerCodingCLI,
		)
		if convertErr != nil {
			return convertErr
		}
		spec["codingCLI"] = codingCLI
	}

	metadata := map[string]interface{}{
		"name":      name,
		"namespace": ns,
	}
	if i.Config.ControllerName != "" {
		metadata["labels"] = map[string]interface{}{
			v1beta1.LabelController: i.Config.ControllerName,
		}
	}
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": v1beta1.GroupName + "/" + v1beta1.Version,
			"kind":       "Manager",
			"metadata":   metadata,
			"spec":       spec,
		},
	}

	_, err = dynClient.Resource(gvr).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create Manager CR: %w", err)
	}
	return nil
}

func managerCodingCLIMap(
	spec *v1beta1.ManagerCodingCLISpec,
) (map[string]interface{}, error) {
	// 逻辑说明：先按 CRD 规则校验 Coding CLI 配置，再转换为 unstructured map 供动态客户端写入；验证或转换失败均携带字段阶段返回。
	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("validate Manager coding CLI state: %w", err)
	}
	value, err := runtime.DefaultUnstructuredConverter.ToUnstructured(spec)
	if err != nil {
		return nil, fmt.Errorf("convert Manager coding CLI state: %w", err)
	}
	return value, nil
}

func mergeManagerMCPServers(
	existing []interface{},
	desired []v1beta1.MCPServer,
) []interface{} {
	// 逻辑说明：复制现有 MCP 列表并按 name 建索引，用期望配置替换同名项、追加新项，同时保留未知/非 map 旧项；返回新切片，避免原地改变调用者输入。
	result := append([]interface{}(nil), existing...)
	indexByName := make(map[string]int, len(result))
	for index, raw := range result {
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := item["name"].(string)
		if name != "" {
			indexByName[name] = index
		}
	}
	for _, server := range desired {
		item := map[string]interface{}{
			"name":      server.Name,
			"url":       server.URL,
			"transport": server.Transport,
		}
		if index, exists := indexByName[server.Name]; exists {
			result[index] = item
			continue
		}
		indexByName[server.Name] = len(result)
		result = append(result, item)
	}
	return result
}

// retry calls fn repeatedly until it succeeds or the timeout is reached.
func retry(ctx context.Context, interval, timeout time.Duration, fn func() error) error {
	// 逻辑说明：立即执行操作，失败后按 interval 重试直到成功、总超时或 context 取消；超时时包装最后一次错误，等待通过 select 保证关停不会被 sleep 卡住。
	deadline := time.Now().Add(timeout)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %v: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// isMatrixConnError returns true if the error indicates a transport-level failure
// (connection refused, DNS error, etc.) as opposed to an HTTP-level response.
func isMatrixConnError(err error) bool {
	// 逻辑说明：把错误文本与已知 DNS、TCP、超时、EOF 传输特征逐一匹配，区分“服务不可达”和“服务已响应但拒绝登录”；nil 或 HTTP 业务错误返回 false。
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, sub := range []string{"connection refused", "no such host", "dial tcp", "i/o timeout", "EOF"} {
		if contains(msg, sub) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	// 逻辑说明：先用长度保护避免后续扫描越界，再委托朴素子串搜索；结果只用于 Matrix 连接错误分类。
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	// 逻辑说明：从左到右比较所有可能切片并在首次相等时返回 true；循环上界保证切片合法，遍历完成仍未命中则返回 false。
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
