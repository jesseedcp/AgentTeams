package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/agentconfig"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/credentials"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/gateway"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/matrix"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
)

// Config 是 Controller 启动后的不可变配置快照。它把环境变量中分散的
// 字符串整理为各子系统需要的强类型值。敏感字段可以在内存中传给
// 客户端，但不应整体打印、序列化到 CR 或传给 Agent 提示词。
type Config struct {
	// Controller core
	KubeMode        string // "embedded" or "incluster"
	DataDir         string
	HTTPAddr        string
	MetricsBindAddr string
	ConfigDir       string
	CRDDir          string
	SkillsDir       string

	// ResourcePrefix is the tenant-level prefix used to derive Pod/SA/label/
	// session names created by this controller. Default "agentteams-". Set via
	// AGENTTEAMS_RESOURCE_PREFIX to isolate multiple AgentTeams instances that share
	// a K8s namespace (different Helm releases). Downstream names are all
	// derived from this value — see internal/auth.ResourcePrefix for the
	// full list (worker/manager pods, ServiceAccounts, "app" labels, STS
	// session names). Intentionally does NOT cover OPENCLAW_MDNS_HOSTNAME,
	// CMS service name, or install-script hardcoded names.
	ResourcePrefix string
	// ResourceAutoPrefix controls whether controller should auto-derive
	// resource/container prefixes. When false, default agentteams-* prefixes are
	// disabled unless explicit AGENTTEAMS_PROXY_CONTAINER_PREFIX is provided.
	// Set via AGENTTEAMS_RESOURCE_AUTOPREFIX. Default true.
	ResourceAutoPrefix bool

	// Docker proxy (embedded mode only)
	SocketPath      string
	ContainerPrefix string // worker container/pod name prefix; derived from ResourcePrefix when AGENTTEAMS_PROXY_CONTAINER_PREFIX is unset

	// Auth
	AuthAudience               string // SA token audience for TokenReview
	AuthTokenExpirationSeconds int64

	// Provider selection (driven by Helm values)
	GatewayProvider string // "higress" | "ai-gateway"
	StorageProvider string // "minio"   | "oss"

	// Higress (self-hosted gateway)
	HigressBaseURL       string
	HigressCookieFile    string
	HigressAdminUser     string
	HigressAdminPassword string

	// Worker backend selection
	WorkerBackend        string
	WorkerBackendRuntime string

	// Region (used by AI Gateway / OSS, etc.)
	Region string

	// AI Gateway (Alibaba Cloud APIG) — only used when GatewayProvider == "ai-gateway"
	GWEndpoint   string
	GWGatewayID  string
	GWModelAPIID string
	GWEnvID      string

	// Object storage bucket (shared by minio and oss backends)
	OSSBucket string

	WorkerDepsStorageBucket   string
	WorkerDepsStorageEndpoint string
	WorkerDepsMountAuthType   string
	WorkerDepsMountRoleName   string

	// Credential provider sidecar (agentteams-credential-provider) used by the
	// controller to obtain STS tokens for its own cloud SDK clients (APIG,
	// OSS) and for downstream worker credential issuance. Empty when the
	// sidecar is not deployed (e.g. self-hosted higress+minio stack).
	CredentialProviderURL string

	// Kubernetes Backend
	K8sNamespace    string
	K8sWorkerCPU    string
	K8sWorkerMemory string

	// Legacy sandbox backend knobs. The open-source controller does not
	// register the OpenKruise sandbox backend.
	SandboxProviderType          string
	SandboxCapabilities          string
	SandboxPrewarmSize           int
	SandboxPrewarmSizeConfigured bool

	// Manager deployment (Initializer creates the Manager CR if enabled)
	ManagerEnabled            bool
	ManagerModel              string
	ManagerRuntime            string
	ManagerImage              string
	ManagerDataClaim          string
	ManagerHostPath           string
	ManagerHostReadAllowlist  string
	ManagerHostWriteAllowlist string
	ManagerSpecResources      *v1beta1.AgentResourceRequirements
	ManagerCodingCLI          *v1beta1.ManagerCodingCLISpec
	K8sManagerCPURequest      string
	K8sManagerMemoryRequest   string
	K8sManagerCPU             string
	K8sManagerMemory          string

	// DefaultWorkerRuntime is applied by the Worker reconciler when a Worker
	// CR has spec.runtime unset, before falling back to "openclaw". Sourced
	// from AGENTTEAMS_DEFAULT_WORKER_RUNTIME at install time. Manager pods use
	// ManagerRuntime instead, since Backend.Create is shared between both
	// and only the caller knows which env var applies.
	DefaultWorkerRuntime string

	// Controller URL (advertised to workers for STS refresh etc.)
	ControllerURL string

	// ControllerName identifies this controller instance. When multiple
	// agentteams-controller deployments live in the same namespace (e.g. separate
	// Helm releases), each must use a distinct LeaderElection lease to avoid
	// one instance blocking the other. Sourced from AGENTTEAMS_CONTROLLER_NAME;
	// if empty, leader election falls back to the legacy global lease name.
	ControllerName string

	// Embedded-mode Manager Agent container mounts (host paths, read from env)
	ManagerWorkspaceDir string // e.g. ~/agentteams-manager — mounted as /var/lib/agentteams-manager
	HostShareDir        string // e.g. ~/ — mounted as /host-share
	ManagerHealthPort   string // loopback host port for Manager health/metrics (default: 18888)

	// Pre-generated Manager secrets (from install script env)
	ManagerPassword   string // Matrix password for manager user
	ManagerGatewayKey string // Gateway API key for manager consumer

	// Matrix server
	MatrixServerURL         string
	MatrixDomain            string
	MatrixRegistrationToken string
	MatrixAdminUser         string
	MatrixAdminPassword     string
	MatrixE2EE              bool

	// Matrix AppService mode
	MatrixAppServiceEnabled            bool
	MatrixAppServiceID                 string
	MatrixAppServiceASToken            string
	MatrixAppServiceHSToken            string
	MatrixAppServiceSenderLocalpart    string
	MatrixAppServiceUserNamespaceRegex string
	MatrixAppServicePushURL            string

	// Auto-generation tracking (not exported to env / child containers)
	MatrixAppServiceASTokenAutoGenerated bool `json:"-"`
	MatrixAppServiceHSTokenAutoGenerated bool `json:"-"`

	// Object storage (embedded MinIO)
	OSSStoragePrefix string

	// AI model
	DefaultModel       string
	EmbeddingModel     string
	Runtime            string
	ModelContextWindow int
	ModelMaxTokens     int

	// LLM provider (for Gateway initialization)
	LLMProvider                string
	LLMAPIKey                  string
	GitHubToken                string
	OpenAIBaseURL              string // AGENTTEAMS_OPENAI_BASE_URL — custom base URL for openai-compat providers
	AIStreamIdleTimeoutSeconds int    // AGENTTEAMS_AI_STREAM_IDLE_TIMEOUT_SECONDS

	// Cinny URL (for Gateway route initialization)
	CinnyURL        string
	ManagerAdminURL string

	// Locale used to render the first-boot Manager onboarding prompt
	// (welcome message). Sourced from the install-time AGENTTEAMS_LANGUAGE
	// (zh / en) and TZ env vars that the install script forwards into
	// the controller container. Both are advisory hints — the controller
	// only embeds them as plain text in the welcome prompt; the agent
	// itself decides how to interpret them when greeting the admin.
	UserLanguage string
	UserTimezone string

	// CMS observability
	CMSTracesEnabled  bool
	CMSMetricsEnabled bool
	CMSEndpoint       string
	CMSLicenseKey     string
	CMSProject        string
	CMSWorkspace      string
	CMSServiceName    string

	// Pre-resolved worker environment defaults (passed to worker containers)
	WorkerEnv WorkerEnvDefaults
}

// WorkerEnvDefaults holds environment variable defaults injected into worker and manager containers.
// All values are resolved once at config load time from the controller's own environment.
type WorkerEnvDefaults struct {
	MatrixDomain            string
	FSEndpoint              string
	FSBucket                string
	StoragePrefix           string
	ControllerURL           string
	AIGatewayURL            string
	MatrixURL               string
	AdminUser               string
	AdminPassword           string
	MatrixRegistrationToken string
	HigressAdminURL         string
	MCPGitHubToken          string
	Runtime                 string // "docker" for embedded, "k8s" for incluster
	DefaultWorkerRuntime    string
	YoloMode                bool // AGENTTEAMS_YOLO=1 — propagated to managers and workers
	MatrixDebug             bool // AGENTTEAMS_MATRIX_DEBUG=1 — propagated to managers and workers,
	// translated to OPENCLAW_MATRIX_DEBUG=1 by the container entrypoints to
	// enable structured INFO-level traces in the openclaw matrix plugin.

	// CMS observability (propagated to all workers and managers)
	CMSTracesEnabled  bool
	CMSMetricsEnabled bool
	CMSEndpoint       string
	CMSLicenseKey     string
	CMSProject        string
	CMSWorkspace      string

	// SkillsAPIURL is propagated to workers as SKILLS_API_URL.
	// Sourced from SKILLS_API_URL, falling back to AGENTTEAMS_SKILLS_API_URL.
	SkillsAPIURL string

	// NacosAuthType is propagated to workers as NACOS_AUTH_TYPE.
	// Sourced from NACOS_AUTH_TYPE.
	// Typical value: "sts-agentteams".
	NacosAuthType string
}

type managerSpecEnv struct {
	Model     string                            `json:"model"`
	Runtime   string                            `json:"runtime"`
	Image     string                            `json:"image"`
	Resources v1beta1.AgentResourceRequirements `json:"resources"`
	CodingCLI *v1beta1.ManagerCodingCLISpec     `json:"codingCLI"`
}

// LoadConfig 在进程启动时读取一次环境并应用默认值。
// 业务代码之后使用这份快照，不再随时 os.Getenv，否则同一进程中不同
// 组件可能看到不同值。需要热加载的内容通过明确 watcher/revision 机制处理，
// 而不是偶然重读环境。
func LoadConfig() *Config {
	// 逻辑说明：一次性读取全部环境、应用模式相关默认和兼容重写，再校验 Manager/AppService 边界后返回进程配置快照。
	kubeMode := envOrDefault("AGENTTEAMS_KUBE_MODE", "embedded")
	metricsBindAddr := os.Getenv("AGENTTEAMS_METRICS_BIND_ADDR")
	if metricsBindAddr == "" {
		if kubeMode == "embedded" {
			metricsBindAddr = "0"
		} else {
			metricsBindAddr = ":8080"
		}
	}

	dataDir := envOrDefault("AGENTTEAMS_DATA_DIR", "/data/agentteams-controller")
	if !filepath.IsAbs(dataDir) {
		if wd, err := os.Getwd(); err == nil {
			dataDir = filepath.Join(wd, dataDir)
		}
	}

	resourceAutoPrefix := envBoolDefault("AGENTTEAMS_RESOURCE_AUTOPREFIX", true)
	resourcePrefix := ""
	if resourceAutoPrefix {
		resourcePrefix = envOrDefault("AGENTTEAMS_RESOURCE_PREFIX", "agentteams-")
	}
	// ContainerPrefix defaults to "${resourcePrefix}worker-" when auto-prefix
	// is enabled. AGENTTEAMS_PROXY_CONTAINER_PREFIX remains an explicit override.
	containerPrefix := os.Getenv("AGENTTEAMS_PROXY_CONTAINER_PREFIX")
	if containerPrefix == "" && resourceAutoPrefix {
		containerPrefix = resourcePrefix + "worker-"
	}

	cfg := &Config{
		KubeMode:        kubeMode,
		DataDir:         dataDir,
		HTTPAddr:        envOrDefault("AGENTTEAMS_HTTP_ADDR", ":8090"),
		MetricsBindAddr: metricsBindAddr,
		ConfigDir:       envOrDefault("AGENTTEAMS_CONFIG_DIR", "/root/agentteams-fs/agentteams-config"),
		CRDDir:          envOrDefault("AGENTTEAMS_CRD_DIR", "/opt/agentteams/config/crd"),
		SkillsDir:       envOrDefault("AGENTTEAMS_SKILLS_DIR", "/opt/agentteams/agent/skills"),

		ResourcePrefix:     resourcePrefix,
		ResourceAutoPrefix: resourceAutoPrefix,

		SocketPath:      envOrDefault("AGENTTEAMS_PROXY_SOCKET", "/var/run/docker.sock"),
		ContainerPrefix: containerPrefix,

		AuthAudience: firstNonEmpty(
			os.Getenv("AGENTTEAMS_AUTH_AUDIENCE"),
			envOrDefault("AGENTTEAMS_AUTH_AUDIENCE", "agentteams-controller"),
		),
		AuthTokenExpirationSeconds: int64(envOrDefaultInt("AGENTTEAMS_AUTH_TOKEN_EXPIRATION_SECONDS", int(backend.DefaultAuthTokenExpirationSeconds))),

		GatewayProvider: envOrDefault("AGENTTEAMS_GATEWAY_PROVIDER", "higress"),
		StorageProvider: envOrDefault("AGENTTEAMS_STORAGE_PROVIDER", "minio"),

		CredentialProviderURL: os.Getenv("AGENTTEAMS_CREDENTIAL_PROVIDER_URL"),

		HigressBaseURL:    envOrDefault("AGENTTEAMS_AI_GATEWAY_ADMIN_URL", "http://127.0.0.1:8001"),
		HigressCookieFile: os.Getenv("HIGRESS_COOKIE_FILE"),
		// Higress and Matrix share the same admin credentials.
		HigressAdminUser:     os.Getenv("AGENTTEAMS_ADMIN_USER"),
		HigressAdminPassword: os.Getenv("AGENTTEAMS_ADMIN_PASSWORD"),

		WorkerBackend: firstNonEmpty(
			os.Getenv("AGENTTEAMS_WORKER_BACKEND"),
			os.Getenv("AGENTTEAMS_ALIYUN_WORKER_BACKEND"),
		),
		WorkerBackendRuntime: os.Getenv("AGENTTEAMS_WORKER_BACKEND_RUNTIME"),

		Region: envOrDefault("AGENTTEAMS_REGION", "cn-hangzhou"),

		GWEndpoint:   os.Getenv("AGENTTEAMS_APIG_ENDPOINT"),
		GWGatewayID:  os.Getenv("AGENTTEAMS_GW_GATEWAY_ID"),
		GWModelAPIID: os.Getenv("AGENTTEAMS_GW_MODEL_API_ID"),
		GWEnvID:      os.Getenv("AGENTTEAMS_GW_ENV_ID"),

		OSSBucket: envOrDefault("AGENTTEAMS_FS_BUCKET", "agentteams-storage"),
		WorkerDepsStorageBucket: firstNonEmpty(
			os.Getenv("AGENTTEAMS_WORKER_DEPS_STORAGE_BUCKET"),
			os.Getenv("AGENTTEAMS_FS_BUCKET"),
			os.Getenv("AGENTTEAMS_FS_BUCKET"),
			"agentteams-storage",
		),
		WorkerDepsStorageEndpoint: firstNonEmpty(
			os.Getenv("AGENTTEAMS_WORKER_DEPS_STORAGE_ENDPOINT"),
			os.Getenv("AGENTTEAMS_FS_ENDPOINT"),
			os.Getenv("AGENTTEAMS_FS_ENDPOINT"),
		),
		WorkerDepsMountAuthType: envOrDefault("AGENTTEAMS_MOUNT_AUTH_TYPE", "RRSA"),
		WorkerDepsMountRoleName: os.Getenv("AGENTTEAMS_MOUNT_ROLE_NAME"),

		K8sNamespace:    os.Getenv("AGENTTEAMS_K8S_NAMESPACE"),
		K8sWorkerCPU:    envOrDefault("AGENTTEAMS_K8S_WORKER_CPU", "1000m"),
		K8sWorkerMemory: envOrDefault("AGENTTEAMS_K8S_WORKER_MEMORY", "2Gi"),

		SandboxProviderType:          envOrDefault("AGENTTEAMS_SANDBOX_PROVIDER_TYPE", "openkruise"),
		SandboxCapabilities:          os.Getenv("AGENTTEAMS_SANDBOX_CAPABILITIES"),
		SandboxPrewarmSize:           envOrDefaultInt("AGENTTEAMS_SANDBOX_PREWARM_SIZE", backend.DefaultSandboxPrewarmSize),
		SandboxPrewarmSizeConfigured: os.Getenv("AGENTTEAMS_SANDBOX_PREWARM_SIZE") != "",

		ManagerEnabled:            envOrDefault("AGENTTEAMS_MANAGER_ENABLED", "true") == "true",
		ManagerModel:              firstNonEmpty(os.Getenv("AGENTTEAMS_MANAGER_MODEL"), envOrDefault("AGENTTEAMS_DEFAULT_MODEL", "qwen3.6-plus")),
		ManagerRuntime:            envOrDefault("AGENTTEAMS_MANAGER_RUNTIME", backend.RuntimeAgentScope),
		ManagerImage:              envOrDefault("AGENTTEAMS_MANAGER_IMAGE", "agentteams/agentteams-manager:latest"),
		ManagerDataClaim:          os.Getenv("AGENTTEAMS_MANAGER_DATA_CLAIM"),
		ManagerHostPath:           os.Getenv("AGENTTEAMS_MANAGER_HOST_PATH"),
		ManagerHostReadAllowlist:  os.Getenv("AGENTTEAMS_MANAGER_HOST_READ_ALLOWLIST"),
		ManagerHostWriteAllowlist: os.Getenv("AGENTTEAMS_MANAGER_HOST_WRITE_ALLOWLIST"),
		DefaultWorkerRuntime:      os.Getenv("AGENTTEAMS_DEFAULT_WORKER_RUNTIME"),
		K8sManagerCPURequest:      envOrDefault("AGENTTEAMS_K8S_MANAGER_CPU_REQUEST", "500m"),
		K8sManagerMemoryRequest:   envOrDefault("AGENTTEAMS_K8S_MANAGER_MEMORY_REQUEST", "1Gi"),
		K8sManagerCPU:             envOrDefault("AGENTTEAMS_K8S_MANAGER_CPU", "2"),
		K8sManagerMemory:          envOrDefault("AGENTTEAMS_K8S_MANAGER_MEMORY", "4Gi"),

		ControllerURL:  os.Getenv("AGENTTEAMS_CONTROLLER_URL"),
		ControllerName: os.Getenv("AGENTTEAMS_CONTROLLER_NAME"),

		ManagerWorkspaceDir: os.Getenv("AGENTTEAMS_WORKSPACE_DIR"),
		HostShareDir:        os.Getenv("AGENTTEAMS_HOST_SHARE_DIR"),
		ManagerHealthPort: firstNonEmpty(
			os.Getenv("AGENTTEAMS_PORT_MANAGER_HEALTH"),
			os.Getenv("AGENTTEAMS_PORT_MANAGER_CONSOLE"),
			"18888",
		),
		ManagerPassword:   os.Getenv("AGENTTEAMS_MANAGER_PASSWORD"),
		ManagerGatewayKey: os.Getenv("AGENTTEAMS_MANAGER_GATEWAY_KEY"),

		MatrixServerURL:         envOrDefault("AGENTTEAMS_MATRIX_URL", "http://matrix-local.agentteams.io:8080"),
		MatrixDomain:            envOrDefault("AGENTTEAMS_MATRIX_DOMAIN", "matrix-local.agentteams.io:8080"),
		MatrixRegistrationToken: envOrDefault("AGENTTEAMS_MATRIX_REGISTRATION_TOKEN", os.Getenv("AGENTTEAMS_REGISTRATION_TOKEN")),
		MatrixAdminUser:         os.Getenv("AGENTTEAMS_ADMIN_USER"),
		MatrixAdminPassword:     os.Getenv("AGENTTEAMS_ADMIN_PASSWORD"),
		MatrixE2EE:              os.Getenv("AGENTTEAMS_MATRIX_E2EE") == "1" || os.Getenv("AGENTTEAMS_MATRIX_E2EE") == "true",

		MatrixAppServiceEnabled:            os.Getenv("AGENTTEAMS_MATRIX_APPSERVICE_ENABLED") != "0" && os.Getenv("AGENTTEAMS_MATRIX_APPSERVICE_ENABLED") != "false",
		MatrixAppServiceID:                 envOrDefault("AGENTTEAMS_MATRIX_APPSERVICE_ID", "agentteams-controller"),
		MatrixAppServiceASToken:            os.Getenv("AGENTTEAMS_MATRIX_APPSERVICE_AS_TOKEN"),
		MatrixAppServiceHSToken:            os.Getenv("AGENTTEAMS_MATRIX_APPSERVICE_HS_TOKEN"),
		MatrixAppServiceSenderLocalpart:    envOrDefault("AGENTTEAMS_MATRIX_APPSERVICE_SENDER_LOCALPART", "agentteams-controller"),
		MatrixAppServiceUserNamespaceRegex: os.Getenv("AGENTTEAMS_MATRIX_APPSERVICE_USER_NAMESPACE_REGEX"),

		OSSStoragePrefix: envOrDefault("AGENTTEAMS_STORAGE_PREFIX", "agentteams/agentteams-storage"),

		DefaultModel:       envOrDefault("AGENTTEAMS_DEFAULT_MODEL", "qwen3.6-plus"),
		EmbeddingModel:     os.Getenv("AGENTTEAMS_EMBEDDING_MODEL"),
		Runtime:            envOrDefault("AGENTTEAMS_RUNTIME", "docker"),
		ModelContextWindow: envOrDefaultInt("AGENTTEAMS_MODEL_CONTEXT_WINDOW", 0),
		ModelMaxTokens:     envOrDefaultInt("AGENTTEAMS_MODEL_MAX_TOKENS", 0),

		LLMProvider:                envOrDefault("AGENTTEAMS_LLM_PROVIDER", "qwen"),
		LLMAPIKey:                  os.Getenv("AGENTTEAMS_LLM_API_KEY"),
		GitHubToken:                firstNonEmpty(os.Getenv("AGENTTEAMS_MCP_GITHUB_TOKEN"), os.Getenv("AGENTTEAMS_GITHUB_TOKEN")),
		OpenAIBaseURL:              os.Getenv("AGENTTEAMS_OPENAI_BASE_URL"),
		AIStreamIdleTimeoutSeconds: envOrDefaultInt("AGENTTEAMS_AI_STREAM_IDLE_TIMEOUT_SECONDS", 900),
		CinnyURL: firstNonEmpty(
			os.Getenv("AGENTTEAMS_CINNY_URL"),
			os.Getenv("AGENTTEAMS_ELEMENT_WEB_URL"),
		),
		ManagerAdminURL: os.Getenv("AGENTTEAMS_MANAGER_ADMIN_URL"),

		UserLanguage: envOrDefault("AGENTTEAMS_LANGUAGE", "zh"),
		UserTimezone: envOrDefault("TZ", "Asia/Shanghai"),

		CMSTracesEnabled:  envBool("AGENTTEAMS_CMS_TRACES_ENABLED"),
		CMSMetricsEnabled: envBool("AGENTTEAMS_CMS_METRICS_ENABLED"),
		CMSEndpoint:       os.Getenv("AGENTTEAMS_CMS_ENDPOINT"),
		CMSLicenseKey:     os.Getenv("AGENTTEAMS_CMS_LICENSE_KEY"),
		CMSProject:        os.Getenv("AGENTTEAMS_CMS_PROJECT"),
		CMSWorkspace:      os.Getenv("AGENTTEAMS_CMS_WORKSPACE"),
		CMSServiceName:    envOrDefault("AGENTTEAMS_CMS_SERVICE_NAME", "agentteams-manager"),

		WorkerEnv: WorkerEnvDefaults{
			MatrixDomain:            envOrDefault("AGENTTEAMS_MATRIX_DOMAIN", "matrix-local.agentteams.io:8080"),
			FSEndpoint:              os.Getenv("AGENTTEAMS_FS_ENDPOINT"),
			FSBucket:                envOrDefault("AGENTTEAMS_FS_BUCKET", "agentteams-storage"),
			StoragePrefix:           envOrDefault("AGENTTEAMS_STORAGE_PREFIX", "agentteams/agentteams-storage"),
			ControllerURL:           os.Getenv("AGENTTEAMS_CONTROLLER_URL"),
			AIGatewayURL:            envOrDefault("AGENTTEAMS_AI_GATEWAY_URL", "http://aigw-local.agentteams.io:8080"),
			MatrixURL:               envOrDefault("AGENTTEAMS_MATRIX_URL", "http://matrix-local.agentteams.io:8080"),
			AdminUser:               os.Getenv("AGENTTEAMS_ADMIN_USER"),
			AdminPassword:           os.Getenv("AGENTTEAMS_ADMIN_PASSWORD"),
			MatrixRegistrationToken: envOrDefault("AGENTTEAMS_MATRIX_REGISTRATION_TOKEN", os.Getenv("AGENTTEAMS_REGISTRATION_TOKEN")),
			HigressAdminURL:         envOrDefault("AGENTTEAMS_AI_GATEWAY_ADMIN_URL", "http://127.0.0.1:8001"),
			MCPGitHubToken:          firstNonEmpty(os.Getenv("AGENTTEAMS_MCP_GITHUB_TOKEN"), os.Getenv("AGENTTEAMS_GITHUB_TOKEN")),
			Runtime:                 kubeMode,
			DefaultWorkerRuntime:    os.Getenv("AGENTTEAMS_DEFAULT_WORKER_RUNTIME"),
			YoloMode:                envBool("AGENTTEAMS_YOLO"),
			MatrixDebug:             envBool("AGENTTEAMS_MATRIX_DEBUG"),

			// CMS observability (propagated from controller env to all workers/managers)
			CMSTracesEnabled:  envBool("AGENTTEAMS_CMS_TRACES_ENABLED"),
			CMSMetricsEnabled: envBool("AGENTTEAMS_CMS_METRICS_ENABLED"),
			CMSEndpoint:       os.Getenv("AGENTTEAMS_CMS_ENDPOINT"),
			CMSLicenseKey:     os.Getenv("AGENTTEAMS_CMS_LICENSE_KEY"),
			CMSProject:        os.Getenv("AGENTTEAMS_CMS_PROJECT"),
			CMSWorkspace:      os.Getenv("AGENTTEAMS_CMS_WORKSPACE"),
			SkillsAPIURL:      envOrDefault("SKILLS_API_URL", os.Getenv("AGENTTEAMS_SKILLS_API_URL")),
			NacosAuthType:     os.Getenv("NACOS_AUTH_TYPE"),
		},
	}

	// In embedded mode, services (Tuwunel, MinIO) run inside the controller container.
	// The controller itself uses 127.0.0.1, but child containers (Manager, Workers) must
	// reach them via the controller's Docker network hostname.
	if cfg.KubeMode == "embedded" {
		if ctrlHost := extractHost(cfg.WorkerEnv.ControllerURL); ctrlHost != "" {
			cfg.WorkerEnv.MatrixURL = replaceHost(cfg.WorkerEnv.MatrixURL, ctrlHost)
			cfg.WorkerEnv.FSEndpoint = replaceHost(cfg.WorkerEnv.FSEndpoint, ctrlHost)
			cfg.WorkerEnv.AIGatewayURL = replaceHost(cfg.WorkerEnv.AIGatewayURL, ctrlHost)
			cfg.WorkerEnv.HigressAdminURL = replaceHost(cfg.WorkerEnv.HigressAdminURL, ctrlHost)
		}
	}
	// S3/MinIO API is never on the Higress HTTP gateway port (8080). Misconfigured
	// AGENTTEAMS_FS_DOMAIN:8080 URLs are rewritten to the MinIO object port.
	cfg.WorkerEnv.FSEndpoint = normalizeMinIOS3Endpoint(cfg.WorkerEnv.FSEndpoint)

	if specJSON := os.Getenv("AGENTTEAMS_MANAGER_SPEC"); specJSON != "" {
		if err := applyManagerSpec(cfg, specJSON); err != nil {
			panic(fmt.Sprintf("invalid AGENTTEAMS_MANAGER_SPEC: %v", err))
		}
	}
	if !backend.ValidManagerRuntime(cfg.ManagerRuntime) {
		panic(
			fmt.Sprintf(
				"invalid AGENTTEAMS_MANAGER_RUNTIME %q: only %q is supported",
				cfg.ManagerRuntime,
				backend.RuntimeAgentScope,
			),
		)
	}
	cfg.ManagerRuntime = backend.ResolveManagerRuntime(cfg.ManagerRuntime)

	// Validate AppService tokens when AS mode is enabled.
	// Tokens must be provided via env vars (set by install script or manually).
	// We do NOT auto-generate at runtime to prevent token drift across restarts.
	if cfg.MatrixAppServiceEnabled {
		matrixControllerURL := firstNonEmpty(os.Getenv("AGENTTEAMS_MATRIX_APPSERVICE_CONTROLLER_URL"), cfg.ControllerURL)
		cfg.MatrixAppServicePushURL = appServicePushURL(matrixControllerURL)
		if cfg.MatrixAppServiceASToken == "" {
			panic("AGENTTEAMS_MATRIX_APPSERVICE_AS_TOKEN is required when AppService mode is enabled; run install script or set env var")
		}
		if cfg.MatrixAppServiceHSToken == "" {
			panic("AGENTTEAMS_MATRIX_APPSERVICE_HS_TOKEN is required when AppService mode is enabled; run install script or set env var")
		}
	}

	return cfg
}

// Namespace returns the effective K8s namespace, defaulting to "default".
func (c *Config) Namespace() string {
	// 逻辑说明：显式 K8s namespace 优先，空值统一回退 default，所有 client/reconciler 因而使用同一作用域。
	if c.K8sNamespace != "" {
		return c.K8sNamespace
	}
	return "default"
}

// HasMinIOAdmin reports whether the local MinIO admin API is available.
func (c *Config) HasMinIOAdmin() bool {
	// 逻辑说明：以已解析的文件存储 endpoint 判断本地 MinIO 管理能力是否可构造。
	return c.WorkerEnv.FSEndpoint != ""
}

// CredsDir returns the directory for persisted worker credentials (embedded mode).
func (c *Config) CredsDir() string {
	// 逻辑说明：运行时允许环境覆盖嵌入式凭据目录，否则使用受控持久路径。
	return envOrDefault("AGENTTEAMS_CREDS_DIR", "/data/worker-creds")
}

// AgentFSDir returns the local filesystem root for agent workspaces.
func (c *Config) AgentFSDir() string {
	// 逻辑说明：返回 Agent 工作区根路径，支持显式覆盖并保持嵌入式默认布局。
	return envOrDefault("AGENTTEAMS_AGENT_FS_DIR", "/root/agentteams-fs/agents")
}

// WorkerAgentDir returns the source directory for builtin worker agent files.
func (c *Config) WorkerAgentDir() string {
	// 逻辑说明：解析内置 Worker prompt/skill 源目录，未配置时使用镜像内固定路径。
	return envOrDefault("AGENTTEAMS_WORKER_AGENT_DIR", "/opt/agentteams/agent/worker-agent")
}

// RegistryPath returns the path to the workers-registry.json (embedded mode).
func (c *Config) RegistryPath() string {
	// 逻辑说明：解析嵌入模式 Worker registry 文件位置，允许测试或自定义镜像覆盖。
	return envOrDefault("AGENTTEAMS_REGISTRY_PATH", "/root/workers-registry.json")
}

// ManagerResources returns the resource requirements for the Manager Pod.
func (c *Config) ManagerResources() *backend.ResourceRequirements {
	// 逻辑说明：把拆分的请求/限制字段组合成 backend 所需对象，不在此重新读取环境。
	return &backend.ResourceRequirements{
		CPURequest:    c.K8sManagerCPURequest,
		CPULimit:      c.K8sManagerCPU,
		MemoryRequest: c.K8sManagerMemoryRequest,
		MemoryLimit:   c.K8sManagerMemory,
	}
}

func (c *Config) DockerConfig() backend.DockerConfig {
	// 逻辑说明：组合 socket、五类镜像和默认网络；镜像可用环境覆盖，其余来自已冻结配置。
	return backend.DockerConfig{
		SocketPath:         c.SocketPath,
		ManagerImage:       c.ManagerImage,
		WorkerImage:        envOrDefault("AGENTTEAMS_WORKER_IMAGE", "agentteams/agentteams-worker:latest"),
		CopawWorkerImage:   envOrDefault("AGENTTEAMS_COPAW_WORKER_IMAGE", "agentteams/agentteams-copaw-worker:latest"),
		HermesWorkerImage:  envOrDefault("AGENTTEAMS_HERMES_WORKER_IMAGE", "agentteams/agentteams-hermes-worker:latest"),
		QwenPawWorkerImage: envOrDefault("AGENTTEAMS_QWENPAW_WORKER_IMAGE", "agentteams/agentteams-qwenpaw-worker:latest"),
		DefaultNetwork:     envOrDefault("AGENTTEAMS_DOCKER_NETWORK", "agentteams-net"),
	}
}

func (c *Config) STSConfig() credentials.STSConfig {
	// 逻辑说明：以显式 FS endpoint 优先、Worker 环境 endpoint 兜底，组合 sidecar 签发所需存储信息。
	return credentials.STSConfig{
		OSSBucket:   c.OSSBucket,
		OSSEndpoint: firstNonEmpty(os.Getenv("AGENTTEAMS_FS_ENDPOINT"), c.WorkerEnv.FSEndpoint),
	}
}

// AIGatewayConfig returns the gateway.AIGatewayConfig used when
// GatewayProvider == "ai-gateway".
func (c *Config) AIGatewayConfig() gateway.AIGatewayConfig {
	// 逻辑说明：将地域、endpoint、gateway/model/environment ID 汇总成云 AI Gateway client 配置。
	return gateway.AIGatewayConfig{
		Region:     c.Region,
		Endpoint:   c.GWEndpoint,
		GatewayID:  c.GWGatewayID,
		ModelAPIID: c.GWModelAPIID,
		EnvID:      c.GWEnvID,
	}
}

// UsesAIGateway reports whether the controller should wire the AI Gateway
// (APIG) implementation of gateway.Client.
func (c *Config) UsesAIGateway() bool {
	// 逻辑说明：通过规范化 provider 名选择云 AI Gateway 实现，而不是按零散字段是否存在猜测。
	return c.GatewayProvider == "ai-gateway"
}

// UsesExternalOSS reports whether the controller should talk to Alibaba
// Cloud OSS (existing bucket) instead of an embedded MinIO.
func (c *Config) UsesExternalOSS() bool {
	// 逻辑说明：显式 storage provider 为 oss 时选择外部对象存储，否则保持嵌入式 MinIO 路径。
	return c.StorageProvider == "oss"
}

func (c *Config) K8sConfig() backend.K8sConfig {
	// 逻辑说明：把 namespace、镜像、资源和 controller 隔离信息投影为 K8s backend 配置。
	return backend.K8sConfig{
		Namespace:          c.K8sNamespace,
		ManagerImage:       c.ManagerImage,
		ManagerDataClaim:   c.ManagerDataClaim,
		ManagerHostPath:    c.ManagerHostPath,
		WorkerImage:        envOrDefault("AGENTTEAMS_WORKER_IMAGE", "agentteams/agentteams-worker:latest"),
		CopawWorkerImage:   envOrDefault("AGENTTEAMS_COPAW_WORKER_IMAGE", "agentteams/agentteams-copaw-worker:latest"),
		HermesWorkerImage:  envOrDefault("AGENTTEAMS_HERMES_WORKER_IMAGE", "agentteams/agentteams-hermes-worker:latest"),
		QwenPawWorkerImage: envOrDefault("AGENTTEAMS_QWENPAW_WORKER_IMAGE", "agentteams/agentteams-qwenpaw-worker:latest"),
		WorkerCPU:          c.K8sWorkerCPU,
		WorkerMemory:       c.K8sWorkerMemory,
		ControllerName:     c.ControllerName,
		ResourcePrefix:     c.ResourcePrefix,
	}
}

func (c *Config) SandboxConfig() backend.SandboxConfig {
	// 逻辑说明：组合 sandbox provider、预热参数、镜像和资源限制；是否显式配置预热大小单独保留。
	return backend.SandboxConfig{
		Namespace:                    c.K8sNamespace,
		ProviderType:                 c.SandboxProviderType,
		AgentRuntimeImage:            os.Getenv("AGENTTEAMS_SANDBOX_AGENT_RUNTIME_IMAGE"),
		ManagerImage:                 c.ManagerImage,
		WorkerImage:                  envOrDefault("AGENTTEAMS_WORKER_IMAGE", "agentteams/agentteams-worker:latest"),
		CopawWorkerImage:             envOrDefault("AGENTTEAMS_COPAW_WORKER_IMAGE", "agentteams/agentteams-copaw-worker:latest"),
		HermesWorkerImage:            envOrDefault("AGENTTEAMS_HERMES_WORKER_IMAGE", "agentteams/agentteams-hermes-worker:latest"),
		QwenPawWorkerImage:           envOrDefault("AGENTTEAMS_QWENPAW_WORKER_IMAGE", "agentteams/agentteams-qwenpaw-worker:latest"),
		WorkerCPU:                    c.K8sWorkerCPU,
		WorkerMemory:                 c.K8sWorkerMemory,
		SandboxPrewarmSize:           c.SandboxPrewarmSize,
		SandboxPrewarmSizeConfigured: c.SandboxPrewarmSizeConfigured,
		ControllerName:               c.ControllerName,
		ResourcePrefix:               c.ResourcePrefix,
	}
}

func envOrDefault(key, defaultVal string) string {
	// 逻辑说明：只把非空环境值视为显式覆盖，避免“变量存在但为空”意外抹掉可工作的默认值。
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// generateRandomHex returns a cryptographically random hex string of n bytes (2n hex chars).
func generateRandomHex(n int) string {
	// 逻辑说明：使用密码学安全随机源生成 n 字节令牌；随机源失败时立即终止启动，避免退化为可预测凭据。
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

func envOrDefaultInt(key string, defaultVal int) int {
	// 逻辑说明：读取十进制环境变量；空值或非法整数都回退默认值，使错误配置不会把端口、超时等字段变成零值。
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}

func envBool(key string) bool {
	// 逻辑说明：把部署清单常见的 1/true 大小写形式统一解释为开启，其他值保持关闭。
	v := os.Getenv(key)
	return v == "1" || v == "true" || v == "True" || v == "TRUE"
}

func envBoolDefault(key string, defaultVal bool) bool {
	// 逻辑说明：未设置变量时保留调用方指定的默认策略；一旦显式设置，则按受支持的真值集合解析。
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	return v == "1" || v == "true" || v == "True" || v == "TRUE"
}

func firstNonEmpty(values ...string) string {
	// 逻辑说明：按新配置到旧兼容配置的优先顺序返回首个非空值，所有候选都为空时返回空串。
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func applyManagerSpec(cfg *Config, specJSON string) error {
	// 逻辑说明：解析单一 JSON 规范并以其中的非空字段覆盖环境配置；资源和 Coding CLI 会进一步校验，失败时拒绝整次启动配置。
	var spec managerSpecEnv
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		return err
	}

	if spec.Model != "" {
		cfg.ManagerModel = spec.Model
	}
	if spec.Runtime != "" {
		cfg.ManagerRuntime = spec.Runtime
	}
	if spec.Image != "" {
		cfg.ManagerImage = spec.Image
	}
	if !agentResourcesEmpty(spec.Resources) {
		resources := spec.Resources
		cfg.ManagerSpecResources = &resources
	}
	if spec.Resources.Requests.CPU != "" {
		cfg.K8sManagerCPURequest = spec.Resources.Requests.CPU
	}
	if spec.Resources.Requests.Memory != "" {
		cfg.K8sManagerMemoryRequest = spec.Resources.Requests.Memory
	}
	if spec.Resources.Limits.CPU != "" {
		cfg.K8sManagerCPU = spec.Resources.Limits.CPU
	}
	if spec.Resources.Limits.Memory != "" {
		cfg.K8sManagerMemory = spec.Resources.Limits.Memory
	}
	if spec.CodingCLI != nil {
		if err := spec.CodingCLI.Validate(); err != nil {
			return err
		}
		cfg.ManagerCodingCLI = spec.CodingCLI.DeepCopy()
	}

	return nil
}

func agentResourcesEmpty(r v1beta1.AgentResourceRequirements) bool {
	// 逻辑说明：同时检查 request 与 limit 四个维度，只有完全未声明资源时才允许沿用旧环境变量配置。
	return r.Requests.CPU == "" &&
		r.Requests.Memory == "" &&
		r.Limits.CPU == "" &&
		r.Limits.Memory == ""
}

// extractHost returns the hostname from a URL (e.g. "http://agentteams-controller:8090" → "agentteams-controller").
func extractHost(rawURL string) string {
	// 逻辑说明：从服务 URL 安全提取不含端口的主机名；解析失败返回空值，让调用方保留原地址而非构造损坏 URL。
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// replaceHost replaces the hostname in a URL while preserving scheme, port, and path.
func replaceHost(rawURL, newHost string) string {
	// 逻辑说明：只替换 URL 主机名并保留协议、端口和路径；输入缺失或解析失败时原样返回以支持幂等兼容重写。
	if rawURL == "" || newHost == "" {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	if u.Port() != "" {
		u.Host = newHost + ":" + u.Port()
	} else {
		u.Host = newHost
	}
	return u.String()
}

// normalizeMinIOS3Endpoint rewrites a common misconfiguration: the S3/MinIO API
// is served on the object store port (9000 in AgentTeams), not the Higress HTTP
// gateway (8080). A URL like http://fs-local.agentteams.io:8080 breaks mc silently.
func normalizeMinIOS3Endpoint(raw string) string {
	// 逻辑说明：仅把已知误配的 8080 网关端口改写为 MinIO API 的 9000；其他端口、空值和非法 URL 均保持原样。
	if raw == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Port() != "8080" {
		return raw
	}
	hostname := u.Hostname()
	if hostname == "" {
		return raw
	}
	u.Host = hostname + ":9000"
	return u.String()
}

func (c *Config) MatrixConfig() matrix.Config {
	// 逻辑说明：把进程级 Matrix 与 AppService 字段投影为客户端专用配置，避免下游直接依赖庞大的 Config。
	return matrix.Config{
		ServerURL:                    c.MatrixServerURL,
		Domain:                       c.MatrixDomain,
		RegistrationToken:            c.MatrixRegistrationToken,
		AdminUser:                    c.MatrixAdminUser,
		AdminPassword:                c.MatrixAdminPassword,
		E2EEEnabled:                  c.MatrixE2EE,
		AppServiceEnabled:            c.MatrixAppServiceEnabled,
		AppServiceID:                 c.MatrixAppServiceID,
		AppServiceToken:              c.MatrixAppServiceASToken,
		AppServiceHSToken:            c.MatrixAppServiceHSToken,
		AppServiceSenderLocalpart:    c.MatrixAppServiceSenderLocalpart,
		AppServiceUserNamespaceRegex: c.MatrixAppServiceUserNamespaceRegex,
		AppServicePushURL:            c.MatrixAppServicePushURL,
	}
}

func appServicePushURL(controllerURL string) string {
	// 逻辑说明：清理控制器公开地址两端空白和末尾斜杠，空输入保持禁用，供 AppService 注册信息稳定复用。
	controllerURL = strings.TrimRight(strings.TrimSpace(controllerURL), "/")
	if controllerURL == "" {
		return ""
	}
	return controllerURL
}

func (c *Config) GatewayConfig() gateway.Config {
	// 逻辑说明：组合 Higress 管理面与数据面地址；只有嵌入模式允许默认管理员回退，外部部署必须使用显式凭据。
	return gateway.Config{
		ConsoleURL:                c.HigressBaseURL,
		AdminUser:                 c.HigressAdminUser,
		AdminPassword:             c.HigressAdminPassword,
		AllowDefaultAdminFallback: c.KubeMode == "embedded",
		DataPlaneURL:              c.WorkerEnv.AIGatewayURL,
	}
}

func (c *Config) OSSConfig() oss.Config {
	// 逻辑说明：按新旧环境变量优先级选择对象存储凭据和 endpoint，并修正常见 MinIO 端口误配后交给 OSS 客户端。
	accessKey := firstNonEmpty(os.Getenv("AGENTTEAMS_FS_ACCESS_KEY"), os.Getenv("AGENTTEAMS_MINIO_USER"))
	secretKey := firstNonEmpty(os.Getenv("AGENTTEAMS_FS_SECRET_KEY"), os.Getenv("AGENTTEAMS_MINIO_PASSWORD"))
	endpoint := firstNonEmpty(os.Getenv("AGENTTEAMS_FS_ENDPOINT"), c.WorkerEnv.FSEndpoint)
	return oss.Config{
		StoragePrefix: c.OSSStoragePrefix,
		Bucket:        c.OSSBucket,
		Endpoint:      normalizeMinIOS3Endpoint(endpoint),
		AccessKey:     accessKey,
		SecretKey:     secretKey,
	}
}

// ManagerAgentEnv returns environment variables that a standalone Manager Agent
// container needs to connect to the infrastructure services in the embedded
// controller container. These are passed via DockerBackend.Create.
func (c *Config) ManagerAgentEnv() map[string]string {
	// 逻辑说明：从已校验配置构造独立 Manager 容器的环境快照，只传递非空项，并解析外部渠道 JSON 后按引用复制所需密钥。
	env := map[string]string{}
	setIfNonEmpty := func(k, v string) {
		if v != "" {
			env[k] = v
		}
	}
	setIfNonEmpty("AGENTTEAMS_MINIO_USER", os.Getenv("AGENTTEAMS_MINIO_USER"))
	setIfNonEmpty("AGENTTEAMS_MINIO_PASSWORD", os.Getenv("AGENTTEAMS_MINIO_PASSWORD"))
	setIfNonEmpty("AGENTTEAMS_ADMIN_USER", c.MatrixAdminUser)
	setIfNonEmpty("AGENTTEAMS_ADMIN_PASSWORD", c.MatrixAdminPassword)
	setIfNonEmpty("AGENTTEAMS_REGISTRATION_TOKEN", c.MatrixRegistrationToken)
	setIfNonEmpty("AGENTTEAMS_AI_GATEWAY_ADMIN_URL", c.HigressBaseURL)
	setIfNonEmpty("AGENTTEAMS_MATRIX_URL", c.WorkerEnv.MatrixURL)
	setIfNonEmpty("AGENTTEAMS_AI_GATEWAY_URL", c.WorkerEnv.AIGatewayURL)
	setIfNonEmpty("AGENTTEAMS_FS_ENDPOINT", c.WorkerEnv.FSEndpoint)
	setIfNonEmpty("AGENTTEAMS_FS_BUCKET", c.WorkerEnv.FSBucket)
	setIfNonEmpty("AGENTTEAMS_FS_ACCESS_KEY", firstNonEmpty(os.Getenv("AGENTTEAMS_FS_ACCESS_KEY"), os.Getenv("AGENTTEAMS_MINIO_USER")))
	setIfNonEmpty("AGENTTEAMS_FS_SECRET_KEY", firstNonEmpty(os.Getenv("AGENTTEAMS_FS_SECRET_KEY"), os.Getenv("AGENTTEAMS_MINIO_PASSWORD")))
	setIfNonEmpty("AGENTTEAMS_STORAGE_PREFIX", c.OSSStoragePrefix)
	setIfNonEmpty("AGENTTEAMS_MATRIX_DOMAIN", c.MatrixDomain)
	setIfNonEmpty("AGENTTEAMS_DEFAULT_MODEL", c.DefaultModel)
	setIfNonEmpty("AGENTTEAMS_EMBEDDING_MODEL", c.EmbeddingModel)
	setIfNonEmpty("AGENTTEAMS_LLM_PROVIDER", c.LLMProvider)
	setIfNonEmpty("AGENTTEAMS_LLM_API_KEY", c.LLMAPIKey)
	setIfNonEmpty("AGENTTEAMS_MCP_GITHUB_TOKEN", c.GitHubToken)
	if c.AIStreamIdleTimeoutSeconds > 0 {
		env["AGENTTEAMS_AI_STREAM_IDLE_TIMEOUT_SECONDS"] = strconv.Itoa(c.AIStreamIdleTimeoutSeconds)
	}
	setIfNonEmpty("AGENTTEAMS_CINNY_URL", c.CinnyURL)
	if c.ManagerHostPath != "" {
		env["AGENTTEAMS_HOST_SHARE_ROOT"] = "/host-share"
		setIfNonEmpty(
			"AGENTTEAMS_HOST_READ_ALLOWLIST",
			c.ManagerHostReadAllowlist,
		)
		setIfNonEmpty(
			"AGENTTEAMS_HOST_WRITE_ALLOWLIST",
			c.ManagerHostWriteAllowlist,
		)
	}
	setIfNonEmpty(
		"AGENTTEAMS_EXTERNAL_CHANNELS",
		os.Getenv("AGENTTEAMS_EXTERNAL_CHANNELS"),
	)
	for _, name := range externalChannelSecretNames(
		os.Getenv("AGENTTEAMS_EXTERNAL_CHANNELS"),
	) {
		setIfNonEmpty(name, os.Getenv(name))
	}
	if c.MatrixE2EE {
		env["AGENTTEAMS_MATRIX_E2EE"] = "1"
	}
	if c.WorkerEnv.MatrixDebug {
		env["AGENTTEAMS_MATRIX_DEBUG"] = "1"
	}
	if c.CMSTracesEnabled {
		env["AGENTTEAMS_CMS_TRACES_ENABLED"] = "1"
	}
	if c.CMSMetricsEnabled {
		env["AGENTTEAMS_CMS_METRICS_ENABLED"] = "1"
	}
	setIfNonEmpty("AGENTTEAMS_CMS_ENDPOINT", c.CMSEndpoint)
	setIfNonEmpty("AGENTTEAMS_CMS_LICENSE_KEY", c.CMSLicenseKey)
	setIfNonEmpty("AGENTTEAMS_CMS_PROJECT", c.CMSProject)
	setIfNonEmpty("AGENTTEAMS_CMS_WORKSPACE", c.CMSWorkspace)
	setIfNonEmpty("AGENTTEAMS_CMS_SERVICE_NAME", c.CMSServiceName)
	return env
}

func externalChannelSecretNames(raw string) []string {
	// 逻辑说明：解析渠道配置里的 env: 引用，去重并排序返回实际需要透传的密钥名；非法 JSON 安全视为没有引用。
	if raw == "" {
		return nil
	}
	var documents []struct {
		TokenEnv         string            `json:"token_env"`
		WebhookSecretEnv string            `json:"webhook_secret_env"`
		SecretEnvs       map[string]string `json:"secret_envs"`
	}
	if err := json.Unmarshal([]byte(raw), &documents); err != nil {
		return nil
	}
	names := make(map[string]struct{})
	for _, document := range documents {
		for _, reference := range []string{
			document.TokenEnv,
			document.WebhookSecretEnv,
		} {
			name := strings.TrimPrefix(reference, "env:")
			if name != reference && name != "" {
				names[name] = struct{}{}
			}
		}
		for _, reference := range document.SecretEnvs {
			name := strings.TrimPrefix(reference, "env:")
			if name != reference && name != "" {
				names[name] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (c *Config) AgentConfig() agentconfig.Config {
	// 逻辑说明：为 Worker 配置生成器选择其容器内可达的服务 URL；嵌入模式优先使用已做主机替换的 WorkerEnv 地址。
	// Use WorkerEnv URLs (host-replaced in embedded mode) since openclaw.json
	// is consumed by worker containers, not the controller itself.
	matrixURL := c.MatrixServerURL
	aiGatewayURL := envOrDefault("AGENTTEAMS_AI_GATEWAY_URL", "http://aigw-local.agentteams.io:8080")
	if c.KubeMode == "embedded" {
		if c.WorkerEnv.MatrixURL != "" {
			matrixURL = c.WorkerEnv.MatrixURL
		}
		if c.WorkerEnv.AIGatewayURL != "" {
			aiGatewayURL = c.WorkerEnv.AIGatewayURL
		}
	}
	return agentconfig.Config{
		MatrixDomain:       c.MatrixDomain,
		MatrixServerURL:    matrixURL,
		AIGatewayURL:       aiGatewayURL,
		AdminUser:          c.MatrixAdminUser,
		DefaultModel:       c.DefaultModel,
		EmbeddingModel:     c.EmbeddingModel,
		Runtime:            c.Runtime,
		E2EEEnabled:        c.MatrixE2EE,
		ModelContextWindow: c.ModelContextWindow,
		ModelMaxTokens:     c.ModelMaxTokens,
		CMSTracesEnabled:   c.CMSTracesEnabled,
		CMSMetricsEnabled:  c.CMSMetricsEnabled,
		CMSEndpoint:        c.CMSEndpoint,
		CMSLicenseKey:      c.CMSLicenseKey,
		CMSProject:         c.CMSProject,
		CMSWorkspace:       c.CMSWorkspace,
		CMSServiceName:     c.CMSServiceName,
	}
}
