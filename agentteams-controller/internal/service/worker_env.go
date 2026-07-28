package service

import (
	"strconv"
	"strings"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/config"
)

// WorkerEnvBuilder constructs environment variable maps for worker containers.
// Configuration defaults are injected at construction time rather than read
// from os.Getenv at call time, keeping the service layer test-friendly.
type WorkerEnvBuilder struct {
	defaults config.WorkerEnvDefaults
}

func NewWorkerEnvBuilder(defaults config.WorkerEnvDefaults) *WorkerEnvBuilder {
	return &WorkerEnvBuilder{defaults: defaults}
}

// Build returns the env map for a worker container, merging per-worker
// credentials with cluster-wide defaults.
func (b *WorkerEnvBuilder) Build(workerName string, prov *WorkerProvisionResult) map[string]string {
	env := map[string]string{
		"AGENTTEAMS_WORKER_NAME":         workerName,
		"AGENTTEAMS_WORKER_GATEWAY_KEY":  prov.GatewayKey,
		"AGENTTEAMS_WORKER_MATRIX_TOKEN": prov.MatrixToken,
		"AGENTTEAMS_WORKER_ROOM_ID":      prov.RoomID,
		"AGENTTEAMS_FS_ACCESS_KEY":       workerName,
		"AGENTTEAMS_FS_SECRET_KEY":       prov.MinIOPassword,
		"OPENCLAW_DISABLE_BONJOUR":       "1",
		"OPENCLAW_MDNS_HOSTNAME":         "agentteams-w-" + workerName,
		"HOME":                           "/root/agentteams-fs/agents/" + workerName,
	}

	b.applyClusterDefaults(env)
	return env
}

// ApplyWorkerConsoleEnv translates the declarative Worker console desired
// state into the runtime entrypoint contract. Deleting the variable is
// intentional: both supported entrypoints interpret an absent value as
// headless mode.
func ApplyWorkerConsoleEnv(
	env map[string]string,
	runtime string,
	console *v1beta1.WorkerConsoleSpec,
) error {
	delete(env, "AGENTTEAMS_CONSOLE_PORT")
	if console == nil {
		return nil
	}
	if err := console.Validate(runtime); err != nil {
		return err
	}
	if !console.Enabled {
		return nil
	}
	env["AGENTTEAMS_CONSOLE_PORT"] = strconv.Itoa(console.EffectivePort())
	return nil
}

// BuildManager returns the env map for a Manager container.
func (b *WorkerEnvBuilder) BuildManager(managerName string, prov *ManagerProvisionResult, spec v1beta1.ManagerSpec) map[string]string {
	deploymentRuntime := b.defaults.Runtime
	if deploymentRuntime == "" {
		deploymentRuntime = "k8s"
	}
	heartbeatSeconds, err := managerDurationSeconds(
		spec.Config.HeartbeatInterval,
		30*time.Minute,
		"heartbeatInterval",
	)
	if err != nil {
		heartbeatSeconds = int64((30 * time.Minute) / time.Second)
	}
	idleSeconds, err := managerDurationSeconds(
		spec.Config.WorkerIdleTimeout,
		12*time.Hour,
		"workerIdleTimeout",
	)
	if err != nil {
		idleSeconds = int64((12 * time.Hour) / time.Second)
	}
	modelName := strings.TrimPrefix(
		strings.TrimSpace(spec.Model),
		"agentteams-gateway/",
	)
	if modelName == "" {
		modelName = "qwen3.6-plus"
	}
	codingCLI := v1beta1.ManagerCodingCLISpec{}
	if spec.CodingCLI != nil {
		codingCLI = *spec.CodingCLI
	}

	env := map[string]string{
		"AGENTTEAMS_MANAGER_NAME":                        managerName,
		"AGENTTEAMS_MANAGER_MATRIX_USER_ID":              prov.MatrixUserID,
		"AGENTTEAMS_MANAGER_MATRIX_TOKEN":                prov.MatrixToken,
		"AGENTTEAMS_MANAGER_ADMIN_ROOM_ID":               prov.RoomID,
		"AGENTTEAMS_MANAGER_GATEWAY_KEY":                 prov.GatewayKey,
		"AGENTTEAMS_MANAGER_RUNTIME":                     backend.RuntimeAgentScope,
		"AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY":        "manager/agentscope-manager.json",
		"AGENTTEAMS_MANAGER_WORKSPACE":                   "/var/lib/agentteams-manager",
		"AGENTTEAMS_MANAGER_HEARTBEAT_INTERVAL_SECONDS":  strconv.FormatInt(heartbeatSeconds, 10),
		"AGENTTEAMS_MANAGER_WORKER_IDLE_TIMEOUT_SECONDS": strconv.FormatInt(idleSeconds, 10),
		"AGENTTEAMS_DEFAULT_MODEL":                       modelName,
		"AGENTTEAMS_CODING_CLI_ENABLED":                  strconv.FormatBool(codingCLI.Enabled),
		"AGENTTEAMS_CODING_CLI_PROVIDERS":                strings.Join(codingCLI.Providers, ","),
		"AGENTTEAMS_CODING_CLI_TRUSTED_DIRECTORY":        codingCLI.EffectiveTrustedDirectory(),
		"AGENTTEAMS_CODING_CLI_TIMEOUT_SECONDS":          strconv.Itoa(codingCLI.EffectiveTimeoutSeconds()),
		"AGENTTEAMS_CODING_CLI_MAX_OUTPUT_BYTES":         strconv.Itoa(codingCLI.EffectiveMaxOutputBytes()),
		"AGENTTEAMS_FS_ACCESS_KEY":                       managerName,
		"AGENTTEAMS_FS_SECRET_KEY":                       prov.MinIOPassword,
		"AGENTTEAMS_RUNTIME":                             deploymentRuntime,
		"HOME":                                           "/var/lib/agentteams-manager",
	}

	if prov.MatrixPassword != "" {
		env["AGENTTEAMS_MANAGER_MATRIX_PASSWORD"] = prov.MatrixPassword
	}
	if b.defaults.AdminUser != "" {
		env["AGENTTEAMS_ADMIN_USER"] = b.defaults.AdminUser
		env["AGENTTEAMS_HIGRESS_ADMIN_USER"] = b.defaults.AdminUser
	}
	if b.defaults.AdminPassword != "" {
		env["AGENTTEAMS_HIGRESS_ADMIN_PASSWORD"] = b.defaults.AdminPassword
		env["AGENTTEAMS_MANAGER_ADMIN_TOKEN"] = b.defaults.AdminPassword
	}
	if b.defaults.MatrixRegistrationToken != "" {
		env["AGENTTEAMS_MATRIX_REGISTRATION_TOKEN"] = b.defaults.MatrixRegistrationToken
	}
	if b.defaults.HigressAdminURL != "" {
		env["AGENTTEAMS_AI_GATEWAY_ADMIN_URL"] = b.defaults.HigressAdminURL
	}
	if b.defaults.MCPGitHubToken != "" {
		env["AGENTTEAMS_MCP_GITHUB_TOKEN"] = b.defaults.MCPGitHubToken
	}
	if b.defaults.DefaultWorkerRuntime != "" {
		env["AGENTTEAMS_DEFAULT_WORKER_RUNTIME"] = b.defaults.DefaultWorkerRuntime
	}

	if spec.Config.NotifyChannel != "" {
		env["AGENTTEAMS_MANAGER_NOTIFY_CHANNEL"] = spec.Config.NotifyChannel
	}

	b.applyClusterDefaults(env)
	return env
}

func (b *WorkerEnvBuilder) applyClusterDefaults(env map[string]string) {
	for k, v := range map[string]string{
		"AGENTTEAMS_MATRIX_DOMAIN":  b.defaults.MatrixDomain,
		"AGENTTEAMS_FS_ENDPOINT":    b.defaults.FSEndpoint,
		"AGENTTEAMS_FS_BUCKET":      b.defaults.FSBucket,
		"AGENTTEAMS_STORAGE_PREFIX": b.defaults.StoragePrefix,
		"AGENTTEAMS_CONTROLLER_URL": b.defaults.ControllerURL,
		"AGENTTEAMS_AI_GATEWAY_URL": b.defaults.AIGatewayURL,
		"AGENTTEAMS_MATRIX_URL":     b.defaults.MatrixURL,
	} {
		if v != "" {
			env[k] = v
		}
	}

	// YOLO mode: when the controller was started with AGENTTEAMS_YOLO=1, propagate
	// it to every manager and worker container it provisions so the agent's
	// auto-confirm path triggers reliably (otherwise an agent without this
	// signal will block on confirmation prompts during integration tests).
	if b.defaults.YoloMode {
		env["AGENTTEAMS_YOLO"] = "1"
	}

	// Matrix trace logging: Worker entrypoints translate this to their native
	// runtime flags, while the AgentScope Manager reads the AgentTeams name
	// directly.
	if b.defaults.MatrixDebug {
		env["AGENTTEAMS_MATRIX_DEBUG"] = "1"
	}

	// CMS observability configuration
	if b.defaults.CMSTracesEnabled {
		env["AGENTTEAMS_CMS_TRACES_ENABLED"] = "true"
	}
	if b.defaults.CMSMetricsEnabled {
		env["AGENTTEAMS_CMS_METRICS_ENABLED"] = "true"
	}
	if b.defaults.CMSEndpoint != "" {
		env["AGENTTEAMS_CMS_ENDPOINT"] = b.defaults.CMSEndpoint
	}
	if b.defaults.CMSLicenseKey != "" {
		env["AGENTTEAMS_CMS_LICENSE_KEY"] = b.defaults.CMSLicenseKey
	}
	if b.defaults.CMSProject != "" {
		env["AGENTTEAMS_CMS_PROJECT"] = b.defaults.CMSProject
	}
	if b.defaults.CMSWorkspace != "" {
		env["AGENTTEAMS_CMS_WORKSPACE"] = b.defaults.CMSWorkspace
	}
	if b.defaults.SkillsAPIURL != "" {
		env["SKILLS_API_URL"] = b.defaults.SkillsAPIURL
	}
	if b.defaults.NacosAuthType != "" {
		env["NACOS_AUTH_TYPE"] = b.defaults.NacosAuthType
	}
}
