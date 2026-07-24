package agentconfig

import v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"

// Config holds parameters for generating agent runtime configurations.
type Config struct {
	MatrixDomain    string // Matrix domain for user IDs, e.g. "matrix-local.agentteams.io:8080"
	MatrixServerURL string // Matrix CS API URL for agent connections
	AIGatewayURL    string // AI gateway URL for model API calls
	AdminUser       string // admin username
	DefaultModel    string // default model name
	EmbeddingModel  string // embedding model for memory search (optional)
	Runtime         string // "docker", "k8s", "aliyun"
	E2EEEnabled     bool   // enable Matrix E2EE

	// Model parameter overrides (empty = use defaults from model table)
	ModelContextWindow int
	ModelMaxTokens     int
	ModelVision        *bool // nil = use model default
	ModelReasoning     *bool // nil = use model default

	// CMS observability (optional)
	CMSTracesEnabled  bool
	CMSMetricsEnabled bool
	CMSEndpoint       string
	CMSLicenseKey     string
	CMSProject        string
	CMSWorkspace      string
	CMSServiceName    string
}

// HeartbeatConfig describes the heartbeat settings to embed in openclaw.json.
type HeartbeatConfig struct {
	Enabled bool
	Every   string // e.g. "30m", "1h"
}

// WorkerConfigRequest describes everything needed to generate a worker's config files.
type WorkerConfigRequest struct {
	WorkerName     string           // e.g. "worker-alice"
	MatrixToken    string           // worker's Matrix access token
	GatewayKey     string           // worker's gateway API key
	ModelName      string           // optional: override default model
	AIGatewayURL   string           // per-worker AI Gateway URL override (from modelProvider)
	TeamLeaderName string           // if non-empty, this is a team worker
	ChannelPolicy  *ChannelPolicy   // optional communication policy overrides
	Heartbeat      *HeartbeatConfig // optional: team leader heartbeat settings
}

// ChannelPolicy describes additive/subtractive communication rules.
type ChannelPolicy struct {
	GroupAllowExtra []string `json:"groupAllowExtra,omitempty"`
	GroupDenyExtra  []string `json:"groupDenyExtra,omitempty"`
	DMAllowExtra    []string `json:"dmAllowExtra,omitempty"`
	DMDenyExtra     []string `json:"dmDenyExtra,omitempty"`
}

// ModelSpec describes LLM parameters for a specific model.
type ModelSpec struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	ContextWindow int      `json:"contextWindow"`
	MaxTokens     int      `json:"maxTokens"`
	Reasoning     bool     `json:"reasoning"`
	Input         []string `json:"input"` // e.g. ["text", "image"]
}

// ManagerRuntimeRequest contains only secret-free desired state for the
// AgentScope Manager runtime document. Runtime credentials are injected into
// the Manager process environment and must never be added here.
type ManagerRuntimeRequest struct {
	ManagerName              string
	Revision                 int64
	ModelName                string
	Skills                   []string
	MCPServers               []v1beta1.MCPServer
	HeartbeatIntervalSeconds int64
	WorkerIdleTimeoutSeconds int64
}

// ManagerRuntimeDocument is the Controller-owned activation document consumed
// by manager-agentscope. Its JSON shape mirrors Python's RuntimeDocument model.
type ManagerRuntimeDocument struct {
	SchemaVersion            int                        `json:"schema_version"`
	Revision                 int64                      `json:"revision"`
	ManagerName              string                     `json:"manager_name"`
	Model                    string                     `json:"model"`
	ContextWindow            int                        `json:"context_window"`
	MaxTokens                int                        `json:"max_tokens"`
	Reasoning                bool                       `json:"reasoning"`
	InputModalities          []string                   `json:"input_modalities"`
	Skills                   []string                   `json:"skills"`
	MCPServers               []ManagerMCPServerDocument `json:"mcp_servers"`
	PromptSources            ManagerPromptSources       `json:"prompt_sources"`
	HeartbeatIntervalSeconds int64                      `json:"heartbeat_interval_seconds"`
	WorkerIdleTimeoutSeconds int64                      `json:"worker_idle_timeout_seconds"`
}

// ManagerMCPServerDocument is the secret-free subset of an MCP declaration.
type ManagerMCPServerDocument struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Transport string `json:"transport"`
}

// ManagerPromptSources declares the object keys that form the Manager's
// ordered system prompt.
type ManagerPromptSources struct {
	Soul      string `json:"soul"`
	Agents    string `json:"agents"`
	Tools     string `json:"tools"`
	Heartbeat string `json:"heartbeat"`
}

// BuiltinMarkers are the delimiters for merge-managed sections in AGENTS.md.
const (
	BuiltinStart  = "<!-- agentteams-builtin-start -->"
	BuiltinEnd    = "<!-- agentteams-builtin-end -->"
	BuiltinHeader = `<!-- agentteams-builtin-start -->
> ⚠️ **DO NOT EDIT** this section. It is managed by AgentTeams and will be automatically
> replaced on upgrade. To customize, add your content **after** the
> ` + "`<!-- agentteams-builtin-end -->`" + ` marker below.
`
)

// SoulTemplateMarkers are the delimiters for the template-managed section in SOUL.md.
const (
	SoulTemplateStart  = "<!-- agentteams-soul-template-start -->"
	SoulTemplateEnd    = "<!-- agentteams-soul-template-end -->"
	SoulTemplateHeader = `<!-- agentteams-soul-template-start -->
> ⚠️ **DO NOT EDIT** this section. It is managed by AgentTeams and will be automatically
> replaced on upgrade. To customize, add your content **after** the
> ` + "`<!-- agentteams-soul-template-end -->`" + ` marker below.
`
)
