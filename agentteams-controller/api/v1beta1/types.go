// +k8s:deepcopy-gen=package

package v1beta1

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	GroupName = "agentteams.io"
	Version   = "v1beta1"
)

// LabelController marks the agentteams-controller instance that owns a CR.
// The value must equal the owning controller's AGENTTEAMS_CONTROLLER_NAME
// environment variable. When set, the controller's informer cache
// filters CR events by this label so multiple controller instances in
// the same namespace do not reconcile each other's resources.
const LabelController = "agentteams.io/controller"

const (
	LabelWorker  = "agentteams.io/worker"
	LabelManager = "agentteams.io/manager"
	LabelRole    = "agentteams.io/role"
	LabelRuntime = "agentteams.io/runtime"
)

// LabelWorkerSvcName records the ClusterIP Service name created for a
// Worker when spec.serviceEnabled is true. Removed when the service is
// disabled or deleted.
const LabelWorkerSvcName = "agentteams.io/worker-svc-name"

// LabelWorkerEdgeUUID records the per-Worker UUID used to identify this
// worker on an external Edge host (Edge DeployMode). The value is stable
// across reconciles so credential issuance and rotation can target the
// same identity on the remote side.
const LabelWorkerEdgeUUID = "agentteams.io/worker-edge-uuid"

// AnnotationEdgeAppliedUUID tracks the UUID that was last used to issue
// an SA token, used for rotation detection. When the current
// LabelWorkerEdgeUUID differs from this annotation, the controller
// re-issues credentials and updates the annotation to match.
const AnnotationEdgeAppliedUUID = "agentteams.io/edge-applied-uuid"

// AnnotationWorkerTeamName records the effective Team identity for a referenced
// Worker so independent Worker reconciles preserve its scoped team storage access.
const AnnotationWorkerTeamName = "agentteams.io/team-name"

// AccessEntry declares one cloud-permission grant under a logical
// service. v1 supported services: "object-storage", "ai-gateway", "ai-registry", "schedulerx3".
//
// Scope is a schema-less JSON blob in the CR layer: it may reference
// logical names (bucketRef: workspace, gatewayRef: default) and
// template variables (${self.name}, ${self.kind}, ${self.namespace}).
// The agentteams-controller resolves it to real resource values before
// calling agentteams-credential-provider; the provider never sees the
// CR-layer form.
//
// AccessEntry is only honored when the controller runs with a
// credential-provider sidecar (gateway.provider=ai-gateway or
// storage.provider=oss). In local higress+minio deployments the
// field is accepted by the CRD but not read by the controller.
type AccessEntry struct {
	Service     string                `json:"service"`
	Permissions []string              `json:"permissions,omitempty"`
	Scope       *apiextensionsv1.JSON `json:"scope,omitempty"`
}

// AgentIdentitySpec carries the non-secret workload identity facts a runtime
// needs to exchange for scoped data-plane credentials.
type AgentIdentitySpec struct {
	WorkloadIdentityName string `json:"workloadIdentityName,omitempty"`
}

// CredentialRef identifies one runtime credential provider without carrying
// the real credential value.
type CredentialRef struct {
	TokenVaultName               string `json:"tokenVaultName,omitempty"`
	APIKeyCredentialProviderName string `json:"apiKeyCredentialProviderName,omitempty"`
}

// CredentialBinding grants a worker-like member access to one referenced
// runtime credential. The value is resolved by the runtime, not by controller.
type CredentialBinding struct {
	CredentialRef CredentialRef `json:"credentialRef"`
	ToolWhitelist []string      `json:"toolWhitelist,omitempty"`
}

// MCPServer declares one MCP server the agent can call. Worker runtimes project
// it into their compatible configuration; the AgentScope Manager consumes the
// same secret-free endpoint descriptor natively.
// URL is the full endpoint (e.g. https://apig.example.com/mcp-servers/github/mcp).
// Transport: "http" (Streamable HTTP, default) | "sse".
//
// Credentials are never stored in this declaration. Runtimes resolve their
// scoped gateway or integration credentials from the process environment.
type MCPServer struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Transport string `json:"transport,omitempty"`
}

// RemoteSkill identifies one skill from a remote source.
// version and label are mutually exclusive; set at most one.
type RemoteSkill struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Label   string `json:"label,omitempty"`
}

// RemoteSkillSource groups remote skills by source and auth mode.
// Source format: nacos://host:port/{namespace-id}
// AuthType values: "nacos" (username:password embedded in source URL as nacos://user:pass@host:port/namespace),
// "sts-agentteams" (STS credential provider), "none" (unauthenticated). Empty auto-detects:
// embedded username/password selects "nacos"; otherwise "none".
type RemoteSkillSource struct {
	Source   string        `json:"source"`
	AuthType string        `json:"authType,omitempty"`
	Skills   []RemoteSkill `json:"skills"`
}

// AgentResourceRequirements declares optional CPU/memory requests and limits
// for one managed agent Pod. Empty fields fall back to controller/backend
// defaults field-by-field.
type AgentResourceRequirements struct {
	Requests AgentResourceValues `json:"requests,omitempty"`
	Limits   AgentResourceValues `json:"limits,omitempty"`
}

// AgentResourceValues holds Kubernetes quantity strings for CPU and memory.
type AgentResourceValues struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// BackendRuntime constants define backend runtime identifiers used by Worker
// specs.
const (
	BackendRuntimePod     = "pod"
	BackendRuntimeSandbox = "sandbox"
)

// DeployMode constants define where the worker pod runs.
const (
	DeployModeLocal  = "Local"
	DeployModeRemote = "Remote"
	DeployModeEdge   = "Edge"
)

// WorkerResourceSpec defines a compact resource shape. CPU and memory are
// applied as both requests and limits where this helper is used.
type WorkerResourceSpec struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

const DefaultWorkerConsolePort = 8088

// WorkerConsoleSpec declares whether a runtime's optional web console should
// be started. The console is disabled when this field is absent or Enabled is
// false. Port zero means DefaultWorkerConsolePort.
type WorkerConsoleSpec struct {
	Enabled bool `json:"enabled"`
	Port    int  `json:"port,omitempty"`
}

// EffectivePort returns the configured console port after applying the
// runtime-independent default.
func (s WorkerConsoleSpec) EffectivePort() int {
	// 逻辑说明：显式端口优先；零值回退统一默认端口，使旧 CR 无需迁移也得到确定配置。
	if s.Port != 0 {
		return s.Port
	}
	return DefaultWorkerConsolePort
}

// Validate checks the console contract against an already-resolved Worker
// runtime. Only CoPaw and QwenPaw currently expose a supported web console.
func (s WorkerConsoleSpec) Validate(runtime string) error {
	// 逻辑说明：先拒绝越界端口；console 未启用时允许其余默认值，启用后再校验有效端口和受支持 runtime。
	if s.Port < 0 || s.Port > 65535 {
		return fmt.Errorf("worker console port must be between 1 and 65535")
	}
	if !s.Enabled {
		return nil
	}
	if s.EffectivePort() < 1 || s.EffectivePort() > 65535 {
		return fmt.Errorf("worker console port must be between 1 and 65535")
	}
	if runtime != "copaw" && runtime != "qwenpaw" {
		return fmt.Errorf("worker console is not supported by runtime %q", runtime)
	}
	return nil
}

// Worker volume provider constants.
const (
	WorkerVolumeTypeOSS = "OSS"
)

// +genclient
// +kubebuilder:subresource:status
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Worker represents an AI agent worker in AgentTeams.
// Worker 表示一个可独立运行的 AI Agent。Spec 是用户期望，Status
// 是 Controller 对 Matrix 身份、房间和容器实际状态的观察结果。
type Worker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkerSpec   `json:"spec"`
	Status            WorkerStatus `json:"status,omitempty"`
}

// WorkerSpec 声明 Worker 应该使用的模型、runtime、配置和运行状态。
// 它不应包含 Matrix token、模型 API key 等真实密钥；这些值由
// Controller 在部署边界注入，否则会进入 Kubernetes API 的普通 CR 数据。
type WorkerSpec struct {
	Model         string                     `json:"model"`
	ModelProvider string                     `json:"modelProvider,omitempty"` // APIG Model API name for per-worker LLM provider
	Runtime       string                     `json:"runtime,omitempty"`       // openclaw | copaw | hermes | qwenpaw (default: openclaw)
	Image         string                     `json:"image,omitempty"`         // custom Docker image
	WorkerName    string                     `json:"workerName,omitempty"`    // business/runtime identity (Matrix localpart, OSS path key)
	Identity      string                     `json:"identity,omitempty"`
	Soul          string                     `json:"soul,omitempty"`
	Agents        string                     `json:"agents,omitempty"`
	Skills        []string                   `json:"skills,omitempty"`       // built-in skills only
	RemoteSkills  []RemoteSkillSource        `json:"remoteSkills,omitempty"` // remote skills from source registries
	McpServers    []MCPServer                `json:"mcpServers,omitempty"`
	Package       string                     `json:"package,omitempty"` // file://, http(s)://, or nacos://[user:pass@]host:port/...; optional ?authType=nacos|sts-agentteams|none
	Expose        []ExposePort               `json:"expose,omitempty"`  // ports to expose via Higress gateway
	Console       *WorkerConsoleSpec         `json:"console,omitempty"` // optional CoPaw/QwenPaw web console
	ChannelPolicy *ChannelPolicySpec         `json:"channelPolicy,omitempty"`
	Channels      *ChannelsSpec              `json:"channels,omitempty"`
	Resources     *AgentResourceRequirements `json:"resources,omitempty"`
	IdleTimeout   string                     `json:"idleTimeout,omitempty"`

	// ContainerManaged indicates whether the controller should manage
	// container lifecycle for this worker. When false, container
	// reconciliation is skipped entirely (for remote/pip workers).
	// Default is true (controller manages container).
	ContainerManaged *bool `json:"containerManaged,omitempty"`

	// State is the desired lifecycle state of the worker.
	// Valid values: "Running" (default), "Sleeping", "Stopped".
	// The controller reconciles actual backend state toward this desired state.
	State *string `json:"state,omitempty"`

	// AccessEntries declares the cloud permissions this worker should be
	// granted via agentteams-credential-provider. See AccessEntry for semantics.
	// When empty the controller applies a sensible default (object-storage
	// scoped to agents/<name>/* and shared/*).
	AccessEntries []AccessEntry `json:"accessEntries,omitempty"`

	// AgentIdentity carries non-secret workload identity metadata used by
	// managed runtimes when resolving runtime credential bindings.
	AgentIdentity *AgentIdentitySpec `json:"agentIdentity,omitempty"`

	// CredentialBindings declares credential references available to the
	// worker runtime. Bindings never contain real credential values and are
	// intentionally separate from Env, which is container-global.
	CredentialBindings []CredentialBinding `json:"credentialBindings,omitempty"`

	// DeployMode specifies where the worker pod runs.
	// "Local" (default): created in the controller's own cluster.
	// "Edge": externally hosted outside the controller's managed pod path.
	DeployMode *string `json:"deployMode,omitempty"`

	// ServiceEnabled controls whether a ClusterIP Service is created
	// alongside the worker pod (same cluster, namespace, name).
	ServiceEnabled *bool `json:"serviceEnabled,omitempty"`

	// Env holds user-defined environment variables injected into the worker
	// container. Keys that collide with variables already set by the
	// controller or backend (AGENTTEAMS_*, OPENCLAW_*, HOME, and similar
	// internal keys) are silently ignored with a warning log — the system
	// value always wins.
	Env map[string]string `json:"env,omitempty"`

	// BackendRuntime specifies the container runtime backend for this worker.
	// "pod" (default): creates a standard Kubernetes Pod.
	// Only effective in incluster mode; ignored in embedded (Docker) mode.
	BackendRuntime *string `json:"backendRuntime,omitempty"`

	// Labels are user-defined Pod labels stamped onto the worker Pod.
	// Merged under the four-layer priority order (see controller docs):
	// pod-template < CR metadata.labels < CR spec.labels < controller
	// system labels. Entries whose keys collide with controller-forced
	// system labels (agentteams.io/controller, agentteams.io/worker, etc.) are
	// silently overridden. Must carry the omitempty tag so Teams that
	// embed WorkerSpec-shaped hashes keep a stable spec hash when the
	// field is absent.
	Labels map[string]string `json:"labels,omitempty"`

	// Volumes is reserved for runtimes that provide custom external storage
	// mounts. It is not supported by the open-source pod backend.
	Volumes []WorkerVolumeSpec `json:"volumes,omitempty"`

	// Mounts is reserved for runtimes that provide custom dynamic mounts. It is
	// not supported by the open-source pod backend.
	Mounts []WorkerMountSpec `json:"mounts,omitempty"`
}

// WorkerVolumeSpec 声明 Worker 要使用的一个具名外部卷。
type WorkerVolumeSpec struct {
	Name string               `json:"name"`
	Type string               `json:"type"` // OSS
	OSS  *WorkerOSSVolumeSpec `json:"oss,omitempty"`
}

// WorkerMountSpec 把已声明卷的子路径挂载到 Agent 容器的目标路径。
type WorkerMountSpec struct {
	Name      string `json:"name"`
	VolumeRef string `json:"volumeRef"`
	SubPath   string `json:"subPath"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly"`
}

// WorkerOSSVolumeSpec 描述 OSS bucket、endpoint 和挂载认证方式。
type WorkerOSSVolumeSpec struct {
	Bucket   string            `json:"bucket,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
	Auth     WorkerOSSAuthSpec `json:"auth,omitempty"`
}

// WorkerOSSAuthSpec 在 RRSA 与 Secret 引用形式的 AccessKey 认证中二选一。
type WorkerOSSAuthSpec struct {
	Type      string                   `json:"type,omitempty"`
	RRSA      *WorkerOSSRRSASpec       `json:"rrsa,omitempty"`
	AccessKey *WorkerAccessKeyAuthSpec `json:"accessKey,omitempty"`
}

// WorkerOSSRRSASpec 声明通过工作负载身份换取 OSS 权限的角色信息。
type WorkerOSSRRSASpec struct {
	RoleName string `json:"roleName,omitempty"`
	RoleARN  string `json:"roleArn,omitempty"`
}

// WorkerAccessKeyAuthSpec 引用目标集群中供 CSI driver 读取的密钥 Secret。
type WorkerAccessKeyAuthSpec struct {
	// SecretRef names the target-cluster Secret used by the CSI driver for
	// mounting this OSS volume. The controller does not read this Secret when
	// writing worker-deps objects; those are written through the main AgentTeams
	// workspace OSS client.
	SecretRef NamespacedSecretRef `json:"secretRef,omitempty"`
}

// NamespacedSecretRef 引用 Kubernetes Secret，但不包含 Secret 中的真实值。
type NamespacedSecretRef struct {
	Name string `json:"name,omitempty"`
	// Namespace must be omitted for worker OSS AccessKey auth. The mount
	// Secret is resolved in the worker targetNamespace.
	Namespace string `json:"namespace,omitempty"`
}

// GetBackendRuntime returns the explicitly set backendRuntime from spec, or empty string
// if not set. Empty means "use cluster-level default from AGENTTEAMS_WORKER_BACKEND_RUNTIME".
func (s WorkerSpec) GetBackendRuntime() string {
	// 逻辑说明：仅返回 CR 显式设置的非空 backend；空字符串保留给集群级默认值解析。
	if s.BackendRuntime != nil && *s.BackendRuntime != "" {
		return *s.BackendRuntime
	}
	return ""
}

// DesiredContainerMan returns the effective desired containerManaged, defaulting to true.
func (s WorkerSpec) DesiredContainerMan() bool {
	// 逻辑说明：指针区分“未设置”和显式 false；未设置按向后兼容规则由 Controller 管理容器。
	if s.ContainerManaged != nil {
		return *s.ContainerManaged
	}
	return true
}

// DesiredState returns the effective desired state, defaulting to "Running".
func (s WorkerSpec) DesiredState() string {
	// 逻辑说明：显式非空状态优先；旧 CR 的 nil/空状态统一解释为 Running。
	if s.State != nil && *s.State != "" {
		return *s.State
	}
	return "Running"
}

// EffectiveWorkerName returns the runtime identity key for a Worker.
// Empty WorkerName falls back to metadata.name supplied by caller.
func (s WorkerSpec) EffectiveWorkerName(metadataName string) string {
	// 逻辑说明：业务身份显式配置时使用 WorkerName，否则回退不可缺失的 Kubernetes metadata.name。
	if s.WorkerName != "" {
		return s.WorkerName
	}
	return metadataName
}

// ExposePort defines a container port to expose via the Higress gateway.
type ExposePort struct {
	Port     int    `json:"port"`
	Protocol string `json:"protocol,omitempty"` // http (default) | grpc
}

// ChannelPolicySpec defines additive/subtractive overrides on top of default
// communication policies. Values are Matrix user IDs (@user:domain) or
// short usernames (auto-resolved to full IDs by config generation scripts).
type ChannelPolicySpec struct {
	GroupAllowExtra []string `json:"groupAllowExtra,omitempty"`
	GroupDenyExtra  []string `json:"groupDenyExtra,omitempty"`
	DmAllowExtra    []string `json:"dmAllowExtra,omitempty"`
	DmDenyExtra     []string `json:"dmDenyExtra,omitempty"`
}

// ChannelsSpec 集中声明 Worker 除 Matrix 之外的可选消息通道。
type ChannelsSpec struct {
	DingTalk *DingTalkChannelSpec `json:"dingtalk,omitempty"`
}

// DingTalkChannelSpec 声明钉钉机器人通道的期望行为与连接参数。
// 生产部署中敏感值应由 Secret/环境注入，不应直接写入普通 CR。
type DingTalkChannelSpec struct {
	Enabled          *bool  `json:"enabled"`
	ClientID         string `json:"clientId,omitempty"`
	ClientSecret     string `json:"clientSecret,omitempty"`
	RobotCode        string `json:"robotCode,omitempty"`
	ShowThinking     bool   `json:"showThinking,omitempty"`
	ShowToolCalls    bool   `json:"showToolCalls,omitempty"`
	StreamingEnabled bool   `json:"streamingEnabled,omitempty"`
	MessageType      string `json:"messageType,omitempty"`
	CardTemplateID   string `json:"cardTemplateId,omitempty"`
}

// WorkerStatus 记录 Controller 已观察到的 Worker 实际状态。
// ObservedGeneration 等于 metadata.generation 时，表示当前 spec 已完成一次
// 无错误的 reconcile；不相等时，即使 Phase 仍显示 Running，也可能只是
// 上一版 spec 的状态。
type WorkerStatus struct {
	ObservedGeneration int64               `json:"observedGeneration,omitempty"`
	SpecHash           string              `json:"specHash,omitempty"`
	Phase              string              `json:"phase,omitempty"` // Pending/Running/Sleeping/Failed
	MatrixUserID       string              `json:"matrixUserID,omitempty"`
	RoomID             string              `json:"roomID,omitempty"`
	ContainerState     string              `json:"containerState,omitempty"`
	LastHeartbeat      string              `json:"lastHeartbeat,omitempty"`
	LastActiveAt       string              `json:"lastActiveAt,omitempty"`
	Message            string              `json:"message,omitempty"`
	ExposedPorts       []ExposedPortStatus `json:"exposedPorts,omitempty"`

	// BackendRuntime records the backend type currently used for this worker's container.
	// Set after successful creation or backend switch.
	// Values: "pod" (default), or "" before the first successful deployment.
	// Only meaningful in incluster mode; Docker mode leaves this empty.
	BackendRuntime string `json:"backendRuntime,omitempty"`

	// DeployMode records where the current backend resource was actually
	// provisioned.
	DeployMode string `json:"deployMode,omitempty"`
}

// ExposedPortStatus records a port that has been exposed via Higress.
type ExposedPortStatus struct {
	Port   int    `json:"port"`
	Domain string `json:"domain"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WorkerList 是 Kubernetes API 返回多个 Worker 时使用的列表类型。
type WorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Worker `json:"items"`
}

// +genclient
// +kubebuilder:subresource:status
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Team represents a group of workers led by a Team Leader.
// Team 表示由一个 Team Leader 和多个 Worker 组成的协作单元。
// Team 通过名称引用已存在的 Worker CR，不再内嵌第二份 Worker 期望状态。
type Team struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TeamSpec   `json:"spec"`
	Status            TeamStatus `json:"status,omitempty"`
}

// TeamSpec 声明团队成员、角色、人类管理员与通信策略。
// WorkerMembers 中的名称指向 Worker.metadata.name，TeamReconciler 会读取
// 被引用的 Worker spec，但它们的独立 WorkerReconciler 不再部署团队成员。
type TeamSpec struct {
	Description  string           `json:"description,omitempty"`
	TeamName     string           `json:"teamName,omitempty"`
	Admin        *TeamAdminSpec   `json:"admin,omitempty"`
	HumanMembers []TeamMemberSpec `json:"humanMembers,omitempty"`

	// WorkerMembers references existing Worker CRs as team members.
	// The TeamReconciler validates membership, provisions rooms, injects
	// runtime context, and aggregates member status from these references.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	WorkerMembers []TeamWorkerRef `json:"workerMembers,omitempty"`

	PeerMentions  *bool              `json:"peerMentions,omitempty"`  // default true
	ChannelPolicy *ChannelPolicySpec `json:"channelPolicy,omitempty"` // team-wide overrides

	// HeartbeatEvery configures the Team Leader agent's periodic heartbeat
	// check interval. The TeamReconciler writes this value into the leader
	// Worker's openclaw.json and coordination context AGENTS.md.
	// Example: "30m". Empty means leader heartbeat is disabled.
	HeartbeatEvery string `json:"heartbeatEvery,omitempty"`
}

// TeamWorkerRef references an existing Worker CR as a team member.
type TeamWorkerRef struct {
	// Name is the metadata.name of the referenced Worker CR.
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// Role is this member's role within the team: "team_leader" or "worker".
	// +kubebuilder:validation:Enum=team_leader;worker
	Role string `json:"role"`
}

// EffectiveTeamName 返回对外系统使用的团队身份名。显式 TeamName
// 为空时回退到 Kubernetes metadata.name，使老 CR 仍有稳定的身份键。
func (s TeamSpec) EffectiveTeamName(metadataName string) string {
	// 逻辑说明：显式团队身份优先，旧对象没有 TeamName 时回退 metadata.name 保持跨系统键稳定。
	if s.TeamName != "" {
		return s.TeamName
	}
	return metadataName
}

// TeamAdminSpec 引用团队的人类管理员及其可选 Matrix user ID。
type TeamAdminSpec struct {
	Name         string `json:"name"`
	MatrixUserID string `json:"matrixUserId,omitempty"`
}

// TeamMemberSpec 声明一个可进入团队房间的人类成员及其角色。
type TeamMemberSpec struct {
	Name         string `json:"name"`
	MatrixUserID string `json:"matrixUserId,omitempty"`
	Role         string `json:"role,omitempty"` // coordinator (default)
}

// TeamStatus 聚合团队房间与每个成员的实际状态。
// 这是观察值而不是任务计划；用户要修改成员时应写 Spec.WorkerMembers，
// 由 Controller 重新计算并覆盖 Status.Members。
type TeamStatus struct {
	Phase          string `json:"phase,omitempty"` // Pending/Active/Degraded/Failed
	TeamRoomID     string `json:"teamRoomID,omitempty"`
	LeaderDMRoomID string `json:"leaderDMRoomID,omitempty"`
	LeaderReady    bool   `json:"leaderReady,omitempty"`
	ReadyWorkers   int    `json:"readyWorkers,omitempty"`
	TotalWorkers   int    `json:"totalWorkers,omitempty"`
	Message        string `json:"message,omitempty"`
	// Members carries per-member state (one entry per leader + worker).
	// TeamReconciler sorts the slice by Name for stable status patches and
	// deterministic test assertions.
	//
	// This slice replaces the previous ObservedMembers / MemberSpecHashes /
	// WorkerExposedPorts trio — each of which maintained its own stale-
	// cleanup loop and contributed independent patch churn. Consolidating
	// them here means adding a new per-member field costs one struct field
	// (vs one status field + one map + one cleanup loop + one consumer).
	Members []TeamMemberStatus `json:"members,omitempty"`
}

// MemberByName returns a pointer to the TeamMemberStatus entry for name,
// or nil when no such member has been recorded. Callers that need to
// create-on-absent must use the controller-package memberStatus helper
// instead — we keep creation out of the API types to avoid accidental
// mutation from API response codepaths.
func (s *TeamStatus) MemberByName(name string) *TeamMemberStatus {
	// 逻辑说明：线性查找并返回切片内真实元素指针，调用方修改时作用于状态本身；未命中不隐式创建。
	for i := range s.Members {
		if s.Members[i].Name == name {
			return &s.Members[i]
		}
	}
	return nil
}

// TeamMemberStatus captures all per-member state for one team member
// (leader or worker). Collects the fields that previously lived in the
// scattered ObservedMembers / MemberSpecHashes / WorkerExposedPorts maps.
type TeamMemberStatus struct {
	// Name is the member's canonical Worker CR name from
	// Team.Spec.WorkerMembers. Uniquely identifies the entry within
	// Team.Status.Members.
	Name string `json:"name"`
	// RuntimeName is the member's runtime identity key (Matrix localpart,
	// OSS path key, room alias key). Empty falls back to Name.
	RuntimeName string `json:"runtimeName,omitempty"`
	// Role is "team_leader" or "worker". Mirrors MemberContext.Role and the
	// synthesized WorkerResponse.Role exposed via /api/v1/workers/<name>.
	Role string `json:"role,omitempty"`
	// RoomID is the member's personal communication room with the Manager —
	// same semantic as Worker.Status.RoomID for standalone workers. Distinct
	// from Team.Status.TeamRoomID (shared team room) and
	// Team.Status.LeaderDMRoomID (Leader↔Admin DM). Consumers reading this
	// include the AgentTeams CLI (`agt get workers <name> -o json | jq .roomID`)
	// and the Manager Agent when it needs to target a specific member.
	RoomID string `json:"roomID,omitempty"`
	// MatrixUserID is the member's Matrix MXID. Populated by
	// ReconcileMemberInfra alongside RoomID.
	MatrixUserID string `json:"matrixUserID,omitempty"`
	// SpecHash mirrors the referenced Worker.Status.SpecHash after status
	// aggregation so Team consumers can inspect the member runtime revision.
	SpecHash string `json:"specHash,omitempty"`
	// Observed flips to true the instant ReconcileMemberInfra succeeds and
	// stays true even if later phases fail.
	Observed bool `json:"observed,omitempty"`
	// Ready mirrors backend.Status ∈ {Running, Ready}, re-evaluated by
	// summarizeBackendReadiness on each reconcile pass. Aggregates into
	// Team.Status.LeaderReady and Team.Status.ReadyWorkers.
	Ready bool `json:"ready,omitempty"`
	// Phase is the member lifecycle phase: Pending, Starting, Running,
	// Updating, Stopping, Sleeping, Stopped, Failed.
	Phase string `json:"phase,omitempty"`
	// ContainerState is the raw backend container status.
	ContainerState string `json:"containerState,omitempty"`
	// Message holds per-member error detail from reconcile. Cleared on success.
	Message string `json:"message,omitempty"`
	// LastActiveAt is the latest runtime-reported business activity time.
	LastActiveAt string `json:"lastActiveAt,omitempty"`
	// LastHeartbeat is the latest heartbeat timestamp for this member.
	LastHeartbeat string `json:"lastHeartbeat,omitempty"`
	// ExposedPorts records the ports currently exposed via Higress for this
	// member. Leader members never expose ports (this field stays nil).
	ExposedPorts []ExposedPortStatus `json:"exposedPorts,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TeamList 是 Kubernetes API 返回多个 Team 时使用的列表类型。
type TeamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Team `json:"items"`
}

// +genclient
// +kubebuilder:subresource:status
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Human represents a real human user with configurable access permissions.
// Human 表示一个真实人类用户及其可访问的 Team/Worker。
// Human 没有 Agent 容器；Controller 只对其 Matrix 身份和房间成员关系做收敛。
type Human struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              HumanSpec   `json:"spec"`
	Status            HumanStatus `json:"status,omitempty"`
}

// HumanSpec 声明人类用户的展示身份、身份来源与可访问资源。
// AccessibleTeams/Workers 是期望权限集合；从列表移除一项后，
// reconcile 会把该用户从对应 Matrix 房间移除。
type HumanSpec struct {
	DisplayName       string              `json:"displayName"`
	Username          string              `json:"username,omitempty"`
	Email             string              `json:"email,omitempty"`
	PermissionLevel   int                 `json:"permissionLevel"` // 1=Admin, 2=Team, 3=Worker
	AccessibleTeams   []string            `json:"accessibleTeams,omitempty"`
	AccessibleWorkers []string            `json:"accessibleWorkers,omitempty"`
	IdentitySource    *IdentitySourceSpec `json:"identitySource,omitempty"`
	Note              string              `json:"note,omitempty"`
}

// IdentitySourceSpec 用稳定 issuer/subject 对标外部 SSO 中的人类身份。
type IdentitySourceSpec struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}

// HumanStatus 记录已创建的 Matrix user ID 和已同步房间。
// InitialPassword 是为兼容旧密码流程保留的一次性交付字段，不应当作
// 长期凭据来源；外部 SSO 身份不在这里保存密码。
type HumanStatus struct {
	Phase                       string   `json:"phase,omitempty"` // Pending/Active/Failed/Degraded
	MatrixUserID                string   `json:"matrixUserID,omitempty"`
	InitialPassword             string   `json:"initialPassword,omitempty"` // Set on creation, shown once
	DisplayNameSyncedGeneration int64    `json:"displayNameSyncedGeneration,omitempty"`
	Rooms                       []string `json:"rooms,omitempty"`
	EmailSent                   bool     `json:"emailSent,omitempty"`
	Message                     string   `json:"message,omitempty"`
}

// EffectiveUsername returns the Matrix localpart for a Human.
// Empty username falls back to metadata.name supplied by caller.
func (s HumanSpec) EffectiveUsername(metadataName string) string {
	// 逻辑说明：显式 Matrix localpart 优先；未配置时以资源名作为兼容且稳定的登录身份。
	if s.Username != "" {
		return s.Username
	}
	return metadataName
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HumanList 是 Kubernetes API 返回多个 Human 时使用的列表类型。
type HumanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Human `json:"items"`
}

// +genclient
// +kubebuilder:subresource:status
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Manager represents the AgentTeams Manager Agent — the coordinator that receives
// natural-language instructions from Admin and orchestrates Workers/Teams via
// the agt CLI / Controller REST API.
// Manager 表示 AgentTeams 中接收管理员自然语言指令的 AgentScope 管家。
// Manager 通过 agt CLI/Controller REST API 管理 Worker 与 Team，不直接更改
// Kubernetes、Matrix 或 Higress 底层状态。
type Manager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ManagerSpec   `json:"spec"`
	Status            ManagerStatus `json:"status,omitempty"`
}

// ManagerSpec 声明 AgentScope Manager 的模型、提示词来源、技能与生命周期。
// Runtime 只支持 agentscope；Worker 的 openclaw/copaw/hermes/qwenpaw 是独立选择，
// 不是 Manager 启动分支。
type ManagerSpec struct {
	Model         string                     `json:"model"`
	ModelProvider string                     `json:"modelProvider,omitempty"` // APIG Model API name for per-manager LLM provider
	Runtime       string                     `json:"runtime,omitempty"`       // agentscope (the only Manager runtime)
	Image         string                     `json:"image,omitempty"`         // custom Docker image
	Soul          string                     `json:"soul,omitempty"`          // custom SOUL.md content
	Identity      string                     `json:"identity,omitempty"`      // managed Identity & Personality section
	Agents        string                     `json:"agents,omitempty"`        // custom AGENTS.md content
	Skills        []string                   `json:"skills,omitempty"`        // retained Manager skills advertised in the active revision
	McpServers    []MCPServer                `json:"mcpServers,omitempty"`    // MCP servers callable natively through AgentScope
	Package       string                     `json:"package,omitempty"`       // file://, http(s)://, or nacos://; optional ?authType= for Nacos
	Config        ManagerConfig              `json:"config,omitempty"`
	Resources     *AgentResourceRequirements `json:"resources,omitempty"`
	CodingCLI     *ManagerCodingCLISpec      `json:"codingCLI,omitempty"`

	// State is the desired lifecycle state of the manager.
	// Valid values: "Running" (default), "Sleeping", "Stopped".
	// The controller reconciles actual backend state toward this desired state.
	State *string `json:"state,omitempty"`

	// AccessEntries declares the cloud permissions this manager should be
	// granted via agentteams-credential-provider. See AccessEntry for semantics.
	// When empty the controller applies a sensible default (object-storage
	// scoped to agents/<name>/*, shared/*, and manager/*).
	AccessEntries []AccessEntry `json:"accessEntries,omitempty"`

	// Env holds user-defined environment variables injected into the
	// manager container. See WorkerSpec.Env for the collision policy.
	Env map[string]string `json:"env,omitempty"`

	// Labels are user-defined Pod labels stamped onto the manager Pod.
	// Merged under the four-layer priority order (see WorkerSpec.Labels
	// godoc): pod-template < CR metadata.labels < CR spec.labels <
	// controller system labels.
	Labels map[string]string `json:"labels,omitempty"`
}

const (
	DefaultManagerCodingCLIMountPath      = "/opt/agentteams/coding-cli"
	DefaultManagerCodingCLITimeoutSeconds = 600
	DefaultManagerCodingCLIMaxOutputBytes = 64 * 1024
)

// ManagerCodingCLISpec declares the optional, closed coding-CLI execution
// boundary used by the AgentScope Manager. The base image contains no vendor
// CLI; operators either mount a read-only directory or provide a derived image.
type ManagerCodingCLISpec struct {
	Enabled          bool     `json:"enabled"`
	Providers        []string `json:"providers,omitempty"`
	HostPath         string   `json:"hostPath,omitempty"`
	MountPath        string   `json:"mountPath,omitempty"`
	TrustedDirectory string   `json:"trustedDirectory,omitempty"`
	TimeoutSeconds   int      `json:"timeoutSeconds,omitempty"`
	MaxOutputBytes   int      `json:"maxOutputBytes,omitempty"`
}

// EffectiveMountPath 返回 coding CLI 在 Manager 容器内的挂载路径。
func (s ManagerCodingCLISpec) EffectiveMountPath() string {
	// 逻辑说明：清理显式容器路径；空值使用只读 coding CLI 的固定默认挂载根。
	if s.MountPath != "" {
		return path.Clean(s.MountPath)
	}
	return DefaultManagerCodingCLIMountPath
}

// EffectiveTrustedDirectory 返回允许 Manager 查找 CLI 可执行文件的受信任目录。
func (s ManagerCodingCLISpec) EffectiveTrustedDirectory() string {
	// 逻辑说明：显式可信目录先清理；未设置时限定为有效挂载路径下的 bin 子目录。
	if s.TrustedDirectory != "" {
		return path.Clean(s.TrustedDirectory)
	}
	return path.Join(s.EffectiveMountPath(), "bin")
}

// EffectiveTimeoutSeconds 返回单次 coding CLI 进程的超时上限。
func (s ManagerCodingCLISpec) EffectiveTimeoutSeconds() int {
	// 逻辑说明：非零显式上限优先，否则使用受控默认值，避免进程无限运行。
	if s.TimeoutSeconds != 0 {
		return s.TimeoutSeconds
	}
	return DefaultManagerCodingCLITimeoutSeconds
}

// EffectiveMaxOutputBytes 返回允许带回 Manager 上下文的最大输出字节数。
func (s ManagerCodingCLISpec) EffectiveMaxOutputBytes() int {
	// 逻辑说明：非零显式限制优先，否则使用默认字节上限，防止 CLI 输出挤占 Manager 上下文。
	if s.MaxOutputBytes != 0 {
		return s.MaxOutputBytes
	}
	return DefaultManagerCodingCLIMaxOutputBytes
}

// Validate rejects implicit executables, unsafe paths, and unbounded process
// settings before the reconciler creates a Manager container.
func (s ManagerCodingCLISpec) Validate() error {
	// 逻辑说明：联合校验 provider、路径包含关系、超时与输出上限，容器创建前拒绝隐式可执行文件和越界配置。
	if s.Enabled && len(s.Providers) == 0 {
		return fmt.Errorf("enabled Manager coding CLI requires at least one provider")
	}
	seen := make(map[string]struct{}, len(s.Providers))
	for _, provider := range s.Providers {
		switch provider {
		case "claude", "gemini", "qodercli":
		default:
			return fmt.Errorf("unsupported Manager coding CLI provider %q", provider)
		}
		if _, exists := seen[provider]; exists {
			return fmt.Errorf("duplicate Manager coding CLI provider %q", provider)
		}
		seen[provider] = struct{}{}
	}
	if s.HostPath != "" &&
		!path.IsAbs(s.HostPath) &&
		!filepath.IsAbs(s.HostPath) {
		return fmt.Errorf("Manager coding CLI hostPath must be absolute")
	}
	mountPath := s.EffectiveMountPath()
	if !path.IsAbs(mountPath) {
		return fmt.Errorf("Manager coding CLI mountPath must be absolute")
	}
	trustedDirectory := s.EffectiveTrustedDirectory()
	if !path.IsAbs(trustedDirectory) {
		return fmt.Errorf("Manager coding CLI trustedDirectory must be absolute")
	}
	if s.HostPath != "" &&
		trustedDirectory != mountPath &&
		!strings.HasPrefix(trustedDirectory, strings.TrimRight(mountPath, "/")+"/") {
		return fmt.Errorf("Manager coding CLI trustedDirectory must be inside mountPath")
	}
	timeout := s.EffectiveTimeoutSeconds()
	if timeout < 1 || timeout > 3600 {
		return fmt.Errorf("Manager coding CLI timeoutSeconds must be between 1 and 3600")
	}
	maxOutput := s.EffectiveMaxOutputBytes()
	if maxOutput < 1024 || maxOutput > 1024*1024 {
		return fmt.Errorf("Manager coding CLI maxOutputBytes must be between 1024 and 1048576")
	}
	return nil
}

// DesiredState returns the effective desired state, defaulting to "Running".
func (s ManagerSpec) DesiredState() string {
	// 逻辑说明：显式非空生命周期状态优先；未设置的旧 Manager 仍默认保持 Running。
	if s.State != nil && *s.State != "" {
		return *s.State
	}
	return "Running"
}

// ManagerConfig 包含不属于模型或容器的 Manager 行为参数。
type ManagerConfig struct {
	HeartbeatInterval string `json:"heartbeatInterval,omitempty"` // default: 30m
	WorkerIdleTimeout string `json:"workerIdleTimeout,omitempty"` // default: 12h
	NotifyChannel     string `json:"notifyChannel,omitempty"`     // default: admin-dm
}

// ManagerStatus 记录 Manager 的 Matrix 身份、管理员直聊房间与运行状态。
// SpecHash/ObservedGeneration 用来区分“新期望已经部署”和“页面仍显示旧容器
// 的 Running”。WelcomeSent 则是一次性欢迎消息的幂等标记。
type ManagerStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	SpecHash           string `json:"specHash,omitempty"`
	Phase              string `json:"phase,omitempty"` // Pending/Running/Updating/Failed
	MatrixUserID       string `json:"matrixUserID,omitempty"`
	RoomID             string `json:"roomID,omitempty"` // Admin DM room
	ContainerState     string `json:"containerState,omitempty"`
	Version            string `json:"version,omitempty"`
	Message            string `json:"message,omitempty"`

	// WelcomeSent records whether the controller has already delivered the
	// first-boot onboarding prompt to the Admin DM room. Used as the
	// idempotency guard for reconcileManagerWelcome — once true the
	// controller will not re-send even if the Manager container is recreated.
	// Completion of the Q&A is represented by spec.identity, which the typed
	// Manager workflow updates after Admin confirmation.
	WelcomeSent bool `json:"welcomeSent,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ManagerList 是 Kubernetes API 返回多个 Manager 时使用的列表类型。
type ManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Manager `json:"items"`
}
