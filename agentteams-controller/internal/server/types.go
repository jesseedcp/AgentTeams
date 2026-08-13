package server

import v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"

// --- Worker API types ---

// CreateWorkerRequest 是 REST API 接受的 Worker 创建输入，之后会被显式
// 转换为 v1beta1.WorkerSpec。与 CR 类型分开能避免客户端写入 status、
// resourceVersion 或 Controller 管理的 metadata。
type CreateWorkerRequest struct {
	Name          string                             `json:"name"`
	WorkerName    string                             `json:"workerName,omitempty"`
	Model         string                             `json:"model,omitempty"`
	ModelProvider string                             `json:"modelProvider,omitempty"`
	Runtime       string                             `json:"runtime,omitempty"`
	Image         string                             `json:"image,omitempty"`
	Identity      string                             `json:"identity,omitempty"`
	Soul          string                             `json:"soul,omitempty"`
	Agents        string                             `json:"agents,omitempty"`
	Skills        []string                           `json:"skills,omitempty"`
	McpServers    []v1beta1.MCPServer                `json:"mcpServers,omitempty"`
	Package       string                             `json:"package,omitempty"`
	Expose        []v1beta1.ExposePort               `json:"expose,omitempty"`
	Console       *v1beta1.WorkerConsoleSpec         `json:"console,omitempty"`
	ChannelPolicy *v1beta1.ChannelPolicySpec         `json:"channelPolicy,omitempty"`
	Resources     *v1beta1.AgentResourceRequirements `json:"resources,omitempty"`

	// ContainerManaged indicates whether the controller should manage
	// container lifecycle for this worker. When false, container
	// reconciliation is skipped entirely (for remote/pip workers).
	// Default is true (controller manages container).
	ContainerManaged *bool   `json:"containerManaged,omitempty"`
	State            *string `json:"state,omitempty"` // desired lifecycle state: Running, Sleeping, Stopped

}

// UpdateWorkerRequest 使用指针区分“客户端未提供字段”与“显式设为零值”。
// 如果把它们换成普通 string/bool，部分更新会无法判断某个空字符串
// 是要清空配置，还是应保留旧值。
type UpdateWorkerRequest struct {
	WorkerName    *string                            `json:"workerName,omitempty"`
	Model         *string                            `json:"model,omitempty"`
	ModelProvider *string                            `json:"modelProvider,omitempty"`
	Runtime       *string                            `json:"runtime,omitempty"`
	Image         *string                            `json:"image,omitempty"`
	Identity      *string                            `json:"identity,omitempty"`
	Soul          *string                            `json:"soul,omitempty"`
	Agents        *string                            `json:"agents,omitempty"`
	Skills        *[]string                          `json:"skills,omitempty"`
	McpServers    *[]v1beta1.MCPServer               `json:"mcpServers,omitempty"`
	Package       *string                            `json:"package,omitempty"`
	Expose        *[]v1beta1.ExposePort              `json:"expose,omitempty"`
	Console       *v1beta1.WorkerConsoleSpec         `json:"console,omitempty"`
	ChannelPolicy *v1beta1.ChannelPolicySpec         `json:"channelPolicy,omitempty"`
	Resources     *v1beta1.AgentResourceRequirements `json:"resources,omitempty"`

	// ContainerManaged indicates whether the controller should manage
	// container lifecycle for this worker. When false, container
	// reconciliation is skipped entirely (for remote/pip workers).
	// Default is true (controller manages container).
	ContainerManaged *bool   `json:"containerManaged,omitempty"`
	State            *string `json:"state,omitempty"` // desired lifecycle state: Running, Sleeping, Stopped
}

// WorkerResponse 合并 Worker spec、status 与团队角色，为 CLI/Manager 提供
// 稳定、无 Kubernetes 内部字段的 JSON 视图。
type WorkerResponse struct {
	Name             string                     `json:"name"`
	WorkerName       string                     `json:"workerName,omitempty"`
	Phase            string                     `json:"phase"`
	ContainerManaged bool                       `json:"containerManaged"`
	State            string                     `json:"state,omitempty"` // desired lifecycle state
	Model            string                     `json:"model,omitempty"`
	Runtime          string                     `json:"runtime,omitempty"`
	Image            string                     `json:"image,omitempty"`
	Identity         string                     `json:"identity,omitempty"`
	Soul             string                     `json:"soul,omitempty"`
	Agents           string                     `json:"agents,omitempty"`
	Skills           []string                   `json:"skills,omitempty"`
	McpServers       []v1beta1.MCPServer        `json:"mcpServers,omitempty"`
	Package          string                     `json:"package,omitempty"`
	Expose           []v1beta1.ExposePort       `json:"expose,omitempty"`
	Console          *v1beta1.WorkerConsoleSpec `json:"console,omitempty"`
	BackendRuntime   string                     `json:"backendRuntime,omitempty"`
	ChannelPolicy    *v1beta1.ChannelPolicySpec `json:"channelPolicy,omitempty"`
	ContainerState   string                     `json:"containerState,omitempty"`
	MatrixUserID     string                     `json:"matrixUserID,omitempty"`
	RoomID           string                     `json:"roomID,omitempty"`
	Message          string                     `json:"message,omitempty"`
	ExposedPorts     []ExposedPortInfo          `json:"exposedPorts,omitempty"`
	Team             string                     `json:"team,omitempty"`
	Role             string                     `json:"role,omitempty"`
}

type ExposedPortInfo struct {
	Port   int    `json:"port"`
	Domain string `json:"domain"`
}

type WorkerListResponse struct {
	Workers []WorkerResponse `json:"workers"`
	Total   int              `json:"total"`
}

// --- Team API types ---

// CreateTeamRequest 是 Team CR 的 REST 创建输入，WorkerMembers 引用现有 Worker。
type CreateTeamRequest struct {
	Name           string                     `json:"name"`
	TeamName       string                     `json:"teamName,omitempty"`
	Description    string                     `json:"description,omitempty"`
	Admin          *v1beta1.TeamAdminSpec     `json:"admin,omitempty"`
	HumanMembers   []v1beta1.TeamMemberSpec   `json:"humanMembers,omitempty"`
	WorkerMembers  []v1beta1.TeamWorkerRef    `json:"workerMembers"`
	HeartbeatEvery string                     `json:"heartbeatEvery,omitempty"`
	PeerMentions   *bool                      `json:"peerMentions,omitempty"`
	ChannelPolicy  *v1beta1.ChannelPolicySpec `json:"channelPolicy,omitempty"`
}

type UpdateTeamRequest struct {
	TeamName       string                     `json:"teamName,omitempty"`
	Description    string                     `json:"description,omitempty"`
	Admin          *v1beta1.TeamAdminSpec     `json:"admin,omitempty"`
	HumanMembers   []v1beta1.TeamMemberSpec   `json:"humanMembers,omitempty"`
	WorkerMembers  []v1beta1.TeamWorkerRef    `json:"workerMembers,omitempty"`
	HeartbeatEvery *string                    `json:"heartbeatEvery,omitempty"`
	PeerMentions   *bool                      `json:"peerMentions,omitempty"`
	ChannelPolicy  *v1beta1.ChannelPolicySpec `json:"channelPolicy,omitempty"`
}

// TeamResponse 将 Team spec 与聚合 status 组合成页面和 CLI 可直接使用的视图。
type TeamResponse struct {
	Name               string                       `json:"name"`
	TeamName           string                       `json:"teamName,omitempty"`
	Phase              string                       `json:"phase"`
	Description        string                       `json:"description,omitempty"`
	Admin              *v1beta1.TeamAdminSpec       `json:"admin,omitempty"`
	HumanMembers       []v1beta1.TeamMemberSpec     `json:"humanMembers,omitempty"`
	WorkerMembers      []v1beta1.TeamWorkerRef      `json:"workerMembers"`
	LeaderName         string                       `json:"leaderName"`
	HeartbeatEvery     string                       `json:"heartbeatEvery,omitempty"`
	PeerMentions       *bool                        `json:"peerMentions,omitempty"`
	TeamRoomID         string                       `json:"teamRoomID,omitempty"`
	LeaderDMRoomID     string                       `json:"leaderDMRoomID,omitempty"`
	LeaderReady        bool                         `json:"leaderReady"`
	ReadyWorkers       int                          `json:"readyWorkers"`
	TotalWorkers       int                          `json:"totalWorkers"`
	Message            string                       `json:"message,omitempty"`
	WorkerNames        []string                     `json:"workerNames,omitempty"`
	WorkerExposedPorts map[string][]ExposedPortInfo `json:"workerExposedPorts,omitempty"`
}

type TeamListResponse struct {
	Teams []TeamResponse `json:"teams"`
	Total int            `json:"total"`
}

// --- Human API types ---

type CreateHumanRequest struct {
	Name              string   `json:"name"`
	DisplayName       string   `json:"displayName"`
	Email             string   `json:"email,omitempty"`
	PermissionLevel   int      `json:"permissionLevel"`
	AccessibleTeams   []string `json:"accessibleTeams,omitempty"`
	AccessibleWorkers []string `json:"accessibleWorkers,omitempty"`
	Note              string   `json:"note,omitempty"`
}

type UpdateHumanRequest struct {
	DisplayName       *string   `json:"displayName,omitempty"`
	Email             *string   `json:"email,omitempty"`
	PermissionLevel   *int      `json:"permissionLevel,omitempty"`
	AccessibleTeams   *[]string `json:"accessibleTeams,omitempty"`
	AccessibleWorkers *[]string `json:"accessibleWorkers,omitempty"`
	Note              *string   `json:"note,omitempty"`
}

type HumanResponse struct {
	Name              string   `json:"name"`
	Phase             string   `json:"phase"`
	DisplayName       string   `json:"displayName"`
	Email             string   `json:"email,omitempty"`
	PermissionLevel   int      `json:"permissionLevel"`
	AccessibleTeams   []string `json:"accessibleTeams,omitempty"`
	AccessibleWorkers []string `json:"accessibleWorkers,omitempty"`
	Note              string   `json:"note,omitempty"`
	MatrixUserID      string   `json:"matrixUserID,omitempty"`
	InitialPassword   string   `json:"initialPassword,omitempty"`
	Rooms             []string `json:"rooms,omitempty"`
	Message           string   `json:"message,omitempty"`
}

type HumanListResponse struct {
	Humans []HumanResponse `json:"humans"`
	Total  int             `json:"total"`
}

// --- Manager API types ---

// CreateManagerRequest 是 AgentScope Manager 的 REST 创建输入，不允许客户端提交
// Matrix token、gateway key 等 Controller 管理的凭据。
type CreateManagerRequest struct {
	Name          string                             `json:"name"`
	Model         string                             `json:"model"`
	ModelProvider string                             `json:"modelProvider,omitempty"`
	Runtime       string                             `json:"runtime,omitempty"`
	Image         string                             `json:"image,omitempty"`
	Soul          string                             `json:"soul,omitempty"`
	Identity      string                             `json:"identity,omitempty"`
	Agents        string                             `json:"agents,omitempty"`
	Skills        []string                           `json:"skills,omitempty"`
	McpServers    []v1beta1.MCPServer                `json:"mcpServers,omitempty"`
	Package       string                             `json:"package,omitempty"`
	Config        *v1beta1.ManagerConfig             `json:"config,omitempty"`
	State         *string                            `json:"state,omitempty"` // desired lifecycle state: Running, Sleeping, Stopped
	Resources     *v1beta1.AgentResourceRequirements `json:"resources,omitempty"`
	CodingCLI     *v1beta1.ManagerCodingCLISpec      `json:"codingCLI,omitempty"`
}

type UpdateManagerRequest struct {
	Model         string                             `json:"model,omitempty"`
	ModelProvider string                             `json:"modelProvider,omitempty"`
	Runtime       string                             `json:"runtime,omitempty"`
	Image         string                             `json:"image,omitempty"`
	Soul          string                             `json:"soul,omitempty"`
	Identity      string                             `json:"identity,omitempty"`
	Agents        string                             `json:"agents,omitempty"`
	Skills        []string                           `json:"skills,omitempty"`
	McpServers    []v1beta1.MCPServer                `json:"mcpServers,omitempty"`
	Package       string                             `json:"package,omitempty"`
	Config        *v1beta1.ManagerConfig             `json:"config,omitempty"`
	State         *string                            `json:"state,omitempty"` // desired lifecycle state: Running, Sleeping, Stopped
	Resources     *v1beta1.AgentResourceRequirements `json:"resources,omitempty"`
	CodingCLI     *v1beta1.ManagerCodingCLISpec      `json:"codingCLI,omitempty"`
}

// ManagerResponse 是 Manager 对外的无密文状态视图。
type ManagerResponse struct {
	Name         string                        `json:"name"`
	Phase        string                        `json:"phase"`
	State        string                        `json:"state,omitempty"` // desired lifecycle state
	Model        string                        `json:"model,omitempty"`
	Runtime      string                        `json:"runtime,omitempty"`
	Image        string                        `json:"image,omitempty"`
	Identity     string                        `json:"identity,omitempty"`
	MatrixUserID string                        `json:"matrixUserID,omitempty"`
	RoomID       string                        `json:"roomID,omitempty"`
	Version      string                        `json:"version,omitempty"`
	Message      string                        `json:"message,omitempty"`
	McpServers   []v1beta1.MCPServer           `json:"mcpServers,omitempty"`
	CodingCLI    *v1beta1.ManagerCodingCLISpec `json:"codingCLI,omitempty"`
	// WelcomeSent mirrors ManagerStatus.WelcomeSent so installers / CLI can
	// poll for first-boot onboarding completion (DM joined + LLM auth ready
	// + welcome prompt actually delivered). Always present (false until set)
	// so the install script can rely on a stable JSON shape.
	WelcomeSent bool `json:"welcomeSent"`
}

type ManagerListResponse struct {
	Managers []ManagerResponse `json:"managers"`
	Total    int               `json:"total"`
}

// --- Gateway API types ---

type CreateConsumerRequest struct {
	Name          string `json:"name"`
	CredentialKey string `json:"credential_key,omitempty"`
}

type ConsumerResponse struct {
	Name       string `json:"name"`
	ConsumerID string `json:"consumer_id"`
	APIKey     string `json:"api_key,omitempty"`
	Status     string `json:"status"`
}

// --- Lifecycle API types ---

type WorkerLifecycleResponse struct {
	Name  string `json:"name"`
	Phase string `json:"phase"`
}
