package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"sigs.k8s.io/yaml"
)

const (
	memberRuntimeConfigKind = "MemberRuntimeConfig"
	nativeConfigModel       = "native-config"
)

func memberRuntimeConfigObjectKey(runtimeName string) string {
	return fmt.Sprintf("agents/%s/runtime/runtime.yaml", runtimeName)
}

type memberRuntimeConfigDocument struct {
	APIVersion        string                                `json:"apiVersion"`
	Kind              string                                `json:"kind"`
	Metadata          memberRuntimeConfigMetadata           `json:"metadata"`
	Team              *memberRuntimeConfigTeam              `json:"team,omitempty"`
	Member            memberRuntimeConfigMember             `json:"member"`
	Matrix            *memberRuntimeConfigMatrix            `json:"matrix,omitempty"`
	Desired           memberRuntimeConfigDesired            `json:"desired"`
	Storage           memberRuntimeConfigStorage            `json:"storage"`
	AgentIdentityData *memberRuntimeConfigAgentIdentityData `json:"agentIdentityData,omitempty"`
	Credentials       memberRuntimeConfigCredentials        `json:"credentials"`
}

type memberRuntimeConfigMetadata struct {
	Generation int64  `json:"generation,omitempty"`
	UpdatedAt  string `json:"updatedAt"`
}

type memberRuntimeConfigTeam struct {
	Name              string                        `json:"name,omitempty"`
	StorageID         string                        `json:"storageId,omitempty"`
	TeamRoomID        string                        `json:"teamRoomId,omitempty"`
	LeaderName        string                        `json:"leaderName,omitempty"`
	LeaderRuntimeName string                        `json:"leaderRuntimeName,omitempty"`
	LeaderDMRoomID    string                        `json:"leaderDmRoomId,omitempty"`
	Admin             *memberRuntimeConfigTeamAdmin `json:"admin,omitempty"`
	Members           []RuntimeConfigTeamMember     `json:"members,omitempty"`
}

type memberRuntimeConfigTeamAdmin struct {
	Name         string `json:"name,omitempty"`
	MatrixUserID string `json:"matrixUserId,omitempty"`
}

type memberRuntimeConfigMember struct {
	Name           string `json:"name,omitempty"`
	RuntimeName    string `json:"runtimeName"`
	Role           string `json:"role,omitempty"`
	Runtime        string `json:"runtime"`
	MatrixUserID   string `json:"matrixUserId,omitempty"`
	PersonalRoomID string `json:"personalRoomId,omitempty"`
}

type memberRuntimeConfigMatrix struct {
	AccessToken string `json:"accessToken,omitempty"`
}

type memberRuntimeConfigDesired struct {
	Model              *memberRuntimeConfigModel         `json:"model,omitempty"`
	AgentPackage       *memberRuntimeConfigAgentPackage  `json:"agentPackage,omitempty"`
	InlineConfig       *memberRuntimeConfigInlineConfig  `json:"inlineConfig,omitempty"`
	SkillRegistry      *memberRuntimeConfigSkillRegistry `json:"skillRegistry,omitempty"`
	MCPServers         []v1beta1.MCPServer               `json:"mcpServers,omitempty"`
	ChannelPolicy      *v1beta1.ChannelPolicySpec        `json:"channelPolicy,omitempty"`
	AgentIdentity      *v1beta1.AgentIdentitySpec        `json:"agentIdentity,omitempty"`
	CredentialBindings []v1beta1.CredentialBinding       `json:"credentialBindings,omitempty"`
	Channels           *memberRuntimeConfigChannels      `json:"channels,omitempty"`
	State              string                            `json:"state"`
}

type memberRuntimeConfigModel struct {
	ProviderID string `json:"providerId"`
	Model      string `json:"model"`
	GatewayURL string `json:"gatewayUrl,omitempty"`
	GatewayKey string `json:"gatewayKey,omitempty"`
}

type memberRuntimeConfigAgentPackage struct {
	Ref string `json:"ref"`
}

type memberRuntimeConfigInlineConfig struct {
	Identity string `json:"identity,omitempty"`
	Soul     string `json:"soul,omitempty"`
	Agents   string `json:"agents,omitempty"`
}

type memberRuntimeConfigSkillRegistry struct {
	Provider string `json:"provider,omitempty"`
	URL      string `json:"url,omitempty"`
	AuthType string `json:"authType,omitempty"`
}

type memberRuntimeConfigChannels struct {
	DingTalk *memberRuntimeConfigDingTalkChannel `json:"dingtalk,omitempty"`
}

type memberRuntimeConfigDingTalkChannel struct {
	Enabled            bool   `json:"enabled"`
	ClientID           string `json:"client_id,omitempty"`
	ClientSecret       string `json:"client_secret,omitempty"`
	RobotCode          string `json:"robot_code,omitempty"`
	FilterThinking     bool   `json:"filter_thinking"`
	FilterToolMessages bool   `json:"filter_tool_messages"`
	StreamingEnabled   bool   `json:"streaming_enabled"`
	MessageType        string `json:"message_type,omitempty"`
	CardTemplateID     string `json:"card_template_id,omitempty"`
	CardTemplateKey    string `json:"card_template_key,omitempty"`
	CardAutoLayout     bool   `json:"card_auto_layout"`
}

type memberRuntimeConfigStorage struct {
	Provider           string `json:"provider,omitempty"`
	Bucket             string `json:"bucket,omitempty"`
	Endpoint           string `json:"endpoint,omitempty"`
	TeamPrefix         string `json:"teamPrefix,omitempty"`
	SharedPrefix       string `json:"sharedPrefix"`
	GlobalSharedPrefix string `json:"globalSharedPrefix"`
	MemberPrefix       string `json:"memberPrefix"`
}

type memberRuntimeConfigAgentIdentityData struct {
	Endpoint string `json:"endpoint,omitempty"`
}

type memberRuntimeConfigCredentials struct {
	MatrixTokenEnv          string `json:"matrixTokenEnv"`
	GatewayKeyEnv           string `json:"gatewayKeyEnv"`
	StorageAccessKeyEnv     string `json:"storageAccessKeyEnv"`
	StorageSecretKeyEnv     string `json:"storageSecretKeyEnv"`
	ServiceAccountTokenPath string `json:"serviceAccountTokenPath"`
}

// DeployMemberRuntimeConfig writes the controller-to-runtime desired-state
// snapshot consumed by managed worker runtimes such as QwenPaw.
func (d *Deployer) DeployMemberRuntimeConfig(ctx context.Context, req MemberRuntimeConfigDeployRequest) error {
	// 逻辑说明：DeployMemberRuntimeConfig 接收 ctx(context.Context)、req(MemberRuntimeConfigDeployRequest)，依次借助 validateRuntimeCredentialContract、memberRuntimeConfigDocument、preserveExistingRuntimeTeamContext、Marshal部署运行时配置的期望结果。
	// 返回/状态：返回 error；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if d.oss == nil {
		return fmt.Errorf("OSS client is required to deploy runtime config")
	}
	runtimeName := strings.TrimSpace(req.RuntimeName)
	if runtimeName == "" {
		runtimeName = strings.TrimSpace(req.Name)
	}
	if runtimeName == "" {
		return fmt.Errorf("runtimeName is required")
	}
	if err := validateRuntimeCredentialContract(req.Spec); err != nil {
		return err
	}

	doc, err := d.memberRuntimeConfigDocument(req, runtimeName)
	if err != nil {
		return err
	}
	if doc.Team == nil && !req.DropTeamContext {
		d.preserveExistingRuntimeTeamContext(ctx, runtimeName, &doc)
	}
	payload, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal runtime config: %w", err)
	}
	key := memberRuntimeConfigObjectKey(runtimeName)
	if err := d.oss.PutObject(ctx, key, payload); err != nil {
		return fmt.Errorf("write runtime config: %w", err)
	}
	return nil
}

// MergeMemberRuntimeTeamContext updates only the team-facing part of an
// existing runtime.yaml. It is used for remote-managed local workers whose
// WorkerReconciler owns sensitive runtime fields such as matrix tokens and
// gateway keys.
func (d *Deployer) MergeMemberRuntimeTeamContext(ctx context.Context, req MemberRuntimeConfigDeployRequest) error {
	// 逻辑说明：MergeMemberRuntimeTeamContext 接收 ctx(context.Context)、req(MemberRuntimeConfigDeployRequest)，依次借助 memberRuntimeConfigObjectKey、GetObject、Unmarshal、Format合并运行时配置的期望结果。
	// 返回/状态：返回 error；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if d.oss == nil {
		return fmt.Errorf("OSS client is required to deploy runtime config")
	}
	runtimeName := strings.TrimSpace(req.RuntimeName)
	if runtimeName == "" {
		runtimeName = strings.TrimSpace(req.Name)
	}
	if runtimeName == "" {
		return fmt.Errorf("runtimeName is required")
	}

	key := memberRuntimeConfigObjectKey(runtimeName)
	existingPayload, err := d.oss.GetObject(ctx, key)
	if err != nil {
		return fmt.Errorf("read runtime config: %w", err)
	}
	var doc memberRuntimeConfigDocument
	if err := yaml.Unmarshal(existingPayload, &doc); err != nil {
		return fmt.Errorf("unmarshal runtime config: %w", err)
	}
	if doc.APIVersion == "" {
		doc.APIVersion = "agentteams.io/v1beta1"
	}
	if doc.Kind == "" {
		doc.Kind = memberRuntimeConfigKind
	}
	if req.Generation != 0 {
		doc.Metadata.Generation = req.Generation
	}
	doc.Metadata.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if req.Name != "" {
		doc.Member.Name = req.Name
	}
	doc.Member.RuntimeName = runtimeName
	if req.Role != "" {
		doc.Member.Role = req.Role
	}
	if req.MatrixUserID != "" {
		doc.Member.MatrixUserID = req.MatrixUserID
	}
	if req.PersonalRoomID != "" {
		doc.Member.PersonalRoomID = req.PersonalRoomID
	}
	applyRuntimeTeamContext(&doc, req)

	payload, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal runtime config: %w", err)
	}
	if err := d.oss.PutObject(ctx, key, payload); err != nil {
		return fmt.Errorf("write runtime config: %w", err)
	}
	return nil
}

func (d *Deployer) memberRuntimeConfigDocument(req MemberRuntimeConfigDeployRequest, runtimeName string) (memberRuntimeConfigDocument, error) {
	// 逻辑说明：memberRuntimeConfigDocument 接收 req(MemberRuntimeConfigDeployRequest)、runtimeName(string)，依次借助 runtimeAgentIdentity、copyCredentialBindings、DesiredState、isNativeConfigModel处理运行时配置的期望结果。
	// 返回/状态：返回 memberRuntimeConfigDocument、error；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	runtime := strings.TrimSpace(req.Runtime)
	if runtime == "" {
		runtime = req.Spec.Runtime
	}
	if runtime == "" {
		runtime = "openclaw"
	}
	role := strings.TrimSpace(req.Role)
	if role == "" {
		role = "worker"
	}

	desired := memberRuntimeConfigDesired{
		MCPServers:         req.Spec.McpServers,
		ChannelPolicy:      req.Spec.ChannelPolicy,
		AgentIdentity:      runtimeAgentIdentity(req.Spec),
		CredentialBindings: copyCredentialBindings(req.Spec.CredentialBindings),
		State:              req.Spec.DesiredState(),
	}
	if req.Spec.Model != "" && !isNativeConfigModel(req.Spec.Model) {
		gatewayURL := strings.TrimSpace(req.AIGatewayURL)
		if gatewayURL == "" {
			gatewayURL = d.runtimeProjection.AIGatewayURL
		}
		desired.Model = &memberRuntimeConfigModel{
			ProviderID: "agentteams-gateway",
			Model:      req.Spec.Model,
			GatewayURL: gatewayURL,
			GatewayKey: strings.TrimSpace(req.GatewayKey),
		}
	}
	if req.Spec.Package != "" {
		desired.AgentPackage = &memberRuntimeConfigAgentPackage{Ref: req.Spec.Package}
	}
	if req.Spec.Identity != "" || req.Spec.Soul != "" || req.Spec.Agents != "" {
		desired.InlineConfig = &memberRuntimeConfigInlineConfig{
			Identity: req.Spec.Identity,
			Soul:     req.Spec.Soul,
			Agents:   req.Spec.Agents,
		}
	}
	if req.SkillRegistryURL != "" || req.SkillRegistryAuthType != "" {
		desired.SkillRegistry = &memberRuntimeConfigSkillRegistry{
			Provider: "nacos",
			URL:      req.SkillRegistryURL,
			AuthType: req.SkillRegistryAuthType,
		}
	}
	channels, err := memberRuntimeConfigChannelsFromSpec(req.Spec)
	if err != nil {
		return memberRuntimeConfigDocument{}, err
	}
	desired.Channels = channels

	doc := memberRuntimeConfigDocument{
		APIVersion: "agentteams.io/v1beta1",
		Kind:       memberRuntimeConfigKind,
		Metadata: memberRuntimeConfigMetadata{
			Generation: req.Generation,
			UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		},
		Member: memberRuntimeConfigMember{
			Name:           req.Name,
			RuntimeName:    runtimeName,
			Role:           role,
			Runtime:        runtime,
			MatrixUserID:   req.MatrixUserID,
			PersonalRoomID: req.PersonalRoomID,
		},
		Desired: desired,
		Storage: memberRuntimeConfigStorage{
			Provider:           d.runtimeProjection.StorageProvider,
			Bucket:             d.runtimeProjection.StorageBucket,
			Endpoint:           d.runtimeProjection.StorageEndpoint,
			SharedPrefix:       "shared",
			GlobalSharedPrefix: "shared",
			MemberPrefix:       "agents/" + runtimeName,
		},
		AgentIdentityData: runtimeAgentIdentityData(req.Spec, d.runtimeProjection),
		Credentials: memberRuntimeConfigCredentials{
			MatrixTokenEnv:          "AGENTTEAMS_WORKER_MATRIX_TOKEN",
			GatewayKeyEnv:           "AGENTTEAMS_WORKER_GATEWAY_KEY",
			StorageAccessKeyEnv:     "AGENTTEAMS_FS_ACCESS_KEY",
			StorageSecretKeyEnv:     "AGENTTEAMS_FS_SECRET_KEY",
			ServiceAccountTokenPath: "/var/run/secrets/agentteams/token",
		},
	}
	if req.MatrixAccessToken != "" {
		doc.Matrix = &memberRuntimeConfigMatrix{AccessToken: req.MatrixAccessToken}
	}

	applyRuntimeTeamContext(&doc, req)

	return doc, nil
}

func isNativeConfigModel(model string) bool {
	// 逻辑说明：isNativeConfigModel 接收 model(string)，依次借助 EqualFold判断运行时配置的期望结果。
	// 返回/状态：返回 bool；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	return strings.EqualFold(strings.TrimSpace(model), nativeConfigModel)
}

func memberRuntimeConfigChannelsFromSpec(spec v1beta1.WorkerSpec) (*memberRuntimeConfigChannels, error) {
	// 逻辑说明：memberRuntimeConfigChannelsFromSpec 接收 spec(v1beta1.WorkerSpec)，依次借助 memberRuntimeConfigDingTalkFromSpec处理运行时配置的期望结果。
	// 返回/状态：返回 *memberRuntimeConfigChannels、error；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if spec.Channels == nil || spec.Channels.DingTalk == nil {
		return nil, nil
	}
	dingtalk, err := memberRuntimeConfigDingTalkFromSpec(spec.Channels.DingTalk)
	if err != nil {
		return nil, err
	}
	return &memberRuntimeConfigChannels{DingTalk: dingtalk}, nil
}

func memberRuntimeConfigDingTalkFromSpec(spec *v1beta1.DingTalkChannelSpec) (*memberRuntimeConfigDingTalkChannel, error) {
	// 逻辑说明：memberRuntimeConfigDingTalkFromSpec 接收 spec(*v1beta1.DingTalkChannelSpec)，按本函数中的条件与转换步骤处理运行时配置的期望结果。
	// 返回/状态：返回 *memberRuntimeConfigDingTalkChannel、error；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	enabled := false
	if spec.Enabled != nil {
		enabled = *spec.Enabled
	}
	if !enabled {
		return &memberRuntimeConfigDingTalkChannel{Enabled: false}, nil
	}

	clientID := strings.TrimSpace(spec.ClientID)
	if clientID == "" {
		return nil, fmt.Errorf("channels.dingtalk.clientId is required when enabled")
	}
	clientSecret := strings.TrimSpace(spec.ClientSecret)
	if clientSecret == "" {
		return nil, fmt.Errorf("channels.dingtalk.clientSecret is required when enabled")
	}
	messageType := strings.TrimSpace(spec.MessageType)
	if messageType == "" {
		messageType = "markdown"
	}
	if messageType != "markdown" && messageType != "card" {
		return nil, fmt.Errorf("channels.dingtalk.messageType must be markdown or card")
	}
	cardTemplateID := strings.TrimSpace(spec.CardTemplateID)
	if messageType == "card" && cardTemplateID == "" {
		return nil, fmt.Errorf("channels.dingtalk.cardTemplateId is required when messageType is card")
	}

	return &memberRuntimeConfigDingTalkChannel{
		Enabled:            true,
		ClientID:           clientID,
		ClientSecret:       clientSecret,
		RobotCode:          strings.TrimSpace(spec.RobotCode),
		FilterThinking:     !spec.ShowThinking,
		FilterToolMessages: !spec.ShowToolCalls,
		StreamingEnabled:   spec.StreamingEnabled,
		MessageType:        messageType,
		CardTemplateID:     cardTemplateID,
		CardTemplateKey:    "content",
		CardAutoLayout:     false,
	}, nil
}

func validateRuntimeCredentialContract(spec v1beta1.WorkerSpec) error {
	// 逻辑说明：validateRuntimeCredentialContract 接收 spec(v1beta1.WorkerSpec)，按本函数中的条件与转换步骤校验运行时配置的期望结果。
	// 返回/状态：返回 error；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if len(spec.CredentialBindings) == 0 {
		return nil
	}
	if spec.AgentIdentity == nil || strings.TrimSpace(spec.AgentIdentity.WorkloadIdentityName) == "" {
		return fmt.Errorf("agentIdentity.workloadIdentityName is required when credentialBindings are set")
	}
	return nil
}

func runtimeAgentIdentity(spec v1beta1.WorkerSpec) *v1beta1.AgentIdentitySpec {
	// 逻辑说明：runtimeAgentIdentity 接收 spec(v1beta1.WorkerSpec)，按本函数中的条件与转换步骤处理运行时配置的期望结果。
	// 返回/状态：返回 *v1beta1.AgentIdentitySpec；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if spec.AgentIdentity == nil {
		return nil
	}
	name := strings.TrimSpace(spec.AgentIdentity.WorkloadIdentityName)
	if name == "" {
		return nil
	}
	return &v1beta1.AgentIdentitySpec{WorkloadIdentityName: name}
}

func runtimeAgentIdentityData(spec v1beta1.WorkerSpec, projection RuntimeProjectionConfig) *memberRuntimeConfigAgentIdentityData {
	// 逻辑说明：runtimeAgentIdentityData 接收 spec(v1beta1.WorkerSpec)、projection(RuntimeProjectionConfig)，按本函数中的条件与转换步骤处理运行时配置的期望结果。
	// 返回/状态：返回 *memberRuntimeConfigAgentIdentityData；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if len(spec.CredentialBindings) == 0 {
		return nil
	}
	endpoint := strings.TrimSpace(projection.AgentIdentityDataEndpoint)
	if endpoint == "" {
		return nil
	}
	return &memberRuntimeConfigAgentIdentityData{Endpoint: endpoint}
}

func copyCredentialBindings(in []v1beta1.CredentialBinding) []v1beta1.CredentialBinding {
	// 逻辑说明：copyCredentialBindings 接收 in([]v1beta1.CredentialBinding)，依次借助 DeepCopyInto复制运行时配置的期望结果。
	// 返回/状态：返回 []v1beta1.CredentialBinding；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if len(in) == 0 {
		return nil
	}
	out := make([]v1beta1.CredentialBinding, len(in))
	for i := range in {
		in[i].DeepCopyInto(&out[i])
	}
	return out
}

func applyRuntimeTeamContext(doc *memberRuntimeConfigDocument, req MemberRuntimeConfigDeployRequest) {
	// 逻辑说明：applyRuntimeTeamContext 接收 doc(*memberRuntimeConfigDocument)、req(MemberRuntimeConfigDeployRequest)，按本函数中的条件与转换步骤应用运行时配置的期望结果。
	// 返回/状态：返回 无；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if doc == nil {
		return
	}
	if req.TeamName == "" && req.TeamRoomID == "" && req.LeaderRuntimeName == "" && req.LeaderDMRoomID == "" && req.TeamAdminName == "" && req.TeamAdminMatrixID == "" && len(req.TeamMembers) == 0 {
		return
	}
	doc.Team = &memberRuntimeConfigTeam{
		Name:              req.TeamName,
		StorageID:         req.TeamName,
		TeamRoomID:        req.TeamRoomID,
		LeaderName:        req.LeaderName,
		LeaderRuntimeName: req.LeaderRuntimeName,
		LeaderDMRoomID:    req.LeaderDMRoomID,
		Members:           req.TeamMembers,
	}
	if doc.Team.LeaderName == "" {
		doc.Team.LeaderName = req.LeaderRuntimeName
	}
	if req.TeamAdminName != "" || req.TeamAdminMatrixID != "" {
		doc.Team.Admin = &memberRuntimeConfigTeamAdmin{
			Name:         req.TeamAdminName,
			MatrixUserID: req.TeamAdminMatrixID,
		}
	}
	if req.TeamName != "" {
		doc.Storage.TeamPrefix = "teams/" + req.TeamName
		doc.Storage.SharedPrefix = "teams/" + req.TeamName + "/shared"
	}
}

func (d *Deployer) preserveExistingRuntimeTeamContext(ctx context.Context, runtimeName string, doc *memberRuntimeConfigDocument) {
	// 逻辑说明：preserveExistingRuntimeTeamContext 接收 ctx(context.Context)、runtimeName(string)、doc(*memberRuntimeConfigDocument)，依次借助 GetObject、memberRuntimeConfigObjectKey、Unmarshal保留运行时配置的期望结果。
	// 返回/状态：返回 无；可能把配置、技能或运行文档写入本地目录或 MinIO/S3 的稳定对象键。
	// 失败/重试：校验、序列化或 I/O 失败会返回错误；旧配置保持可用，上层可用相同对象键覆盖重试。
	if d.oss == nil || doc == nil {
		return
	}
	existingPayload, err := d.oss.GetObject(ctx, memberRuntimeConfigObjectKey(runtimeName))
	if err != nil || len(existingPayload) == 0 {
		return
	}
	var existing memberRuntimeConfigDocument
	if err := yaml.Unmarshal(existingPayload, &existing); err != nil || existing.Team == nil {
		return
	}
	doc.Team = existing.Team
	if existing.Member.Role != "" {
		doc.Member.Role = existing.Member.Role
	}
	if existing.Storage.TeamPrefix != "" {
		doc.Storage.TeamPrefix = existing.Storage.TeamPrefix
	}
	if existing.Storage.SharedPrefix != "" {
		doc.Storage.SharedPrefix = existing.Storage.SharedPrefix
	}
}
