package legacypassword

import (
	"context"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/controller/humanidentity"
)

type source struct {
	deps humanidentity.Deps
}

func init() {
	// 逻辑说明：init 不接收输入，在包加载时把 legacy-password 工厂注册为可选的 Human 身份来源。
	// 返回/状态：返回 无；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	humanidentity.Register(humanidentity.KeyLegacyPassword, func(deps humanidentity.Deps) humanidentity.IdentitySource {
		return source{deps: deps}
	})
}

func (s source) Key() string {
	return humanidentity.KeyLegacyPassword
}

func (s source) DeriveMatrixUserID(spec *v1beta1.HumanSpec, metadataName string) (string, error) {
	// 逻辑说明：DeriveMatrixUserID 接收 spec(*v1beta1.HumanSpec)、metadataName(string)，依次借助 MatrixUserID、EffectiveUsername处理Human 身份来源的期望结果。
	// 返回/状态：返回 string、error；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	return s.deps.Provisioner.MatrixUserID(spec.EffectiveUsername(metadataName)), nil
}

func (s source) EnsurePrecreated(ctx context.Context, spec *v1beta1.HumanSpec, metadataName string) (humanidentity.Credentials, error) {
	// 逻辑说明：EnsurePrecreated 接收 ctx(context.Context)、spec(*v1beta1.HumanSpec)、metadataName(string)，依次借助 EnsureHumanUser、EffectiveUsername确保Human 身份来源的期望结果。
	// 返回/状态：返回 humanidentity.Credentials、error；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	creds, err := s.deps.Provisioner.EnsureHumanUser(ctx, spec.EffectiveUsername(metadataName))
	if err != nil {
		return humanidentity.Credentials{}, err
	}
	return humanidentity.Credentials{
		UserID:      creds.UserID,
		AccessToken: creds.AccessToken,
		Password:    creds.Password,
		Created:     creds.Created,
	}, nil
}

func (s source) ManagesInitialPassword() bool {
	return true
}

func (s source) EnsureUserToken(ctx context.Context, spec *v1beta1.HumanSpec, status *v1beta1.HumanStatus, metadataName string) (string, error) {
	// 逻辑说明：EnsureUserToken 接收 ctx(context.Context)、spec(*v1beta1.HumanSpec)、status(*v1beta1.HumanStatus)、metadataName(string)，依次借助 EffectiveUsername、MatrixAppServiceEnabled、LoginAppServiceUser、LoginWithPassword确保Human 身份来源的期望结果。
	// 返回/状态：返回 string、error；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	username := spec.EffectiveUsername(metadataName)
	if s.deps.Provisioner.MatrixAppServiceEnabled() {
		return s.deps.Provisioner.LoginAppServiceUser(ctx, username)
	}
	if status.InitialPassword == "" {
		return "", nil
	}
	return s.deps.Provisioner.LoginWithPassword(ctx, username, status.InitialPassword)
}

func (s source) EnsureDeactivated(context.Context, *v1beta1.HumanSpec, *v1beta1.HumanStatus) error {
	return nil
}
