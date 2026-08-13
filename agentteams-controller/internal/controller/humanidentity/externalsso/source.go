package externalsso

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/controller/humanidentity"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const matrixLocalpartHashBytes = 16

type source struct {
	deps humanidentity.Deps
}

func init() {
	// 逻辑说明：init 不接收输入，在包加载时把 external-sso 工厂注册为可选的 Human 身份来源。
	// 返回/状态：返回 无；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	humanidentity.Register(humanidentity.KeyExternalSSO, func(deps humanidentity.Deps) humanidentity.IdentitySource {
		return source{deps: deps}
	})
}

func (s source) Key() string {
	return humanidentity.KeyExternalSSO
}

func (s source) DeriveMatrixUserID(spec *v1beta1.HumanSpec, _ string) (string, error) {
	// 逻辑说明：DeriveMatrixUserID 接收 spec(*v1beta1.HumanSpec)、_(string)，依次借助 matrixLocalpart、MatrixUserID处理Human 身份来源的期望结果。
	// 返回/状态：返回 string、error；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	localpart, err := s.matrixLocalpart(spec)
	if err != nil {
		return "", err
	}
	return s.deps.Provisioner.MatrixUserID(localpart), nil
}

func (s source) EnsurePrecreated(ctx context.Context, spec *v1beta1.HumanSpec, metadataName string) (humanidentity.Credentials, error) {
	// 逻辑说明：EnsurePrecreated 接收 ctx(context.Context)、spec(*v1beta1.HumanSpec)、metadataName(string)，依次借助 WithValues、MatrixAppServiceEnabled、matrixLocalpart、DeriveMatrixUserID确保Human 身份来源的期望结果。
	// 返回/状态：返回 humanidentity.Credentials、error；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	logger := log.FromContext(ctx).WithValues("identitySource", humanidentity.KeyExternalSSO, "human", metadataName)

	if !s.deps.Provisioner.MatrixAppServiceEnabled() {
		logger.Error(nil, "cannot create Matrix account for SSO human: Matrix AppService mode is disabled (set AGENTTEAMS_MATRIX_APPSERVICE_ENABLED)")
		return humanidentity.Credentials{}, fmt.Errorf("external_sso requires AppService mode")
	}

	localpart, err := s.matrixLocalpart(spec)
	if err != nil {
		logger.Error(err, "failed to derive Matrix localpart from identitySource (issuer/subject)")
		return humanidentity.Credentials{}, err
	}
	expectedUserID, err := s.DeriveMatrixUserID(spec, metadataName)
	if err != nil {
		logger.Error(err, "failed to derive Matrix user ID from identitySource")
		return humanidentity.Credentials{}, err
	}

	logger.Info("creating Matrix account for SSO human via AppService register",
		"issuer", spec.IdentitySource.Issuer,
		"subject", spec.IdentitySource.Subject,
		"matrixLocalpart", localpart,
		"matrixUserID", expectedUserID)

	creds, err := s.deps.Provisioner.RegisterAppServiceUser(ctx, localpart)
	if err != nil {
		logger.Error(err, "AppService registration failed for SSO human",
			"matrixLocalpart", localpart, "matrixUserID", expectedUserID)
		return humanidentity.Credentials{}, err
	}

	logger.Info("Matrix account ready for SSO human",
		"matrixUserID", expectedUserID,
		"registeredUserID", creds.UserID,
		"created", creds.Created,
		"hasAccessToken", creds.AccessToken != "")

	return humanidentity.Credentials{
		UserID:      expectedUserID,
		AccessToken: creds.AccessToken,
		Password:    "",
		Created:     creds.Created,
	}, nil
}

func (s source) ManagesInitialPassword() bool {
	return false
}

func (s source) EnsureUserToken(ctx context.Context, spec *v1beta1.HumanSpec, _ *v1beta1.HumanStatus, _ string) (string, error) {
	// 逻辑说明：EnsureUserToken 接收 ctx(context.Context)、spec(*v1beta1.HumanSpec)、_(*v1beta1.HumanStatus)、_(string)，依次借助 MatrixAppServiceEnabled、matrixLocalpart、LoginAppServiceUser确保Human 身份来源的期望结果。
	// 返回/状态：返回 string、error；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	if !s.deps.Provisioner.MatrixAppServiceEnabled() {
		return "", fmt.Errorf("external_sso requires AppService mode")
	}
	localpart, err := s.matrixLocalpart(spec)
	if err != nil {
		return "", err
	}
	return s.deps.Provisioner.LoginAppServiceUser(ctx, localpart)
}

func (s source) EnsureDeactivated(ctx context.Context, spec *v1beta1.HumanSpec, status *v1beta1.HumanStatus) error {
	// 逻辑说明：EnsureDeactivated 接收 ctx(context.Context)、spec(*v1beta1.HumanSpec)、status(*v1beta1.HumanStatus)，依次借助 DeriveMatrixUserID、DeactivateHumanUser确保Human 身份来源的期望结果。
	// 返回/状态：返回 error；会更新 Human 身份来源的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	userID := status.MatrixUserID
	if userID == "" {
		derived, err := s.DeriveMatrixUserID(spec, "")
		if err != nil {
			return err
		}
		userID = derived
	}
	return s.deps.Provisioner.DeactivateHumanUser(ctx, userID)
}

func (s source) matrixLocalpart(spec *v1beta1.HumanSpec) (string, error) {
	// 逻辑说明：matrixLocalpart 接收 spec(*v1beta1.HumanSpec)，依次借助 Sum256、EncodeToString处理Human 身份来源的期望结果。
	// 返回/状态：返回 string、error；可能查询或改变 Matrix 用户、房间、别名、成员或权限状态。
	// 失败/重试：Matrix 请求失败会返回错误；上层下一轮先重新观测实际状态，再只补做尚未满足的步骤。
	if spec.IdentitySource == nil {
		return "", fmt.Errorf("identitySource is required for external_sso")
	}
	issuer := spec.IdentitySource.Issuer
	subject := spec.IdentitySource.Subject
	if issuer == "" {
		return "", fmt.Errorf("identitySource.issuer must not be empty")
	}
	if subject == "" {
		return "", fmt.Errorf("identitySource.subject must not be empty")
	}
	digest := sha256.Sum256([]byte(issuer + "\x00" + subject))
	return hex.EncodeToString(digest[:matrixLocalpartHashBytes]), nil
}
