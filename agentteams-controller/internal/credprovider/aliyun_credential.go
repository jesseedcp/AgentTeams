package credprovider

import (
	"context"

	credential "github.com/aliyun/credentials-go/credentials"
)

// NewAliyunCredential adapts a TokenManager into the
// github.com/aliyun/credentials-go Credential interface, which is what
// Alibaba Cloud SDK clients (APIG, OSS, STS, ...) consume.
//
// Each call the SDK makes to GetCredential() triggers TokenManager.Token,
// which transparently refreshes the STS triple when it is about to
// expire. SDK clients therefore see a credential that never goes stale
// so long as the sidecar stays reachable.
//
// Note: the SDK may call the Get* methods off-ctx; we bridge to a
// background context in those cases. The real work happens in
// GetCredential which the SDK calls before signing each request.
func NewAliyunCredential(tm *TokenManager) credential.Credential {
	return &aliyunCredential{tm: tm}
}

type aliyunCredential struct {
	tm *TokenManager
}

// credentialType must match one of the types the Aliyun SDK knows about so
// that SDK-internal code (particularly gateway-pop's credential type
// dispatch) treats the returned security token correctly. "sts" is the
// sentinel used for AK + SK + SecurityToken triples.
const credentialType = "sts"

func (a *aliyunCredential) token() (*IssueResponse, error) {
	// 逻辑说明：桥接不携带 context 的阿里云 SDK 凭据接口，以后台 context 向 TokenManager 取可用 STS；缓存与刷新策略完全由 TokenManager 负责。
	return a.tm.Token(context.Background())
}

func (a *aliyunCredential) GetAccessKeyId() (*string, error) {
	// 逻辑说明：获取当前有效 STS 后只返回 AccessKeyID 指针；刷新失败原样返回错误，绝不返回空指针冒充成功。
	t, err := a.token()
	if err != nil {
		return nil, err
	}
	return strPtr(t.AccessKeyID), nil
}

func (a *aliyunCredential) GetAccessKeySecret() (*string, error) {
	// 逻辑说明：获取当前有效 STS 后只返回 AccessKeySecret 指针供 SDK 签名；取 token 失败时不暴露旧密钥。
	t, err := a.token()
	if err != nil {
		return nil, err
	}
	return strPtr(t.AccessKeySecret), nil
}

func (a *aliyunCredential) GetSecurityToken() (*string, error) {
	// 逻辑说明：获取当前有效 STS 的 SecurityToken 并转成 SDK 要求的指针；刷新错误直接上抛，避免 AK/SK 与安全令牌跨代混用。
	t, err := a.token()
	if err != nil {
		return nil, err
	}
	return strPtr(t.SecurityToken), nil
}

func (a *aliyunCredential) GetBearerToken() *string {
	empty := ""
	return &empty
}

func (a *aliyunCredential) GetType() *string {
	t := credentialType
	return &t
}

func (a *aliyunCredential) GetCredential() (*credential.CredentialModel, error) {
	// 逻辑说明：一次性取得同一批 STS 三元组并组装阿里云 SDK CredentialModel，同时标记类型与提供方；获取失败时不构造半完整模型。
	t, err := a.token()
	if err != nil {
		return nil, err
	}
	typ := credentialType
	provider := "agentteams-credential-provider"
	return &credential.CredentialModel{
		AccessKeyId:     strPtr(t.AccessKeyID),
		AccessKeySecret: strPtr(t.AccessKeySecret),
		SecurityToken:   strPtr(t.SecurityToken),
		Type:            &typ,
		ProviderName:    &provider,
	}, nil
}

func strPtr(s string) *string { return &s }
