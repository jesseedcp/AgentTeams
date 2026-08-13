package matrix

import (
	"crypto/rand"
	"encoding/hex"
)

// AppserviceConfig holds the configuration for registering the controller
// as a Matrix Application Service with the homeserver (Conduwuit/Tuwunel).
type AppserviceConfig struct {
	Enabled bool
	ID      string // e.g. "agentteams-watcher"
	ASToken string // appservice → homeserver authentication token
	HSToken string // homeserver → appservice authentication token
	URL     string // controller HTTP endpoint reachable from homeserver
}

// EnsureTokens fills in any empty token with a random 64-byte hex string.
// Safe to call multiple times; already-set values are left untouched.
func (c *AppserviceConfig) EnsureTokens() {
	// 逻辑说明：只为尚未配置的 AS/HS token 生成随机值，保留显式配置，保证重复启动不会无故轮换现有 appservice 凭据。
	if c.ASToken == "" {
		c.ASToken = randomHex(32)
	}
	if c.HSToken == "" {
		c.HSToken = randomHex(32)
	}
}

func randomHex(n int) string {
	// 逻辑说明：从密码学安全随机源读取 n 字节并编码为十六进制；随机源失败属于不可恢复的启动错误，因此直接 panic，避免生成弱 token。
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}
