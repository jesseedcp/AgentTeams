package mail

import (
	"fmt"
	"net/smtp"
	"os"
	"strings"
)

// Config holds SMTP configuration from environment variables.
type Config struct {
	Host string
	Port string
	User string
	Pass string
	From string
}

// ConfigFromEnv reads SMTP config from AGENTTEAMS_SMTP_* environment variables.
func ConfigFromEnv() *Config {
	// 逻辑说明：从 `AGENTTEAMS_SMTP_*` 读取邮件配置；Host 为空表示功能未启用并返回 nil，端口和发件人缺省时补稳定默认值，不测试网络连接。
	host := os.Getenv("AGENTTEAMS_SMTP_HOST")
	if host == "" {
		return nil
	}
	return &Config{
		Host: host,
		Port: envOrDefault("AGENTTEAMS_SMTP_PORT", "465"),
		User: os.Getenv("AGENTTEAMS_SMTP_USER"),
		Pass: os.Getenv("AGENTTEAMS_SMTP_PASS"),
		From: envOrDefault("AGENTTEAMS_SMTP_FROM", "AgentTeams <noreply@agentteams.io>"),
	}
}

// SendWelcome sends a welcome email to a newly created human user.
func SendWelcome(cfg *Config, to, displayName, matrixUserID, password, cinnyURL string) error {
	// 逻辑说明：校验 SMTP 已配置后生成包含 Matrix 初始账号的 UTF-8 文本邮件，并用 PlainAuth 发送给单一收件人；连接或认证失败直接返回，调用方负责避免在日志中打印密码正文。
	if cfg == nil {
		return fmt.Errorf("SMTP not configured")
	}

	subject := "Welcome to AgentTeams - Your Account Details"
	body := fmt.Sprintf(`Hi %s,

Your AgentTeams account has been created:

  Username: %s
  Password: %s
  Login URL: %s

Please log in using Cinny and change your password immediately.

— AgentTeams`, displayName, matrixUserID, password, cinnyURL)

	msg := strings.Join([]string{
		fmt.Sprintf("From: %s", cfg.From),
		fmt.Sprintf("To: %s", to),
		fmt.Sprintf("Subject: %s", subject),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
		"",
		body,
	}, "\r\n")

	addr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	auth := smtp.PlainAuth("", cfg.User, cfg.Pass, cfg.Host)

	return smtp.SendMail(addr, auth, cfg.From, []string{to}, []byte(msg))
}

func envOrDefault(key, def string) string {
	// 逻辑说明：读取指定环境变量并在非空时返回，否则返回调用方给出的默认值；只读进程环境，不修改全局配置。
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
