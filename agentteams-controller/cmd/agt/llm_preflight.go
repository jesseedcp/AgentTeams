package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultOpenAICompatibleBaseURL  = "https://api.openai.com/v1"
	defaultQwenCompatibleBaseURL    = "https://dashscope.aliyuncs.com/compatible-mode/v1"
	defaultLLMPreflightTimeout      = 30 * time.Second
	defaultLLMPreflightRetries      = 2
	defaultLLMPreflightRetryBackoff = 500 * time.Millisecond
)

type llmPreflightOptions struct {
	Provider string
	APIKey   string
	BaseURL  string
	Model    string
	Timeout  time.Duration
	Retries  int
	Strict   bool

	HTTPClient   *http.Client
	RetryBackoff time.Duration
}

type llmPreflightConfig struct {
	Provider string
	APIKey   string
	BaseURL  string
	Model    string
	Timeout  time.Duration
	Retries  int
	Backoff  time.Duration
}

type llmPreflightStatusError struct {
	StatusCode int
	Body       string
	APIKey     string
}

func (e *llmPreflightStatusError) Error() string {
	// 逻辑说明：先遮蔽 API key 并截断供应商响应，再组合状态码提示；空 body 时不制造无意义诊断。
	body := sanitizePreflightBody(e.Body, e.APIKey)
	if body == "" {
		return fmt.Sprintf("LLM preflight failed with HTTP %d: %s", e.StatusCode, preflightStatusHint(e.StatusCode))
	}
	return fmt.Sprintf("LLM preflight failed with HTTP %d: %s. Response body: %s",
		e.StatusCode, preflightStatusHint(e.StatusCode), body)
}

type llmPreflightTransportError struct {
	URL string
	Err error
}

func (e *llmPreflightTransportError) Error() string {
	// 逻辑说明：保留无凭据 endpoint 和底层网络错误，便于判断 DNS/TLS/超时问题。
	return fmt.Sprintf("LLM preflight request to %s failed: %v", e.URL, e.Err)
}

func (e *llmPreflightTransportError) Unwrap() error {
	// 逻辑说明：暴露底层错误给 errors.Is/As，重试判断可识别 context 取消和 deadline。
	return e.Err
}

func llmPreflightCmd() *cobra.Command {
	// 逻辑说明：以环境配置为 flag 默认值，执行最小模型请求；非 strict 模式只告警，strict 模式返回非零错误。
	opts := llmPreflightOptionsFromEnv()
	timeoutSeconds := int(opts.Timeout.Seconds())

	cmd := &cobra.Command{
		Use:   "llm-preflight",
		Short: "Validate LLM API key, base URL, and model before startup",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Timeout = time.Duration(timeoutSeconds) * time.Second
			if err := runLLMPreflight(cmd.Context(), opts); err != nil {
				if !opts.Strict {
					fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: %v\n", err)
					return nil
				}
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "LLM preflight passed")
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.Provider, "provider", opts.Provider, "LLM provider (openai-compat|qwen|custom)")
	cmd.Flags().StringVar(&opts.APIKey, "api-key", opts.APIKey, "LLM API key")
	cmd.Flags().StringVar(&opts.BaseURL, "base-url", opts.BaseURL, "OpenAI-compatible base URL")
	cmd.Flags().StringVar(&opts.Model, "model", opts.Model, "Model name to probe")
	cmd.Flags().IntVar(&timeoutSeconds, "timeout", timeoutSeconds, "HTTP timeout in seconds")
	cmd.Flags().IntVar(&opts.Retries, "retries", opts.Retries, "Retry count for transient failures")
	cmd.Flags().BoolVar(&opts.Strict, "strict", opts.Strict, "Return non-zero when preflight fails")
	return cmd
}

func llmPreflightOptionsFromEnv() llmPreflightOptions {
	// 逻辑说明：集中读取 provider/key/URL/model 与重试参数，缺失值留给后续解析器统一补默认或报错。
	return llmPreflightOptions{
		Provider: envOrDefaultLocal("AGENTTEAMS_LLM_PROVIDER", "openai-compat"),
		APIKey:   os.Getenv("AGENTTEAMS_LLM_API_KEY"),
		BaseURL:  os.Getenv("AGENTTEAMS_OPENAI_BASE_URL"),
		Model:    os.Getenv("AGENTTEAMS_DEFAULT_MODEL"),
		Timeout: time.Duration(envIntDefaultLocal(
			"AGENTTEAMS_LLM_PREFLIGHT_TIMEOUT_SECONDS",
			int(defaultLLMPreflightTimeout.Seconds()),
		)) * time.Second,
		Retries: envIntDefaultLocal("AGENTTEAMS_LLM_PREFLIGHT_RETRIES", defaultLLMPreflightRetries),
		Strict:  envBoolDefaultLocal("AGENTTEAMS_LLM_PREFLIGHT_STRICT", true),
	}
}

func resolveLLMPreflightConfig(opts llmPreflightOptions) (llmPreflightConfig, error) {
	// 逻辑说明：清理字符串、要求 key/model、规范化超时/重试/退避，再解析 provider 对应的安全 base URL。
	cfg := llmPreflightConfig{
		Provider: strings.TrimSpace(opts.Provider),
		APIKey:   strings.TrimSpace(opts.APIKey),
		BaseURL:  strings.TrimSpace(opts.BaseURL),
		Model:    strings.TrimSpace(opts.Model),
		Timeout:  opts.Timeout,
		Retries:  opts.Retries,
		Backoff:  opts.RetryBackoff,
	}
	if cfg.Provider == "" {
		cfg.Provider = "openai-compat"
	}
	if cfg.APIKey == "" {
		return cfg, fmt.Errorf("LLM API key is required (set AGENTTEAMS_LLM_API_KEY or --api-key)")
	}
	if cfg.Model == "" {
		return cfg, fmt.Errorf("LLM model is required (set AGENTTEAMS_DEFAULT_MODEL or --model)")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultLLMPreflightTimeout
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.Backoff == 0 {
		cfg.Backoff = defaultLLMPreflightRetryBackoff
	}
	if cfg.Backoff < 0 {
		cfg.Backoff = 0
	}

	baseURL, err := resolveLLMPreflightBaseURL(cfg.Provider, cfg.BaseURL)
	if err != nil {
		return cfg, err
	}
	cfg.BaseURL = baseURL
	return cfg, nil
}

func resolveLLMPreflightBaseURL(provider, baseURL string) (string, error) {
	// 逻辑说明：已知 provider 可补官方兼容地址，自定义 provider 必须显式 URL；最终只接受带 host 的 HTTP(S)。
	provider = strings.TrimSpace(provider)
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		switch provider {
		case "", "openai-compat", "openai":
			baseURL = defaultOpenAICompatibleBaseURL
		case "qwen":
			baseURL = defaultQwenCompatibleBaseURL
		default:
			return "", fmt.Errorf("LLM base URL is required for provider %q (set AGENTTEAMS_OPENAI_BASE_URL or --base-url)", provider)
		}
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid LLM base URL %q: %w", baseURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("invalid LLM base URL %q: scheme must be http or https", baseURL)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("invalid LLM base URL %q: host is required", baseURL)
	}
	return strings.TrimRight(baseURL, "/"), nil
}

// runLLMPreflight 在启动完整 AgentTeams 前用最小 chat/completions 请求验证
// API key、base URL 和模型 ID。它只对超时、429 和 5xx 等短暂错误做有限重试；
// 401/403 等配置错误继续重试只会浪费时间并可能触发供应商限制。
// 返回错误前会对 response body 脱敏，避免供应商在错误中回显的 key 进入日志。
func runLLMPreflight(ctx context.Context, opts llmPreflightOptions) error {
	// 逻辑说明：解析配置并选择有界 client，最多执行 retries+1 次；仅瞬时错误指数退避，取消或配置错误立即返回。
	cfg, err := resolveLLMPreflightConfig(opts)
	if err != nil {
		return err
	}

	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}

	var lastErr error
	for attempt := 0; attempt <= cfg.Retries; attempt++ {
		lastErr = runLLMPreflightAttempt(ctx, client, cfg)
		if lastErr == nil {
			return nil
		}
		if !isRetryableLLMPreflightError(lastErr) || attempt == cfg.Retries {
			break
		}
		if err := waitLLMPreflightRetryBackoff(ctx, cfg.Backoff, attempt); err != nil {
			return err
		}
	}
	return lastErr
}

func runLLMPreflightAttempt(ctx context.Context, client *http.Client, cfg llmPreflightConfig) error {
	// 逻辑说明：向 chat/completions 发送一个 max_tokens=1 的最小请求；2xx 即通过，错误 body 最多读取 4 KiB。
	endpoint := strings.TrimRight(cfg.BaseURL, "/") + "/chat/completions"
	payload := map[string]interface{}{
		"model":      cfg.Model,
		"max_tokens": 1,
		"messages": []map[string]string{
			{"role": "user", "content": "Reply with only one word: ok"},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal LLM preflight request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build LLM preflight request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "AgentTeams/llm-preflight")

	resp, err := client.Do(req)
	if err != nil {
		return &llmPreflightTransportError{URL: endpoint, Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return &llmPreflightStatusError{
		StatusCode: resp.StatusCode,
		Body:       string(body),
		APIKey:     cfg.APIKey,
	}
}

func isRetryableLLMPreflightError(err error) bool {
	// 逻辑说明：context 终止不重试；网络错误、429 和 5xx 可重试，其他状态通常是确定配置错误。
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var transportErr *llmPreflightTransportError
	if errors.As(err, &transportErr) {
		return true
	}
	var statusErr *llmPreflightStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusTooManyRequests || statusErr.StatusCode >= 500
	}
	return false
}

func waitLLMPreflightRetryBackoff(ctx context.Context, baseDelay time.Duration, attempt int) error {
	// 逻辑说明：按尝试次数指数放大退避，并用可取消 timer 等待；零退避直接继续。
	if baseDelay <= 0 {
		return nil
	}
	delay := baseDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func preflightStatusHint(status int) string {
	// 逻辑说明：把常见 HTTP 状态翻译成不含响应正文的操作提示，未知状态使用保守通用描述。
	switch status {
	case http.StatusBadRequest:
		return "model name or request format was rejected"
	case http.StatusUnauthorized, http.StatusForbidden:
		return "API key is invalid, lacks permission, or the model is not enabled"
	case http.StatusNotFound:
		return "base URL path or model endpoint may be incorrect"
	case http.StatusTooManyRequests:
		return "quota is exhausted or the provider is rate limiting requests"
	default:
		if status >= 500 {
			return "provider service is unavailable"
		}
		return "provider rejected the preflight request"
	}
}

func sanitizePreflightBody(body, apiKey string) string {
	// 逻辑说明：去两端空白、替换所有 key 回显并限制 1000 字节，供应商错误不会把凭据或超长文本写入日志。
	body = strings.TrimSpace(body)
	if apiKey != "" {
		body = strings.ReplaceAll(body, apiKey, "[REDACTED]")
	}
	if len(body) > 1000 {
		body = body[:1000] + "...(truncated)"
	}
	return body
}

func envOrDefaultLocal(key, defaultVal string) string {
	// 逻辑说明：环境变量非空时优先，否则返回调用点给出的明确默认值。
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envIntDefaultLocal(key string, defaultVal int) int {
	// 逻辑说明：环境值仅在可解析为整数时生效，缺失或格式错误回退默认，后续领域校验再处理范围。
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}

func envBoolDefaultLocal(key string, defaultVal bool) bool {
	// 逻辑说明：接受常见真假拼写并忽略大小写/空白；未知文本保留默认值，避免误开启 strict 行为。
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return defaultVal
}
