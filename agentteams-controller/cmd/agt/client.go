package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"
)

// APIClient is a thin HTTP wrapper for the agentteams-controller REST API.
type APIClient struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

// APIError represents a non-2xx response from the controller.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	// 逻辑说明：把结构化 HTTP 状态与安全消息渲染为 CLI 错误文本，上层仍可用类型断言读取 StatusCode。
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

// NewAPIClient constructs a client from environment variables.
func NewAPIClient() *APIClient {
	// 逻辑说明：从环境解析并清理 Controller 基址，按优先级发现 token，并固定 30 秒 HTTP 超时。
	baseURL := os.Getenv("AGENTTEAMS_CONTROLLER_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8090"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	return &APIClient{
		BaseURL: baseURL,
		Token:   discoverToken(),
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// discoverToken returns a bearer token using the AgentTeams runtime contract:
//  1. AGENTTEAMS_AUTH_TOKEN env var
//  2. AGENTTEAMS_AUTH_TOKEN_FILE token file
//  3. empty string (unauthenticated, for controllers with auth disabled)
func discoverToken() string {
	// 逻辑说明：环境内 token 优先，其次读取并去空白 token 文件；读取失败或空文件安全降级为无认证。
	if token := os.Getenv("AGENTTEAMS_AUTH_TOKEN"); token != "" {
		return token
	}
	if path := os.Getenv("AGENTTEAMS_AUTH_TOKEN_FILE"); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			if t := strings.TrimSpace(string(data)); t != "" {
				return t
			}
		}
	}
	return ""
}

// Do sends an HTTP request and returns the raw response.
// body may be nil for methods that have no request body.
// Do 发送一次 Controller API 请求，并保留 HTTP response 给上层判断。
// token 作为 Authorization header 注入，不放在 URL，避免它进入代理日志、
// 浏览器历史或错误信息。请求 context 承载超时/取消，调用方不应在其后
// 绕过客户端重新发起一个无界等待的请求。
func (c *APIClient) Do(method, path string, body interface{}) (*http.Response, error) {
	// 逻辑说明：可选 body 序列化为 JSON，token 仅放 Authorization header，再交给有界 HTTP client 执行。
	url := c.BaseURL + path

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	return c.HTTPClient.Do(req)
}

// DoJSON sends a request, checks for 2xx, and decodes the response body into result.
// result may be nil if the caller does not need the response body (e.g. DELETE → 204).
// DoJSON 执行请求、检查非 2xx 错误并把成功回答解码到 result。
// 它为所有 CLI 子命令提供统一错误语义，因此 AgentScope workflow 能区分
// 认证失败、资源不存在与网络超时，而不需要解析人类文本。
func (c *APIClient) DoJSON(method, path string, body, result interface{}) error {
	// 逻辑说明：统一关闭/读取 response；非 2xx 优先提取 JSON error，成功且有目标时才解码响应对象。
	resp, err := c.Do(method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(respBody))
		// Try to extract "error" field from JSON error response
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			msg = errResp.Error
		}
		return &APIError{StatusCode: resp.StatusCode, Message: msg}
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// DoMultipart uploads a file via multipart/form-data.
// fieldName is the form field name for the file (e.g. "file").
// Extra string key-value pairs are sent as form fields.
func (c *APIClient) DoMultipart(path, fieldName, fileName string, fileData []byte, fields map[string]string, result interface{}) error {
	// 逻辑说明：在内存中构造完整 multipart 表单并关闭 writer 后发送，统一鉴权、状态错误提取和结果解码。
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			return fmt.Errorf("write field %s: %w", k, err)
		}
	}

	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		return fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(fileData); err != nil {
		return fmt.Errorf("write file data: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close multipart writer: %w", err)
	}

	url := c.BaseURL + path
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(respBody))
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			msg = errResp.Error
		}
		return &APIError{StatusCode: resp.StatusCode, Message: msg}
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// ResourceExists checks whether a resource exists by issuing a GET request.
// Returns true on 2xx, false on 404, and an error for other status codes.
func (c *APIClient) ResourceExists(path string) (bool, error) {
	// 逻辑说明：用 GET 探测并立即关闭 body；2xx/404 分别映射 true/false，其余状态保留为类型化 APIError。
	resp, err := c.Do("GET", path, nil)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, &APIError{StatusCode: resp.StatusCode, Message: "unexpected status checking resource"}
}
