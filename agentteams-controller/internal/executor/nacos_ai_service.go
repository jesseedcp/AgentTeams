package executor

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type nacosV3Response struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type nacosAgentSpec struct {
	NamespaceID string                             `json:"namespaceId"`
	Name        string                             `json:"name"`
	Description string                             `json:"description"`
	BizTags     string                             `json:"bizTags,omitempty"`
	Content     string                             `json:"content"`
	Resource    map[string]*nacosAgentSpecResource `json:"resource,omitempty"`
}

type nacosAgentSpecResource struct {
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	Content  string                 `json:"content"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type nacosAgentSpecMeta struct {
	NamespaceID string                          `json:"namespaceId"`
	Name        string                          `json:"name"`
	Description string                          `json:"description"`
	OnlineCnt   int                             `json:"onlineCnt"`
	Labels      map[string]string               `json:"labels,omitempty"`
	Versions    []nacosAgentSpecVersionMetadata `json:"versions,omitempty"`
}

type nacosAgentSpecVersionMetadata struct {
	Version string `json:"version"`
	Status  string `json:"status"`
}

type nacosAgentSpecSummary struct {
	NamespaceID string            `json:"namespaceId"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Enable      bool              `json:"enable"`
	Labels      map[string]string `json:"labels,omitempty"`
	OnlineCnt   int               `json:"onlineCnt"`
}

type nacosAgentSpecListResponse struct {
	TotalCount int                     `json:"totalCount"`
	PageItems  []nacosAgentSpecSummary `json:"pageItems"`
}

// GetSkill fetches a Skill ZIP from Nacos and extracts it into outputDir/{name}/.
// version may be empty (latest) or a specific version string.
// label may be used instead of version; both may not be set simultaneously.
func (c *NacosAIClient) GetSkill(ctx context.Context, name, outputDir, version, label string) error {
	// 逻辑说明：组装 namespace/name/version/label 请求并刷新认证后下载 Skill zip 到临时文件，再创建目标目录并安全解压；HTTP 错误分类返回，临时文件始终删除，失败不会报告可用 Skill。
	params := url.Values{}
	params.Set("namespaceId", c.namespace)
	params.Set("name", name)
	if version != "" {
		params.Set("version", version)
	}
	if label != "" {
		params.Set("label", label)
	}

	apiURL := fmt.Sprintf("http://%s/nacos/v3/client/ai/skills?%s", c.serverAddr, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	if err := c.prepareRequest(ctx, req); err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get skill: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return parseNacosHTTPError(resp.StatusCode, body, "get skill")
	}

	// Write the ZIP response to a temp file.
	tmp, err := os.CreateTemp("", "nacos-skill-*.zip")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return fmt.Errorf("failed to download skill ZIP: %w", err)
	}
	tmp.Close()

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create skill directory: %w", err)
	}

	if err := extractSkillZip(tmpPath, outputDir); err != nil {
		return fmt.Errorf("failed to extract skill ZIP for %s: %w", name, err)
	}
	return nil
}

func extractSkillZip(zipPath, outputDir string) error {
	// 逻辑说明：逐个校验 zip 条目路径、文件类型和最终绝对路径必须位于目标根内，拒绝反斜杠、绝对路径、`..`、符号链接和特殊文件后才创建目录/写文件，防止 Zip Slip。
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	outputAbs, err := filepath.Abs(outputDir)
	if err != nil {
		return err
	}

	for _, entry := range reader.File {
		rel, err := cleanZipEntryName(entry.Name)
		if err != nil {
			return fmt.Errorf("unsafe ZIP entry %q: %w", entry.Name, err)
		}

		mode := entry.FileInfo().Mode()
		if mode&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe ZIP entry %q: symlinks are not allowed", entry.Name)
		}
		if typ := mode.Type(); typ != 0 && typ != os.ModeDir {
			return fmt.Errorf("unsafe ZIP entry %q: special files are not allowed", entry.Name)
		}

		dest := filepath.Join(outputDir, filepath.FromSlash(rel))
		destAbs, err := filepath.Abs(dest)
		if err != nil {
			return err
		}
		if !isPathInside(destAbs, outputAbs) {
			return fmt.Errorf("unsafe ZIP entry %q: escapes destination", entry.Name)
		}

		if entry.FileInfo().IsDir() {
			if err := os.MkdirAll(destAbs, 0o755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(destAbs), 0o755); err != nil {
			return err
		}
		if err := writeZipFile(entry, destAbs); err != nil {
			return err
		}
	}
	return nil
}

func cleanZipEntryName(name string) (string, error) {
	// 逻辑说明：拒绝空名、Windows 反斜杠/卷名、绝对路径与清理后上跳路径，返回统一斜杠相对名；这是解压前第一道路径穿越门禁。
	if name == "" || strings.Contains(name, "\\") || filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
		return "", fmt.Errorf("invalid path")
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path traversal is not allowed")
	}
	return clean, nil
}

func isPathInside(pathAbs, rootAbs string) bool {
	// 逻辑说明：用 filepath.Rel 判断目标绝对路径等于或位于根目录下；关系计算失败、`..` 或以父目录开头均返回 false，兼容平台分隔符。
	if pathAbs == rootAbs {
		return true
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func writeZipFile(entry *zip.File, dest string) error {
	// 逻辑说明：打开 zip 条目，保留安全权限位或回退 0644，以截断方式写入已校验目标并复制完整内容；输入输出句柄均 defer 关闭，任何 I/O 错误返回。
	in, err := entry.Open()
	if err != nil {
		return err
	}
	defer in.Close()

	mode := entry.FileInfo().Mode().Perm()
	if mode == 0 {
		mode = 0o644
	}
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func (c *NacosAIClient) GetAgentSpec(ctx context.Context, name, outputDir string, version, label string) error {
	// 逻辑说明：把不需要摘要的兼容调用委托给完整下载流程；仍会访问 Nacos 并写入 outputDir，只丢弃成功时返回的规范摘要，失败原样上抛。
	_, err := c.GetAgentSpecWithDigest(ctx, name, outputDir, version, label)
	return err
}

// GetAgentSpecWithDigest materializes an AgentSpec and returns a canonical
// digest of the logical Nacos payload. The digest is independent of JSON key
// ordering and is used to bind Manager discovery to Controller fetch.
func (c *NacosAIClient) GetAgentSpecWithDigest(ctx context.Context, name, outputDir string, version, label string) (string, error) {
	// 逻辑说明：获取 AgentSpec 后按资源类型/名称落盘，按 metadata 解码 base64，写 manifest，最后返回逻辑 payload 的规范摘要；目录、解码或写入任一步失败均不返回 digest。
	spec, err := c.fetchAgentSpec(ctx, name, version, label)
	if err != nil {
		return "", err
	}

	specDir := filepath.Join(outputDir, name)
	if err := os.MkdirAll(specDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	for _, res := range spec.Resource {
		if res == nil || res.Content == "" {
			continue
		}

		rel := buildAgentSpecResourcePath(res)
		if rel == "" {
			continue
		}

		filePath := filepath.Join(specDir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			return "", fmt.Errorf("failed to create resource directory: %w", err)
		}

		data := []byte(res.Content)
		if encoding, ok := res.Metadata["encoding"].(string); ok && encoding == "base64" {
			decoded, err := base64.StdEncoding.DecodeString(res.Content)
			if err != nil {
				return "", fmt.Errorf("failed to decode base64 resource %s: %w", res.Name, err)
			}
			data = decoded
		}

		if err := os.WriteFile(filePath, data, 0o644); err != nil {
			return "", fmt.Errorf("failed to write resource file %s: %w", res.Name, err)
		}
	}

	if err := writeAgentSpecManifest(specDir, spec.Content); err != nil {
		return "", err
	}
	return digestNacosAgentSpec(spec)
}

func digestNacosAgentSpec(spec *nacosAgentSpec) (string, error) {
	// 逻辑说明：先编码类型对象、再解码成通用 JSON 并重新编码以稳定 map 键顺序，最后计算 SHA-256 并加算法前缀；任一规范化失败均返回上下文错误。
	raw, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("failed to encode agentspec digest: %w", err)
	}
	var canonical interface{}
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return "", fmt.Errorf("failed to normalize agentspec digest: %w", err)
	}
	normalized, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize agentspec digest: %w", err)
	}
	digest := sha256.Sum256(normalized)
	return fmt.Sprintf("sha256:%x", digest), nil
}

func (c *NacosAIClient) CheckAgentSpecExists(ctx context.Context, name, version, label string) error {
	// 逻辑说明：先精确查询摘要确认 AgentSpec 启用且至少有在线版本；指定 version/label 时再取实际 spec 验证指向有效版本，并把 404 改写成具体可操作提示。
	summary, err := c.fetchAgentSpecSummary(ctx, name)
	if err != nil {
		return err
	}

	if !summary.Enable {
		return formatNacosHTTPError("check agentspec", http.StatusNotFound, "", fmt.Sprintf("agentspec %q is disabled", name))
	}
	if summary.OnlineCnt <= 0 {
		return formatNacosHTTPError("check agentspec", http.StatusNotFound, "", fmt.Sprintf("agentspec %q has no online version", name))
	}
	if version == "" && label == "" {
		return nil
	}

	if _, err := c.fetchAgentSpec(ctx, name, version, label); err != nil {
		if isNacosHTTPStatus(err, http.StatusNotFound) {
			if version != "" {
				return formatNacosHTTPError("check agentspec", http.StatusNotFound, "", fmt.Sprintf("online version %q not found for agentspec %q", version, name))
			}
			if label != "" {
				return formatNacosHTTPError("check agentspec", http.StatusNotFound, "", fmt.Sprintf("label %q for agentspec %q does not point to an online version", label, name))
			}
		}
		return err
	}
	return nil
}

func isNacosHTTPStatus(err error, statusCode int) bool {
	// 逻辑说明：nil 返回 false，否则匹配统一 Nacos 错误格式中的 HTTP 状态标记；只用于把 404 细化为版本/标签提示。
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), fmt.Sprintf("(HTTP %d)", statusCode))
}

func (c *NacosAIClient) fetchAgentSpecSummary(ctx context.Context, name string) (*nacosAgentSpecSummary, error) {
	// 逻辑说明：向 Nacos 管理列表接口发起精确单项查询，刷新认证并验证 HTTP/v3 业务码与 JSON 层级；只有返回项名称完全匹配才成功，否则构造类型化 404 提示。
	params := url.Values{}
	params.Set("namespaceId", c.namespace)
	params.Set("agentSpecName", name)
	params.Set("search", "accurate")
	params.Set("pageNo", "1")
	params.Set("pageSize", "1")

	apiURL := fmt.Sprintf("http://%s/nacos/v3/admin/ai/agentspecs/list?%s", c.serverAddr, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	if err := c.prepareRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get agentspec meta: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, parseNacosHTTPError(resp.StatusCode, respBody, "check agentspec")
	}

	var v3Resp nacosV3Response
	if err := json.Unmarshal(respBody, &v3Resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	if v3Resp.Code != 0 {
		return nil, fmt.Errorf("check agentspec failed: code=%d, message=%s", v3Resp.Code, v3Resp.Message)
	}

	var listResp nacosAgentSpecListResponse
	if err := json.Unmarshal(v3Resp.Data, &listResp); err != nil {
		return nil, fmt.Errorf("failed to parse agentspec list: %w", err)
	}
	for _, item := range listResp.PageItems {
		if item.Name == name {
			return &item, nil
		}
	}
	return nil, formatNacosHTTPError("check agentspec", http.StatusNotFound, "", fmt.Sprintf("agentspec %q not found", name))
}

func (c *NacosAIClient) fetchAgentSpec(ctx context.Context, name, version, label string) (*nacosAgentSpec, error) {
	// 逻辑说明：按 namespace/name 及可选 version/label 调用 Nacos 客户端接口，准备认证并依次验证 HTTP、v3 code 和数据 JSON；响应体始终关闭，失败不返回部分 spec。
	params := url.Values{}
	params.Set("namespaceId", c.namespace)
	params.Set("name", name)
	if version != "" {
		params.Set("version", version)
	}
	if label != "" {
		params.Set("label", label)
	}

	apiURL := fmt.Sprintf("http://%s/nacos/v3/client/ai/agentspecs?%s", c.serverAddr, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	if err := c.prepareRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get agentspec: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, parseNacosHTTPError(resp.StatusCode, respBody, "get agentspec")
	}

	var v3Resp nacosV3Response
	if err := json.Unmarshal(respBody, &v3Resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	if v3Resp.Code != 0 {
		return nil, fmt.Errorf("get agentspec failed: code=%d, message=%s", v3Resp.Code, v3Resp.Message)
	}

	var spec nacosAgentSpec
	if err := json.Unmarshal(v3Resp.Data, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse agentspec: %w", err)
	}
	return &spec, nil
}

func buildAgentSpecResourcePath(res *nacosAgentSpecResource) string {
	// 逻辑说明：用去空白后的资源 type 作为目录前缀；name 已含同前缀时不重复，type 为空保留 name，nil 返回空路径供调用方跳过。
	if res == nil {
		return ""
	}

	resourceType := strings.TrimSpace(res.Type)
	resourceName := strings.TrimSpace(res.Name)
	if resourceType == "" {
		return resourceName
	}

	prefix := resourceType + "/"
	if strings.HasPrefix(resourceName, prefix) {
		return resourceName
	}
	return prefix + resourceName
}

func writeAgentSpecManifest(specDir, content string) error {
	// 逻辑说明：内容是合法 JSON 时尽量格式化缩进，非法/无法缩进则保留原文，再以 0644 写入固定 `manifest.json`；不解释或执行内容。
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(content), &raw); err == nil {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, raw, "", "  "); err == nil {
			content = pretty.String()
		}
	}

	return os.WriteFile(filepath.Join(specDir, "manifest.json"), []byte(content), 0o644)
}

func parseNacosHTTPError(statusCode int, body []byte, operation string) error {
	// 逻辑说明：优先从 v3 JSON 提取服务端 message，再按 401/403/404/500 添加定向排障提示；未知响应限制正文到 200 字符，避免错误日志无限放大。
	serverMessage := ""
	if len(body) > 0 {
		var response nacosV3Response
		if err := json.Unmarshal(body, &response); err == nil && response.Message != "" {
			serverMessage = response.Message
		}
	}

	switch statusCode {
	case http.StatusUnauthorized:
		return formatNacosHTTPError(operation, statusCode, serverMessage, "authentication required; check username:password in the nacos URL or set AGENTTEAMS_NACOS_USERNAME/AGENTTEAMS_NACOS_PASSWORD")
	case http.StatusForbidden:
		return formatNacosHTTPError(operation, statusCode, serverMessage, "access denied; token may be expired or permissions may be missing")
	case http.StatusNotFound:
		return formatNacosHTTPError(operation, statusCode, serverMessage, "resource not found; check the namespace, name, version, or label")
	case http.StatusInternalServerError:
		return formatNacosHTTPError(operation, statusCode, serverMessage, "server internal error; inspect Nacos logs for details")
	default:
		if serverMessage != "" {
			return fmt.Errorf("%s failed (HTTP %d): %s", operation, statusCode, serverMessage)
		}
		if len(body) > 0 {
			bodyText := strings.TrimSpace(string(body))
			if len(bodyText) > 200 {
				bodyText = bodyText[:200] + "..."
			}
			return fmt.Errorf("%s failed (HTTP %d): %s", operation, statusCode, bodyText)
		}
		return fmt.Errorf("%s failed (HTTP %d)", operation, statusCode)
	}
}

func formatNacosHTTPError(operation string, statusCode int, serverMessage string, hint string) error {
	// 逻辑说明：统一组合操作名、HTTP 状态、可选服务端消息和本地提示；服务端无消息时仍保留 hint，方便上层字符串识别状态。
	if serverMessage != "" {
		return fmt.Errorf("%s failed (HTTP %d): %s; hint: %s", operation, statusCode, serverMessage, hint)
	}
	return fmt.Errorf("%s failed (HTTP %d): %s", operation, statusCode, hint)
}
