package oss

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// MinIOClient implements StorageClient using the mc (MinIO Client) CLI.
// This provides zero-migration-risk compatibility with the existing shell scripts
// while hiding the mc implementation detail behind the StorageClient interface.
//
// The client supports two credential modes:
//
//   - Static (default): AccessKey/SecretKey from Config are installed once via
//     `mc alias set` and reused for every subsequent command.
//   - Dynamic (credSource != nil): the client skips persistent alias setup and
//     instead exports MC_HOST_<alias> on every invocation, populated from
//     CredentialSource.Resolve. This mode is what the external-OSS deployment
//     uses to feed STS triples from the credential-provider sidecar.
type MinIOClient struct {
	config     Config
	credSource CredentialSource
	aliasReady bool
}

// NewMinIOClient creates a StorageClient backed by the mc CLI.
func NewMinIOClient(cfg Config) *MinIOClient {
	// 逻辑说明：为未配置的 mc 可执行文件和 alias 补安全默认值，只保存连接配置；此时不启动进程也不验证凭据，首次操作才建立 alias。
	if cfg.MCBinary == "" {
		cfg.MCBinary = "mc"
	}
	if cfg.Alias == "" {
		cfg.Alias = "agentteams"
	}
	return &MinIOClient{config: cfg}
}

// WithCredentialSource returns a copy of the client that fetches credentials
// dynamically on every mc invocation. Intended for external-OSS deployments
// where STS tokens expire periodically.
func (c *MinIOClient) WithCredentialSource(src CredentialSource) *MinIOClient {
	// 逻辑说明：复制 client 而不修改原实例，切换为每次命令动态解析 STS 凭据，并清除静态 alias 就绪标志，防止过期身份被复用。
	clone := *c
	clone.credSource = src
	clone.aliasReady = false
	return &clone
}

func (c *MinIOClient) ensureAlias(ctx context.Context) error {
	// 逻辑说明：动态凭据模式交给每次 runMC 注入环境变量；静态模式仅在首次需要时执行 mc alias set，命令成功后才标记就绪以允许失败重试。
	if c.credSource != nil {
		// Dynamic mode: no persistent alias. MC_HOST_* env vars are
		// prepared per call in runMC.
		return nil
	}
	if c.aliasReady || c.config.Endpoint == "" {
		return nil
	}
	_, err := c.runMC(ctx, "alias", "set", c.config.Alias, c.config.Endpoint, c.config.AccessKey, c.config.SecretKey)
	if err != nil {
		return fmt.Errorf("mc alias set: %w", err)
	}
	c.aliasReady = true
	return nil
}

func (c *MinIOClient) fullPath(key string) string {
	return c.config.StoragePrefix + "/" + strings.TrimPrefix(key, "/")
}

func (c *MinIOClient) PutObject(ctx context.Context, key string, data []byte) error {
	// 逻辑说明：先确保访问身份可用，再把内存数据写入受控临时文件并委托 PutFile 计算摘要上传；任何写入失败都会关闭文件，defer 最终删除本地副本。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp("", "agentteams-oss-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	tmpFile.Close()

	return c.PutFile(ctx, tmpFile.Name(), key)
}

func (c *MinIOClient) PutFile(ctx context.Context, localPath, key string) error {
	// 逻辑说明：先准备 alias/动态身份，再通过 copyArgs 读取源文件计算 SHA-256 属性，最后执行 mc cp；源不可读或命令失败均不报告对象已上传。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	args, err := c.copyArgs(localPath, key)
	if err != nil {
		return err
	}
	_, err = c.runMC(ctx, args...)
	return err
}

func (c *MinIOClient) copyArgs(localPath, key string) ([]string, error) {
	// 逻辑说明：打开并完整读取本地源计算内容摘要，把摘要随 mc cp 写入对象元数据；这样后续可以校验上传内容，而打开/读取失败会在外部命令前返回。
	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open upload source: %w", err)
	}
	defer file.Close()

	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return nil, fmt.Errorf("hash upload source: %w", err)
	}
	return []string{
		"cp",
		"--attr",
		fmt.Sprintf("sha256=%x", digest.Sum(nil)),
		localPath,
		c.fullPath(key),
	}, nil
}

func (c *MinIOClient) GetObject(ctx context.Context, key string) ([]byte, error) {
	// 逻辑说明：通过 mc cat 读取加前缀后的对象；把 MinIO 的不存在/退出状态归一为 os.ErrNotExist，便于上层用 errors.Is 区分首次创建与其他存储故障。
	if err := c.ensureAlias(ctx); err != nil {
		return nil, err
	}
	out, err := c.runMC(ctx, "cat", c.fullPath(key))
	if err != nil {
		if strings.Contains(err.Error(), "Object does not exist") ||
			strings.Contains(err.Error(), "exit status") {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return []byte(out), nil
}

func (c *MinIOClient) Stat(ctx context.Context, key string) error {
	// 逻辑说明：查询带存储前缀的对象元数据，并将对象不存在的 CLI 文本归一为 os.ErrNotExist；其他执行错误保留给调用方处理。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	_, err := c.runMC(ctx, "stat", c.fullPath(key))
	if err != nil {
		if strings.Contains(err.Error(), "Object does not exist") ||
			strings.Contains(err.Error(), "exit status") {
			return os.ErrNotExist
		}
		return err
	}
	return nil
}

func (c *MinIOClient) DeleteObject(ctx context.Context, key string) error {
	// 逻辑说明：执行单对象删除并把“对象已不存在”当作幂等成功，使 reconcile 重试不会因目标已清理而持续失败。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	_, err := c.runMC(ctx, "rm", c.fullPath(key))
	if err != nil && strings.Contains(err.Error(), "Object does not exist") {
		return nil
	}
	return err
}

func (c *MinIOClient) Mirror(ctx context.Context, src, dst string, opts MirrorOptions) error {
	// 逻辑说明：远端路径统一补 storage prefix，本地绝对路径保持原样；再把覆盖和排除规则翻译为 mc mirror 参数，命令失败原样返回以阻止错误对账。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	// Apply storage prefix to paths that are not local (don't start with /).
	// This makes Mirror consistent with PutObject/GetObject which auto-prefix keys.
	if !strings.HasPrefix(src, "/") {
		src = c.fullPath(src)
	}
	if !strings.HasPrefix(dst, "/") {
		dst = c.fullPath(dst)
	}
	args := []string{"mirror", src, dst}
	if opts.Overwrite {
		args = append(args, "--overwrite")
	}
	for _, pattern := range opts.Exclude {
		args = append(args, "--exclude", pattern)
	}
	_, err := c.runMC(ctx, args...)
	return err
}

func (c *MinIOClient) DeletePrefix(ctx context.Context, prefix string) error {
	// 逻辑说明：确保身份后对规范化前缀执行递归强制删除；context 取消会终止 mc 子进程，调用方据返回值决定是否重试清理。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	_, err := c.runMC(ctx, "rm", "--recursive", "--force", c.fullPath(prefix))
	return err
}

func (c *MinIOClient) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	// 逻辑说明：运行 mc ls 后逐行忽略空白，并从 CLI 的日期/大小/名称列中提取末列对象名；命令失败不返回可能不完整的列表。
	if err := c.ensureAlias(ctx); err != nil {
		return nil, err
	}
	out, err := c.runMC(ctx, "ls", c.fullPath(prefix))
	if err != nil {
		return nil, err
	}

	var names []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// mc ls output format: "[date] [size] filename"
		parts := strings.Fields(line)
		if len(parts) > 0 {
			names = append(names, parts[len(parts)-1])
		}
	}
	return names, nil
}

// EnsureBucket creates the configured bucket if it does not already exist.
func (c *MinIOClient) EnsureBucket(ctx context.Context) error {
	// 逻辑说明：在身份可用后对配置的 alias/bucket 执行带 --ignore-existing 的创建，使首次部署和重复 reconcile 使用同一幂等路径。
	if err := c.ensureAlias(ctx); err != nil {
		return err
	}
	target := c.config.Alias + "/" + c.config.Bucket
	_, err := c.runMC(ctx, "mb", target, "--ignore-existing")
	return err
}

func (c *MinIOClient) runMC(ctx context.Context, args ...string) (string, error) {
	// 逻辑说明：无标准输入的普通 mc 操作统一委托 runMCWithInput，从而共享动态 STS 注入、context 取消和 stderr 错误处理。
	return c.runMCWithInput(ctx, nil, args...)
}

func (c *MinIOClient) runMCWithInput(ctx context.Context, stdin []byte, args ...string) (string, error) {
	// 逻辑说明：构造受 context 管理的 mc 子进程并分别捕获 stdout/stderr；动态模式每次解析最新 STS 三元组并只注入该进程环境，执行失败返回带命令与 stderr 的诊断。
	cmd := exec.CommandContext(ctx, c.config.MCBinary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}

	if c.credSource != nil {
		creds, err := c.credSource.Resolve(ctx)
		if err != nil {
			return "", fmt.Errorf("resolve oss credentials: %w", err)
		}
		hostEnv, herr := buildMCHostEnv(c.config.Alias, c.config.Endpoint, creds)
		if herr != nil {
			return "", herr
		}
		cmd.Env = append(os.Environ(), hostEnv)
	}

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("mc %s: %w (stderr: %s)",
			strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// buildMCHostEnv renders a single MC_HOST_<alias>=<scheme>://<ak>:<sk>[:<token>]@<host>
// environment-variable binding. The mc CLI accepts this form as an
// alternative to persistent ~/.mc/config.json alias entries, and
// honours the security-token component when present.
//
// The endpoint is supplied by the caller (normally MinIOClient.config.Endpoint,
// sourced from AGENTTEAMS_FS_ENDPOINT). A bare hostname (e.g.
// "oss-cn-hangzhou.aliyuncs.com") without a URL scheme is accepted; in
// that case we default to https.
//
// IMPORTANT: mc (tested with RELEASE.2025-08-13) does NOT URL-decode the
// userinfo segment of MC_HOST_* before using the values. Any percent-
// encoding applied here is forwarded verbatim into the X-Amz-Security-
// Token header (and the signed AK/SK), which Alibaba Cloud OSS rejects
// with InvalidSecurityToken. We therefore pass the triple raw; STS
// credentials issued by Alibaba Cloud contain only characters (base64
// alphabet plus "+/=") that Go's url.Parse accepts inside userinfo.
func buildMCHostEnv(alias string, endpoint string, c Credentials) (string, error) {
	// 逻辑说明：校验并规范 endpoint（裸主机默认 HTTPS），按 mc 要求把 AK/SK/可选 token 原样放入 userinfo，最终生成单个 MC_HOST_alias 环境绑定；无效地址在启动 mc 前失败。
	if endpoint == "" {
		return "", fmt.Errorf("storage endpoint is not configured (AGENTTEAMS_FS_ENDPOINT is empty)")
	}
	normalized := endpoint
	if !strings.HasPrefix(normalized, "http://") && !strings.HasPrefix(normalized, "https://") {
		normalized = "https://" + normalized
	}
	u, err := url.Parse(normalized)
	if err != nil {
		return "", fmt.Errorf("parse endpoint %q: %w", endpoint, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("endpoint %q must include scheme and host", endpoint)
	}

	userinfo := c.AccessKeyID + ":" + c.AccessKeySecret
	if c.SecurityToken != "" {
		userinfo += ":" + c.SecurityToken
	}
	value := fmt.Sprintf("%s://%s@%s", u.Scheme, userinfo, u.Host)
	return fmt.Sprintf("MC_HOST_%s=%s", alias, value), nil
}
