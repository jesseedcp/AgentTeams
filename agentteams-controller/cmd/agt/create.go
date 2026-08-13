package main

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/spf13/cobra"
)

func createCmd() *cobra.Command {
	// 逻辑说明：创建资源命令组并注册 Worker、Team、Human、Manager 四个独立创建入口。
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a resource",
	}
	cmd.AddCommand(createWorkerCmd())
	cmd.AddCommand(createTeamCmd())
	cmd.AddCommand(createHumanCmd())
	cmd.AddCommand(createManagerCmd())
	return cmd
}

// ---------------------------------------------------------------------------
// create worker
// ---------------------------------------------------------------------------

func createWorkerCmd() *cobra.Command {
	// 逻辑说明：解析并校验 Worker flag，提交异步创建；可立即返回，也可轮询到 Ready/Failed/超时再输出。
	var (
		name        string
		model       string
		runtime     string
		image       string
		identity    string
		soul        string
		soulFile    string
		skills      string
		packageURI  string
		expose      string
		console     bool
		consolePort int
		outputFmt   string
		waitTimeout time.Duration
		noWait      bool
	)

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Create a Worker",
		Long: `Create a new Worker resource via the controller REST API.

  agt create worker --name alice --model qwen3.6-plus
  agt create worker --name alice --soul-file /path/to/SOUL.md --skills github-operations
  agt create worker --name charlie --runtime copaw --expose 8080,3000
  To configure CPU/memory resources, use a YAML manifest and pass it with 'agt apply -f worker.yaml'.
  To configure mcpServers, use a YAML manifest and pass it with 'agt apply -f worker.yaml'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if err := validateWorkerName(name); err != nil {
				return err
			}
			if model == "" {
				model = defaultWorkerModel()
			}
			if soulFile != "" {
				data, err := os.ReadFile(soulFile)
				if err != nil {
					return fmt.Errorf("read --soul-file %q: %w", soulFile, err)
				}
				soul = string(data)
			}
			if packageURI != "" {
				var err error
				packageURI, err = expandPackageURI(packageURI)
				if err != nil {
					return err
				}
			}

			req := map[string]interface{}{
				"name":  name,
				"model": model,
			}
			setIfNotEmpty(req, "runtime", runtime)
			setIfNotEmpty(req, "image", image)
			setIfNotEmpty(req, "identity", identity)
			setIfNotEmpty(req, "soul", soul)
			setIfNotEmpty(req, "package", packageURI)
			if skills != "" {
				req["skills"] = splitCSV(skills)
			}
			if expose != "" {
				ports, err := parseExposePorts(expose)
				if err != nil {
					return fmt.Errorf("--expose: %w", err)
				}
				req["expose"] = ports
			}
			if console || cmd.Flags().Changed("console-port") {
				if consolePort < 1 || consolePort > 65535 {
					return fmt.Errorf("--console-port must be between 1 and 65535")
				}
				req["console"] = map[string]interface{}{
					"enabled": true,
					"port":    consolePort,
				}
			}

			client := NewAPIClient()
			var createResp map[string]interface{}
			if err := client.DoJSON("POST", "/api/v1/workers", req, &createResp); err != nil {
				return fmt.Errorf("create worker: %w", err)
			}

			if noWait {
				if outputFmt == "json" {
					printJSON(createResp)
				} else {
					fmt.Printf("worker/%s create accepted (poll `agt get workers -o json` for phase=Running)\n", name)
				}
				return nil
			}

			finalStatus, err := waitForWorkerReady(client, name, waitTimeout)
			if err != nil {
				return err
			}

			if outputFmt == "json" {
				printJSON(finalStatus)
			} else {
				fmt.Printf("worker/%s ready\n", name)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Worker name (required)")
	cmd.Flags().StringVar(&model, "model", "", "LLM model ID (default: $AGENTTEAMS_DEFAULT_MODEL, else qwen3.6-plus)")
	cmd.Flags().StringVar(&runtime, "runtime", "", "Agent runtime (openclaw|copaw|hermes|qwenpaw)")
	cmd.Flags().StringVar(&image, "image", "", "Container image override")
	cmd.Flags().StringVar(&identity, "identity", "", "Worker identity description")
	cmd.Flags().StringVar(&soul, "soul", "", "Worker SOUL.md content (inline)")
	cmd.Flags().StringVar(&soulFile, "soul-file", "", "Path to SOUL.md file (overrides --soul)")
	cmd.Flags().StringVar(&skills, "skills", "", "Comma-separated built-in skills")
	cmd.Flags().StringVar(&packageURI, "package", "", "Package URI (nacos://[?authType=...], http://, oss://) or shorthand")
	cmd.Flags().StringVar(&expose, "expose", "", "Comma-separated ports to expose (e.g. 8080,3000)")
	cmd.Flags().BoolVar(&console, "console", false, "Enable the CoPaw/QwenPaw web console")
	cmd.Flags().IntVar(
		&consolePort,
		"console-port",
		v1beta1.DefaultWorkerConsolePort,
		"Web console container port (implies --console)",
	)
	cmd.Flags().StringVarP(&outputFmt, "output", "o", "", "Output format (json)")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", 3*time.Minute, "Maximum time to wait for the Worker to report Ready")
	cmd.Flags().BoolVar(&noWait, "no-wait", false, "Return immediately after the controller accepts the create request, without polling for Ready")
	return cmd
}

// waitForWorkerReady 在 Worker CR 已被接受后轮询实际运行状态。
// “创建 CR”与“Agent Ready”之间包含账号、房间、配置、容器与网关等
// 多个异步阶段，因此这里不把 POST 成功误报为 Worker 已可用。启动窗口
// 中的 404/5xx 可重试，而显式 Failed 立即返回详细状态。
func waitForWorkerReady(client *APIClient, name string, timeout time.Duration) (*workerResp, error) {
	// 逻辑说明：每两秒读取真实 status，Ready 成功、Failed 立即失败；仅 404/5xx 可重试，截止时带最后状态返回。
	deadline := time.Now().Add(timeout)
	last := &workerResp{Name: name, Phase: "Pending"}

	for {
		var resp workerResp
		err := client.DoJSON("GET", "/api/v1/workers/"+name+"/status", nil, &resp)
		if err == nil {
			last = &resp
			switch resp.Phase {
			case "Ready":
				return &resp, nil
			case "Failed":
				return nil, fmt.Errorf("worker/%s failed during startup: %s", name, renderWorkerStatusSummary(&resp))
			}
		} else {
			var apiErr *APIError
			if !isRetryableWorkerStatusError(err, &apiErr) {
				return nil, fmt.Errorf("wait for worker/%s ready: %w", name, err)
			}
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("worker/%s did not become ready within %s (last status: %s)", name, timeout, renderWorkerStatusSummary(last))
		}

		time.Sleep(2 * time.Second)
	}
}

func isRetryableWorkerStatusError(err error, apiErr **APIError) bool {
	// 逻辑说明：只把类型化 APIError 的 404 或 5xx 判为启动期瞬时错误，并可把解析出的错误回传调用方。
	if err == nil {
		return false
	}
	typed, ok := err.(*APIError)
	if !ok {
		return false
	}
	if apiErr != nil {
		*apiErr = typed
	}
	return typed.StatusCode == 404 || typed.StatusCode >= 500
}

func renderWorkerStatusSummary(resp *workerResp) string {
	// 逻辑说明：从非空 phase/container state/message 生成诊断摘要；没有可用字段时稳定返回 unknown。
	if resp == nil {
		return "unknown"
	}

	parts := []string{}
	if phase := strings.TrimSpace(resp.Phase); phase != "" {
		parts = append(parts, "phase="+phase)
	}
	if state := strings.TrimSpace(resp.ContainerState); state != "" {
		parts = append(parts, "state="+state)
	}
	if msg := strings.TrimSpace(resp.Message); msg != "" {
		parts = append(parts, "message="+msg)
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, ", ")
}

// ---------------------------------------------------------------------------
// create team
// ---------------------------------------------------------------------------

func createTeamCmd() *cobra.Command {
	// 逻辑说明：要求 Team/Leader 名，构造 Leader 加普通 Worker 的有角色成员表，并附可选管理员和协调配置创建资源。
	var (
		name                 string
		teamName             string
		leaderName           string
		leaderHeartbeatEvery string
		workers              string
		description          string
		adminName            string
		adminMatrixID        string
		peerMentions         bool
	)

	cmd := &cobra.Command{
		Use:   "team",
		Short: "Create a Team",
		Long: `Create a new Team resource that references existing Worker resources.

  agt create team --name alpha --leader-name alpha-lead
  agt create team --name alpha --leader-name alpha-lead --workers alice,bob

Create or update each Worker separately to configure its model, runtime, image,
resources, skills, and lifecycle state.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if leaderName == "" {
				return fmt.Errorf("--leader-name is required")
			}

			workerMembers := []interface{}{
				map[string]interface{}{"name": leaderName, "role": "team_leader"},
			}
			if workers != "" {
				for _, w := range splitCSV(workers) {
					workerMembers = append(workerMembers, map[string]interface{}{"name": w, "role": "worker"})
				}
			}

			req := map[string]interface{}{
				"name":          name,
				"workerMembers": workerMembers,
			}
			setIfNotEmpty(req, "teamName", teamName)
			setIfNotEmpty(req, "description", description)
			setIfNotEmpty(req, "heartbeatEvery", leaderHeartbeatEvery)
			if adminName != "" {
				req["admin"] = map[string]interface{}{"name": adminName, "matrixUserId": adminMatrixID}
			}
			req["peerMentions"] = peerMentions

			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("POST", "/api/v1/teams", req, &resp); err != nil {
				return fmt.Errorf("create team: %w", err)
			}
			fmt.Printf("team/%s created\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Team name (required)")
	cmd.Flags().StringVar(&teamName, "team-name", "", "Runtime/storage team name (defaults to --name)")
	cmd.Flags().StringVar(&leaderName, "leader-name", "", "Leader worker name (required)")
	cmd.Flags().StringVar(&leaderHeartbeatEvery, "leader-heartbeat-every", "", "Leader heartbeat interval (e.g. 30m)")
	cmd.Flags().StringVar(&workers, "workers", "", "Comma-separated existing Worker resource names")
	cmd.Flags().StringVar(&description, "description", "", "Team description")
	cmd.Flags().StringVar(&adminName, "admin", "", "Existing Human resource used as Team Admin")
	cmd.Flags().StringVar(&adminMatrixID, "admin-matrix-id", "", "Expected Matrix user ID for the Team Admin")
	cmd.Flags().BoolVar(&peerMentions, "peer-mentions", true, "Allow Team Workers to mention peers")
	return cmd
}

// ---------------------------------------------------------------------------
// create human
// ---------------------------------------------------------------------------

func createHumanCmd() *cobra.Command {
	// 逻辑说明：要求稳定名称和显示名，解析可访问资源列表与权限字段后创建 Human CR。
	var (
		name              string
		displayName       string
		email             string
		permissionLevel   int
		accessibleTeams   string
		accessibleWorkers string
		note              string
	)

	cmd := &cobra.Command{
		Use:   "human",
		Short: "Create a Human user",
		Long: `Create a new Human resource (Matrix account + room access).

  agt create human --name bob --display-name "Bob Chen"
  agt create human --name alice --display-name "Alice" --email alice@example.com --permission-level 50`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if displayName == "" {
				return fmt.Errorf("--display-name is required")
			}

			req := map[string]interface{}{
				"name":            name,
				"displayName":     displayName,
				"permissionLevel": permissionLevel,
			}
			setIfNotEmpty(req, "email", email)
			setIfNotEmpty(req, "note", note)
			if accessibleTeams != "" {
				req["accessibleTeams"] = splitCSV(accessibleTeams)
			}
			if accessibleWorkers != "" {
				req["accessibleWorkers"] = splitCSV(accessibleWorkers)
			}

			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("POST", "/api/v1/humans", req, &resp); err != nil {
				return fmt.Errorf("create human: %w", err)
			}
			fmt.Printf("human/%s created\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Human username (required)")
	cmd.Flags().StringVar(&displayName, "display-name", "", "Display name (required)")
	cmd.Flags().StringVar(&email, "email", "", "Email address")
	cmd.Flags().IntVar(&permissionLevel, "permission-level", 0, "Permission level (0-100)")
	cmd.Flags().StringVar(&accessibleTeams, "accessible-teams", "", "Comma-separated team names")
	cmd.Flags().StringVar(&accessibleWorkers, "accessible-workers", "", "Comma-separated worker names")
	cmd.Flags().StringVar(&note, "note", "", "Note for the Human user")
	return cmd
}

// ---------------------------------------------------------------------------
// create manager
// ---------------------------------------------------------------------------

func createManagerCmd() *cobra.Command {
	// 逻辑说明：要求 Manager 名与模型，只附加显式 runtime/image/identity/soul 后提交创建。
	var (
		name     string
		model    string
		runtime  string
		image    string
		identity string
		soul     string
	)

	cmd := &cobra.Command{
		Use:   "manager",
		Short: "Create a Manager agent",
		Long: `Create a new Manager resource.

  agt create manager --name default --model qwen3.6-plus
  agt create manager --name default --model claude-sonnet-4-6 --runtime agentscope
  To configure CPU/memory resources, use a YAML manifest and pass it with 'agt apply -f manager.yaml'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if model == "" {
				return fmt.Errorf("--model is required")
			}

			req := map[string]interface{}{
				"name":  name,
				"model": model,
			}
			setIfNotEmpty(req, "runtime", runtime)
			setIfNotEmpty(req, "image", image)
			setIfNotEmpty(req, "identity", identity)
			setIfNotEmpty(req, "soul", soul)

			client := NewAPIClient()
			var resp map[string]interface{}
			if err := client.DoJSON("POST", "/api/v1/managers", req, &resp); err != nil {
				return fmt.Errorf("create manager: %w", err)
			}
			fmt.Printf("manager/%s created\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Manager name (required)")
	cmd.Flags().StringVar(&model, "model", "", "LLM model ID (required)")
	cmd.Flags().StringVar(&runtime, "runtime", "", "Manager runtime (agentscope)")
	cmd.Flags().StringVar(&image, "image", "", "Container image override")
	cmd.Flags().StringVar(&identity, "identity", "", "Manager identity section")
	cmd.Flags().StringVar(&soul, "soul", "", "Manager SOUL.md content")
	return cmd
}

// ---------------------------------------------------------------------------
// Helpers (migrated from old main.go)
// ---------------------------------------------------------------------------

var workerNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// defaultWorkerModel returns the model ID to use when a CLI flag does not
// specify --model. It prefers the install-time configured model
// (AGENTTEAMS_DEFAULT_MODEL, propagated by the controller into both the manager
// and worker containers via WorkerEnvBuilder); only when the env var is unset
// does it fall back to the "qwen3.6-plus" default. Without this
// fallback every `agt create worker` / `agt apply worker` invoked by the
// Manager Agent would silently override the admin's install-time model choice.
func defaultWorkerModel() string {
	// 逻辑说明：优先沿用安装时传播的默认模型并去空白，未配置才使用内置 qwen 默认，避免 CLI 偷换模型。
	if m := strings.TrimSpace(os.Getenv("AGENTTEAMS_DEFAULT_MODEL")); m != "" {
		return m
	}
	return "qwen3.6-plus"
}

func validateWorkerName(name string) error {
	// 逻辑说明：去空白后要求 DNS 风格小写名称，防止非法资源名进入 REST 路径或 Kubernetes metadata。
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("invalid worker name: name is required")
	}
	if !workerNamePattern.MatchString(name) {
		return fmt.Errorf("invalid worker name %q: must start with a lowercase letter or digit and contain only lowercase letters, digits, and hyphens", name)
	}
	return nil
}

func expandPackageURI(raw string) (string, error) {
	// 逻辑说明：完整 URI 原样保留；简写则验证 Nacos registry 基址并逐段 PathEscape，拒绝空路径段。
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.Contains(raw, "://") {
		return raw, nil
	}

	base := strings.TrimSpace(os.Getenv("AGENTTEAMS_NACOS_REGISTRY_URI"))
	if base == "" {
		base = "nacos://market.agentteams.io:80/public"
	}
	if !strings.HasPrefix(base, "nacos://") {
		return "", fmt.Errorf("invalid AGENTTEAMS_NACOS_REGISTRY_URI %q: must start with nacos://", base)
	}
	base = strings.TrimRight(base, "/")
	if base == "nacos:" || base == "nacos:/" || base == "nacos://" {
		return "", fmt.Errorf("invalid AGENTTEAMS_NACOS_REGISTRY_URI %q: missing host/namespace", base)
	}

	parts := strings.Split(raw, "/")
	encoded := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", fmt.Errorf("invalid package shorthand %q: empty path segment", raw)
		}
		encoded = append(encoded, url.PathEscape(part))
	}

	return base + "/" + strings.Join(encoded, "/"), nil
}

func splitCSV(s string) []string {
	// 逻辑说明：按逗号拆分、去两侧空白并过滤空项，保持用户输入顺序供成员/技能列表使用。
	result := make([]string, 0)
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func parseExposePorts(s string) ([]map[string]interface{}, error) {
	// 逻辑说明：复用 CSV 解析，逐个校验 1..65535 且去重，再转成 Controller API 所需对象数组。
	ports := make([]map[string]interface{}, 0)
	seen := make(map[int]struct{})
	for _, p := range splitCSV(s) {
		value, err := strconv.Atoi(p)
		if err != nil || value < 1 || value > 65535 {
			return nil, fmt.Errorf("port %q must be an integer from 1 to 65535", p)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("port %d must be unique", value)
		}
		seen[value] = struct{}{}
		port := map[string]interface{}{"port": value}
		ports = append(ports, port)
	}
	return ports, nil
}

func setIfNotEmpty(m map[string]interface{}, key, value string) {
	// 逻辑说明：只写入显式非空字符串，省略字段不会意外覆盖 Controller 默认值。
	if value != "" {
		m[key] = value
	}
}
