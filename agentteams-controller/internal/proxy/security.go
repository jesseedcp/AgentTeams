package proxy

import (
	"fmt"
	"os"
	"strings"
)

// Higress registry pattern: higress-registry.{region}.cr.aliyuncs.com
const higressRegistrySuffix = ".cr.aliyuncs.com/"

func isHigressRegistry(image string) bool {
	// 逻辑说明：定位阿里云镜像仓库固定后缀，再要求后缀前缀以 higress-registry 开头；两项同时满足才允许任意地域的官方 Higress 镜像。
	// Match higress-registry-*.cr.aliyuncs.com/* or higress-registry.*.cr.aliyuncs.com/*
	idx := strings.Index(image, higressRegistrySuffix)
	if idx < 0 {
		return false
	}
	prefix := image[:idx]
	return strings.HasPrefix(prefix, "higress-registry")
}

func isLocalImage(image string) bool {
	// 逻辑说明：检查首个路径段是否含点号来区分显式远端 registry；无斜杠或首段无点按 Docker 本地镜像命名处理。
	// Local images have no dots before the first slash: e.g. "agentteams/worker-agent:latest"
	// Registry images have dots: e.g. "registry.example.com/repo/image:tag"
	slashIdx := strings.Index(image, "/")
	if slashIdx < 0 {
		// No slash at all (e.g. "ubuntu:latest") — treat as local
		return true
	}
	firstPart := image[:slashIdx]
	return !strings.Contains(firstPart, ".")
}

func isLocalhostImage(image string) bool {
	// 逻辑说明：只接受 localhost/127.0.0.1 的带端口或路径前缀，避免把名称中间偶然包含 localhost 的远端地址误判为本机镜像。
	return strings.HasPrefix(image, "localhost/") || strings.HasPrefix(image, "localhost:") ||
		strings.HasPrefix(image, "127.0.0.1/") || strings.HasPrefix(image, "127.0.0.1:")
}

// ContainerCreateRequest is a minimal representation of Docker's container create payload.
// Only fields relevant to security validation are included.
type ContainerCreateRequest struct {
	Image      string      `json:"Image"`
	HostConfig *HostConfig `json:"HostConfig,omitempty"`
}

type HostConfig struct {
	Binds       []string `json:"Binds,omitempty"`
	Mounts      []Mount  `json:"Mounts,omitempty"`
	Privileged  bool     `json:"Privileged,omitempty"`
	NetworkMode string   `json:"NetworkMode,omitempty"`
	PidMode     string   `json:"PidMode,omitempty"`
	CapAdd      []string `json:"CapAdd,omitempty"`
}

type Mount struct {
	Type string `json:"Type,omitempty"`
}

// SecurityValidator enforces container creation policies.
type SecurityValidator struct {
	AllowedRegistries []string
	ContainerPrefix   string
	DangerousCaps     map[string]bool
}

// NewSecurityValidator creates a validator from environment variables.
func NewSecurityValidator() *SecurityValidator {
	// 逻辑说明：解析并清理附加 registry 白名单，再按显式容器前缀、资源自动前缀开关和默认值的优先级确定命名边界，同时建立禁止提权 capability 集合。
	// Additional allowed image sources — can be a registry (e.g. "ghcr.io")
	// or registry+path (e.g. "ghcr.io/myorg", "registry.example.com/team/workers")
	var allowedRegistries []string
	if env := os.Getenv("AGENTTEAMS_PROXY_ALLOWED_REGISTRIES"); env != "" {
		for _, r := range strings.Split(env, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				allowedRegistries = append(allowedRegistries, r)
			}
		}
	}

	// Container name prefix: AGENTTEAMS_PROXY_CONTAINER_PREFIX takes precedence.
	// If unset and AGENTTEAMS_RESOURCE_AUTOPREFIX=true (default), derive from
	// AGENTTEAMS_RESOURCE_PREFIX with fallback "agentteams-". If auto-prefix is
	// disabled, keep prefix empty and skip prefix enforcement.
	autoPrefix := true
	if v := os.Getenv("AGENTTEAMS_RESOURCE_AUTOPREFIX"); v != "" {
		autoPrefix = v == "1" || v == "true" || v == "True" || v == "TRUE"
	}
	prefix := ""
	if env := os.Getenv("AGENTTEAMS_PROXY_CONTAINER_PREFIX"); env != "" {
		prefix = env
	} else if autoPrefix {
		rp := os.Getenv("AGENTTEAMS_RESOURCE_PREFIX")
		if rp == "" {
			rp = "agentteams-"
		}
		prefix = rp + "worker-"
	}

	return &SecurityValidator{
		AllowedRegistries: allowedRegistries,
		ContainerPrefix:   prefix,
		DangerousCaps: map[string]bool{
			"SYS_ADMIN":    true,
			"SYS_PTRACE":   true,
			"DAC_OVERRIDE": true,
			"NET_ADMIN":    true,
			"SYS_RAWIO":    true,
			"SYS_MODULE":   true,
		},
	}
}

// ValidateContainerCreate checks a container creation request against security policies.
func (v *SecurityValidator) ValidateContainerCreate(req ContainerCreateRequest, containerName string) error {
	// 逻辑说明：按容器名、镜像源、主机挂载、privileged、host 网络/PID 和危险 capability 顺序拒绝越权配置；HostConfig 缺失时在名称与镜像已通过后安全返回。
	// 1. Container name prefix
	if containerName != "" && !strings.HasPrefix(containerName, v.ContainerPrefix) {
		return fmt.Errorf("container name %q must start with %q", containerName, v.ContainerPrefix)
	}
	if strings.Contains(containerName, "/") || strings.Contains(containerName, "..") {
		return fmt.Errorf("container name %q contains invalid characters", containerName)
	}

	// 2. Image allowlist
	if !v.isImageAllowed(req.Image) {
		return fmt.Errorf("image %q is not allowed (not a local image, localhost, Higress registry, or configured registry)", req.Image)
	}

	if req.HostConfig == nil {
		return nil
	}

	// 3. No bind mounts (workers use MinIO, not host volumes)
	if len(req.HostConfig.Binds) > 0 {
		return fmt.Errorf("bind mounts are not allowed (got %d bind(s))", len(req.HostConfig.Binds))
	}
	for _, m := range req.HostConfig.Mounts {
		if strings.EqualFold(m.Type, "bind") {
			return fmt.Errorf("bind-type mounts are not allowed")
		}
	}

	// 4. No privileged mode
	if req.HostConfig.Privileged {
		return fmt.Errorf("privileged mode is not allowed")
	}

	// 5. No host network
	if req.HostConfig.NetworkMode == "host" {
		return fmt.Errorf("host network mode is not allowed")
	}

	// 6. No host PID
	if req.HostConfig.PidMode == "host" {
		return fmt.Errorf("host PID mode is not allowed")
	}

	// 7. No dangerous capabilities
	for _, cap := range req.HostConfig.CapAdd {
		if v.DangerousCaps[strings.ToUpper(cap)] {
			return fmt.Errorf("capability %q is not allowed", cap)
		}
	}

	return nil
}

func (v *SecurityValidator) isImageAllowed(image string) bool {
	// 逻辑说明：依次接受官方 Higress、本地命名、loopback registry 和管理员配置的 registry/path 前缀；全部不匹配才拒绝，避免任意公网镜像进入 Docker daemon。
	// Allow all images from Higress registries (any region)
	if isHigressRegistry(image) {
		return true
	}
	// Allow local images (no registry prefix, e.g. "agentteams/worker-agent:latest")
	if isLocalImage(image) {
		return true
	}
	// Allow localhost images
	if isLocalhostImage(image) {
		return true
	}
	// Check configured allowed image sources (registry or registry/path prefix)
	for _, reg := range v.AllowedRegistries {
		if strings.HasPrefix(image, reg+"/") {
			return true
		}
	}
	return false
}
