package service

import (
	"strings"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/config"
)

func TestWorkerEnvBuilderBuildIncludesFinalRuntimeEnv(t *testing.T) {
	builder := NewWorkerEnvBuilder(config.WorkerEnvDefaults{
		MatrixDomain:  "matrix.example.com",
		FSEndpoint:    "http://fs.example.com:9000",
		FSBucket:      "agentteams-fs",
		StoragePrefix: "teams/demo",
		ControllerURL: "http://controller.example.com:8090",
		AIGatewayURL:  "http://aigw.example.com:8080",
		MatrixURL:     "http://matrix.example.com:8080",
		Runtime:       "docker",
		SkillsAPIURL:  "nacos://skills.example.com:8848/public",
		NacosAuthType: "sts-agentteams",
	})

	env := builder.Build("alice", &WorkerProvisionResult{
		GatewayKey:    "gateway-key",
		MatrixToken:   "matrix-token",
		RoomID:        "!room123:matrix.example.com",
		MinIOPassword: "secret",
	})

	for key, want := range map[string]string{
		"AGENTTEAMS_WORKER_NAME":         "alice",
		"AGENTTEAMS_FS_ACCESS_KEY":       "alice",
		"AGENTTEAMS_FS_SECRET_KEY":       "secret",
		"AGENTTEAMS_FS_ENDPOINT":         "http://fs.example.com:9000",
		"AGENTTEAMS_FS_BUCKET":           "agentteams-fs",
		"AGENTTEAMS_STORAGE_PREFIX":      "teams/demo",
		"AGENTTEAMS_CONTROLLER_URL":      "http://controller.example.com:8090",
		"AGENTTEAMS_AI_GATEWAY_URL":      "http://aigw.example.com:8080",
		"AGENTTEAMS_MATRIX_URL":          "http://matrix.example.com:8080",
		"AGENTTEAMS_MATRIX_DOMAIN":       "matrix.example.com",
		"OPENCLAW_DISABLE_BONJOUR":       "1",
		"OPENCLAW_MDNS_HOSTNAME":         "agentteams-w-alice",
		"HOME":                           "/root/agentteams-fs/agents/alice",
		"AGENTTEAMS_WORKER_GATEWAY_KEY":  "gateway-key",
		"AGENTTEAMS_WORKER_MATRIX_TOKEN": "matrix-token",
		"AGENTTEAMS_WORKER_ROOM_ID":      "!room123:matrix.example.com",
		"SKILLS_API_URL":                 "nacos://skills.example.com:8848/public",
		"NACOS_AUTH_TYPE":                "sts-agentteams",
	} {
		if got := env[key]; got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	for _, legacyKey := range []string{"AGENTTEAMS_MINIO_ENDPOINT", "AGENTTEAMS_MINIO_BUCKET"} {
		if _, ok := env[legacyKey]; ok {
			t.Fatalf("unexpected legacy env %s in worker env", legacyKey)
		}
	}
}

func TestWorkerEnvBuilderBuildManagerUsesAgentScopeContract(t *testing.T) {
	builder := NewWorkerEnvBuilder(config.WorkerEnvDefaults{
		MatrixDomain:         "matrix.example.com",
		FSEndpoint:           "http://fs.example.com:9000",
		FSBucket:             "agentteams-fs",
		StoragePrefix:        "teams/demo",
		ControllerURL:        "http://controller.example.com:8090",
		AIGatewayURL:         "http://aigw.example.com:8080",
		MatrixURL:            "http://matrix.example.com:8080",
		AdminUser:            "admin",
		AdminPassword:        "admin-password",
		HigressAdminURL:      "http://higress.example.com:8001",
		MCPGitHubToken:       "github-secret",
		Runtime:              "docker",
		DefaultWorkerRuntime: "copaw",
		SkillsAPIURL:         "nacos://skills.example.com:8848/public",
	})

	env := builder.BuildManager("manager", &ManagerProvisionResult{
		MatrixUserID:   "@manager:matrix.example.com",
		MatrixToken:    "matrix-token",
		RoomID:         "!manager-room:matrix.example.com",
		GatewayKey:     "gateway-key",
		MatrixPassword: "matrix-password",
		MinIOPassword:  "secret",
	}, v1beta1.ManagerSpec{
		Model:   "qwen3.6-plus",
		Runtime: "agentscope",
		Config: v1beta1.ManagerConfig{
			HeartbeatInterval: "30m",
			WorkerIdleTimeout: "12h",
		},
	})

	for key, want := range map[string]string{
		"AGENTTEAMS_MANAGER_NAME":                        "manager",
		"AGENTTEAMS_MANAGER_MATRIX_USER_ID":              "@manager:matrix.example.com",
		"AGENTTEAMS_MANAGER_MATRIX_TOKEN":                "matrix-token",
		"AGENTTEAMS_MANAGER_ADMIN_ROOM_ID":               "!manager-room:matrix.example.com",
		"AGENTTEAMS_MANAGER_GATEWAY_KEY":                 "gateway-key",
		"AGENTTEAMS_MANAGER_RUNTIME":                     "agentscope",
		"AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY":        "manager/agentscope-manager.json",
		"AGENTTEAMS_MANAGER_WORKSPACE":                   "/var/lib/agentteams-manager",
		"AGENTTEAMS_MANAGER_HEARTBEAT_INTERVAL_SECONDS":  "1800",
		"AGENTTEAMS_MANAGER_WORKER_IDLE_TIMEOUT_SECONDS": "43200",
		"AGENTTEAMS_DEFAULT_MODEL":                       "qwen3.6-plus",
		"AGENTTEAMS_FS_ACCESS_KEY":                       "manager",
		"AGENTTEAMS_FS_SECRET_KEY":                       "secret",
		"AGENTTEAMS_FS_BUCKET":                           "agentteams-fs",
		"AGENTTEAMS_RUNTIME":                             "docker",
		"AGENTTEAMS_DEFAULT_WORKER_RUNTIME":              "copaw",
		"AGENTTEAMS_ADMIN_USER":                          "admin",
		"AGENTTEAMS_HIGRESS_ADMIN_USER":                  "admin",
		"AGENTTEAMS_HIGRESS_ADMIN_PASSWORD":              "admin-password",
		"AGENTTEAMS_AI_GATEWAY_ADMIN_URL":                "http://higress.example.com:8001",
		"AGENTTEAMS_MCP_GITHUB_TOKEN":                    "github-secret",
		"SKILLS_API_URL":                                 "nacos://skills.example.com:8848/public",
	} {
		if got := env[key]; got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	for _, legacyKey := range []string{
		"AGENTTEAMS_MANAGER_PASSWORD",
		"OPENCLAW_DISABLE_BONJOUR",
		"OPENCLAW_MDNS_HOSTNAME",
		"AGENTTEAMS_MINIO_ACCESS_KEY",
		"AGENTTEAMS_MINIO_SECRET_KEY",
		"AGENTTEAMS_MINIO_BUCKET",
	} {
		if _, ok := env[legacyKey]; ok {
			t.Fatalf("unexpected legacy env %s in manager env", legacyKey)
		}
	}
}

func TestWorkerEnvNeverReceivesManagerMCPSecrets(t *testing.T) {
	builder := NewWorkerEnvBuilder(config.WorkerEnvDefaults{
		MCPGitHubToken: "github-secret",
	})

	env := builder.Build("alice", &WorkerProvisionResult{})
	if _, exists := env["AGENTTEAMS_MCP_GITHUB_TOKEN"]; exists {
		t.Fatal("Worker environment contains Manager GitHub MCP secret")
	}
}

func TestApplyWorkerConsoleEnvUsesDeclarativeDesiredState(t *testing.T) {
	tests := []struct {
		name        string
		runtime     string
		console     *v1beta1.WorkerConsoleSpec
		wantPort    string
		wantErrPart string
	}{
		{
			name:    "absent is disabled",
			runtime: backend.RuntimeCopaw,
		},
		{
			name:    "explicit false is disabled",
			runtime: backend.RuntimeQwenPaw,
			console: &v1beta1.WorkerConsoleSpec{Enabled: false},
		},
		{
			name:     "copaw defaults to port 8088",
			runtime:  backend.RuntimeCopaw,
			console:  &v1beta1.WorkerConsoleSpec{Enabled: true},
			wantPort: "8088",
		},
		{
			name:     "qwenpaw accepts a custom port",
			runtime:  backend.RuntimeQwenPaw,
			console:  &v1beta1.WorkerConsoleSpec{Enabled: true, Port: 9090},
			wantPort: "9090",
		},
		{
			name:        "openclaw rejects enabled console",
			runtime:     backend.RuntimeOpenClaw,
			console:     &v1beta1.WorkerConsoleSpec{Enabled: true},
			wantErrPart: "not supported",
		},
		{
			name:        "invalid port is rejected",
			runtime:     backend.RuntimeCopaw,
			console:     &v1beta1.WorkerConsoleSpec{Enabled: true, Port: 70000},
			wantErrPart: "between 1 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"AGENTTEAMS_CONSOLE_PORT": "legacy"}
			err := ApplyWorkerConsoleEnv(env, tt.runtime, tt.console)
			if tt.wantErrPart != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrPart) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErrPart)
				}
				return
			}
			if err != nil {
				t.Fatalf("ApplyWorkerConsoleEnv returned error: %v", err)
			}
			got, exists := env["AGENTTEAMS_CONSOLE_PORT"]
			if tt.wantPort == "" {
				if exists {
					t.Fatalf("console env must be absent when disabled, got %q", got)
				}
				return
			}
			if !exists || got != tt.wantPort {
				t.Fatalf("console port = %q (exists=%v), want %q", got, exists, tt.wantPort)
			}
		})
	}
}

func TestWorkerEnvBuilderDoesNotEnableConsoleImplicitly(t *testing.T) {
	env := NewWorkerEnvBuilder(config.WorkerEnvDefaults{}).
		Build("alice", &WorkerProvisionResult{})
	if got, exists := env["AGENTTEAMS_CONSOLE_PORT"]; exists {
		t.Fatalf("base worker env unexpectedly enables console on port %q", got)
	}
}
