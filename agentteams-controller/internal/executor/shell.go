package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Result holds the parsed output from a shell script execution.
type Result struct {
	Raw          string
	JSON         map[string]interface{}
	MatrixUserID string
	RoomID       string
}

// Shell executes AgentTeams bash scripts and parses their output.
type Shell struct {
	ScriptsDir string // base path, e.g. /opt/agentteams/agent/skills
	Timeout    time.Duration
}

func NewShell(scriptsDir string) *Shell {
	return &Shell{
		ScriptsDir: scriptsDir,
		Timeout:    10 * time.Minute,
	}
}

// Run executes a script with the given arguments and returns the parsed result.
func (s *Shell) Run(ctx context.Context, script string, args ...string) (*Result, error) {
	// 逻辑说明：给 bash 子进程套执行超时并分别捕获 stdout/stderr；非零退出返回两路诊断，成功时可解析 `---RESULT---` 后 JSON 并提取 Matrix/Room ID，普通输出仍完整保留。
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", append([]string{script}, args...)...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("script %s failed: %w\nstderr: %s\nstdout: %s",
			script, err, stderr.String(), stdout.String())
	}

	result := &Result{Raw: stdout.String()}

	// Parse ---RESULT--- JSON block if present
	if idx := strings.Index(result.Raw, "---RESULT---"); idx >= 0 {
		jsonStr := strings.TrimSpace(result.Raw[idx+len("---RESULT---"):])
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err == nil {
			result.JSON = parsed
			if uid, ok := parsed["matrix_user_id"].(string); ok {
				result.MatrixUserID = uid
			}
			if rid, ok := parsed["room_id"].(string); ok {
				result.RoomID = rid
			}
		}
	}

	return result, nil
}

// RunSimple executes a script and returns raw output without JSON parsing.
func (s *Shell) RunSimple(ctx context.Context, script string, args ...string) (string, error) {
	// 逻辑说明：在统一超时内执行 bash 脚本并返回原始 stdout；失败时附 stderr 而不尝试 JSON 解析，context 取消通过 CommandContext 终止子进程。
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", append([]string{script}, args...)...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("script %s failed: %w\nstderr: %s", script, err, stderr.String())
	}

	return stdout.String(), nil
}
