package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func main() {
	// 逻辑说明：以实际可执行文件名构建根命令并执行；Cobra 返回错误时用非零码结束供脚本识别。
	rootCmd := newRootCommand(filepath.Base(os.Args[0]))
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand(commandName string) *cobra.Command {
	// 逻辑说明：创建唯一根命令并注册所有资源、生命周期、预检与轮换子命令，入口本身不执行网络请求。
	rootCmd := &cobra.Command{
		Use:   commandName,
		Short: "AgentTeams resource management CLI",
		Long: `AgentTeams CLI — manages Workers, Teams, Humans, and Managers via the
agentteams-controller REST API.

Environment variables:
  AGENTTEAMS_CONTROLLER_URL
      Controller base URL (default: http://localhost:8090)
  AGENTTEAMS_AUTH_TOKEN
      Bearer token for authentication
  AGENTTEAMS_AUTH_TOKEN_FILE
      Path to a file containing the bearer token (K8s projected volume)`,
	}

	rootCmd.AddCommand(applyCmd())
	rootCmd.AddCommand(createCmd())
	rootCmd.AddCommand(getCmd())
	rootCmd.AddCommand(updateCmd())
	rootCmd.AddCommand(deleteCmd())
	rootCmd.AddCommand(workerCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(versionCmd())
	rootCmd.AddCommand(llmPreflightCmd())
	rootCmd.AddCommand(rotateCmd())
	return rootCmd
}
