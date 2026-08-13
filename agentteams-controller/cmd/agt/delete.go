package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func deleteCmd() *cobra.Command {
	// 逻辑说明：创建 delete 命令组并挂载四类资源删除子命令，实际副作用只发生在各 RunE 中。
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a resource",
	}
	cmd.AddCommand(deleteWorkerCmd())
	cmd.AddCommand(deleteTeamCmd())
	cmd.AddCommand(deleteHumanCmd())
	cmd.AddCommand(deleteManagerCmd())
	return cmd
}

func deleteWorkerCmd() *cobra.Command {
	// 逻辑说明：要求一个 Worker 名参数，并复用统一资源删除请求与错误包装。
	return &cobra.Command{
		Use:   "worker <name>",
		Short: "Delete a Worker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return deleteResource("worker", args[0])
		},
	}
}

func deleteTeamCmd() *cobra.Command {
	// 逻辑说明：要求一个 Team 名参数，再交给统一 DELETE 路径构造器执行。
	return &cobra.Command{
		Use:   "team <name>",
		Short: "Delete a Team",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return deleteResource("team", args[0])
		},
	}
}

func deleteHumanCmd() *cobra.Command {
	// 逻辑说明：要求一个 Human 名参数并复用统一删除响应处理。
	return &cobra.Command{
		Use:   "human <name>",
		Short: "Delete a Human",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return deleteResource("human", args[0])
		},
	}
}

func deleteManagerCmd() *cobra.Command {
	// 逻辑说明：要求一个 Manager 名参数并通过同一 API client 删除。
	return &cobra.Command{
		Use:   "manager <name>",
		Short: "Delete a Manager",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return deleteResource("manager", args[0])
		},
	}
}

func deleteResource(kind, name string) error {
	// 逻辑说明：按内部固定 kind 构造 REST 复数路径，要求 2xx 后才打印删除成功；失败保留资源类型上下文。
	client := NewAPIClient()
	if err := client.DoJSON("DELETE", fmt.Sprintf("/api/v1/%ss/%s", kind, name), nil, nil); err != nil {
		return fmt.Errorf("delete %s: %w", kind, err)
	}
	fmt.Printf("%s/%s deleted\n", kind, name)
	return nil
}
