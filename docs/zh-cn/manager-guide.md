# AgentScope Manager 指南

AgentTeams Manager 是基于 **AgentScope 2.0.4.post1** 的长生命周期 Python
服务。AgentScope 负责模型与工具循环；确定性的工作流负责持久化、重试、恢复、
权限检查和外部副作用。

Manager 只有一个运行时：`agentscope`。OpenClaw、CoPaw、Hermes、QwenPaw
都是 Worker 运行时。

## 状态边界

| 系统 | 权威状态 |
|---|---|
| Controller | Manager、Worker、Team、Human 的期望状态 |
| Matrix | 房间、成员、消息、线程和媒体 |
| SQLite WAL | 活跃会话、操作记录、定时任务、拓扑缓存 |
| MinIO/S3 | 不可变操作事件、校验快照、任务和项目产物 |
| Higress | 模型路由、MCP Server/Consumer、服务发布路由 |

Manager 不使用临时拼出的 shell 命令修改 Controller 资源。所有 Controller
调用都经过 typed `AgtClient`。Matrix 回合直接调用 AgentScope
`reply_stream`，运行时变更只在两个回合之间生效。

## 安装配置

安装器会写入 `agentteams-manager.env`，并固定
`AGENTTEAMS_MANAGER_RUNTIME=agentscope`。

| 变量 | 默认值 | 用途 |
|---|---|---|
| `AGENTTEAMS_WORKSPACE_DIR` | `~/agentteams-manager` | 宿主机持久化目录，挂载到 `/var/lib/agentteams-manager` |
| `AGENTTEAMS_DEFAULT_WORKER_RUNTIME` | `openclaw` | 默认 Worker 运行时 |
| `AGENTTEAMS_GITHUB_TOKEN` | 空 | 可选 GitHub PAT，会规范化为 `AGENTTEAMS_MCP_GITHUB_TOKEN` |
| `AGENTTEAMS_MATRIX_E2EE` | 安装时选择 | 启用 Matrix 端到端加密会话 |
| `AGENTTEAMS_YOLO` | 未设置 | 设为 `1` 时，可信无人值守环境可跳过交互确认 |
| `AGENTTEAMS_MATRIX_DEBUG` | 未设置 | 设为 `1` 时输出更多 Matrix 结构化追踪 |

Helm 使用 `manager.runtime=agentscope`，GitHub token 可通过
`credentials.githubToken` 提供。Token 保存在 runtime Secret 中，由
Controller 使用，并且只注入 Manager；Worker 资源和运行时文档不会包含它。

## 首次对话与身份设置

全新安装后，Manager 会询问管理员：

1. Manager 的称呼；
2. 沟通风格；
3. 行为准则；
4. 默认语言。

Manager 展示完整方案并得到确认后，调用 `update_manager_identity`。Controller
把身份写入 `Manager.spec.identity`，只合并 SOUL 的
`Identity & Personality` 区段，发布更高的运行时 revision，Manager 再在回合
之间热加载。Manager 不会直接写 `SOUL.md`，也不依赖完成标记文件。

旧 OpenClaw/CoPaw Manager 的会话不会迁移。Controller 资源和远端产物仍然
可见。

## 房间权限

每个 Matrix 房间都会单独构建工具集合：

- **管理员私聊**：全部 Manager 能力；高风险变更需要确认。
- **Worker 房间**：只允许该 Worker 的任务与通信能力。
- **Leader 房间**：通过 Team Leader 委派和查看 Team 范围信息。
- **Project 房间**：只允许该项目的任务、成员和产物。
- **Human/频道房间**：遵守 Controller Human 资源的权限范围。
- **未知房间**：不提供管理变更工具。

模型不能通过 MCP、生成命令或换一个工具绕过隐藏能力；工具执行时还会再次
检查权限。

## 功能范围

保留的 19 项 Manager skill 覆盖 Worker 发现/导入和生命周期、Team、Human、
任务与周期调度、项目、频道、Matrix 管理、文件同步、Git 委派、模型切换、
MCP 管理、服务发布和任务协调。

机器可读的覆盖关系在
[`tests/manager-skill-parity.json`](../../tests/manager-skill-parity.json)。
其中每个 skill 都映射到已注册的 AgentScope 工具和可执行测试证据。

支持五种 Worker 运行时：

| 运行时 | 值 |
|---|---|
| OpenClaw | `openclaw` |
| CoPaw 兼容运行时 | `copaw` |
| Hermes | `hermes` |
| QwenPaw | `qwenpaw` |

## 模型与 MCP

`switch_model` 会先通过 OpenAI 兼容网关探测目标模型。探测失败时不修改期望
状态；成功后必须观察到更高的 Controller 运行时 revision。当前回合仍在原
模型上结束。

安装时可选的 GitHub token 能自动引导原生 GitHub MCP。Controller 把密钥
配置到 Higress，只向 Manager 发布无密钥描述。动态 MCP 工具由 AgentScope
原生 `MCPRegistry` 管理；Worker MCP Consumer 仍然单独授权。

## 持久化与恢复

本地数据库路径：

```text
/var/lib/agentteams-manager/state/manager.db
```

数据库使用 Python 标准库 SQLite 和 WAL。系统只有一个活跃 Manager 写入者，
此时引入 Redis 不会提高正确性，反而会增加网络依赖和故障点。

执行外部副作用之前，工作流先记录本地意图，并向对象存储
`manager/journal/` 追加已脱敏的不可变事件；带校验的 SQLite 快照放在
`manager/snapshots/`。启动时 Manager 会恢复最新有效快照，重放后续事件，
再根据外部事实收敛未完成操作。因此超时会被视为“结果不确定”，不会盲目
重复创建、发送或发布。

## 健康检查与诊断

Manager 在容器端口 `18799` 提供运维 HTTP：

- `GET /healthz`：进程存活；
- `GET /readyz`：依赖和运行时就绪；
- `GET /metrics`：Prometheus 文本指标。

嵌入式安装默认映射到宿主机回环端口 `18888`。这是健康/指标端点，不是
OpenClaw 控制台。

```bash
docker logs agentteams-manager -f
curl -fsS http://127.0.0.1:18888/readyz
curl -fsS http://127.0.0.1:18888/metrics
python scripts/export-debug-log.py --range 1h --container agentteams-manager
```

调试导出器以 SQLite 只读方式提取 AgentScope 会话，输出默认脱敏的 JSONL，
并继续识别各 Worker 运行时的会话布局。

## 运维注意事项

- 身份变更使用 `update_manager_identity`，不要直接编辑 SOUL。
- 模型变更使用 `switch_model`，不要编辑 provider 文件。
- `AGENTTEAMS_YOLO=1` 只应在可信隔离环境中使用；不存在运行时标记文件开关。
- Manager 工作空间包含 Matrix E2EE 材料和活跃 SQLite 状态，应保持私有。
- 做运维级备份时，应同时备份对象存储桶和宿主机工作空间。

健康检查、损坏数据库恢复、密钥轮换和运行时 revision 排障的具体步骤见
[`agentscope-manager-operations.md`](agentscope-manager-operations.md)。
