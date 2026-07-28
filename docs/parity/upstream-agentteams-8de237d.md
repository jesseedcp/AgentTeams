# AgentTeams 功能差异审计

审计日期：2026-07-28  
官方基线：`agentscope-ai/AgentTeams@8de237da736a542766e132836b29c0a2a9c48740`  
本项目：`jesseedcp/AgentTeams`，AgentScope 2.0 Manager 架构

## 结论

本项目已经覆盖本轮列出的八项功能差异。这里的“对齐”指用户可见行为、
权限边界和运维结果对齐，不要求复制官方 Manager 的内部实现。

官方最新提交 `8de237d` 只修改独立 `agentteams-dashboard` 的安装、旧 Worker
凭据抽取、数据卷清理和验证计数。这些代码不能直接合并进本项目，因为本项目
没有独立 Dashboard 容器：管理页面由 AgentScope Manager 内置提供，Worker
身份由 CRD 和 Kubernetes Secret 管理。对应能力已经归类为“有意替换”，不是
遗漏。

## 八项差异

| 能力 | 状态 | 本项目实现 | 主要证据 |
|---|---|---|---|
| Fork 发布与镜像链路 | 已实现 | 安装器、Helm 和 CI 默认使用 `jesseedcp` 发行源与 GHCR；删除 OpenHuman 构建目标 | `tests/check-fork-release-chain.sh`、`.github/workflows/build.yml` |
| 外部聊天渠道 | 已实现，真实平台待凭据验收 | Discord、Telegram、Slack、飞书、WhatsApp、钉钉使用平台原生签名和出站 API；Signal 明确使用 relay；旧自定义 HMAC 配置迁移为 relay | `channels/http_providers.py`、`tests/unit/channels/` |
| IM 斜杠命令 | 已实现 | `/model`、`/models`、`/help`、`/commands`、`/stop`、`/think`、`/reasoning`、`/verbose`、`/elevated`、`/queue`，设置持久化到 SQLite | `matrix/commands.py`、`test_k8s_matrix_commands.py` |
| 完整管理面板 | 已实现 | `/manager-admin/` 内置 UI 和带认证、幂等键、确认门槛的 Worker/Team/Project CRUD API | `admin/commands.py`、`admin/ui.py`、`test_k8s_admin_and_console.py` |
| CoPaw/QwenPaw Console 开关 | 已实现 | `Worker.spec.console` 为声明式期望状态；Controller 增删 `AGENTTEAMS_CONSOLE_PORT` 并重建 Pod/容器 | `api/v1beta1/types.go`、`internal/service/worker_env.go` |
| v1.2 前升级兼容 | 已实现 | Bash、PowerShell 安装器按最终镜像版本选择旧环境变量前缀，包含预发布版本比较 | `install/lib/version-compat.sh`、安装器测试 |
| Coding CLI 委托 | 已实现，默认关闭 | Claude、Gemini、Qoder 使用固定程序与参数、stdin 输入、受限目录、超时/输出上限、确认、lease、journal 和恢复 | `clients/coding_cli.py`、`workflows/coding_cli.py` |
| Higress 通用管理 | 已实现 | Provider、AI Route、Consumer 的类型化 list/get/upsert/delete；密钥脱敏，危险写操作确认 | `clients/higress.py`、`tools/integrations.py` |

## 核心上游行为

| 行为 | 分类 | 说明 |
|---|---|---|
| 独立 Worker CR；Team 只引用 `workerMembers` | 已实现 | Team 创建、修改、删除都通过 Controller 资源契约；删除 Team 保留 Worker CR |
| 项目返修、任务重分配、参与者变更、计划修改 | 已实现 | SQLite 保存项目/任务状态；服务层执行状态机、重分配、成员变更和 major/minor 计划修订 |
| Matrix 中的管理员确认 | 已实现 | confirmation 全局持久化，`/confirm`、`/deny`、`/reset` 可恢复或终止原 AgentScope continuation |
| 持久化与恢复 | 已实现 | SQLite WAL 是单 Manager 写入权威；MinIO 保存不可变 journal 和校验快照；K8s 使用 Manager PVC |
| Worker 运行时 | 已实现 | 保留 OpenClaw、CoPaw、Hermes、QwenPaw 四种 Worker |

## 有意替换

| 官方实现 | 本项目实现 | 原因 |
|---|---|---|
| OpenClaw/CoPaw/Hermes Manager | AgentScope `2.0.4.post1` Manager | 保持单一模型/工具循环、类型化边界和统一恢复机制 |
| Element | Cinny | 用户已选择完全替换；Matrix 账号、房间和消息不变 |
| 独立 `agentteams-dashboard` 容器 | Manager 内置 `/manager-admin/` | 不再维护第二套鉴权、部署和数据卷 |
| `workers-registry.json` 和容器内凭据抽取 | Worker CRD、Controller、Kubernetes Secret | 凭据不进入工作区文件，重建由声明式状态恢复 |
| Manager 运行时文件或 Redis | Python 标准库 SQLite WAL + MinIO journal | 当前只有一个 Manager 写者，不需要 Redis 服务；减少运维组件 |
| shell/curl 技能执行 | Pydantic 请求 + 服务层 + Controller/Higress 客户端 | 便于权限、确认、幂等、审计和单元测试 |

## 有意删除

- OpenHuman Worker 不再发布、安装或构建。它不是四种保留 Worker runtime
  之一，CI 会拒绝重新引入对应镜像目标。
- 官方实验性技能的大段 shell 文档没有原样复制；功能由
  `coding-cli-management` 和 `higress-gateway-management` 的类型化工具提供。

## 官方 `8de237d` 最新增量的处置

| 官方改动 | 本项目处置 |
|---|---|
| Quick Start 默认启用独立 Dashboard | 不适用；内置管理台随 Manager 始终部署 |
| 从 `workers-registry.json` 抽取旧 Worker 密码/房间 | 由 CRD 状态和 Kubernetes Secret 替代 |
| 卸载时删除 `agentteams-dashboard-data` volume | 不适用；本项目不创建该 volume |
| Dashboard verifier 增加 `check_skip` | 不适用；本项目的静态、pytest 和 K8s 验收分别统计 |

## 仍需外部条件才能验证

- Discord、Telegram、Slack、飞书、WhatsApp、钉钉和 Signal 的真实租户
  Webhook/出站请求，需要用户提供各平台应用和密钥。本地测试已验证协议形状、
  签名、时间窗、challenge、去重和错误路径。
- Claude、Gemini、Qoder 的真实 CLI 运行，需要运维者把对应二进制只读挂载到
  配置的可信目录并自行完成供应商认证。基础 Manager 镜像故意不捆绑二进制或
  凭据；管理状态会显示 `available: false`。
- GitHub MCP、云厂商 STS 和真实远程 Higress 的在线行为需要对应外部凭据。

这些项目属于“外部未验证”，不是代码路径缺失。未提供外部条件时功能会明确
返回 unavailable/认证错误，不会伪造成功。
