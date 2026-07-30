# AgentTeams 功能差异审计

审计日期：2026-07-31
官方基线：`agentscope-ai/AgentTeams@fb3a40be1f005bd584f45544fc73bd4601d5c52a`
（审计时远程 `main` 最新提交）
本项目：`jesseedcp/AgentTeams`，AgentScope 2.0 Manager 架构

## 结论

本项目已经覆盖最初列出的八项功能差异，并补齐后续真实页面测试发现的
Manager Agent 行为差异。这里的“对齐”指用户可见行为、权限边界和运维结果
对齐，不要求复制官方 Manager 的内部实现。

官方 `v1.2.0` 在此前 Dashboard 修复之外，还加入旧版存储前缀、CoPaw 默认
workspace 投影、Team DAG 收敛等待和 Worker 同步 I/O 修复。此后截至
`fb3a40b`，官方又完成 QwenPaw 2.0 运行时迁移和 Manager 诊断收敛修复。
本项目逐项审计：可复用行为已移植；独立 Dashboard、AgentLoop 宣传链接和
官方仓库发行元数据按本项目架构明确替换或排除。

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

## Manager Agent 行为补齐

| 能力 | 状态 | 本项目实现 | 主要证据 |
|---|---|---|---|
| 可控审批频率 | 已实现 | `/elevated off` 只审批高风险操作，`ask` 审批全部工具，`full` 在管理员私聊完全免审批；普通创建、派发和通知不再重复审批 | `runtime/session_manager.py`、`test_session_commands.py` |
| Matrix 通道可靠消费 | 已实现 | 失败事件持久化重试，达到上限进入 dead letter 并通知管理员；重启后继续处理未完成事件 | `matrix/router.py`、`test_router.py` |
| Worker 异步创建 | 已实现 | 创建请求先返回可追踪状态，Controller 后台调和账号、房间、存储与 Runtime；超时前先查询事实再决定是否重试 | `workflows/resources.py`、`test_ambiguous_resource_create.py` |
| 主动监督 | 已实现 | heartbeat 根据进度、截止时间、阻塞和连续失联周期主动催办或升级，不依赖用户反复询问 | `workflows/heartbeat.py`、`test_supervision.py` |
| 语义验收和返修 | 已实现 | Manager 读取并校验结果文件后作出接受、返修、阻塞或中断判断；返修创建关联任务并阻止依赖任务提前启动 | `workflows/tasks.py`、`test_semantic_supervision.py`、`test_project_changes.py` |
| 项目计划确认与变更 | 已实现 | 新项目先进入 planning，管理员确认后激活；minor/major 计划变更、参与者变化、任务重分配均版本化和幂等 | `workflows/projects.py`、`test_projects.py` |
| 长短期记忆 | 已实现 | SQLite 保存每日记忆、长期记忆、项目决策和 Worker 评估；冷启动按房间权限投影，快照恢复不丢失 | `workflows/memory.py`、`test_memory_workflow.py`、`test_snapshot_recovery.py` |

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

## 官方 `v1.2.0` 最新增量的处置

| 官方改动 | 本项目处置 |
|---|---|
| Quick Start 默认启用独立 Dashboard | 不适用；内置管理台随 Manager 始终部署 |
| 从 `workers-registry.json` 抽取旧 Worker 密码/房间 | 由 CRD 状态和 Kubernetes Secret 替代 |
| 卸载时删除 `agentteams-dashboard-data` volume | 不适用；本项目不创建该 volume |
| Dashboard verifier 增加 `check_skip` | 不适用；本项目的静态、pytest 和 K8s 验收分别统计 |
| v1.2 前镜像使用旧存储前缀 | 已移植到 Bash 和 PowerShell 安装器，并覆盖 `latest` 解析 |
| CoPaw 使用默认 workspace | 已移植并扩展到本项目支持的 CoPaw 两代配置格式 |
| Team DAG 验收等待角色 overlay 收敛 | 已移植有界重试，不用固定瞬时状态判断失败 |
| Worker 存储同步 I/O 放大 | 已移植独立成功 watermark、小批量 `mc cp`、大批量单次 mirror、jq 1.7 安全合并和 Controller-only mirror；CoPaw、QwenPaw、Hermes 也只在整批上传成功后推进 watermark |
| README 增加 AgentLoop 链接 | 不引入；AgentLoop 是可选外部项目，不是本项目运行依赖 |
| 官方 v1.2.0 changelog 和 fallback tag | 保持 Fork 专属发行链路；安装器查询 `jesseedcp/AgentTeams`，Fork 首次发布前继续明确使用 `latest`/源码构建，不回退官方镜像 |

## 官方 `793db24..fb3a40b` 增量的处置

| 官方改动 | 本项目处置 |
|---|---|
| QwenPaw 运行时升级到 2.0.1 | 已移植；模型、Matrix、MCP、策略、Agent 和技能改为通过 QwenPaw 2.0 本地 API 配置 |
| Matrix、TeamHarness、Workerflow 改为原生 QwenPaw 插件 | 已移植；镜像内置三个插件，并保留本项目对过期异步回复的抑制逻辑 |
| QwenPaw desired state 等本地 API 就绪后应用 | 已移植；Worker 准备阶段不提前写入未启动运行时，启动后再完成配置收敛 |
| QwenPaw Node CLI 运行时 | 已加固；官方镜像的 Debian Node 20 已低于当前 `mcporter` 的 Node 24 要求，本项目安装 Node 24 并在构建时校验主版本 |
| Team storage 与角色上下文收敛 | 已移植；独立 Worker 调和不会覆盖 Team 有效存储，Team 上下文投影到活动 workspace |
| Manager 避免重复诊断循环 | 已移植到 AgentScope Manager 提示与技能规则；同一只读诊断最多重复两次，Controller 确认 Worker 不存在后停止探测旧房间 |

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
