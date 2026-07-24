# AgentScope 2.0 AgentTeams Manager 设计规格

日期：2026-07-23

状态：设计已确认，进入实现

上游基线：`agentscope-ai/AgentTeams` `main`，提交 `0ff89f07a205b82cd81d18385c7095ec352a083f`

AgentScope 基线：`agentscope-ai/agentscope` `v2.0.4.post1`

## 1. 决策摘要

本项目从最新 AgentTeams 上游基线创建一个独立的新项目快照，将 Manager 中的 OpenClaw/CoPaw 运行时整体替换为直接基于 AgentScope 2.0 的 Python 运行时。目标仓库只使用 `jesseedcp/AgentTeams` 的 `main`，不创建功能分支，也不继承上游 Git 提交历史。

外部系统和现有职责保持不变：

- Controller 继续作为 Worker、Team、Human 和 Manager 资源生命周期的最终权威。
- Matrix 继续承担管理员、Manager、Team Leader 和 Worker 之间的通信与房间拓扑。
- MinIO/OSS 继续保存任务、项目、工作区和交付物。
- Higress 继续提供模型与 MCP 网关。
- Element 继续作为可选的人类交互界面。
- Worker 继续支持 `openclaw`、`copaw`、`hermes`、`qwenpaw`、`openhuman`。

Manager 内部则改为：

- AgentScope 2.0 原生 Agent 负责推理、流式回复、技能和工具调用。
- Python 工作流负责可靠地执行创建资源、分派任务、回收结果、心跳检查和故障恢复。
- 大模型决定“做什么”；确定性代码保证“怎样完整、安全、可恢复地做完”。

这是一次 Manager 的硬替换，不迁移旧 OpenClaw/CoPaw Manager 会话、运行状态或记忆。第一版必须一次覆盖最新上游 Manager 的全部 16 项技能能力，不以长期双运行时或逐功能替换作为交付形态。

项目基线采用“完整但经过职责裁剪”的上游快照：

- 保留 Controller、Helm、安装器、文档与测试基础、Matrix、MinIO/OSS、Higress、Element、基础设施资源以及五种 Worker 运行时；
- 保留 16 个 Manager 技能的名称、外部契约、安全规则和仍有价值的参考资料；
- 不复制已由 AgentScope Manager 取代的两种旧 Manager 镜像、入口、运行时配置、prompt overlay、supervisord/legacy all-in-one 文件和 Manager 专用 shell 执行器；
- 不复制只用于手工生成 OpenClaw Worker 配置的模板；
- 所有保留的 Manager 技能文档必须改写为调用 typed AgentScope tools，不能继续引用已删除脚本。

裁剪后的源码、规格和实现计划共同形成一个无父提交的全新根提交；后续实现使用普通提交追加，不 graft、squash 或携带上游历史。

## 2. 原版工作方式

最新上游 Manager 并不是一个独立的确定性编排服务。它由 OpenClaw 或 CoPaw 承担 Agent 运行时，大模型读取 `AGENTS.md`、`HEARTBEAT.md` 和各个 `SKILL.md`，再依次调用：

- `agt` CLI 管理 Controller 资源；
- Matrix `message` 工具或 `copaw channels send` 发送消息；
- `mc` 管理 MinIO 文件；
- `manage-state.sh` 等脚本维护 `~/state.json`；
- Heartbeat 提示词定期检查任务和 Worker。

原版的状态分布如下：

| 数据 | 权威来源 |
| --- | --- |
| Worker/Team/Human 资源和运行状态 | Controller |
| Matrix 房间、成员和消息 | Matrix |
| Manager 对话会话 | OpenClaw/CoPaw |
| Manager 当前任务清单 | `~/state.json` |
| 等待启动的 Worker | `~/pending-workers.json` |
| 任务、项目和交付物 | MinIO/OSS |
| 周期检查逻辑 | Heartbeat 提示词和大模型 |

原版已具有房间内串行处理、Matrix 同步令牌、允许名单、Tool Guard、临时文件原子替换和部分重复操作防护，但跨系统流程仍主要依赖大模型遵守操作手册。例如有限任务的典型顺序是：

1. 生成任务 ID 和文件；
2. 上传 MinIO；
3. 给 Worker Room 发消息；
4. 再调用脚本登记 `state.json`。

这些步骤不是同一个可恢复事务。Manager 在任意两步之间重启，都需要下一次大模型运行根据外部现状自行判断。

## 3. 目标与非目标

### 3.1 目标

1. 用 AgentScope 2.0 直接承担 Manager 的模型循环、会话、技能、工具和流式输出。
2. 保持最新上游 AgentTeams 对管理员可见的全部 Manager 能力。
3. 保持 Controller API、Worker 运行时、Matrix 拓扑、MinIO 文件协议及 Team Leader 协作模型兼容。
4. 将跨 Controller、Matrix 和 MinIO 的操作变成可记录、可重试、可接管、可审计的工作流。
5. 用代码执行房间权限、工具暴露和资源权限，而不是只依赖提示词约束。
6. 同时支持本地安装、Kubernetes 和阿里云部署路径。
7. 提供能够证明与最新上游功能等价的自动化测试矩阵。

### 3.2 非目标

- 不重写 Controller。
- 不使用 AgentScope 内建 Team/AgentCreate 代替 AgentTeams 的 Worker/Team CRD。
- 不改变 Team Leader 与 Team Worker 的责任边界。
- 不把 Worker 统一迁移到 AgentScope。
- 不迁移旧 Manager 的聊天历史、`state.json`、`pending-workers.json` 或运行中任务。
- 不增加 QwenPaw 项目独有的钉钉、飞书、QQ 等渠道能力。
- 不引入 Redis，也不使用 AgentScope `create_app` 作为 Manager 主运行方式。
- 不承诺与旧 Manager 逐字一致的回复，只承诺功能、权限和工作流结果兼容。

## 4. 总体架构

```text
管理员 / Element
       │
       ▼
    Matrix
       │
       ▼
┌────────────────────────────────────────────┐
│ AgentScope Manager                         │
│                                            │
│ MatrixTransport → EventRouter              │
│      │              │                      │
│      │       RoomPolicyResolver            │
│      │              │                      │
│      └────→ RoomSessionManager              │
│                     │                      │
│               AgentScope Agent             │
│                     │                      │
│               Typed Toolkit                │
│                     │                      │
│     WorkflowCoordinator / OperationSupervisor
└──────────────┬──────────────┬──────────────┘
               │              │
          Controller       MinIO/OSS
               │
        Worker / Team CR
```

AgentScope Manager 是一个直接运行的长生命周期 Python daemon。Matrix 事件适配器直接调用 `await agent.reply_stream(...)`；运行时不再启动 OpenClaw Gateway，也不再启动 CoPaw FastAPI 应用。

## 5. 内部组件

### 5.1 MatrixTransport

负责与 Matrix 通信，但不包含业务决策：

- 登录、同步令牌和断线重连；
-邀请自动加入和房间成员更新；
- DM、群聊、提及和线程识别；
- E2EE 密钥与设备状态；
- 历史消息、媒体、typing 和流式回复；
- 入站事件去重；
- 出站事务 ID、重试和限流；
- readiness 状态。

实现优先复用并拆分上游 CoPaw Matrix Channel 已验证的 `matrix-nio[e2e]` 能力，避免从零重写 3,000 余行协议处理。业务逻辑不得继续留在传输层。

### 5.2 EventRouter

将 Matrix 原始事件标准化为内部事件：

```text
InboundEvent {
  event_id,
  room_id,
  sender_id,
  thread_id,
  event_type,
  content,
  attachments,
  received_at
}
```

路由器先使用 `event_id` 去重，再按 `room_id` 串行投递。同一房间保持顺序，不同房间可以并行。涉及同一 Worker、Team 或任务的跨房间操作，额外获取资源键锁。

### 5.3 RoomPolicyResolver

根据 Controller 资源、Matrix 成员关系和本地拓扑缓存识别房间类型：

- `admin_dm`
- `worker_room`
- `leader_room`
- `team_room`
- `human_or_channel_room`
- `unknown`

房间类型决定系统提示、可见上下文和可调用工具：

| 房间 | 主要用途 | 允许的高权限操作 |
| --- | --- | --- |
| Admin DM | 管理、创建、删除、配置、汇总 | 完整管理工具 |
| Worker Room | 派发任务、进度和结果 | 该 Worker 范围内任务工具 |
| Leader Room | Team 任务、进度和结果 | 该 Team 范围内委派工具 |
| Team Room | 原则上 Manager 不加入 | 无 Manager 管理操作 |
| Human/Channel Room | 人类协作和通知 | 明确授权的有限操作 |
| Unknown | 安全降级 | 只读说明，不允许变更 |

房间权限由代码检查，提示词只解释规则，不能扩大权限。

### 5.4 RoomSessionManager

保持与原版一致的房间级会话语义：

```text
session_id = matrix:<room_id>
```

每个会话保存：

- AgentScope 对话状态；
- 房间策略版本；
- 最近处理的 Matrix 事件；
- 当前关联 Worker/Team/任务；
- token 裁剪摘要；
- 最近活跃时间。

同一房间参与者共享任务上下文，但真实 `sender_id` 始终保留在消息元数据中，用于权限判断和审计。

### 5.5 AgentFactory

为每个房间会话创建或恢复 AgentScope Agent：

- 固定使用 AgentScope `v2.0.4.post1` API；
- 模型通过 Higress 的 OpenAI 兼容接口访问；
- 系统提示由基础人格、Manager 规则、房间策略和相关技能组合；
- 支持流式文本和工具调用；
- 在一轮开始时绑定不可变配置版本；
- 一轮进行中即使配置刷新，也不改变本轮模型或工具集合。

### 5.6 SkillRegistry

保留上游 AgentTeams 技能中仍有价值的知识内容，但区分两类信息：

1. 解释性知识继续以技能文档供 AgentScope Agent 按需加载；
2. 必须按顺序、安全执行的业务步骤进入 Python 工作流，不再只写在提示词中。

首版功能基线为最新上游的 16 项 Manager 技能：

1. `agentteams-find-worker`
2. `channel-management`
3. `file-sync-management`
4. `git-delegation-management`
5. `human-management`
6. `matrix-server-management`
7. `mcp-server-management`
8. `mcporter`
9. `model-switch`
10. `project-management`
11. `service-publishing`
12. `task-coordination`
13. `task-management`
14. `team-management`
15. `worker-management`
16. `worker-model-switch`

每项技能都必须有明确的 AgentScope 工具映射和兼容性测试，不能只把旧 `SKILL.md` 复制过来后宣称完成。旧脚本执行说明、OpenClaw/CoPaw Manager 命令、旧 JSON registry 操作和手工 Worker runtime payload 模板属于被替换实现，不进入新项目基线；它们承载的业务规则必须转移到 typed workflow、重写后的技能文档和测试。

技能实现分组固定如下：

| 工作流模块 | 覆盖技能 |
| --- | --- |
| ResourceWorkflow | `worker-management`、`team-management`、`human-management`、`channel-management`、`matrix-server-management`、`agentteams-find-worker` |
| TaskWorkflow | `task-management`、`task-coordination`、`project-management`、`git-delegation-management` |
| IntegrationWorkflow | `mcp-server-management`、`mcporter`、`service-publishing` |
| ConfigurationWorkflow | `model-switch`、`worker-model-switch` |
| StorageWorkflow | `file-sync-management` |

技能可以共享底层工具，但每项技能都必须保留独立的功能对等测试，以避免分组后漏掉上游行为。

### 5.7 Typed Toolkit

Agent 不直接拼接管理 shell 命令，而调用有类型的 Python 工具，例如：

- `create_worker`
- `list_workers`
- `stop_worker`
- `create_team`
- `delegate_task`
- `complete_task`
- `switch_manager_model`
- `switch_worker_model`
- `configure_mcp_server`
- `publish_service`
- `sync_files`

首版所有 Controller 资源查询和变更都由 `AgtClient` 调用上游 `agt` CLI 完成，不在 Agent 工具中直接调用 Controller HTTP API。`AgtClient` 使用参数数组和 `shell=False`，只接受结构化参数，强制 `-o json`，设置超时并隐藏凭据。若未来需要直接 Controller API，必须作为单独设计变更处理，不能在实现过程中临时混用两条控制路径。

确有必要执行 Git 或项目命令时，使用受限进程执行器：

- 工作目录必须位于允许的任务工作区；
- 命令类型、环境变量和超时受策略限制；
- 不允许模型绕过 Toolkit 调用任意宿主 shell；
- 高风险操作进入 AgentScope Permission 审批流程。

### 5.8 WorkflowCoordinator

负责跨房间、跨服务的业务工作流：

- 创建和管理 Worker；
- 创建和管理 Team；
- 有限任务和无限任务；
- Team Leader 委派；
- 项目和 Git 委派；
- 文件同步和结果收集；
- Human、Channel、MCP 和服务发布；
- 模型切换；
- 管理员通知。

它只编排领域动作，不负责 Matrix 协议或 Agent 推理。

### 5.9 OperationSupervisor

为每个可能产生外部副作用的操作生成稳定的 `operation_id`，并维护状态机：

```text
planned
  → prepared
  → dispatched
  → acknowledged
  → running
  → succeeded

任意中间状态
  → retry_wait
  → reconciling
  → succeeded | failed | needs_attention
```

关键规则：

- 在调用 Controller、发送 Matrix 消息或写入 MinIO之前，先持久化操作意图。
- 所有可重试调用携带稳定的幂等键。
- 网络超时不等于失败；先查询 Controller/Matrix/MinIO 现状，再决定接管或重试。
- 创建 Worker 出现不确定结果时，不重复创建同名 Worker，而是按名称和操作标签查找并接管。
- 管理员重复发送同一请求时，返回已有操作进度，不生成第二个资源。
- 只有永久错误或超过恢复预算的操作进入 `needs_attention`。

### 5.10 StateStore

运行态使用 Python 标准库 SQLite，不引入 Redis。SQLite 保存：

- 房间会话索引；
- Matrix 事件去重表；
- Worker/Team/Room 拓扑缓存；
- Task/Project 状态；
- Operation Journal；
- 心跳计划；
- 配置版本；
- 通知投递状态；
- 审计事件。

SQLite 开启 WAL，事务只覆盖本地状态，不假装能够与 Controller、Matrix 或 MinIO形成分布式数据库事务。

为应对 Manager 容器或节点丢失：

- 数据库位于 Manager 持久工作区；
- 在外部副作用前，将精简操作意图同步写入 MinIO 的不可变 journal 对象；
- 使用 SQLite backup API 生成一致快照，再上传 MinIO；
- 启动时先恢复最新快照，再重放快照之后的 journal；
- 重放后必须向 Controller、Matrix 和 MinIO 对账，不能仅相信本地缓存。

MinIO 保存恢复副本和业务文件，但不承担高并发事务数据库职责。

### 5.11 HeartbeatScheduler

原版 Heartbeat 是提示词清单；新版把确定性检查移入调度器：

- 查询到期任务和计划；
- 轮询异步 Worker/Team 创建状态；
- 检测运行超时、失联和待通知操作；
- 对账 Controller 资源；
- 触发 MinIO 快照；
- 清理过期去重记录。

只有需要语义判断、进度总结或管理员沟通时才调用 AgentScope Agent。纯状态查询和恢复不消耗模型调用。

### 5.12 ConfigWatcher 与可观测性

Controller/Manager CR 配置转换成带版本号的不可变运行时配置。更新只在下一轮 Agent 调用生效。

服务暴露：

- liveness：进程和事件循环存活；
- readiness：配置、Matrix 登录、StateStore 和 Controller 可用；
- 结构化日志：包含 `trace_id`、`operation_id`、`room_id`、`task_id`；
- 指标：消息延迟、工具耗时、重试、失败、待恢复操作、模型 token；
- 审计日志：记录发起人、房间、工具、参数摘要和结果，不记录密钥。

## 6. 核心数据流

### 6.1 普通消息

1. MatrixTransport 收到事件。
2. EventRouter 去重并识别房间。
3. RoomPolicyResolver 生成权限策略。
4. RoomSessionManager 恢复 `matrix:<room_id>` 会话。
5. AgentScope Agent 流式推理。
6. 无副作用回复直接发送；工具调用交给 Toolkit。
7. MatrixTransport 使用稳定事务 ID 发送回复。
8. StateStore 提交本轮会话状态和事件游标。

### 6.2 创建 Worker

1. 管理员在 Admin DM 请求创建 Worker。
2. AgentScope Agent 调用 `create_worker`。
3. OperationSupervisor 写入 `planned` 操作和 MinIO journal。
4. AgtClient 使用 `--no-wait` 调用 Controller。
5. 返回明确成功时记录资源 ID；网络结果不明时进入 `reconciling`。
6. Supervisor 根据名称、标签和 Controller 状态接管实际资源。
7. Worker 变为 `Running` 后发送欢迎消息。
8. 通知管理员，并将操作标记为 `succeeded`。

不再使用 `pending-workers.json`；其职责由 Operation Journal 和确定性 Heartbeat 接管。

### 6.3 分派有限任务

1. 生成稳定 `task_id` 和 `operation_id`。
2. 在 StateStore 创建 `prepared` 任务。
3. 写入任务说明和元数据，并上传 MinIO。
4. 验证对象存在和校验和。
5. 发送 Worker Room 或 Leader Room 消息，事务 ID 包含 `operation_id`。
6. 记录消息事件 ID，任务进入 `dispatched`。
7. 收到接受、进度或完成事件后推进状态。
8. 完成时下载结果、验证交付物、更新元数据和记忆摘要。
9. 发送一次管理员通知并记录投递结果。

与原版相比，任务在发消息前已经登记，因此 Manager 在“消息已发但 state.json 未写”窗口崩溃时仍能恢复。

### 6.4 Team

1. Manager 调用 `agt create team`。
2. Controller Reconciler 创建 Team Leader、Team Workers、Team Room、Leader DM、Leader Room 和共享存储。
3. Manager 验证拓扑：
   - 必须在 Leader Room；
   - 不应在 Team Room；
   - 不应在 Leader DM；
   - 不应在 Team Worker Room。
4. Manager 只向 Leader Room 分派团队任务。
5. Team Leader 负责分解任务并协调 Team Workers。
6. Manager 的 Heartbeat 只向 Team Leader 查询进度。
7. 结果通过 Leader Room 和共享 MinIO 汇总。

AgentScope 内建 Team 不参与这一流程，避免形成第二套 Team 权威。

## 7. 状态权威与冲突规则

| 对象 | 权威来源 | Manager 本地状态的作用 |
| --- | --- | --- |
| Worker/Team/Human/Manager CR | Controller | 缓存、操作关联、恢复线索 |
| Matrix 房间成员和消息 | Matrix | 游标、去重、业务索引 |
| 任务和工作流进度 | Manager StateStore | 主要业务状态 |
| 任务文件和交付物 | MinIO/OSS | 元数据索引和校验和 |
| 对话历史 | AgentScope StateStore | MinIO 恢复快照 |
| 模型/MCP 路由实际效果 | Controller/Higress | 配置版本和期望状态 |

冲突处理遵循以下优先级：

1. 资源是否存在、是否运行，以 Controller 为准。
2. 消息是否已经投递，以 Matrix 事件为准。
3. 文件是否存在及内容版本，以 MinIO 对象元数据和校验和为准。
4. Manager 工作流根据上述外部事实前滚或标记异常，不反向覆盖外部事实。
5. 无法自动判断时进入 `needs_attention`，向管理员说明事实和可选动作，不静默重复副作用。

目标是可观察的“效果只发生一次”。由于三个外部系统不支持共同事务，不宣称数学意义上的 exactly-once；实现采用幂等键、操作日志和对账达到业务级有效一次。

## 8. 错误处理

错误分为：

- `validation`：参数、权限或房间类型不合法，立即拒绝；
- `transient`：超时、限流、临时断线，指数退避并加抖动；
- `ambiguous`：调用可能已经成功，先对账，禁止直接重复创建；
- `permanent`：Controller 拒绝、对象冲突、配置错误，给出明确原因；
- `policy`：请求超出房间或用户权限，记录审计并拒绝；
- `corruption`：状态快照、journal 或交付物校验失败，停止自动推进并告警。

每类错误都有最大重试次数、最长恢复时间和管理员通知策略。日志和用户消息不得包含 Matrix access token、Higress key、模型 key、MinIO secret 或完整敏感参数。

## 9. 部署和代码边界

新增独立的 AgentScope Manager Python 包，建议目录：

```text
manager-agentscope/
├── pyproject.toml
├── src/agentteams_manager/
│   ├── main.py
│   ├── config.py
│   ├── matrix/
│   ├── agent/
│   ├── tools/
│   ├── workflows/
│   ├── state/
│   └── observability/
└── tests/
```

同时更新：

- Manager Dockerfile 和启动脚本；
- Controller Manager runtime 校验和镜像选择；
- Helm values、CR 示例和本地安装脚本；
- readiness/liveness 探针；
- 构建、发布和集成测试；
- 用户文档和 changelog。

新项目的 Manager runtime 只有 `agentscope`。旧的 OpenClaw/CoPaw Manager 启动分支不作为生产回退路径保留；CoPaw 代码仍可被 `copaw` Worker 使用。

首版依赖固定到 `agentscope[s3]==2.0.4.post1`，Python 版本不低于 AgentScope 该版本要求。升级 AgentScope 必须单独通过会话、工具、流式输出、权限和状态恢复兼容测试。

## 10. 实现工作包

“一次覆盖全部功能”表示同一个首版发布必须通过全部完成标准，不表示代码必须在一个不可审查的大提交中完成。实现拆成以下相互有明确接口的工作包：

1. Runtime Core：配置、StateStore、OperationSupervisor、AgentScope 适配和基础可观测性。
2. Matrix Adapter：协议传输、E2EE、事件路由、房间策略和会话。
3. Resource Management：Worker、Team、Human、Channel、Matrix Server 和发现能力。
4. Task and Project：任务、协调、项目、Git 委派、文件同步和结果回收。
5. Integrations：模型切换、MCP、mcporter 和服务发布。
6. Deployment and Parity：镜像、Controller/Helm/install 接线、故障恢复和完整对等矩阵。

各工作包可以独立测试，但不单独作为功能不完整的正式版本发布。

## 11. 测试策略

### 11.1 单元测试

- 房间分类和工具权限；
- Matrix 事件去重和房间串行；
- Operation 状态转换；
- 幂等键生成；
- SQLite 事务和并发资源锁；
- 配置版本切换；
- 错误分类、重试预算和脱敏；
- AgentScope 工具 schema。

### 11.2 契约测试

- AgtClient 与最新版 `agt -o json` 输出；
- Controller Worker/Team/Human API；
- Matrix 入站、出站、线程、媒体和 E2EE；
- MinIO 上传、下载、条件写入和校验；
- Higress 模型及 MCP 路由。

### 11.3 故障注入

对每个外部副作用边界模拟进程退出和网络结果不明：

- Controller 已创建但响应丢失；
- MinIO 已上传但本地提交失败；
- Matrix 已发送但响应超时；
- 完成通知发送后进程退出；
- SQLite 快照上传中断；
- Manager 重启后 Controller 状态已变化。

验证重启后能够接管已有资源、避免重复消息和继续推进工作流。

### 11.4 功能对等测试

为 16 项 Manager 技能建立清单，每项至少包含：

- 正常路径；
- 权限拒绝；
- 外部系统临时失败；
- Manager 重启恢复；
- 用户可见结果与上游语义对等。

### 11.5 端到端测试矩阵

- 本地安装和 Kubernetes；
- 明文 Matrix 和 E2EE；
- 五种 Worker runtime；
- 单 Worker、Team Leader + 多 Worker；
- 有限任务、无限任务、项目、Git 委派；
- Human/Channel、模型切换、MCP、服务发布；
- Manager 容器重启和节点级恢复。

现有 Controller、Helm、安装和 Worker 回归测试必须继续通过。

## 12. 完成标准

只有同时满足以下条件，才算“全部替换完成”：

1. Manager 容器中运行 AgentScope 2.0 daemon，不运行 OpenClaw Gateway 或 CoPaw App。
2. 16 项最新上游 Manager 技能均有真实工具/工作流实现和自动化测试。
3. 五种 Worker runtime 均可由新 Manager 创建、通信、分派任务和回收结果。
4. Team 仍由 Controller 创建，Manager 只通过 Team Leader 协调。
5. Controller、Matrix、MinIO、Higress、Element 的外部职责和协议保持兼容。
6. 创建 Worker、分派任务和完成通知在故障注入后不会产生不可控重复副作用。
7. 房间权限由代码强制执行。
8. 本地与 Kubernetes 端到端路径通过。
9. 旧 Manager 数据不会被隐式导入；新安装以空 AgentScope 状态启动。
10. 文档明确说明新旧实现差异、状态权威、运维恢复和版本约束。

## 13. 主要风险与缓解

### Matrix 适配复杂

上游 Matrix Channel 已包含 E2EE、重试、媒体和线程等大量边界。直接重写容易回归。实现时先提取传输契约并建立回归测试，再逐步移除 CoPaw 耦合。

### “全部功能”容易只做到提示词兼容

每项技能必须映射到代码入口、权限策略、状态变化和测试用例。只有复制技能文档不计为完成。

### 多系统无法原子提交

不设计虚假的分布式事务。通过操作意图先落盘、稳定幂等键、外部事实对账和可恢复状态机控制风险。

### AgentScope API 升级

固定首版版本并把 AgentScope 隔离在 AgentFactory、SessionAdapter 和 ToolkitAdapter 后面，升级时不扩散到业务工作流。

### 硬替换缺少生产回退

通过镜像版本回滚，而不是在同一 Manager 进程保留两套运行时。发布前必须通过完整对等矩阵；回滚部署不会尝试把新 AgentScope 状态转换回旧会话格式。

## 14. 最终原则

本项目不是把原版 `SKILL.md` 换一种方式交给另一个大模型，也不是把 AgentScope 包在 CoPaw 里面继续使用原流程。

最终结构必须满足：

```text
大模型：理解意图、选择能力、生成内容
AgentScope：Agent 循环、会话、技能、工具、权限、状态接口
Manager 工作流：顺序、幂等、恢复、跨房间协调
Controller：Worker/Team 等资源生命周期权威
Matrix：通信事实权威
MinIO：文件和恢复副本权威
```

这一区分是新 AgentTeams Manager 与原版最根本的差异，也是后续实现和验收不得偏离的架构边界。
