# 初学者代码导读：从一条 Cinny 消息到一个可用的 Worker

这份导读解决一个很具体的问题：当你在 Cinny 中发送“创建一个名为 `alice`
的 QwenPaw Worker”时，项目里的哪些代码真正参与了工作？它们各自为什么存在？
最后为什么不能把这件事简单理解成“Manager 执行一条 Docker 命令”？

我们会始终沿着这一条消息向前走，不把 Python、Go、Matrix、AgentScope 和
Kubernetes 拆成互不相干的概念。读完以后，你应该能从页面上的一条消息一路追到
`Worker` 的 Kubernetes 资源、Pod 和 Matrix 房间，再沿着回复链路回到 Cinny。

> 本文讲的是当前项目的真实主链路。先不要逐行阅读整个仓库；按照文中的文件顺序
> 打开代码，每次只回答“输入从哪里来、状态在哪里改变、结果交给谁”这三个问题。

## 先建立一个完整画面

假设管理员在 Cinny 的 Admin DM 中发送：

```text
创建一个名为 alice 的 QwenPaw Worker，模型使用 deepseek-v4-flash。
```

这条操作的主链路是：

```text
Cinny
  -> Matrix homeserver
  -> Manager 的 MatrixClient / EventRouter
  -> 房间对应的 AgentScope Agent
  -> create_worker typed tool
  -> ResourceService 持久化 Operation
  -> Python AgtClient 启动 agt CLI
  -> agt 调用 Controller REST API
  -> Controller 创建 Worker CR
  -> WorkerReconciler 让实际基础设施逐步收敛
  -> Matrix 账号与房间、模型凭据、运行配置、Pod/容器准备完成
  -> Manager 后台核实 Worker Ready
  -> Matrix 中发送完成通知
  -> Cinny 显示最终结果
```

这里有三种角色很容易混淆。`AgentScope Manager` 负责理解“管理员想要什么”，
但它不是 Kubernetes 的资源权威；`agt` 是一个类型明确的命令行边界，负责把 Python
侧的请求安全地交给 Go Controller；`Controller` 才负责让 `Worker` 的期望状态最终
变成真实账号、房间、配置和运行实例。项目采用这种分工，是为了避免大模型自己拼接
任意 shell 命令，也避免一次网络超时导致系统不知道 Worker 到底有没有创建成功。

## 第零步：程序启动时先把所有零件接起来

运行入口是
[`manager-agentscope/src/agentteams_manager/main.py`](../../manager-agentscope/src/agentteams_manager/main.py)。
`main()` 从环境变量加载 `ManagerConfig`，然后用 `asyncio.run()` 启动异步程序。
`asyncio` 可以把它理解成一个调度员：当 Matrix 网络请求正在等待响应时，它允许心跳、
其他房间和健康检查继续工作，而不是让整个 Manager 停住。

真正把系统零件连接起来的是
[`bootstrap.py`](../../manager-agentscope/src/agentteams_manager/bootstrap.py) 中的
`build_application()`。这个函数叫 composition root，即“组装根”：它创建 SQLite
仓储、MinIO journal、Matrix client、`AgtClient`、工作流、typed tools、AgentScope
Agent factory、房间会话管理器、恢复任务和心跳，再把它们交给
[`ManagerApplication`](../../manager-agentscope/src/agentteams_manager/application.py)。

`ManagerApplication.start()` 的顺序很重要：先启动健康接口和 SQLite，再恢复中断操作，
然后加载运行配置、启动 Matrix 同步，最后启动心跳。如果恢复尚未完成就开始处理新消息，
系统可能在不知道旧操作结果的情况下再次创建同名 Worker。停止时则按相反方向关闭，
并保存房间会话。

初学者第一次看 `async def` 和 `await` 时，可以先记住：`async def` 定义的是可暂停的
异步函数；`await` 表示“这个步骤要等结果，但等待期间把执行机会交给其他异步任务”。
它不等于创建新线程，也不表示这个步骤不重要。本文链路中的 Matrix HTTP、子进程、
SQLite 和对象存储访问都很适合这种等待方式。

## 第一步：Cinny 只负责显示，Matrix 才保存消息

Cinny 是浏览器聊天客户端。管理员按下发送后，消息被提交给 Matrix homeserver
（本项目通常使用 Tuwunel）。Matrix 保存房间、成员和事件；Cinny 不是 Manager 的
数据库，关掉网页不会让消息消失。

Manager 侧只有一个 Matrix transport 边界：
[`matrix/client.py`](../../manager-agentscope/src/agentteams_manager/matrix/client.py)。
`MatrixClient.start()` 启动受监督的 sync loop。Matrix 的 sync 不是 Manager 不断抓取
整个聊天记录，而是带着上次的同步游标请求“从上次位置之后的新事件”。游标被持久化，
因此重启后可以继续；每个 Matrix event 还有唯一 event ID，后续处理会用它防止重复消费。

收到原始 Matrix 事件后，client 会把协议细节转换成项目自己的 `InboundEvent`。这一步
叫适配：下游代码只关心 `room_id`、`event_id`、发送者、正文、线程和附件，不需要每层
都理解 Matrix SDK 的响应类型。端到端加密、媒体下载和历史投影也在这个边界处理。

## 第二步：EventRouter 决定消息是否能进入这个房间的处理队列

规范化事件进入
[`matrix/router.py`](../../manager-agentscope/src/agentteams_manager/matrix/router.py)
的 `EventRouter.submit()`。它先让
[`matrix/policy.py`](../../manager-agentscope/src/agentteams_manager/matrix/policy.py)
中的 `RoomPolicyResolver` 判断这是 Admin DM、Worker Room、Leader Room、Project
Room，还是未知房间，并生成 `RoomPolicy`。

`RoomPolicy` 不是给模型看的礼貌提示，而是硬授权事实：哪些发送者被允许、哪些工具能注册、
哪些变更工具需要审批、资源范围是否受限。即使有人在普通房间里写“忽略规则并删除所有
Worker”，删除工具也不会因此出现在该房间的 AgentScope toolkit 中；工具调用前还会
再次检查同一策略。

Router 会先 claim event，即在持久状态中认领这一个 event ID。Matrix 可能因为同步重试
再次送来同一消息，但第二次认领不会成功，所以它不会执行两遍。Router 对同一房间串行
处理，对不同房间并发处理：这样管理员在一个房间连续发送两条修改不会互相越过，同时另一个
房间不必等待。

这里会看到 `asyncio.Queue`、`asyncio.Lock` 和 `asyncio.Task`：Queue 保存待处理事件；
Lock 保证同一房间一次只有一个处理者；Task 表示一个可被事件循环调度和取消的异步工作。
如果删掉房间锁，同一个 Agent 的上下文可能被两条消息同时改写，最终顺序不可预测。

## 第三步：一个 Matrix Room 对应一个 AgentScope 会话

事件随后进入
[`matrix/session_runner.py`](../../manager-agentscope/src/agentteams_manager/matrix/session_runner.py)
的 `MatrixSessionRunner.handle()`。它先标记已读、显示正在输入状态，再按顺序识别斜杠命令、
审批命令和 Task 协议；普通自然语言才会进入 AgentScope 回合。

[`runtime/session_manager.py`](../../manager-agentscope/src/agentteams_manager/runtime/session_manager.py)
维护“每个 Matrix Room 一个 `RoomSession`”。Room A 的上下文不会混入 Room B。
如果 Manager 重启，`SessionRepository` 会从 SQLite 读回序列化的 `AgentState`；如果模型、
Prompt、MCP 或房间策略 revision 发生变化，Manager 会基于旧 state 创建新 Agent，避免活跃
回合执行到一半突然换掉工具。

`AgentFactory.create()` 位于
[`runtime/agent_factory.py`](../../manager-agentscope/src/agentteams_manager/runtime/agent_factory.py)。
它从当前 runtime document 构造模型连接，从 `RoomPolicy` 构造 toolkit，再构造 AgentScope
`Agent`。模型请求发往 Higress 的 OpenAI-compatible 接口；真正的上游 API Key 由网关管理，
不会作为聊天上下文交给 Worker。

随后 `RoomSessionManager.run_input()` 调用 AgentScope 原生 `reply_stream()`。这里的 stream
表示模型产生的文本、思考、工具调用和工具结果可以逐个事件返回；
`MatrixSessionRunner._run_and_project()` 把这些事件投影成 Matrix 中逐步更新的一条可见回复。
所以页面上看到一条消息不断被编辑，不代表 Manager 反复发送最终答案，而是流式事件被合并
进同一个 Matrix message。

## 第四步：模型选择 create_worker，但模型不能直接创建资源

AgentScope 根据系统 Prompt、管理员消息和工具 schema，决定调用 `create_worker`。工具由
[`tools/resources.py`](../../manager-agentscope/src/agentteams_manager/tools/resources.py)
中的 `ResourceToolkit` 注册。`WorkerCreateRequest` 是 Pydantic model：它规定 `name`、
`runtime`、`model`、`skills` 等字段的类型，并拒绝额外字段。

Pydantic 可以把它理解成进入业务逻辑之前的海关。模型生成的 JSON 必须先被验证，不能把
拼错的字段或任意 shell 片段直接交给系统。`ResourceToolkit._create_worker()` 还会再次检查
当前 `RoomPolicy` 的资源范围，并从当前 Matrix event 与 AgentScope tool call 取得稳定身份。

审批也发生在 typed tool 边界，而不是依靠模型“记得询问”。
[`runtime/permissions.py`](../../manager-agentscope/src/agentteams_manager/runtime/permissions.py)
会返回 `ALLOW`、`ASK` 或 `DENY`。若需要 `ASK`，AgentScope 产生
`RequireUserConfirmEvent`，Runner 将它保存为全局 `ConfirmationRequest` 并通知 Admin DM；
管理员发送 `/confirm <id>` 后，系统用 `UserConfirmResultEvent` 继续原来的 AgentScope
reply，而不是让模型猜测审批已经发生。

`/elevated off` 使用房间原本的确认集合；`/elevated ask` 会让当前 Admin DM 的所有已允许
工具都要求确认；`/elevated full` 会清空确认集合，但不会扩大该房间原本允许使用的工具。
“无需确认”和“拥有任意权限”是两件不同的事。

## 第五步：workflow 先记录 Operation，再产生外部副作用

工具不会自己执行一连串不受控步骤，而是调用
[`workflows/resources.py`](../../manager-agentscope/src/agentteams_manager/workflows/resources.py)
中的 `ResourceService.create_worker()`。它先让
[`OperationSupervisor`](../../manager-agentscope/src/agentteams_manager/workflows/supervisor.py)
创建 Operation，Operation ID 来自 Matrix room、event 和 tool call 的稳定组合。

这里最重要的不是“记录日志”，而是保证一次管理意图只有一个身份。考虑这个真实故障：
Controller 已经接受创建 `alice`，但响应返回前网络超时。如果 Manager 直接把超时记为失败，
重试时可能再创建一次；如果直接记为成功，又可能掩盖真正失败。当前协议会在外部请求前记录
`effect_planned`，超时则进入可协调状态，之后查询 Controller 的事实来判断 `alice` 是否存在、
配置是否匹配。

SQLite WAL 保存活跃 Manager 的事务状态。WAL（write-ahead log，预写日志）意味着变更先
追加到日志，再并入主数据库；一个写入者工作时，读取者仍可读取一致视图。当前架构只有一个
活跃 Manager 写入，因此不需要额外部署 Redis。MinIO/S3 journal 和快照提供远端恢复材料，
但它们不取代 SQLite 的本地事务职责。

`create_worker()` 的当前行为是异步受理：`_accept_worker_create()` 先把请求交给 Controller，
然后 `_schedule_worker_create_finalization()` 创建后台 Task。Manager 可以先回复“已受理、后台
创建中”，无需在一个聊天回合中阻塞数分钟；后台会持续核对 Worker room 与 Ready 状态，成功
后向 Worker Room 发送欢迎语，并在 Admin DM 发送完成通知。

## 第六步：Python 只允许用参数数组调用 agt

Python 到 Controller 的唯一业务入口是
[`clients/agt.py`](../../manager-agentscope/src/agentteams_manager/clients/agt.py)。
`AgtClient.create_worker()` 把已验证对象转换成这样的参数数组：

```text
agt create worker --name alice --runtime qwenpaw \
  --model deepseek-v4-flash --no-wait -o json
```

注意代码使用的是数组，而不是一整段 shell 字符串。
[`clients/process.py`](../../manager-agentscope/src/agentteams_manager/clients/process.py)
调用 `asyncio.create_subprocess_exec(*argv)`，并且只允许预先列入 allowlist 的可执行文件。
如果把模型文本拼成 `shell=True` 命令，名字里夹带引号、分号或命令替换语法就可能执行额外
命令；参数数组让每个值只作为一个参数传递。

`agt` 是 Go 写的命令行客户端。入口
[`agentteams-controller/cmd/agt/create.go`](../../agentteams-controller/cmd/agt/create.go)
使用 Cobra 解析 flag、验证 Worker name、填充默认 model，并把请求编码成 JSON。
因为 Manager 传了 `--no-wait`，CLI 在 Controller 接受请求后立刻返回；后台 Ready 核实由
Manager workflow 负责。

[`cmd/agt/client.go`](../../agentteams-controller/cmd/agt/client.go) 的 `APIClient` 从部署环境读取
Controller URL 与 bearer token，向 `/api/v1/workers` 发 `POST`。这里的 bearer token 是
Manager 到 Controller 的服务身份，不应进入模型 Prompt、错误回复或文档示例。

## 第七步：Controller 创建的是 Worker CR，不是直接创建 Pod

Go HTTP 路由在
[`internal/server/http.go`](../../agentteams-controller/internal/server/http.go)
注册；auth middleware 先核对调用者是否能创建 Worker，才进入
[`internal/server/resource_handler.go`](../../agentteams-controller/internal/server/resource_handler.go)
的 `CreateWorker()`。

Handler 解码 JSON、检查 runtime、构造 `v1beta1.Worker`，再通过 Kubernetes client
创建它。这个对象称为 CR（Custom Resource，自定义资源）：Kubernetes 原生只认识 Pod、
Service 等常见资源，而 CRD（CustomResourceDefinition）让项目定义 `Worker`、`Team`、
`Manager`、`Human` 这些新资源类型。类型源头在
[`api/v1beta1/types.go`](../../agentteams-controller/api/v1beta1/types.go)。

CR 的 `spec` 与 `status` 必须分开理解：`spec` 是 Desired State，例如 `alice` 应使用
QwenPaw 和指定模型；`status` 是 Controller 实际观测到的 Matrix user ID、room ID、phase、
容器状态和错误信息。收到 HTTP 201 只证明 CR 创建成功，不证明 Worker 已经 Ready。

## 第八步：Reconcile 把期望状态反复推向真实状态

Kubernetes 发现新 Worker CR 后，会调用
[`internal/controller/worker_controller.go`](../../agentteams-controller/internal/controller/worker_controller.go)
的 `WorkerReconciler.Reconcile()`。Reconcile 不是“只运行一次的创建脚本”，而是一个可以被
反复调用的收敛函数：每次读取 Desired State 与 Observed State，补齐缺失部分，然后更新
status。Pod 重启、网络临时失败、人工删除 Service 或 spec 更新都会再次触发它。

这解释了为什么 reconcile 代码必须幂等。幂等不是“不产生变化”，而是同一个目标执行一次或
多次，最终结果相同。例如 `EnsureUser` 要复用已经存在的 Matrix 账号，不能每次 reconcile
都生成新身份；创建房间时要处理已有 alias，而不能悄悄把稳定 alias 留在历史旧房间。

普通集群内 Worker 会依次经过这些阶段：

1. `ReconcileMemberInfra` 确保 Matrix、存储和 gateway 等基础身份存在；
2. `EnsureModelProviderAuth` 与 `EnsureMemberServiceAccount` 准备受限访问身份；
3. `ReconcileMemberConfig` 生成该 runtime 需要的无密钥或受控配置；
4. `ReconcileMemberContainer` 创建或更新实际 Pod/容器；
5. `ReconcileMemberService` 与 expose 逻辑按需要提供网络入口；
6. 把 room ID、Matrix user ID、container state、spec hash 等写回 `Worker.status`。

基础设施实现集中在
[`internal/service/provisioner.go`](../../agentteams-controller/internal/service/provisioner.go)。
以 `ProvisionWorker()` 为例，它加载或生成凭据、注册 Matrix 账号、配置存储用户、创建
Worker Room、准备 Higress consumer。Pod/容器的具体形态则由 runtime backend 与 deployer
处理，所以 OpenClaw、CoPaw、Hermes 和 QwenPaw 可以共存，而 Controller 仍提供统一资源
生命周期。

Go 中经常出现 `context.Context` 和 `defer`。`context.Context` 不是业务数据容器，而是把
取消、超时和请求生命周期向下传递；上游请求取消后，下游网络操作应尽快停止。`defer`
表示在当前函数返回前执行清理或收尾，本链路中常用于关闭响应体、记录指标和统一 patch
status。`goroutine` 是 Go 的轻量并发执行单元；Controller runtime 可以并发协调不同资源，
但同一对象的状态更新仍依赖 resource version 和冲突重试，不能假设“只会调用一次”。

## 第九步：为什么 Manager 最终能在 Cinny 中报告 Ready

Controller 每次 reconcile 都更新 `Worker.status`。Manager 后台的
`resume_worker_create()` 通过 `AgtClient.get_worker()` 轮询这个权威事实，直到 Worker
达到可用 phase 且存在 Matrix room。它还会验证实际 Worker 与原请求匹配，再刷新 topology；
如果只看到“HTTP 创建成功”就通知完成，管理员可能点进一个尚不存在或指向旧 alias 的房间。

验证完成后，workflow 使用从 Operation ID 派生的 Matrix transaction ID 发送欢迎语和完成
通知。Matrix transaction ID 让重试发送保持幂等：如果网络让 Manager 不确定第一条消息
是否送达，相同 transaction ID 的重试不会产生两条重复欢迎语。消息写入 homeserver 后，
Cinny 的 sync 收到它并显示给管理员，于是完整链路闭环。

如果 Manager 在中途重启，启动恢复会从 SQLite snapshot 和不可变 journal 找出未完成
Operation，调用对应 `resume_*` 工作流查询外部事实。它不是从模型的聊天文字猜“上次做到哪”，
而是根据 Controller、Matrix、Higress 和存储的可验证状态继续收敛。

## Shell、PowerShell、Docker 和 Helm 在这条链路之前做什么

前面的运行链路成立有一个前提：所有组件已经以匹配的配置启动。根
[`Makefile`](../../Makefile)、各模块 Dockerfile、
[`install/`](../../install/) 安装器和
[`helm/agentteams/`](../../helm/agentteams/) Chart 负责这个部署阶段。

Shell 脚本里的 `set -euo pipefail` 通常表示：命令失败时停止、使用未定义变量时停止、管道中
任一步失败都算失败。删除它可能让安装器在中间失败后继续，最终显示“完成”但组件缺失。
PowerShell 传递的往往是对象而非纯文本，`$ErrorActionPreference = 'Stop'` 等设置决定非终止
错误是否真正中断安装。Dockerfile 按层构建镜像，改动靠前的层会让后续缓存失效；Helm 则把
`values.yaml` 的用户输入渲染成 Kubernetes YAML。

Helm template 中的 `{{ ... }}` 不是 Kubernetes 语法，而是安装前由 Helm 求值的模板语法。
最终交给 Kubernetes 的必须是合法 YAML。Secret 中的值通常经过 base64 表示，但 base64
只是编码，不是加密；因此不要把渲染后的 Secret、API Key 或 bearer token粘进注释和文档。

## 建议的第一次代码阅读顺序

现在回到同一个 `alice` 例子，按下面顺序打开文件。每看完一个文件，只需确认它接收什么，
改变什么，交给谁：

1. `manager-agentscope/src/agentteams_manager/main.py`
2. `manager-agentscope/src/agentteams_manager/bootstrap.py`
3. `manager-agentscope/src/agentteams_manager/matrix/client.py`
4. `manager-agentscope/src/agentteams_manager/matrix/router.py`
5. `manager-agentscope/src/agentteams_manager/matrix/policy.py`
6. `manager-agentscope/src/agentteams_manager/runtime/session_manager.py`
7. `manager-agentscope/src/agentteams_manager/runtime/agent_factory.py`
8. `manager-agentscope/src/agentteams_manager/tools/resources.py`
9. `manager-agentscope/src/agentteams_manager/workflows/resources.py`
10. `manager-agentscope/src/agentteams_manager/workflows/supervisor.py`
11. `manager-agentscope/src/agentteams_manager/clients/agt.py`
12. `agentteams-controller/cmd/agt/create.go`
13. `agentteams-controller/internal/server/resource_handler.go`
14. `agentteams-controller/api/v1beta1/types.go`
15. `agentteams-controller/internal/controller/worker_controller.go`
16. `agentteams-controller/internal/service/provisioner.go`

遇到一个新名词时，先查根目录 [`CONTEXT.md`](../../CONTEXT.md)。它固定“这个词在 AgentTeams
业务中是什么意思”；遇到实现选择时再查
[`architecture.md`](architecture.md)。阅读源代码注释时，可用
[`commenting-guide.md`](commenting-guide.md) 判断一条注释是在解释业务责任、数据流还是风险，
不要把注释误当成比测试和运行事实更高的权威。

## 你可以怎样验证自己真的理解了

不要只背组件名字。尝试用 `alice` 回答以下问题：

- 为什么 Cinny 页面刷新后消息仍在？因为消息权威是 Matrix，不是浏览器内存。
- 为什么模型说“创建成功”不能作为成功证明？因为模型不拥有 Worker Observed State。
- 为什么 Controller 返回 HTTP 201 后还要后台等待？因为这只创建了 CR，真实基础设施尚在收敛。
- 为什么同一 Matrix 事件不能处理两次？否则一次用户意图可能产生重复外部副作用。
- 为什么超时不能直接判定失败？请求可能已经在远端成功，只是响应丢失。
- 为什么 `--no-wait` 不是“放任不管”？Manager 持久化了 Operation，并由后台与恢复流程继续核实。
- 为什么 `/elevated full` 仍不能在未知房间删除 Worker？它只改变确认策略，不扩大 Room Policy。

当这些问题都能沿真实文件回答时，你掌握的就不只是架构图，而是这套系统为什么能在失败、
重试和多房间并发下仍保持可管理。
