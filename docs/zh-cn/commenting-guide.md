# AgentTeams 中文注释规范与覆盖清单

这份规范解决两个问题。第一，刚接触 AgentTeams 的开发者打开源文件时，应该能够从注释中
看懂这个文件位于哪条系统链路、状态在哪里改变、失败以后怎样恢复。第二，维护者需要一张
可核对的清单，确认注释不只集中在 AgentScope Manager，而是覆盖正式项目的全部第一方
运行、构建、部署和验证模块。

本轮注释是纯说明改造：允许增加注释、docstring、package documentation 和文档链接，
不允许顺手更改条件、默认值、命令参数、接口、Prompt 行为或部署结果。代码审查时应能把
每一处修改归类为“解释现有行为”，不能归类的修改就不属于本轮范围。

## 写给谁看

目标读者是有少量基本编程经验、但第一次接触 Agent、Matrix、AgentScope、Kubernetes 和
分布式恢复的初学者。注释需要解释项目机制和会影响行为的语言机制，但不逐行翻译赋值、
普通条件和显然的函数调用。

例如下面的注释没有提供额外信息：

```python
# 把计数加一。
count += 1
```

而下面的注释解释了仅从语法看不出的失败语义：

```python
# Controller 可能已经接受请求，只是响应在返回途中超时。因此这里不能把
# timeout 当作确定失败；先把 Operation 标成待协调，再查询实际 Worker 状态。
```

“初学者友好”不等于把每一行改写成中文，而是补足读者无法从语法本身推导出的上下文。
Python 的 `async/await`、Go 的 `context.Context`、Controller reconcile、Shell 的
`set -euo pipefail` 等关键前置知识，先在
[`beginner-code-guide.md`](beginner-code-guide.md) 中建立完整直觉；源文件中的注释再解释
该机制在当前局部为什么必要。

## 三层注释结构

第一层是文件或 package 说明。它应回答“这个文件接收什么、负责什么、把结果交给谁”，
必要时指出权威边界。不要只写“工具函数”或“Worker 逻辑”。例如 Matrix adapter 的文件
说明应说明它把 homeserver event 规范化成什么，以及下游为何不直接依赖 SDK 类型。

第二层是公开类型、关键类和函数说明。它应覆盖输入、返回结果、持久状态、外部副作用与失败
语义中真正重要的部分。Go 的导出标识符注释以标识符名开头，以便 `go doc` 和 lint 工具
识别；Python 使用自然的中文 docstring，参数名和异常名保留英文。

第三层是关键局部注释。它只用于解释“为什么这样写”，尤其是：

- 幂等键、稳定 ID、重复事件和重试；
- 超时为何属于不确定结果，以及之后依据什么外部事实恢复；
- 锁、事务、revision、resource version 和并发顺序；
- Room Policy、审批、凭据隔离和脱敏；
- `spec`/`status`、finalizer、reconcile 与最终收敛；
- 跨 runtime、旧版本、不同平台或不同部署形态的兼容原因；
- 看似多余但删除后会破坏安全、恢复或生命周期的步骤。

局部注释不要复述下一行的函数名，不记录可能很快过期的临时调试结论，也不要把 Issue
讨论原样复制进代码。若解释超过约一个屏幕，应把完整流程放到文档，并在代码处链接到稳定
章节。

## 语言与术语

说明以中文为主，但代码标识符和固定技术名保持原样，例如 `AgentScope`、`Matrix`、
`Worker`、`RoomPolicy`、`Operation ID`、`SQLite WAL`、`Higress` 和 `reconcile`。
已有准确英文注释不为追求统一而删除；可在确有知识缺口时补充中文上下文。

领域词义以根目录 [`CONTEXT.md`](../../CONTEXT.md) 为准。不要在一个文件中称
`Controller` 为“Manager”，在另一个文件中又把 `Matrix Room` 称为“会话”。注释需要
区分 `Project` 与 `Team`、`Task` 与 `Operation`、`Desired State` 与 `Observed State`。

注释和示例不得出现真实 API Key、Matrix access token、GitHub PAT、密码、Secret 值或
可还原的生产凭据。`base64` 只是编码，不是脱敏。示例统一使用明显的占位符，例如
`<your-api-key>`。

## 各语言和文件类型的写法

Python 模块优先使用文件 docstring 说明边界，核心 class/function 再写 docstring；只在复杂
状态转换附近使用 `#`。解释 `asyncio.Lock` 时要说它保护哪个状态，不要泛泛介绍“这是锁”。
Pydantic model 的注释重点是它守住哪条输入或输出协议。

Go package 使用 `doc.go` 提供总体责任，导出类型和函数遵循 Go doc 形式；reconciler 内部
注释应解释重复调用、status 更新和错误重排队。不要在生成的 DeepCopy 或 CRD YAML 上补
注释，应修改类型源头或 package 文档。

Shell 与 PowerShell 在脚本开头说明目标、前置条件、会修改的外部状态和失败行为。危险或
不可逆步骤在调用点说明精确范围。不要用注释掩盖过长的内联命令；本轮又不允许借机重构
命令结构。

Dockerfile 在阶段边界解释该 stage 的产物、为何从前一 stage 复制以及运行时权限边界。
Makefile 在目标组或不明显依赖处解释“为什么需要这个顺序”。普通 `COPY`、`RUN` 和变量赋值
无需逐行翻译。

Helm/YAML 在 template 或 values 源头解释配置关系、Secret 边界、selector 和持久化要求。
JSON 不支持注释，因此在相邻 Markdown、schema 或读取该 JSON 的源代码中说明，不能写入
非标准 `//` 或 `_comment` 字段改变数据协议。

测试注释描述场景、故障注入和通过标准，尤其说明测试在防止什么回归。不要注释每个 assertion；
测试名和输入已经清楚表达的内容不再重复。

## 正式项目注释覆盖矩阵

下表是覆盖审计的依据。“覆盖”表示该模块的第一方、手写、参与运行或交付的内容，应具有
与复杂度相称的文件级、类型级或关键局部注释；不表示每个文件必须增加同样数量的文字。

| 模块 | 在系统中的位置 | 本轮注释重点 | 明确不采用的做法 |
|---|---|---|---|
| `manager-agentscope/` | 唯一 AgentScope Manager 的 Python 实现 | 启停顺序、Matrix 输入、房间隔离、AgentScope streaming、typed tool、审批、workflow、SQLite/Journal、恢复、模型/MCP 热加载、外部 client 边界 | 不给显然的 Pydantic 字段和普通分支逐行翻译；不改运行逻辑 |
| `agentteams-controller/` | Go Controller、CRD 类型源头、REST API 与 `agt` CLI | `spec`/`status`、reconcile、finalizer、幂等 provisioning、auth、backend、配置生成、CLI→REST 数据流、错误与重排队 | 不直接编辑生成的 DeepCopy、生成 CRD 或覆盖率产物 |
| `manager/` 的镜像与启动代码 | 打包并启动 AgentScope Manager 及配套服务 | 镜像 stage、entrypoint 生命周期、进程监督、配置和持久化挂载、Cinny/Tuwunel/MinIO 启动边界 | 不把容器命令逐行翻译成中文 |
| `manager/agent/` | Manager 的 Prompt、SOUL、工具边界与 Skills | 在相邻代码导读和架构文档解释 Prompt、Skill 与 executable tool 的边界；Markdown 内原有教学性说明按运行内容审查 | 不为“加注释”往 Prompt 插入额外解释；原因见下方 Prompt 排除规则 |
| `worker/`、`openclaw-base/` | OpenClaw Worker 镜像与共享基础镜像 | Worker 启动、凭据隔离、配置挂载、共享存储与 Matrix 接入 | 不修改 vendored OpenClaw 内容或依赖包 |
| `copaw/` | CoPaw Worker 包、镜像与入口 | runtime 配置生成、控制台、启动/停止、共享空间和 Matrix 生命周期 | 不注释上游依赖代码 |
| `hermes/` | Hermes Worker 镜像与入口 | sandbox、工作区、运行配置、终止信号和产物边界 | 不复制 Hermes 上游文档到每个脚本 |
| `qwenpaw/` | QwenPaw Worker 包、镜像、plugin reload 与入口 | sync loop、heartbeat、worker API、配置更新、TeamHarness plugin 生命周期 | 不把第三方 QwenPaw package 当成本项目第一方源码改写 |
| `shared/` | 多 runtime 共用的启动或配置材料 | 谁消费共享文件、兼容边界、变更影响面 | 不对静态数据和自解释模板机械加注释 |
| `plugins/` | TeamHarness、WorkerFlow 等扩展集成 | plugin 边界、宿主/插件责任、Prompt 与 executable capability 的区别 | 不在运行时 Prompt 中插教学注释，不改外部插件协议 |
| `helm/agentteams/` | Kubernetes 安装模板与默认配置 | values→resource 的关系、Secret、持久卷、Service selector、hook、RBAC、可选组件 | 不修改打包依赖或生成 CRD 镜像；不让注释影响模板缩进 |
| `install/` | Bash/PowerShell 安装、升级、导入与验证 | 前置条件、持久数据、镜像来源、失败/回滚、跨平台差异、安全输入 | 不注释自动嵌入的大段 payload；不更改默认值 |
| `scripts/` | 调试导出、replay 和维护工具 | 输入输出、隐私脱敏、破坏性边界、运行前提 | 不在示例中写真实凭据 |
| `migrate/` | 迁移 skill、分析和打包脚本 | 在脚本与相邻文档解释迁移来源/目标、保留内容、验证和可恢复性 | `SKILL.md` 属于运行指导，不插入无关教学段落 |
| `hack/` | 本地 kind、镜像镜像源和开发辅助 | 本地环境假设、集群生命周期、镜像流向、清理范围 | 不把临时机器状态写成永久注释 |
| `tests/` | 集成、安装、发布与回归检查 | 每个场景防止的回归、准备条件、故障点、通过标准、cleanup | 不逐条复述 assertion；fixture 数据保持协议原样 |
| `.github/workflows/` | CI、镜像构建、测试、翻译与发布 | job 目的、权限、artifact/镜像流、触发条件和需要外部凭据的边界 | 不注释每个常规 `checkout`/`setup` step；不暴露 Secret |
| `Makefile` | 统一构建、测试、镜像和部署入口 | 目标分组、依赖顺序、缓存/平台差异、会产生的镜像 | 不逐行解释 make 语法；不借机重排目标 |
| 全部第一方 `Dockerfile*` | 把各组件源代码变成可运行镜像 | build/runtime stage、复制产物、用户权限、entrypoint 与架构契约 | 不为普通包安装命令写同义注释 |

`openhuman/` 不属于当前保留的 Worker runtime 集合，也不参与正式运行基线，因此不把它作为
“其他 Worker”补写注释。若将来重新纳入产品，需要先恢复明确的 runtime contract、构建与
测试入口，再按本表标准覆盖；不能仅靠补注释把退役目录变回受支持功能。

## 为什么有些文件必须排除

自动生成文件排除直接修改，包括 Go DeepCopy、生成的 CRD 副本、coverage 输出、编译产物和
安装器生成的内嵌 payload。手写注释会在下一次生成时消失，或者造成两个生成副本不一致。
需要解释的知识应写在生成源、生成脚本或相邻文档中。

第三方和 vendored 代码排除直接修改。项目不拥有这些代码的设计，升级时会被上游覆盖；本地
注释还可能让未来读者误以为项目承诺维护其行为。只在我们的 adapter、Dockerfile、入口或
边界文档说明“我们怎样调用它”。

缓存和临时内容不属于源码，包括 `.venv/`、`.pytest_cache/`、`.mypy_cache/`、
`.ruff_cache/`、`dist/`、coverage 文件和 `tmp-coding/`。这些内容不可审查、不可稳定复现，
也不应提交。

Prompt、`SOUL.md`、`AGENTS.md` 和 `SKILL.md` 需要单独说明：它们看起来像 Markdown，
但在运行时会进入 Agent 上下文并改变模型行为、token 数量或能力选择。往里面插入“这里只是
注释”的自然语言并不是真正的注释，而是产品行为变更，违反本轮“纯注释、运行行为不变”的
边界。因此本轮通过代码导读、架构文档和 Prompt 加载代码旁的注释解释它们；只有本来就属于
Prompt 契约的说明缺失时，才应在一个单独的功能变更中修改并做行为测试。

## 怎样核查覆盖，而不是只数注释行

完成一轮注释后，先按上表逐模块检查第一方入口和复杂核心文件。对每个模块随机选择至少一个
入口和一个关键状态变化点，确认读者能回答：输入来自哪里、状态归谁、外部副作用在哪里发生、
失败后如何处理。某个简单文件没有新增注释并不自动代表遗漏；如果文件名、类型和三行代码已经
完整表达行为，机械注释反而不合格。

然后检查纯说明约束：

```bash
git diff --check
git diff --word-diff=porcelain
```

还要运行语言和配置验证。注释也可能意外破坏 Python docstring、Shell heredoc、PowerShell
block comment、YAML 缩进或 Helm template，所以“没有改逻辑”不能代替测试。至少应执行项目
`AGENTS.md` 指定的 Python、Go、Shell、JSON/YAML、Helm 和 CRD 同步检查。

审查 diff 时重点寻找这些危险信号：条件、字符串值、flag、环境变量默认值、函数签名、JSON
schema、YAML key、容器命令或测试期望发生变化；Prompt/Skill 出现非必要新增文字；生成文件
与源文件同时出现手写改动；注释包含凭据。如果出现任何一项，应从本轮注释改造中移除并作为
独立功能变更处理。

## 如何搭配其他文档阅读

先用 [`beginner-code-guide.md`](beginner-code-guide.md) 沿“创建 Worker”的真实链路建立直觉，
不懂领域词时查 [`CONTEXT.md`](../../CONTEXT.md)，需要系统级权威边界时看
[`architecture.md`](architecture.md)。准备修改代码时再回到本文，判断注释应该放在文件层、
类型层还是局部原因层。这样文档负责完整教学，源代码注释负责在正确位置提醒关键约束，二者
不会重复成两套互相漂移的说明。
