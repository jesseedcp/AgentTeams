# AgentTeams 协作域

AgentTeams 协作域描述人类如何通过一个可见、可审计的沟通空间，管理由
Manager 组织、由 Worker 执行的多 Agent 协作。

## 参与者与职责

**Administrator（管理员）**：
拥有 AgentTeams 全局管理权限，并对需要人工授权的管理变更作出决定的人类参与者。
_Avoid_：普通用户、Human、Manager

**Human**：
以受控身份参与指定协作范围的人类成员，不天然拥有全局管理权限。
_Avoid_：Administrator、终端用户、人工 Agent

**Manager**：
理解管理意图、组织协作并监督任务生命周期的唯一管理 Agent；它不充当 Worker
运行时。
_Avoid_：QwenPaw Manager、OpenClaw Manager、Controller

**Controller**：
接受 Managed Resource 的 Desired State，并持续维护和报告 Observed State 的控制角色。
_Avoid_：Manager、Worker、聊天机器人

**Worker**：
承担被分配工作的独立 Agent，拥有自己的身份、能力和协作空间。
_Avoid_：成员容器、机器人账号、Manager

**Team**：
由承担 Team Leader 或普通成员角色的 Worker，以及可选 Human 成员组成的协作边界。
_Avoid_：房间、Worker 列表、Project

**Team Leader**：
代表一个 Team 分解、协调和汇总团队工作的 Worker 角色。
_Avoid_：Manager、Administrator、Team Owner

**Participant（参与者）**：
被明确纳入某个 Project 的 Worker；Team Leader 参与 Project 时仍使用其 Worker 身份。
_Avoid_：Human、所有系统账号、房间成员

## 工作与协作

**Project**：
围绕一个可确认计划组织的持续工作单元，包含参与者、任务关系和整体状态。
_Avoid_：Team、聊天房间、一次对话

**Project Plan（项目计划）**：
Project 开始执行前，由管理员确认的目标、阶段、任务关系和验收约定。
_Avoid_：聊天摘要、Task 列表、Manager 提示词

**Task**：
Project 内可被独立分配、跟踪、验收或返修的最小工作单元。
_Avoid_：消息、Agent 回合、Operation

**Delegation（委派）**：
把一个明确 Task 交给有资格的 Participant 执行的协作承诺。
_Avoid_：发送普通消息、创建 Worker、分配权限

**Revision Task（返修任务）**：
因已有 Task 的结果未通过验收而产生，并与原 Task 保持来源关系的新工作单元。
_Avoid_：直接重写原结果、重新发送同一消息、重试 Operation

**Reassignment（重新分配）**：
在保留同一 Task 身份和历史的前提下，更换其负责 Participant。
_Avoid_：创建新 Task、返修、修改 Team

**Blocker（阻塞）**：
使 Task 暂时不能继续，并且需要额外信息、依赖或人工处理的明确事实。
_Avoid_：一般错误、等待回复、Task 失败

**Acceptance（验收）**：
依据 Task 的预期结果决定其交付是否满足要求的判断。
_Avoid_：Worker 自报完成、消息已送达、Operation 成功

## 沟通边界

**Matrix Room（Matrix 房间）**：
一组明确成员共享消息、上下文和权限关系的可见协作边界。
_Avoid_：Agent 会话、频道适配器、Project

**Admin DM（管理员私聊）**：
Administrator 与 Manager 进行全局管理、接收审批和查看系统通知的专用 Matrix
Room。
_Avoid_：管理员面板、Project Room、Worker Room

**Worker Room**：
Administrator、Manager 与一个独立 Worker 直接协作的 Matrix Room。
_Avoid_：Team Room、Worker 控制台、Worker 进程

**Team Room**：
Team 成员围绕团队工作进行可见协作的 Matrix Room。
_Avoid_：Project Room、Leader Room、Team

**Leader Room**：
Manager 与 Team Leader 进行团队委派和监督的 Matrix Room。
_Avoid_：Team Room、Admin DM、Worker Room

**Project Room**：
Project 的参与者观察项目级进展、变更和结果的 Matrix Room。
_Avoid_：Team Room、Task、Project

**Room Policy（房间策略）**：
规定某类 Matrix Room 中哪些发送者能够使用哪些 Manager 能力的授权边界。
_Avoid_：Prompt、角色描述、Matrix Power Level

**Manager Tool（Manager 工具）**：
具有明确输入契约和 Room Policy 约束、能够执行一项 Manager 能力的命名入口。
_Avoid_：Skill、Prompt、自然语言命令

**Skill**：
指导 Agent 如何判断和使用能力的行为知识，本身不授予或实现 Manager Tool。
_Avoid_：Manager Tool、脚本、权限

**Primary Channel（主渠道）**：
某个外部身份与 Manager 交互时，被指定为主要沟通入口的渠道关系。
_Avoid_：Admin DM、默认房间、可信渠道

**Trusted Channel（可信渠道）**：
被明确允许代表某个外部身份传递受信消息的渠道关系。
_Avoid_：Primary Channel、已加入的任意房间、管理员渠道

## 管理变更与安全

**Management Mutation（管理变更）**：
会改变资源、协作关系、配置或外部系统状态的 Manager 操作。
_Avoid_：查询、模型推理、普通回复

**Confirmation Request（审批请求）**：
一个等待 Administrator 明确批准、拒绝或取消的 Management Mutation 提案。
_Avoid_：Project Plan 确认、普通提问、权限授予

**Elevated Mode（提升模式）**：
Administrator 为当前 Admin DM 会话选择的管理变更确认强度。
_Avoid_：Administrator 角色、系统权限、Worker 权限

**Operation（操作）**：
一次具有稳定身份、可追踪结果并能在中断后继续核对的 Management Mutation
执行记录。
_Avoid_：Task、工具调用、Matrix 消息

**Operation ID（操作 ID）**：
在重复投递、重试和恢复过程中始终指向同一次 Operation 的稳定标识。
_Avoid_：Task ID、Matrix Event ID、随机请求日志 ID

**Desired State（期望状态）**：
管理员希望某个受管资源最终具备的声明。
_Avoid_：当前状态、执行步骤、聊天要求

**Observed State（观测状态）**：
系统能够证明某个受管资源当前实际达到的状态。
_Avoid_：Desired State、模型判断、创建请求已接受

**Convergence（收敛）**：
Observed State 已经与 Desired State 一致的受验证结果。
_Avoid_：请求成功、进程已启动、模型回复“完成”

## 资源与运行身份

**Managed Resource（受管资源）**：
由 AgentTeams 持续维护其 Desired State 与 Observed State 的领域实体。
_Avoid_：任意容器、Matrix 消息、临时 Agent

**Worker Runtime（Worker 运行时）**：
承载一个 Worker 行为与工具能力的可替换执行实现。
_Avoid_：模型、Worker 镜像标签、Manager runtime

**AgentScope Manager Runtime**：
承载唯一 Manager 的运行时身份，不属于可替换的 Worker Runtime 集合。
_Avoid_：AgentScope Worker、QwenPaw Manager、Manager 模型

**Model Route（模型路由）**：
把一个逻辑模型选择映射到受控模型服务的访问关系。
_Avoid_：模型名称、API Key、Worker Runtime

**Shared Workspace（共享工作区）**：
允许获授权的 Agent 交换任务材料和产物的共同文件边界。
_Avoid_：Manager 会话、容器根目录、公开文件区
