# Manager Agent 内容说明

本目录中的 `AGENTS.md`、`SOUL.md`、`TOOLS.md`、`HEARTBEAT.md` 和 `skills/` 会被装入
AgentScope Manager 的模型上下文。它们是 **guidance（行为指导）**，不是 executable
capability（可执行能力）。例如在 skill 中写“创建 Worker”只能教模型何时调用
`create_worker`；如果 Python 没有注册这个 typed tool，或者当前 Matrix room policy 不
允许它，模型仍然无法创建 Worker。

可以把完整能力理解为五层：

1. `AGENTS.md`、`SOUL.md` 和 skill 告诉模型应该怎样判断与协作。
2. `manager-agentscope/.../tools/` 定义模型可以提交的结构化参数。
3. `matrix/policy.py` 按房间与发送者裁剪当前 turn 的工具集合，并决定是否确认。
4. `workflows/` 负责顺序、幂等、恢复和外部效果，不能依赖模型“记住步骤”。
5. Controller、Matrix、Higress、MinIO 等系统保存各自的权威事实。

因此，修改本目录的运行文本可能直接改变 Agent 的行为，即使它看起来只是新增一段说明。
本次初学者注释工作没有改动这些 prompt/skill 正文，而把阅读说明放在这个不会由
`PromptBuilder` 自动拼入 system prompt 的独立 README 中。以后新增能力时，不能只改
skill；还要同步 typed tool、room policy、workflow、recovery、测试和 tool/skill parity
清单。

`/elevated` 也遵守这条边界：`full` 只取消 Admin DM 中的工具级审批，不会增加该房间
原本没有的工具，也不会放宽 sender 身份、资源范围、路径 allowlist 或 Secret 保护。
Project plan 是独立的产品确认门；除显式 YOLO policy 外，即使 `full` 也不能替管理员
自动确认计划。
