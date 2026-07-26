# Matrix 待确认消息保护设计

日期：2026-07-26

## 问题

AgentScope 在高风险工具调用前会产生 `RequireUserConfirmEvent`，Manager 将其
持久化到房间会话并提示管理员发送 `/confirm <reply_id>` 或
`/deny <reply_id>`。当前 `MatrixSessionRunner.handle()` 只识别格式完全正确的
确认命令；如果管理员在待确认期间发送普通文字，消息会继续进入
`agent.reply_stream()`。此时 AgentScope 正在等待 `UserConfirmResultEvent`，
因此抛出 `ValueError`，Matrix 路由只记录异常而不会给房间发送回复。

线上证据：

- Manager 日志显示 AgentScope 正在等待 1 个工具调用结果，却收到了普通输入；
- SQLite 中对应房间仍保留 `status=awaiting` 的
  `update_manager_identity` 确认；
- Manager Pod 没有重启，Matrix 与 Cinny 均正常。

## 目标行为

1. 房间存在待确认操作时，只有匹配当前 `reply_id` 的 `/confirm` 或 `/deny`
   可以继续 AgentScope 回复。
2. 普通文字、格式不完整的命令和错误 `reply_id` 不进入 AgentScope。
3. 上述无效输入收到一条确定性提示，包含待确认工具名以及当前正确的确认和拒绝
   命令。
4. 待确认状态保持不变，直到管理员使用正确命令。
5. 正确确认、拒绝、权限检查、进程重建恢复和无待确认时的普通对话行为保持不变。

## 方案选择

### 采用：Matrix 适配层前置保护

`MatrixSessionRunner` 在构造 `UserMsg` 前读取房间 Agent 状态。若存在待确认：

- 匹配的确认命令进入现有 `_handle_confirmation()`；
- 其他输入由新的提示方法直接回复并返回。

同一房间的事件已经由 `EventRouter` 串行处理，因此检查待确认状态与后续处理之间
不存在同房间消息并发竞争。保护逻辑位于 Matrix 与 AgentScope 的边界，能够阻止
非法输入进入已经暂停的 AgentScope 状态机。

### 不采用：全局捕获 AgentScope `ValueError`

该做法只隐藏症状，会把其他状态损坏或真正的 AgentScope 错误误报成确认提示。

### 不采用：新消息自动取消待确认操作

自动取消会改变安全语义，并可能让管理员已经审阅的高风险操作静默丢失。

## 消息与幂等性

提示文本使用当前持久化的 `reply_id` 和工具名，不包含工具参数或凭据。Matrix
事务 ID 继续由房间 ID、当前入站事件 ID 和确认 `reply_id` 确定性生成，重放同一
事件不会产生重复消息。

## 测试

- 普通文字在待确认期间只产生提示，不再次调用 Agent；
- 错误确认 ID 只产生提示，待确认状态不变；
- 正确 `/confirm` 仍继续同一 AgentScope reply；
- 既有进程重建确认测试继续通过；
- Manager 全量单元、集成和契约测试通过；
- 部署后使用当前 K8s 房间验证普通文字不再触发异常，并可用正确命令解除确认。

## 不在范围

- 不允许自然语言代替正式确认命令；
- 不改变确认工具列表或管理员权限；
- 不修改 Cinny、Tuwunel、Controller 或 Matrix 数据；
- 不自动确认或拒绝当前线上待确认操作。
