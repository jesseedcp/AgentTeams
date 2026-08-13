# AgentScope Manager 源码阅读入口

这个目录实现的是 AgentTeams 的唯一 Manager runtime。它不是另一套聊天前端，也不是
Controller 的替代品：Cinny 负责显示消息，Matrix 保存房间和消息，Controller 管理
Worker/Team/Human 等资源，AgentScope Manager 负责理解管理员意图，并通过受控工具把
意图交给确定性 workflow 执行。

初学者可以沿着“一条创建 Worker 的消息”读代码：

1. `matrix/client.py` 从 homeserver 同步事件，并转换成 `InboundEvent`。
2. `matrix/router.py` 对事件做一次性 claim；同一房间串行处理，不同房间可并行。
3. `matrix/policy.py` 根据房间和发送者决定这个 turn 真正可见的工具。
4. `matrix/session_runner.py` 组装上下文并驱动 AgentScope 的 `reply_stream`。
5. `tools/resources.py` 校验结构化参数和当前 room policy，然后进入资源 workflow。
6. `workflows/resources.py` 先通过 `workflows/supervisor.py` 记录操作意图，再调用
   `clients/agt.py` 修改 Controller 资源，最后读取实际状态核验结果。
7. `state/operations.py` 与 `state/database.py` 保存本地事务状态；`state/journal.py`
   把脱敏 journal 和快照放到 MinIO，供进程或本地磁盘丢失后的恢复使用。
8. Worker Ready 后，workflow 创建/修复 Matrix 房间拓扑并另发通知；最初的模型 turn
   不需要一直阻塞等待 Pod。

## 先理解三条边界

第一，`manager/agent/` 下的 AGENTS、SOUL、TOOLS 和 skills 是给模型看的 guidance。
它们能帮助模型选择正确工具，却不能新增 capability。真正能力必须同时存在于 typed
tool、room policy、deterministic workflow、恢复 handler 和测试中。

第二，workflow 不把“API 返回了”直接等同于成功。网络 timeout 表示结果不确定：外部
系统可能已经执行，只是回执丢了。因此代码会把 Operation 标为 `reconciling`，再读取
Controller、Matrix、Higress 或 MinIO 的实际状态；立刻重复 create/send 容易产生重复
Worker、重复房间或重复消息。

第三，SQLite、MinIO 和外部系统各有不同职责。SQLite WAL 是当前单 Manager 的事务状态，
MinIO journal/snapshot 用于跨 Pod 灾难恢复；Controller 是资源权威，Matrix 是房间与消息
权威，Higress 是模型/MCP/路由权威。记忆和聊天上下文都不能代替这些实时事实。

## Python 机制速读

- `async def`/`await` 让一个 room 等待网络时，事件循环可以处理其他 room；它不自动
  解决同一对象的并发写入，所以 room、operation 和 target 仍要使用 lock 或事务。
- `Protocol` 描述 workflow 需要什么能力。生产环境传真实 client，测试可传 fake；
  workflow 因此不必知道 HTTP、CLI 或 SDK 的细节。
- Pydantic model 在外部数据进入系统时校验字段和状态。它能拒绝坏数据，但不负责授权。
- `asyncio.to_thread` 把阻塞的标准库 SQLite 调用移出事件循环。一个数据库 callback
  是一个短事务，网络调用不能放在这个 callback 内。
- `try/finally` 用于无论正常、异常还是取消都必须执行的清理；例如释放 active turn、
  保存完整会话或关闭子进程。取消 turn 时要先恢复旧 AgentState，不能保存半段上下文。

建议先读 `application.py` 和 `bootstrap.py` 看全局接线，再按上面的消息链路逐个文件读。
之后分别进入 `workflows/tasks.py`、`workflows/projects.py` 和
`workflows/integrations.py`，理解任务、项目与集成变更怎样复用同一套 durable Operation
规则。
