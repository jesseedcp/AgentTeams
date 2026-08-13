"""AgentTeams Hermes Worker 的第一方启动与适配包。

上游 hermes-agent 提供 Agent 循环，本包补上 Controller 配置翻译、MinIO 恢复与
Matrix 策略。它只运行 Worker；规划、审批和资源管理仍由 AgentScope Manager 完成。

hermes_worker package: AgentTeams Worker bootstrap on top of hermes-agent.
"""

__version__ = "0.1.0"
