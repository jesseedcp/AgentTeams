"""AgentTeams QwenPaw Worker 适配包。

本包把上游 QwenPaw 作为可被 Controller 管理的 Worker runtime；Manager 始终是
AgentScope 服务，不会从这里启动。主要链路见 ``worker``、``update`` 与 ``sync``。

AgentTeams QwenPaw worker runtime.
"""

__version__ = "0.1.0"
