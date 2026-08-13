"""向 QwenPaw 注册 AgentTeams 第一方 Matrix transport。

QwenPaw 的 plugin API 只需要知道 channel 类型与可配置字段；真正的登录、房间策略、
媒体和回复逻辑位于 ``agentteams_matrix.channel``。这个插件用于 Worker，不会注册
AgentScope Manager 的工具或审批能力。

Register the AgentTeams-owned Matrix transport with QwenPaw.
"""


class AgentTeamsMatrixPlugin:
    """把 Matrix channel 暴露给 QwenPaw 的插件发现机制。"""
    def register(self, api):
        # 逻辑说明：`register` 延迟导入 channel 并注册类型及配置 schema；不启动网络连接。
        from .agentteams_matrix.channel import AgentTeamsMatrixChannel

        api.register_channel(
            AgentTeamsMatrixChannel,
            label="AgentTeams Matrix",
            description="Managed Matrix transport for AgentTeams rooms.",
            config_fields=[
                {"name": "enabled", "label": "Enabled", "type": "switch"},
                {"name": "homeserver", "label": "Homeserver", "type": "text", "required": True},
                {"name": "user_id", "label": "User ID", "type": "text", "required": True},
                {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
                {"name": "encryption", "label": "Encryption", "type": "switch"},
                {"name": "require_mention", "label": "Require mention", "type": "switch"},
                {"name": "show_thinking", "label": "Show thinking", "type": "switch"},
                {"name": "show_tool_calls", "label": "Show tool calls", "type": "switch"},
                {"name": "show_tool_results", "label": "Show tool results", "type": "switch"},
            ],
        )


plugin = AgentTeamsMatrixPlugin()
