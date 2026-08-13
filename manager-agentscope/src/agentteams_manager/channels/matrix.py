"""Matrix escalation edge for external channel messages.

把需要人工或 Manager 介入的外部渠道消息升级到 Matrix。

外部联系人不能直接获得 Manager 的完整管理能力。本模块把可信渠道事件送入预先绑定
的 Matrix 房间，让后续处理继续经过房间 policy、审计和会话隔离。它是渠道与 Matrix
之间的边缘适配层，不负责决定联系人是否可信，也不在本地伪造 Matrix 身份。
"""

from __future__ import annotations

from agentteams_manager.domain.ids import matrix_transaction_id
from agentteams_manager.domain.ports import MatrixPort

from .base import ChannelMessage


class MatrixChannelEscalation:
    def __init__(self, *, matrix: MatrixPort, admin_room_id: str) -> None:
        # 逻辑说明：固定 Matrix 传输和受控管理员房间，外部身份永远不能自行指定特权目标房间。
        self._matrix = matrix
        self._admin_room_id = admin_room_id

    async def first_contact(self, message: ChannelMessage) -> None:
        # 逻辑说明：把陌生联系人升级到 Admin room，并提示管理员走显式审核工具。
        await self._send(
            message,
            "首次外部联系人等待审批",
            "使用 approve_external_contact 或 block_external_contact 处理。",
        )

    async def trusted_message(self, message: ChannelMessage) -> None:
        # 逻辑说明：可信消息仍进入 Matrix policy 边界，而不是从外部渠道直接获得管理能力。
        await self._send(message, "外部渠道消息", message.text)

    async def _send(
        self,
        message: ChannelMessage,
        title: str,
        detail: str,
    ) -> None:
        # 逻辑说明：用 provider、联系人和消息 ID 派生幂等 txn，重试不会生成重复 Matrix 消息。
        seed = (
            f"channel:{message.provider}:{message.external_user_id}:"
            f"{message.message_id}"
        )
        await self._matrix.send_text(
            self._admin_room_id,
            (
                f"**{title}**\n\n"
                f"- 渠道：`{message.provider}`\n"
                f"- 联系人：{message.display_name} "
                f"(`{message.external_user_id}`)\n"
                f"- 内容：{detail}"
            ),
            txn_id=matrix_transaction_id(seed, 0),
        )
