"""Matrix escalation edge for external channel messages."""

from __future__ import annotations

from agentteams_manager.domain.ids import matrix_transaction_id
from agentteams_manager.domain.ports import MatrixPort

from .base import ChannelMessage


class MatrixChannelEscalation:
    def __init__(self, *, matrix: MatrixPort, admin_room_id: str) -> None:
        self._matrix = matrix
        self._admin_room_id = admin_room_id

    async def first_contact(self, message: ChannelMessage) -> None:
        await self._send(
            message,
            "首次外部联系人等待审批",
            "使用 approve_external_contact 或 block_external_contact 处理。",
        )

    async def trusted_message(self, message: ChannelMessage) -> None:
        await self._send(message, "外部渠道消息", message.text)

    async def _send(
        self,
        message: ChannelMessage,
        title: str,
        detail: str,
    ) -> None:
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
