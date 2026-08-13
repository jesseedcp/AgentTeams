"""Admin-only AgentScope tools for external channel contacts.

向 Admin DM 暴露外部渠道联系人审核与回复的 typed tools。

工具只把模型参数解析成稳定输入并调用 ``ChannelService``；是否信任联系人、如何绑定
Matrix 房间和怎样持久化由 service/workflow 决定。非 Admin room 根本不会获得这些工具，
避免外部联系人通过自己的渠道消息批准自己。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.channels.base import Provider
from agentteams_manager.channels.service import ChannelService
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import ManagerTool

CHANNEL_TOOL_NAMES = frozenset(
    {
        "list_external_contacts",
        "approve_external_contact",
        "block_external_contact",
        "set_primary_external_contact",
        "send_external_message",
    },
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _Empty(_Input):
    pass


class _Contact(_Input):
    provider: Provider
    external_user_id: str = Field(min_length=1, max_length=300)


class _Message(_Contact):
    text: str = Field(min_length=1, max_length=10_000)


class ChannelToolkitFactory:
    def __init__(self, *, service: ChannelService, yolo: bool) -> None:
        # 逻辑说明：绑定 channel 服务与 YOLO 策略；构造时不创建联系人或发送消息，具体动作由生成的工具回调执行。
        self._service = service
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        # 逻辑说明：先拒绝非管理员私聊，再把联系人服务的只读或变更方法包装成闭合 schema 工具；房间策略与 YOLO 标志继续交给统一权限层执行。
        if policy.kind is not RoomKind.ADMIN_DM:
            return ()
        specs = (
            (
                "list_external_contacts",
                "List pending, trusted, and blocked external contacts.",
                _Empty,
                self._service.list_contacts,
                True,
            ),
            (
                "approve_external_contact",
                "Trust a pending external contact.",
                _Contact,
                self._service.approve,
                False,
            ),
            (
                "block_external_contact",
                "Block an external contact.",
                _Contact,
                self._service.block,
                False,
            ),
            (
                "set_primary_external_contact",
                "Choose one trusted external contact as primary.",
                _Contact,
                self._service.set_primary,
                False,
            ),
            (
                "send_external_message",
                "Send a message to a trusted external contact.",
                _Message,
                self._service.send,
                False,
            ),
        )
        return tuple(
            ManagerTool(
                name=name,
                description=description,
                input_schema=model.model_json_schema(),
                policy=policy,
                handler=handler,
                is_read_only=read_only,
                yolo=self._yolo,
            )
            for name, description, model, handler, read_only in specs
        )
