"""Matrix thread relations and bounded room history.

识别 Matrix thread 关系，并构造有界的房间历史上下文。

回复线程中的消息应继续回到同一 thread，而不是污染主时间线。本模块从事件 relation
解析 root ID，读取历史时限制消息数和字符数，并过滤不应再次喂给模型的事件类型。
历史只用于上下文，不会改变“当前消息才触发本 turn”的边界。
"""

from __future__ import annotations

from collections import defaultdict, deque

from agentteams_manager.domain.models import InboundEvent


class ThreadProjector:
    """Build Matrix relation payloads without transport side effects."""

    @staticmethod
    def relation(thread_id: str) -> dict[str, object]:
        return {
            "rel_type": "m.thread",
            "event_id": thread_id,
            "is_falling_back": True,
            "m.in_reply_to": {"event_id": thread_id},
        }

    @staticmethod
    def replacement(
        event_id: str,
    ) -> dict[str, str]:
        return {
            "rel_type": "m.replace",
            "event_id": event_id,
        }


class RoomHistory:
    """Keep a strictly bounded normalized history per room."""

    def __init__(self, *, limit: int) -> None:
        # 逻辑说明：校验每房间历史上限并创建按 room_id 延迟分配的有界 deque；负上限直接拒绝，构造过程不访问 Matrix 或数据库。
        if limit < 0:
            raise ValueError("history limit cannot be negative")
        self.limit = limit
        self._events: dict[str, deque[InboundEvent]] = defaultdict(
            lambda: deque(maxlen=self.limit),
        )

    def append(self, event: InboundEvent) -> None:
        # 逻辑说明：上限大于零时把事件追加到所属房间队列，deque 会自动淘汰最旧项；limit=0 明确表示不保留历史。
        if self.limit:
            self._events[event.room_id].append(event)

    def entries(self, room_id: str) -> tuple[InboundEvent, ...]:
        # 逻辑说明：把指定房间当前 deque 复制为不可变 tuple 返回；不存在的房间得到空 tuple，调用方无法借返回值修改内部历史。
        return tuple(self._events.get(room_id, ()))

    def prefix(
        self,
        room_id: str,
        *,
        exclude_event_id: str | None = None,
    ) -> str:
        # 逻辑说明：按保存顺序把房间事件格式化为模型上下文，并可排除正在处理的 event_id；无历史返回空串，避免注入空的上下文标题。
        lines = [
            f"{event.sender_id}: {event.body}"
            for event in self.entries(room_id)
            if event.event_id != exclude_event_id
        ]
        if not lines:
            return ""
        return "[Recent room history]\n" + "\n".join(lines) + "\n\n"
