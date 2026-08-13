"""Project AgentScope events into Matrix-safe public output.

把 AgentScope 内部事件流投影为可以公开到 Matrix 的内容。

reply_stream 会产生思考、工具调用、工具结果、文本和媒体等多类事件。projector 只提取
允许展示的摘要与最终回复，并对参数和结果脱敏；确认事件则交给审批流程。这个边界让
用户看到执行证据，同时避免泄露 chain-of-thought、Secret 或未经裁剪的 subprocess
输出。
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass

from agentscope.event import (
    AgentEvent,
    DataBlockDeltaEvent,
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
    ThinkingBlockDeltaEvent,
    ToolCallEndEvent,
    ToolCallStartEvent,
    ToolResultDataDeltaEvent,
    ToolResultEndEvent,
)
from agentscope.message import ToolCallBlock


@dataclass(frozen=True, slots=True)
class ProjectedToolCall:
    tool_call_id: str
    name: str
    state: str


@dataclass(frozen=True, slots=True)
class ProjectedMedia:
    block_id: str
    media_type: str
    data: str | None = None
    url: str | None = None


@dataclass(frozen=True, slots=True)
class StreamProjection:
    text: str
    tool_calls: tuple[ProjectedToolCall, ...]
    media: tuple[ProjectedMedia, ...]
    pending_confirmations: tuple[ToolCallBlock, ...]
    confirmation_reply_id: str | None = None
    thinking_observed: bool = False


class EventStreamProjector:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._text: list[str] = []
        self._tools: dict[str, ProjectedToolCall] = {}
        self._media: list[ProjectedMedia] = []
        self._confirmations: list[ToolCallBlock] = []
        self._confirmation_reply_id: str | None = None
        self._thinking_observed = False

    async def accept(self, event: AgentEvent) -> StreamProjection:
        """Apply one public event and return the current projection."""
        if isinstance(event, TextBlockDeltaEvent):
            self._text.append(event.delta)
        elif isinstance(event, ThinkingBlockDeltaEvent):
            self._thinking_observed = True
        elif isinstance(event, ToolCallStartEvent):
            self._tools[event.tool_call_id] = ProjectedToolCall(
                tool_call_id=event.tool_call_id,
                name=event.tool_call_name,
                state="running",
            )
        elif isinstance(event, ToolCallEndEvent):
            current = self._tools.get(event.tool_call_id)
            if current is not None:
                self._tools[event.tool_call_id] = ProjectedToolCall(
                    tool_call_id=current.tool_call_id,
                    name=current.name,
                    state="submitted",
                )
        elif isinstance(event, ToolResultEndEvent):
            current = self._tools.get(event.tool_call_id)
            if current is not None:
                self._tools[event.tool_call_id] = ProjectedToolCall(
                    tool_call_id=current.tool_call_id,
                    name=current.name,
                    state=str(event.state),
                )
        elif isinstance(event, RequireUserConfirmEvent):
            self._confirmation_reply_id = event.reply_id
            self._confirmations.extend(event.tool_calls)
            for call in event.tool_calls:
                current = self._tools.get(call.id)
                self._tools[call.id] = ProjectedToolCall(
                    tool_call_id=call.id,
                    name=call.name if current is None else current.name,
                    state="asking",
                )
        elif isinstance(event, DataBlockDeltaEvent):
            self._media.append(
                ProjectedMedia(
                    block_id=event.block_id,
                    media_type=event.media_type,
                    data=event.data,
                ),
            )
        elif isinstance(event, ToolResultDataDeltaEvent):
            self._media.append(
                ProjectedMedia(
                    block_id=event.block_id,
                    media_type=event.media_type,
                    data=event.data,
                    url=event.url,
                ),
            )
        return self.snapshot()

    def snapshot(self) -> StreamProjection:
        return StreamProjection(
            text="".join(self._text),
            tool_calls=tuple(self._tools.values()),
            media=tuple(self._media),
            pending_confirmations=tuple(self._confirmations),
            confirmation_reply_id=self._confirmation_reply_id,
            thinking_observed=self._thinking_observed,
        )

    async def consume(
        self,
        events: AsyncIterable[AgentEvent],
    ) -> StreamProjection:
        self.reset()
        async for event in events:
            await self.accept(event)
        return self.snapshot()
