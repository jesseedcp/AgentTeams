"""Project AgentScope events into Matrix-safe public output."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass

from agentscope.event import (
    AgentEvent,
    DataBlockDeltaEvent,
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
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


class EventStreamProjector:
    async def consume(
        self,
        events: AsyncIterable[AgentEvent],
    ) -> StreamProjection:
        text: list[str] = []
        tools: dict[str, ProjectedToolCall] = {}
        media: list[ProjectedMedia] = []
        confirmations: list[ToolCallBlock] = []

        async for event in events:
            if isinstance(event, TextBlockDeltaEvent):
                text.append(event.delta)
            elif isinstance(event, ToolCallStartEvent):
                tools[event.tool_call_id] = ProjectedToolCall(
                    tool_call_id=event.tool_call_id,
                    name=event.tool_call_name,
                    state="running",
                )
            elif isinstance(event, ToolCallEndEvent):
                current = tools.get(event.tool_call_id)
                if current is not None:
                    tools[event.tool_call_id] = ProjectedToolCall(
                        tool_call_id=current.tool_call_id,
                        name=current.name,
                        state="submitted",
                    )
            elif isinstance(event, ToolResultEndEvent):
                current = tools.get(event.tool_call_id)
                if current is not None:
                    tools[event.tool_call_id] = ProjectedToolCall(
                        tool_call_id=current.tool_call_id,
                        name=current.name,
                        state=str(event.state),
                    )
            elif isinstance(event, RequireUserConfirmEvent):
                confirmations.extend(event.tool_calls)
                for call in event.tool_calls:
                    current = tools.get(call.id)
                    tools[call.id] = ProjectedToolCall(
                        tool_call_id=call.id,
                        name=call.name if current is None else current.name,
                        state="asking",
                    )
            elif isinstance(event, DataBlockDeltaEvent):
                media.append(
                    ProjectedMedia(
                        block_id=event.block_id,
                        media_type=event.media_type,
                        data=event.data,
                    ),
                )
            elif isinstance(event, ToolResultDataDeltaEvent):
                media.append(
                    ProjectedMedia(
                        block_id=event.block_id,
                        media_type=event.media_type,
                        data=event.data,
                        url=event.url,
                    ),
                )

        return StreamProjection(
            text="".join(text),
            tool_calls=tuple(tools.values()),
            media=tuple(media),
            pending_confirmations=tuple(confirmations),
        )
