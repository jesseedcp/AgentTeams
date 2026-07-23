import pytest
from agentscope.event import (
    RequireUserConfirmEvent,
    TextBlockDeltaEvent,
    ThinkingBlockDeltaEvent,
    ToolCallStartEvent,
    ToolResultEndEvent,
)
from agentscope.message import ToolCallBlock, ToolResultState

from agentteams_manager.runtime.event_stream import EventStreamProjector


async def async_events(*events):
    for event in events:
        yield event


@pytest.mark.asyncio
async def test_text_deltas_are_accumulated_and_thinking_is_private() -> None:
    projector = EventStreamProjector()

    projection = await projector.consume(
        async_events(
            TextBlockDeltaEvent(
                reply_id="reply",
                block_id="block",
                delta="hello ",
            ),
            ThinkingBlockDeltaEvent(
                reply_id="reply",
                block_id="thinking",
                delta="private",
            ),
            TextBlockDeltaEvent(
                reply_id="reply",
                block_id="block",
                delta="world",
            ),
        ),
    )

    assert projection.text == "hello world"
    assert "private" not in projection.text


@pytest.mark.asyncio
async def test_tool_metadata_and_confirmation_are_preserved() -> None:
    projector = EventStreamProjector()
    call = ToolCallBlock(
        id="call-1",
        name="delete_worker",
        input='{"name":"bob"}',
        state="asking",
    )

    projection = await projector.consume(
        async_events(
            ToolCallStartEvent(
                reply_id="reply",
                tool_call_id="call-1",
                tool_call_name="delete_worker",
            ),
            RequireUserConfirmEvent(
                reply_id="reply",
                tool_calls=[call],
            ),
            ToolResultEndEvent(
                reply_id="reply",
                tool_call_id="call-1",
                state=ToolResultState.DENIED,
            ),
        ),
    )

    assert projection.tool_calls[0].name == "delete_worker"
    assert projection.tool_calls[0].state == "denied"
    assert projection.pending_confirmations == (call,)
