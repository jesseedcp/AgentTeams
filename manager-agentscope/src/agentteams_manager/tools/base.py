"""Generic typed management tool with mandatory room authorization."""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncGenerator
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from agentscope.message import (
    TextBlock,
    ToolCallBlock,
    ToolResultState,
)
from agentscope.permission import PermissionContext, PermissionDecision
from agentscope.state import AgentState
from agentscope.tool import (
    ToolBase,
    ToolChunk,
    Toolkit,
    ToolResponse,
)
from pydantic import BaseModel

from agentteams_manager.domain.models import RoomPolicy
from agentteams_manager.runtime.permissions import decide_tool_permission

ToolHandler = Callable[..., Awaitable[object] | object]


@dataclass(frozen=True, slots=True)
class ToolInvocationContext:
    room_id: str
    event_id: str
    tool_call_id: str


_TURN_CONTEXT: ContextVar[tuple[str, str] | None] = ContextVar(
    "agentteams_matrix_turn",
    default=None,
)
_TOOL_CALL_ID: ContextVar[str | None] = ContextVar(
    "agentteams_tool_call_id",
    default=None,
)


@contextmanager
def bind_matrix_turn(room_id: str, event_id: str):
    """Bind Matrix identity for all tool calls made by one Agent turn."""
    token = _TURN_CONTEXT.set((room_id, event_id))
    try:
        yield
    finally:
        _TURN_CONTEXT.reset(token)


def current_tool_invocation() -> ToolInvocationContext:
    turn = _TURN_CONTEXT.get()
    tool_call_id = _TOOL_CALL_ID.get()
    if turn is None or not tool_call_id:
        raise RuntimeError(
            "management tool called outside a bound Matrix turn",
        )
    return ToolInvocationContext(
        room_id=turn[0],
        event_id=turn[1],
        tool_call_id=tool_call_id,
    )


class ManagerToolkit(Toolkit):
    """Toolkit that exposes the real AgentScope tool-call ID to workflows."""

    async def call_tool(
        self,
        tool_call: ToolCallBlock,
        state: AgentState,
    ) -> AsyncGenerator[ToolChunk | ToolResponse, None]:
        token = _TOOL_CALL_ID.set(tool_call.id)
        try:
            async for item in super().call_tool(tool_call, state):
                yield item
        finally:
            _TOOL_CALL_ID.reset(token)


class ManagerTool(ToolBase):
    """A closed-schema AgentScope tool backed by one workflow method."""

    def __init__(
        self,
        *,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        policy: RoomPolicy,
        handler: ToolHandler,
        is_read_only: bool = False,
        is_concurrency_safe: bool = False,
        yolo: bool = False,
    ) -> None:
        super().__init__()
        if input_schema.get("type") != "object":
            raise ValueError("Manager tool input schema must be an object")
        if input_schema.get("additionalProperties") is not False:
            raise ValueError(
                "Manager tool input schema must reject extra properties",
            )
        self.name = name
        self.description = description
        self.input_schema = input_schema
        self.is_read_only = is_read_only
        self.is_concurrency_safe = is_concurrency_safe
        self._policy = policy
        self._handler = handler
        self._yolo = yolo

    async def check_permissions(
        self,
        tool_input: dict[str, Any],
        context: PermissionContext,
    ) -> PermissionDecision:
        del tool_input, context
        return decide_tool_permission(
            tool_name=self.name,
            policy=self._policy,
            yolo=self._yolo,
        )

    async def call(self, **kwargs: Any) -> ToolChunk:
        result = self._handler(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, BaseModel):
            text = result.model_dump_json()
            metadata = result.model_dump(mode="json")
        elif isinstance(result, str):
            text = result
            metadata = {}
        else:
            text = json.dumps(
                result,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                default=str,
            )
            metadata = result if isinstance(result, dict) else {}
        return ToolChunk(
            content=[TextBlock(text=text)],
            state=ToolResultState.SUCCESS,
            is_last=True,
            metadata=metadata,
        )
