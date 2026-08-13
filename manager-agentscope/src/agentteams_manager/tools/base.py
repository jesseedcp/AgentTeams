"""Generic typed management tool with mandatory room authorization.

为所有 AgentScope 管理工具提供统一的类型校验与强制授权。

AgentScope 决定“想调用哪个工具”，本模块在执行前读取当前 Matrix turn context，校验
工具是否属于 room policy，并在需要时产生可持久化 confirmation event。只有通过这一
层才进入 deterministic workflow；因此新增 skill 文档不会自动获得 capability，新增
工具也不能绕过房间、发送者、资源范围和确认模式。
"""

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
from agentscope.permission import (
    PermissionBehavior,
    PermissionContext,
    PermissionDecision,
)
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

    def __init__(self, *args: Any, metrics: Any | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._metrics = metrics

    async def call_tool(
        self,
        tool_call: ToolCallBlock,
        state: AgentState,
    ) -> AsyncGenerator[ToolChunk | ToolResponse, None]:
        token = _TOOL_CALL_ID.set(tool_call.id)
        if self._metrics is not None:
            self._metrics.increment(
                "agentteams_manager_tool_calls_total",
            )
        try:
            async for item in super().call_tool(tool_call, state):
                yield item
        except Exception:
            if self._metrics is not None:
                self._metrics.increment(
                    "agentteams_manager_tool_errors_total",
                )
            raise
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
        confirmation_message: str | None = None,
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
        self._confirmation_message = confirmation_message

    async def check_permissions(
        self,
        tool_input: dict[str, Any],
        context: PermissionContext,
    ) -> PermissionDecision:
        del tool_input, context
        decision = decide_tool_permission(
            tool_name=self.name,
            policy=self._policy,
            yolo=self._yolo,
        )
        if (
            decision.behavior is PermissionBehavior.ASK
            and self._confirmation_message is not None
        ):
            return PermissionDecision(
                behavior=decision.behavior,
                message=self._confirmation_message,
                decision_reason=decision.decision_reason,
                bypass_immune=decision.bypass_immune,
            )
        return decision

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
