"""Generic typed management tool with mandatory room authorization."""

from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from typing import Any

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionContext, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk
from pydantic import BaseModel

from agentteams_manager.domain.models import RoomPolicy
from agentteams_manager.runtime.permissions import decide_tool_permission

ToolHandler = Callable[..., Awaitable[object] | object]


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

