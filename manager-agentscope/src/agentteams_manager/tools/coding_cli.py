"""Admin-only AgentScope tools for bounded coding CLI delegation."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, ConfigDict

from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import RoomPolicy
from agentteams_manager.tools.base import (
    ManagerTool,
    current_tool_invocation,
)
from agentteams_manager.workflows.coding_cli import (
    CodingCLIDelegationRequest,
    CodingCLIDelegationService,
)
from agentteams_manager.workflows.resources import MutationContext

CODING_CLI_TOOL_NAMES = frozenset(
    {
        "coding_cli_status",
        "delegate_coding_cli",
    },
)


class _EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]


class CodingCLIToolkit:
    """Expose no coding execution surface outside authorized room policy."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        service: CodingCLIDelegationService | Any,
        context_provider: ContextProvider | None = None,
        yolo: bool = False,
    ) -> None:
        self._policy = policy
        self._service = service
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        specs = (
            (
                "coding_cli_status",
                "Report configured and mounted coding CLI providers.",
                _EmptyInput,
                self._status,
                True,
                None,
            ),
            (
                "delegate_coding_cli",
                "Run one confirmed, bounded coding task in a leased workspace.",
                CodingCLIDelegationRequest,
                self._delegate,
                False,
                (
                    "This coding CLI can modify files in the selected task "
                    "workspace. Confirm the provider, task, and prompt."
                ),
            ),
        )
        return tuple(
            ManagerTool(
                name=name,
                description=description,
                input_schema=request_model.model_json_schema(),
                policy=self._policy,
                handler=self._handler(name, request_model, handler),
                is_read_only=read_only,
                is_concurrency_safe=read_only,
                yolo=self._yolo,
                confirmation_message=confirmation_message,
            )
            for (
                name,
                description,
                request_model,
                handler,
                read_only,
                confirmation_message,
            ) in specs
            if name in self._policy.allowed_tools
        )

    def _handler(
        self,
        name: str,
        request_model: type[BaseModel],
        handler: Callable[[BaseModel], Awaitable[object]],
    ) -> Callable[..., Awaitable[object]]:
        async def invoke(**raw: Any) -> object:
            if name not in self._policy.allowed_tools:
                raise PermissionDeniedError(
                    f"{name} is not allowed in {self._policy.kind.value}",
                )
            return await handler(request_model.model_validate(raw))

        return invoke

    async def _context(self) -> MutationContext:
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned an invalid context")
        return value

    async def _status(self, request: BaseModel) -> object:
        del request
        return self._service.status()

    async def _delegate(self, request: BaseModel) -> object:
        item = CodingCLIDelegationRequest.model_validate(request)
        return await self._service.execute(
            item,
            context=await self._context(),
            confirmed=True,
        )


class CodingCLIToolkitFactory:
    def __init__(
        self,
        *,
        service: CodingCLIDelegationService,
        yolo: bool = False,
    ) -> None:
        self._service = service
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        return CodingCLIToolkit(
            policy=policy,
            service=self._service,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )
