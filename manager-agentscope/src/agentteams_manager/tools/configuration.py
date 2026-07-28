"""Policy-bound AgentScope tools for model configuration."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, Field

from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import (
    ManagerTool,
    current_tool_invocation,
)
from agentteams_manager.workflows.integrations import (
    IntegrationService,
    ManagerIdentityRequest,
    ModelSwitchRequest,
)
from agentteams_manager.workflows.resources import MutationContext

CONFIGURATION_TOOL_NAMES = frozenset(
    {
        "switch_model",
        "switch_worker_model",
        "update_manager_identity",
    },
)


class _ManagerModelInput(ModelSwitchRequest):
    pass


class _WorkerModelInput(ModelSwitchRequest):
    worker: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")


class _ManagerIdentityInput(ManagerIdentityRequest):
    pass


ContextProvider = Callable[
    [],
    MutationContext | Awaitable[MutationContext],
]


class ConfigurationToolkit:
    def __init__(
        self,
        *,
        policy: RoomPolicy,
        service: IntegrationService,
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
        tools = (
            self._tool(
                name="switch_model",
                description=(
                    "Preflight and switch the Manager model for future turns."
                ),
                request_model=_ManagerModelInput,
                handler=self._switch_manager,
            ),
            self._tool(
                name="switch_worker_model",
                description=(
                    "Preflight and switch one Controller Worker model."
                ),
                request_model=_WorkerModelInput,
                handler=self._switch_worker,
            ),
            self._tool(
                name="update_manager_identity",
                description=(
                    "Persist the confirmed Manager name, language, "
                    "communication style, and behavior guidelines."
                ),
                request_model=_ManagerIdentityInput,
                handler=self._update_manager_identity,
            ),
        )
        return tuple(
            tool
            for tool in tools
            if (
                tool.name in self._policy.allowed_tools
                and (
                    (
                        tool.name == "switch_model"
                        and self._policy.kind is RoomKind.ADMIN_DM
                    )
                    or (
                        tool.name == "switch_worker_model"
                        and self._policy.kind
                        in {
                            RoomKind.ADMIN_DM,
                            RoomKind.LEADER_ROOM,
                        }
                    )
                    or (
                        tool.name == "update_manager_identity"
                        and self._policy.kind is RoomKind.ADMIN_DM
                    )
                )
            )
        )

    def _tool(
        self,
        *,
        name: str,
        description: str,
        request_model: type[BaseModel],
        handler: Callable[[BaseModel], Awaitable[object]],
    ) -> ManagerTool:
        async def invoke(**raw: Any) -> object:
            if name not in self._policy.allowed_tools:
                raise PermissionDeniedError(
                    f"{name} is not allowed in this room",
                )
            return await handler(request_model.model_validate(raw))

        return ManagerTool(
            name=name,
            description=description,
            input_schema=request_model.model_json_schema(),
            policy=self._policy,
            handler=invoke,
            yolo=self._yolo,
        )

    async def _context(self) -> MutationContext:
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned invalid context")
        return value

    async def _switch_manager(self, request: BaseModel) -> object:
        item = _ManagerModelInput.model_validate(request)
        return await self._service.switch_manager_model(
            ModelSwitchRequest.model_validate(item.model_dump()),
            context=await self._context(),
        )

    async def _switch_worker(self, request: BaseModel) -> object:
        item = _WorkerModelInput.model_validate(request)
        if (
            not self._policy.resource_scope_all
            and item.worker not in self._policy.allowed_worker_names
            and item.worker != self._policy.resource_name
        ):
            raise PermissionDeniedError(
                f"worker/{item.worker} is outside this room's scope",
            )
        values = item.model_dump(exclude={"worker"})
        return await self._service.switch_worker_model(
            worker=item.worker,
            request=ModelSwitchRequest.model_validate(values),
            context=await self._context(),
        )

    async def _update_manager_identity(
        self,
        request: BaseModel,
    ) -> object:
        item = _ManagerIdentityInput.model_validate(request)
        return await self._service.update_manager_identity(
            ManagerIdentityRequest.model_validate(item.model_dump()),
            context=await self._context(),
        )


class ConfigurationToolkitFactory:
    def __init__(
        self,
        *,
        service: IntegrationService,
        yolo: bool = False,
    ) -> None:
        self._service = service
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        return ConfigurationToolkit(
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
