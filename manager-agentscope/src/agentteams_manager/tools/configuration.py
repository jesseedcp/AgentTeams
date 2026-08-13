"""Policy-bound AgentScope tools for model configuration.

提供 Manager/Worker 模型与 Manager identity 的受控配置工具。

模型切换先验证目标模型和 gateway，再由 Controller 发布新 runtime revision；identity
更新则写入 Controller desired state，而不是直接改容器里的 SOUL 文件。这些变更通常
需要确认，且热更新只在 turn 边界激活，保证正在进行的 Agent loop 配置不漂移。
"""

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
        # 逻辑说明：组合房间 policy、配置服务和 mutation context，并只注册该房间允许的配置工具；不会在构造时写运行配置。
        self._policy = policy
        self._service = service
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        # 逻辑说明：先构造三种配置变更工具，再同时按 allowed_tools 和房间种类过滤；管理员可改 Manager，Leader 只能在资源范围内改 Worker。
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
        # 逻辑说明：把工具元数据、Pydantic 输入模型和固定配置 handler 封装成 ManagerTool；真正调用时再次核验房间白名单并在 schema 验证后才进入 workflow。
        async def invoke(**raw: Any) -> object:
            # 逻辑说明：执行时复核工具授权、验证闭合输入，再调用对应配置 workflow；失败前不会发布 runtime revision。
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
        # 逻辑说明：解析同步或异步 context provider，并验证它返回当前 Matrix turn 的 MutationContext；无效上下文拒绝状态变更。
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned invalid context")
        return value

    async def _switch_manager(self, request: BaseModel) -> object:
        # 逻辑说明：把工具输入规范为通用模型切换请求，附上当前幂等上下文后交给集成服务预检并发布；服务错误直接返回调用链处理。
        item = _ManagerModelInput.model_validate(request)
        return await self._service.switch_manager_model(
            ModelSwitchRequest.model_validate(item.model_dump()),
            context=await self._context(),
        )

    async def _switch_worker(self, request: BaseModel) -> object:
        # 逻辑说明：验证 Worker 请求并按房间 resource scope 二次授权，移除目标名后调用服务；越权时在任何 Controller 副作用前拒绝。
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
        # 逻辑说明：验证 Manager identity 字段并携带当前操作上下文交给服务持久化 desired state；工具层不直接改容器或本地 SOUL 文件。
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
        # 逻辑说明：保存共享配置 workflow 和确认策略，后续按房间构造独立 toolkit；不在 Factory 创建时修改模型、MCP 或身份。
        self._service = service
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        # 逻辑说明：为不可变 RoomPolicy 创建一次房间级 ConfigurationToolkit，并只返回其已按工具白名单过滤的工具元组；Factory 不缓存跨房间权限。
        return ConfigurationToolkit(
            policy=policy,
            service=self._service,
            yolo=self._yolo,
        ).tools


def _current_mutation_context() -> MutationContext:
    # 逻辑说明：把当前工具调用的房间、事件和 call ID 复制为配置 workflow 的幂等身份；脱离 Matrix turn 调用会由统一边界拒绝。
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )
