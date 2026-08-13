"""Admin-only AgentScope tools for bounded coding CLI delegation.

向 Admin DM 暴露受限 Coding CLI 委托工具。

模型提供 task、provider 和 prompt 等结构化字段，tool 生成稳定 operation context 后交给
``CodingCLIDelegationService``。可执行文件、workspace 路径、lease、超时和恢复都不由
模型控制；tool schema 是输入边界，不是任意主机命令入口。
"""

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
        # 逻辑说明：绑定房间策略、委托服务和 mutation context，并据此构建本房间可见工具；构造本身不启动 Coding CLI。
        self._policy = policy
        self._service = service
        self._context_provider = (
            context_provider or _current_mutation_context
        )
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        # 逻辑说明：声明状态查询与代码委托的闭合 schema、只读属性和确认提示，再只注册房间策略允许的工具，避免模型获得任意命令执行入口。
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
        # 逻辑说明：为具体 Coding CLI 工具生成调用闭包，闭包在每次执行时重查房间白名单并验证闭合输入，之后才进入可能启动外部进程的 handler。
        async def invoke(**raw: Any) -> object:
            # 逻辑说明：执行时再次核对 allowed_tools，并用 Pydantic 把原始参数变成请求对象后调用固定 handler；验证或权限失败不会触发 CLI。
            if name not in self._policy.allowed_tools:
                raise PermissionDeniedError(
                    f"{name} is not allowed in {self._policy.kind.value}",
                )
            return await handler(request_model.model_validate(raw))

        return invoke

    async def _context(self) -> MutationContext:
        # 逻辑说明：调用可同步或异步的 context provider，等待后验证类型；只接受绑定当前 Matrix 事件和 tool-call 的 MutationContext，防止伪造幂等身份。
        value = self._context_provider()
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, MutationContext):
            raise TypeError("context_provider returned an invalid context")
        return value

    async def _status(self, request: BaseModel) -> object:
        # 逻辑说明：状态工具忽略空请求并返回本地 Coding CLI 能力快照；不启动外部 CLI，也不创建委托 Operation。
        del request
        return self._service.status()

    async def _delegate(self, request: BaseModel) -> object:
        # 逻辑说明：再次验证委托请求并取得当前操作上下文，再把已通过工具确认的请求交给服务；lease、超时、恢复及文件副作用均由服务层负责。
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
        # 逻辑说明：保存共享 Coding CLI 服务和默认确认策略，供每个房间按自身 policy 创建隔离 toolkit；此处不执行委托。
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
    # 逻辑说明：从统一 ContextVar 读取当前房间、Matrix event 与 tool-call ID，并映射为 workflow 的稳定幂等上下文；未绑定时立即报错。
    invocation = current_tool_invocation()
    return MutationContext(
        room_id=invocation.room_id,
        event_id=invocation.event_id,
        tool_call_id=invocation.tool_call_id,
    )
