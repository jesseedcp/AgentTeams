"""Construct native AgentScope Agents without an app-server wrapper.

为指定 Matrix 房间创建原生 AgentScope Agent 实例。

factory 从当前 runtime generation 选择模型、system prompt、skill toolkit 和房间允许的
typed tools，并可恢复已持久化的 ``AgentState``。每个实例绑定创建时的不可变 generation；
热更新只影响下一个 turn 重建的 Agent，不会在一个正在执行的 tool loop 中途换模型或
工具集合。
"""

from __future__ import annotations

from typing import Any, Literal, Protocol, cast

from agentscope.agent import Agent
from agentscope.credential import OpenAICredential
from agentscope.model import OpenAIChatModel
from agentscope.state import AgentState
from agentscope.tool import Toolkit

from agentteams_manager.config import ManagerConfig, RuntimeDocument
from agentteams_manager.domain.models import RoomPolicy

from .config_watcher import RuntimeRegistry
from .prompts import PromptBuilder


class ToolkitFactory(Protocol):
    async def for_policy(self, policy: RoomPolicy) -> Toolkit: ...


class MCPRegistryPort(Protocol):
    def clients_for(
        self,
        policy: RoomPolicy,
        *,
        revision: int,
    ) -> tuple[Any, ...]: ...

    def retain(self, revision: int) -> None: ...

    async def release(
        self,
        revision: int,
        *,
        active_revision: int,
    ) -> None: ...


ReasoningEffort = Literal[
    "none",
    "minimal",
    "low",
    "medium",
    "high",
    "xhigh",
]


class AgentFactory:
    """从当前不可变 runtime generation 构造房间专属 Agent。

    allowed tools 取自传入 policy，而非 skill 文本；模型、MCP 与 prompt 取自同一 revision，
    避免混用半套新配置。``retire`` 负责在旧 turn 结束后释放 generation-scoped MCP。
    """
    def __init__(
        self,
        *,
        config: ManagerConfig,
        runtime: RuntimeDocument | RuntimeRegistry,
        prompt_builder: PromptBuilder,
        toolkit_factory: ToolkitFactory,
        mcp_registry: MCPRegistryPort | None = None,
    ) -> None:
        # 逻辑说明：保存 Manager 配置、prompt/toolkit/MCP 依赖，并区分固定 RuntimeDocument 与可热更新 RuntimeRegistry；同时初始化 Agent 到 generation 的租约索引，构造阶段不创建模型或 MCP 连接。
        self._config = config
        self._registry = (
            runtime if isinstance(runtime, RuntimeRegistry) else None
        )
        self._runtime = (
            runtime if isinstance(runtime, RuntimeDocument) else None
        )
        self._prompt_builder = prompt_builder
        self._toolkit_factory = toolkit_factory
        self._mcp_registry = mcp_registry
        self._agent_generations: dict[int, int] = {}

    @property
    def runtime_revision(self) -> int:
        # 逻辑说明：从固定 RuntimeDocument 或 registry 当前 generation 统一读取 revision，供会话判断是否需要换代；这里只查询，不创建或退休 Agent。
        return self._current_runtime().revision

    def replace_runtime(self, runtime: RuntimeDocument) -> None:
        # 逻辑说明：仅允许非 registry 模式用 revision 更高的 RuntimeDocument 替换固定配置；registry 模式或 revision 未递增时直接报错，校验失败不会改写当前 runtime。
        if self._registry is not None:
            raise RuntimeError(
                "registry-backed runtime changes through ConfigWatcher",
            )
        current = self._current_runtime()
        if runtime.revision <= current.revision:
            raise ValueError("runtime revision must increase")
        self._runtime = runtime

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
        model_override: str | None = None,
        thinking_effort: str | None = None,
    ) -> Agent:
        # 逻辑说明：从当前 generation 和房间覆盖项构造模型、policy toolkit、MCP 与 system prompt，再用传入 state 或房间 session_id 创建 Agent；成功后才保留 MCP revision 租约，任一步异常均向上传播且不登记 Agent。
        runtime = self._current_runtime()
        credential = OpenAICredential(
            api_key=self._config.gateway_key,
            base_url=f"{self._config.ai_gateway_url}/v1",
        )
        model = OpenAIChatModel(
            credential=credential,
            model=model_override or runtime.model,
            context_size=runtime.context_window,
            parameters=OpenAIChatModel.Parameters(
                max_tokens=runtime.max_tokens,
                thinking_enable=(
                    runtime.reasoning
                    if thinking_effort is None
                    else thinking_effort != "off"
                ),
                reasoning_effort=(
                    None
                    if thinking_effort in {None, "off"}
                    else cast(ReasoningEffort, thinking_effort)
                ),
            ),
        )
        toolkit = await self._toolkit_factory.for_policy(policy)
        if self._mcp_registry is not None:
            clients = self._mcp_registry.clients_for(
                policy,
                revision=runtime.revision,
            )
            toolkit.tool_groups[0].mcps.extend(clients)
        tool_schemas = await toolkit.get_tool_schemas()
        registered_tools = tuple(
            sorted(
                str(schema["function"]["name"])
                for schema in tool_schemas
            ),
        )
        agent = Agent(
            name=self._config.manager_name,
            system_prompt=self._prompt_builder.build(
                policy,
                runtime,
                registered_tools=registered_tools,
            ),
            model=model,
            toolkit=toolkit,
            state=state
            or AgentState(session_id=f"matrix:{room_id}"),
        )
        if self._mcp_registry is not None:
            self._mcp_registry.retain(runtime.revision)
            self._agent_generations[id(agent)] = runtime.revision
        return agent

    async def retire(self, agent: Agent) -> None:
        """Release generation resources after the Agent's turn is idle."""
        # 逻辑说明：按 Agent 对象身份移除其 generation 租约记录，并通知 MCP registry 释放该 revision；未启用 MCP 或 Agent 未登记时为空操作，释放失败由调用方处理。
        if self._mcp_registry is None:
            return
        revision = self._agent_generations.pop(id(agent), None)
        if revision is None:
            return
        await self._mcp_registry.release(
            revision,
            active_revision=self.runtime_revision,
        )

    def _current_runtime(self) -> RuntimeDocument:
        # 逻辑说明：registry 模式返回当前已激活 generation 的文档，否则返回固定 runtime；两种来源都不存在表示 factory 装配错误并抛出 RuntimeError。
        if self._registry is not None:
            return self._registry.current.document
        if self._runtime is None:
            raise RuntimeError("AgentFactory has no runtime document")
        return self._runtime
