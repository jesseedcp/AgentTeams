"""Construct native AgentScope Agents without an app-server wrapper."""

from __future__ import annotations

from typing import Protocol

from agentscope.agent import Agent
from agentscope.credential import OpenAICredential
from agentscope.model import OpenAIChatModel
from agentscope.state import AgentState
from agentscope.tool import Toolkit

from agentteams_manager.config import ManagerConfig, RuntimeDocument
from agentteams_manager.domain.models import RoomPolicy

from .prompts import PromptBuilder
from .config_watcher import RuntimeRegistry


class ToolkitFactory(Protocol):
    async def for_policy(self, policy: RoomPolicy) -> Toolkit: ...


class AgentFactory:
    def __init__(
        self,
        *,
        config: ManagerConfig,
        runtime: RuntimeDocument | RuntimeRegistry,
        prompt_builder: PromptBuilder,
        toolkit_factory: ToolkitFactory,
    ) -> None:
        self._config = config
        self._registry = (
            runtime if isinstance(runtime, RuntimeRegistry) else None
        )
        self._runtime = (
            runtime if isinstance(runtime, RuntimeDocument) else None
        )
        self._prompt_builder = prompt_builder
        self._toolkit_factory = toolkit_factory

    @property
    def runtime_revision(self) -> int:
        return self._current_runtime().revision

    def replace_runtime(self, runtime: RuntimeDocument) -> None:
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
    ) -> Agent:
        runtime = self._current_runtime()
        credential = OpenAICredential(
            api_key=self._config.gateway_key,
            base_url=f"{self._config.ai_gateway_url}/v1",
        )
        model = OpenAIChatModel(
            credential=credential,
            model=runtime.model,
            context_size=runtime.context_window,
            parameters=OpenAIChatModel.Parameters(
                max_tokens=runtime.max_tokens,
                thinking_enable=runtime.reasoning,
            ),
        )
        toolkit = await self._toolkit_factory.for_policy(policy)
        return Agent(
            name=self._config.manager_name,
            system_prompt=self._prompt_builder.build(policy, runtime),
            model=model,
            toolkit=toolkit,
            state=state
            or AgentState(session_id=f"matrix:{room_id}"),
        )

    def _current_runtime(self) -> RuntimeDocument:
        if self._registry is not None:
            return self._registry.current.document
        if self._runtime is None:
            raise RuntimeError("AgentFactory has no runtime document")
        return self._runtime
