"""Admin-only typed tools for durable Manager memory.

提供受限的长期记忆写入、召回与证据记录工具。

记忆工具用于稳定偏好、项目决策和有事实依据的 Worker 评估，不应保存 Secret、原始工具
输出或实时资源状态。scope 由当前房间和输入模型约束；写入经过去重与长度限制，召回
也只返回当前 room 可以看到的投影。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import PermissionDeniedError
from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import (
    ManagerTool,
    ToolInvocationContext,
    current_tool_invocation,
)
from agentteams_manager.workflows.memory import ManagerMemoryService

MEMORY_TOOL_NAMES = frozenset(
    {
        "recall_manager_memory",
        "remember_manager_memory",
        "record_project_decision",
        "record_worker_assessment",
    },
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RecallMemoryInput(_Input):
    query: str | None = Field(default=None, max_length=500)
    project_id: str | None = Field(
        default=None,
        pattern=r"^project-[A-Za-z0-9-]+$",
    )
    worker_name: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    limit: int = Field(default=20, ge=1, le=100)


class RememberMemoryInput(_Input):
    category: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )
    content: str = Field(min_length=1, max_length=20_000)
    importance: float = Field(default=5, ge=0, le=10)


class RecordProjectDecisionInput(_Input):
    project_id: str = Field(pattern=r"^project-[A-Za-z0-9-]+$")
    decision: str = Field(min_length=1, max_length=10_000)
    rationale: str = Field(min_length=1, max_length=20_000)


class RecordWorkerAssessmentInput(_Input):
    worker_name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    capability: str = Field(min_length=1, max_length=200)
    score: float = Field(ge=0, le=1)
    evidence: str = Field(min_length=1, max_length=20_000)


ContextProvider = Callable[[], ToolInvocationContext]


class MemoryToolkit:
    """Expose private durable memory only in the administrator DM."""

    def __init__(
        self,
        *,
        policy: RoomPolicy,
        service: ManagerMemoryService,
        context_provider: ContextProvider = current_tool_invocation,
        yolo: bool = False,
    ) -> None:
        # 逻辑说明：绑定房间记忆策略、服务和 mutation context，并构造可见记忆工具；构造阶段不读取或写入记忆数据库。
        self._policy = policy
        self._service = service
        self._context_provider = context_provider
        self._yolo = yolo
        self.tools = self._build_tools()

    def _build_tools(self) -> tuple[ManagerTool, ...]:
        # 逻辑说明：只在管理员私聊声明召回、长期记忆、项目决策和 Worker 评估工具，并按 allowed_tools 过滤；读工具标记为并发安全，写工具仍走权限策略。
        if self._policy.kind is not RoomKind.ADMIN_DM:
            return ()
        specs: tuple[
            tuple[
                str,
                str,
                type[BaseModel],
                Callable[[BaseModel], Awaitable[object]],
                bool,
            ],
            ...,
        ] = (
            (
                "recall_manager_memory",
                "Recall bounded durable Manager memory and evidence.",
                RecallMemoryInput,
                self._recall,
                True,
            ),
            (
                "remember_manager_memory",
                "Curate one durable long-term Manager memory.",
                RememberMemoryInput,
                self._remember,
                False,
            ),
            (
                "record_project_decision",
                "Record one project decision and its rationale durably.",
                RecordProjectDecisionInput,
                self._record_project_decision,
                False,
            ),
            (
                "record_worker_assessment",
                "Record evidence-backed Worker capability assessment.",
                RecordWorkerAssessmentInput,
                self._record_worker_assessment,
                False,
            ),
        )
        return tuple(
            ManagerTool(
                name=name,
                description=description,
                input_schema=request_model.model_json_schema(),
                policy=self._policy,
                handler=self._handler(request_model, handler),
                is_read_only=read_only,
                is_concurrency_safe=read_only,
                yolo=self._yolo,
            )
            for (
                name,
                description,
                request_model,
                handler,
                read_only,
            ) in specs
            if name in self._policy.allowed_tools
        )

    def _handler(
        self,
        request_model: type[BaseModel],
        handler: Callable[[BaseModel], Awaitable[object]],
    ) -> Callable[..., Awaitable[object]]:
        # 逻辑说明：为私有记忆操作生成 policy-bound 闭包；每次调用先确认仍是 Admin DM 并完成 schema 验证，越权输入不会到达记忆存储。
        async def invoke(**raw: Any) -> object:
            # 逻辑说明：执行时再次确认是 Admin DM，验证闭合输入后调用固定记忆 handler；越权请求不会接触私有记忆仓库。
            if self._policy.kind is not RoomKind.ADMIN_DM:
                raise PermissionDeniedError(
                    "private Manager memory is only available in Admin DM",
                )
            return await handler(request_model.model_validate(raw))

        return invoke

    def _context(self) -> ToolInvocationContext:
        # 逻辑说明：读取当前调用上下文并要求实际 room ID 与冻结的 policy 一致；不匹配立即拒绝，防止 ContextVar 串线泄漏跨房间记忆。
        context = self._context_provider()
        if context.room_id != self._policy.room_id:
            raise PermissionDeniedError(
                "memory invocation room does not match room policy",
            )
        return context

    async def _recall(self, request: BaseModel) -> object:
        # 逻辑说明：验证查询过滤条件，绑定当前 Admin 房间后调用服务返回有界私有投影；limit 控制结果规模，仓库读取失败直接传播。
        item = RecallMemoryInput.model_validate(request)
        context = self._context()
        return await self._service.recall(
            room_id=context.room_id,
            include_private=True,
            project_id=item.project_id,
            worker_name=item.worker_name,
            query=item.query,
            limit=item.limit,
        )

    async def _remember(self, request: BaseModel) -> object:
        # 逻辑说明：验证类别、内容和重要度，用 event ID 与 tool-call ID 生成稳定 source ID 后持久化；重复调用由服务按来源去重。
        item = RememberMemoryInput.model_validate(request)
        context = self._context()
        return await self._service.remember(
            room_id=context.room_id,
            source_event_id=_memory_source_id(context),
            category=item.category,
            content=item.content,
            importance=item.importance,
        )

    async def _record_project_decision(
        self,
        request: BaseModel,
    ) -> object:
        # 逻辑说明：验证项目决策与理由，绑定房间和稳定来源后交给服务写入；工具层不把实时状态或 Secret 混入决策记录。
        item = RecordProjectDecisionInput.model_validate(request)
        context = self._context()
        return await self._service.record_project_decision(
            room_id=context.room_id,
            source_event_id=_memory_source_id(context),
            project_id=item.project_id,
            decision=item.decision,
            rationale=item.rationale,
        )

    async def _record_worker_assessment(
        self,
        request: BaseModel,
    ) -> object:
        # 逻辑说明：验证 Worker、能力、分数和证据，并以当前调用来源持久化评估；服务负责去重与可见性，失败不会返回伪造记录。
        item = RecordWorkerAssessmentInput.model_validate(request)
        context = self._context()
        return await self._service.record_worker_assessment(
            room_id=context.room_id,
            source_event_id=_memory_source_id(context),
            worker_name=item.worker_name,
            capability=item.capability,
            score=item.score,
            evidence=item.evidence,
        )


class MemoryToolkitFactory:
    def __init__(
        self,
        *,
        service: ManagerMemoryService,
        yolo: bool = False,
    ) -> None:
        # 逻辑说明：保存共享 Memory workflow 与默认确认策略，后续为每个房间生成独立工具集合；此处不触发记忆整理。
        self._service = service
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        return MemoryToolkit(
            policy=policy,
            service=self._service,
            yolo=self._yolo,
        ).tools


def _memory_source_id(context: ToolInvocationContext) -> str:
    return f"{context.event_id}:{context.tool_call_id}"
