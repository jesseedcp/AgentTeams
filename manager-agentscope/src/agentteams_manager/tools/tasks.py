"""Closed request contracts for task and project management tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.tasks import TaskReceipt, TaskService


class CreateFiniteTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str = Field(min_length=1)
    specification: str = Field(min_length=1)
    assigned_to: str = Field(min_length=1)
    delegated_to_team: str | None = None
    project_id: str | None = None
    project_room_id: str | None = None


class CompleteTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1)
    worker_event_id: str = Field(min_length=1)
    structured_result: dict[str, Any] | None = None


class CancelTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1)


class TaskTools:
    """Thin typed facade; AgentScope registration is added at the task gate."""

    def __init__(self, service: TaskService) -> None:
        self._service = service

    async def create_finite(
        self,
        request: CreateFiniteTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.create_finite(
            title=request.title,
            spec=request.specification,
            assigned_to=request.assigned_to,
            delegated_to_team=request.delegated_to_team,
            project_id=request.project_id,
            project_room_id=request.project_room_id,
            context=context,
        )

    async def complete(
        self,
        request: CompleteTaskInput,
    ) -> TaskReceipt:
        return await self._service.record_completion(
            task_id=request.task_id,
            worker_event_id=request.worker_event_id,
            structured_result=request.structured_result,
        )

    async def cancel(
        self,
        request: CancelTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.cancel(
            task_id=request.task_id,
            context=context,
        )
