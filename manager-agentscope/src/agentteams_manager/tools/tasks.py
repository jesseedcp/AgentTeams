"""Closed request contracts for task and project management tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.clients.git import GitRequest, GitRequestParser
from agentteams_manager.workflows.git_delegation import (
    GitDelegationReceipt,
    GitDelegationService,
)
from agentteams_manager.workflows.resources import MutationContext
from agentteams_manager.workflows.projects import (
    ProjectReceipt,
    ProjectService,
)
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


class CreateRecurringTaskInput(CreateFiniteTaskInput):
    schedule: str = Field(min_length=1, max_length=128)
    timezone: str = Field(min_length=1)


class RecordTaskExecutionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1)
    worker_event_id: str = Field(min_length=1)


class CancelTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1)


class CreateProjectInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    plan: str = Field(min_length=1)
    participants: tuple[str, ...] = Field(min_length=1)


class AddProjectTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    project_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    specification: str = Field(min_length=1)
    assigned_to: str = Field(min_length=1)
    delegated_to_team: str | None = None


class CloseProjectInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    project_id: str = Field(min_length=1)
    force: bool = False


class GitDelegationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    message: str = Field(min_length=1)


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

    async def create_recurring(
        self,
        request: CreateRecurringTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.create_recurring(
            title=request.title,
            spec=request.specification,
            assigned_to=request.assigned_to,
            schedule=request.schedule,
            timezone=request.timezone,
            delegated_to_team=request.delegated_to_team,
            project_id=request.project_id,
            project_room_id=request.project_room_id,
            context=context,
        )

    async def record_execution(
        self,
        request: RecordTaskExecutionInput,
    ) -> TaskReceipt:
        return await self._service.record_execution(
            task_id=request.task_id,
            worker_event_id=request.worker_event_id,
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


class ProjectTools:
    def __init__(self, service: ProjectService) -> None:
        self._service = service

    async def create(
        self,
        request: CreateProjectInput,
        *,
        context: MutationContext,
    ) -> ProjectReceipt:
        return await self._service.create(
            title=request.title,
            description=request.description,
            plan=request.plan,
            participants=request.participants,
            context=context,
        )

    async def add_task(
        self,
        request: AddProjectTaskInput,
        *,
        context: MutationContext,
    ) -> TaskReceipt:
        return await self._service.add_task(
            project_id=request.project_id,
            title=request.title,
            specification=request.specification,
            assigned_to=request.assigned_to,
            delegated_to_team=request.delegated_to_team,
            context=context,
        )

    async def close(
        self,
        request: CloseProjectInput,
        *,
        context: MutationContext,
    ) -> ProjectReceipt:
        return await self._service.close(
            project_id=request.project_id,
            force=request.force,
            context=context,
        )


class GitDelegationTools:
    def __init__(self, service: GitDelegationService) -> None:
        self._service = service

    @staticmethod
    def inspect(request: GitDelegationInput) -> GitRequest:
        return GitRequestParser.parse(request.message)

    async def execute(
        self,
        request: GitDelegationInput,
        *,
        context: MutationContext,
        confirmed: bool = False,
    ) -> GitDelegationReceipt:
        return await self._service.execute(
            GitRequestParser.parse(request.message),
            context=context,
            confirmed=confirmed,
        )
