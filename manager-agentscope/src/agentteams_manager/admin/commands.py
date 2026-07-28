"""Typed, workflow-backed commands for the local Manager admin API."""

from __future__ import annotations

import json
from typing import Any, Literal, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from agentteams_manager.clients.agt import (
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
)
from agentteams_manager.domain.models import (
    ProjectRecord,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.workflows.projects import (
    ProjectCreateRequest,
    ProjectReceipt,
)
from agentteams_manager.workflows.resources import MutationContext, TeamSpec

AdminMethod = Literal["GET", "POST", "PATCH", "DELETE"]
AdminResource = Literal["workers", "teams", "projects"]


class AdminAPIError(RuntimeError):
    """Stable error safe to expose through the authenticated admin API."""

    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        *,
        details: object | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


class AdminCommand(BaseModel):
    """Transport-neutral admin request dispatched by the HTTP server."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    method: AdminMethod
    resource: AdminResource
    name: str | None = Field(
        default=None,
        min_length=1,
        max_length=253,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    )
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    )


class ResourceWorkflows(Protocol):
    async def list_workers(self) -> tuple[WorkerResource, ...]: ...

    async def get_worker(self, name: str) -> WorkerResource | None: ...

    async def create_worker(
        self,
        request: WorkerCreateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource: ...

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource: ...

    async def delete_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> None: ...

    async def list_teams(self) -> tuple[TeamResource, ...]: ...

    async def get_team(self, name: str) -> TeamResource | None: ...

    async def create_team(
        self,
        spec: TeamSpec,
        *,
        context: MutationContext,
    ) -> TeamResource: ...

    async def apply_team(
        self,
        spec: TeamSpec,
        *,
        context: MutationContext,
    ) -> TeamResource: ...

    async def delete_team(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> tuple[str, ...]: ...


class ProjectRepository(Protocol):
    async def list_all(self) -> tuple[ProjectRecord, ...]: ...

    async def get(self, project_id: str) -> ProjectRecord | None: ...


class ProjectWorkflows(Protocol):
    async def create(
        self,
        *,
        title: str,
        description: str,
        plan: str,
        participants: tuple[str, ...],
        context: MutationContext,
    ) -> ProjectReceipt: ...

    async def revise_plan(
        self,
        *,
        project_id: str,
        plan: str,
        change_kind: str,
        reason: str,
        context: MutationContext,
    ) -> ProjectReceipt: ...

    async def update_participants(
        self,
        *,
        project_id: str,
        add: tuple[str, ...],
        remove: tuple[str, ...],
        reason: str,
        context: MutationContext,
    ) -> ProjectReceipt: ...

    async def close(
        self,
        *,
        project_id: str,
        force: bool,
        context: MutationContext,
    ) -> ProjectReceipt: ...


class _TeamPatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    leader_name: str | None = None
    worker_names: tuple[str, ...] | None = None
    team_name: str | None = None
    description: str | None = None
    heartbeat_every: str | None = None
    admin_name: str | None = None
    admin_matrix_id: str | None = None
    peer_mentions: bool | None = None
    confirmed: bool = False

    @model_validator(mode="after")
    def require_change(self) -> _TeamPatch:
        changed = self.model_fields_set - {"confirmed"}
        if not changed:
            raise ValueError("at least one Team field must change")
        return self


class _ProjectPatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    plan: str | None = Field(default=None, min_length=1)
    change_kind: Literal["minor", "major"] = "minor"
    reason: str = Field(min_length=1)
    add: tuple[str, ...] = ()
    remove: tuple[str, ...] = ()
    confirmed: bool = False

    @model_validator(mode="after")
    def require_one_change_kind(self) -> _ProjectPatch:
        has_plan = self.plan is not None
        has_roster = bool(self.add or self.remove)
        if has_plan == has_roster:
            raise ValueError(
                "Project PATCH must revise a plan or change participants",
            )
        return self


class _DeletePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    confirmed: bool = False
    force: bool = False


class AdminCommandFacade:
    """Map authenticated admin requests onto existing durable workflows."""

    def __init__(
        self,
        *,
        resources: ResourceWorkflows,
        projects: ProjectRepository,
        project_workflows: ProjectWorkflows,
        admin_room_id: str,
    ) -> None:
        if not admin_room_id:
            raise ValueError("admin_room_id is required")
        self._resources = resources
        self._projects = projects
        self._project_workflows = project_workflows
        self._admin_room_id = admin_room_id

    async def execute(
        self,
        command: AdminCommand,
    ) -> dict[str, object]:
        try:
            if (
                command.method == "POST"
                and command.name is not None
            ) or (
                command.method in {"PATCH", "DELETE"}
                and command.name is None
            ):
                raise AdminAPIError(
                    405,
                    "method_not_allowed",
                    (
                        f"{command.method} is not allowed for this "
                        "collection/detail endpoint"
                    ),
                )
            if command.method != "GET" and not command.idempotency_key:
                raise AdminAPIError(
                    400,
                    "idempotency_key_required",
                    "Idempotency-Key is required for admin writes",
                )
            if command.resource == "workers":
                return await self._workers(command)
            if command.resource == "teams":
                return await self._teams(command)
            return await self._projects_command(command)
        except AdminAPIError:
            raise
        except ValidationError as exc:
            raise AdminAPIError(
                422,
                "validation_error",
                "request validation failed",
                details=json.loads(exc.json(include_url=False)),
            ) from exc
        except NotFoundError as exc:
            raise AdminAPIError(404, "not_found", str(exc)) from exc
        except PermissionDeniedError as exc:
            raise AdminAPIError(403, "forbidden", str(exc)) from exc
        except ConflictError as exc:
            raise AdminAPIError(409, "conflict", str(exc)) from exc
        except AmbiguousEffectError as exc:
            raise AdminAPIError(
                202,
                "effect_pending",
                str(exc),
            ) from exc
        except ValueError as exc:
            raise AdminAPIError(
                400,
                "invalid_request",
                str(exc),
            ) from exc

    async def _workers(
        self,
        command: AdminCommand,
    ) -> dict[str, object]:
        if command.method == "GET":
            if command.name is None:
                return _collection(await self._resources.list_workers())
            item = await self._resources.get_worker(command.name)
            return {
                "item": _json_value(
                    _required(item, "worker", command.name),
                ),
            }

        if command.method == "POST":
            create_request = WorkerCreateRequest.model_validate(
                _without_transport(command.payload),
            )
            context = self._context(command, create_request.name)
            item = await self._resources.create_worker(
                create_request,
                context=context,
            )
            return _mutation("workers", item, context)

        name = _require_name(command)
        if command.method == "PATCH":
            payload = dict(command.payload)
            confirmed = bool(payload.pop("confirmed", False))
            current = _required(
                await self._resources.get_worker(name),
                "worker",
                name,
            )
            update_request = WorkerUpdateRequest.model_validate(
                {"name": name, **payload},
            )
            if not confirmed and _worker_replacement(
                current,
                update_request,
            ):
                raise _confirmation_error("Worker runtime/image replacement")
            context = self._context(command, name)
            item = await self._resources.update_worker(
                update_request,
                context=context,
            )
            return _mutation("workers", item, context)

        delete = _DeletePayload.model_validate(command.payload)
        _require_confirmed(delete.confirmed, "Worker deletion")
        context = self._context(command, name)
        await self._resources.delete_worker(name, context=context)
        return _deleted("workers", name, context)

    async def _teams(
        self,
        command: AdminCommand,
    ) -> dict[str, object]:
        if command.method == "GET":
            if command.name is None:
                return _collection(await self._resources.list_teams())
            item = await self._resources.get_team(command.name)
            return {
                "item": _json_value(
                    _required(item, "team", command.name),
                ),
            }

        if command.method == "POST":
            spec = TeamSpec.model_validate(
                _without_transport(command.payload),
            )
            context = self._context(command, spec.name)
            item = await self._resources.create_team(spec, context=context)
            return _mutation("teams", item, context)

        name = _require_name(command)
        if command.method == "PATCH":
            patch = _TeamPatch.model_validate(command.payload)
            current = _required(
                await self._resources.get_team(name),
                "team",
                name,
            )
            spec = _merge_team(name, current, patch)
            current_members = {
                str(current.leader),
                *map(str, current.workers),
            }
            if (
                current_members - set(spec.member_names)
                and not patch.confirmed
            ):
                raise _confirmation_error("Team participant removal")
            context = self._context(command, name)
            item = await self._resources.apply_team(spec, context=context)
            return _mutation("teams", item, context)

        delete = _DeletePayload.model_validate(command.payload)
        _require_confirmed(delete.confirmed, "Team deletion")
        context = self._context(command, name)
        preserved = await self._resources.delete_team(name, context=context)
        result = _deleted("teams", name, context)
        result["preserved_workers"] = list(preserved)
        return result

    async def _projects_command(
        self,
        command: AdminCommand,
    ) -> dict[str, object]:
        if command.method == "GET":
            if command.name is None:
                return _collection(await self._projects.list_all())
            item = await self._projects.get(command.name)
            return {
                "item": _json_value(
                    _required(item, "project", command.name),
                ),
            }

        if command.method == "POST":
            request = ProjectCreateRequest.model_validate(
                {
                    **_without_transport(command.payload),
                    "requester_room_id": self._admin_room_id,
                },
            )
            context = self._context(command, "new")
            created = await self._project_workflows.create(
                title=request.title,
                description=request.description,
                plan=request.plan,
                participants=request.participants,
                context=context,
            )
            return _mutation("projects", created, context)

        name = _require_name(command)
        if command.method == "PATCH":
            patch = _ProjectPatch.model_validate(command.payload)
            _required(await self._projects.get(name), "project", name)
            context = self._context(command, name)
            if patch.plan is not None:
                if patch.change_kind == "major" and not patch.confirmed:
                    raise _confirmation_error("Major project plan revision")
                changed = await self._project_workflows.revise_plan(
                    project_id=name,
                    plan=patch.plan,
                    change_kind=patch.change_kind,
                    reason=patch.reason,
                    context=context,
                )
            else:
                if patch.remove and not patch.confirmed:
                    raise _confirmation_error(
                        "Project participant removal",
                    )
                changed = await self._project_workflows.update_participants(
                    project_id=name,
                    add=patch.add,
                    remove=patch.remove,
                    reason=patch.reason,
                    context=context,
                )
            return _mutation("projects", changed, context)

        delete = _DeletePayload.model_validate(command.payload)
        _require_confirmed(delete.confirmed, "Project closure")
        context = self._context(command, name)
        closed = await self._project_workflows.close(
            project_id=name,
            force=delete.force,
            context=context,
        )
        result = _deleted("projects", name, context)
        result["item"] = _json_value(closed)
        return result

    def _context(
        self,
        command: AdminCommand,
        target: str,
    ) -> MutationContext:
        key = command.idempotency_key
        if not key:
            raise AdminAPIError(
                400,
                "idempotency_key_required",
                "Idempotency-Key is required for admin writes",
            )
        return MutationContext(
            room_id=self._admin_room_id,
            event_id=f"admin-api:{key}",
            tool_call_id=(
                f"{command.method}:{command.resource}:{target}"
            ),
        )


def _without_transport(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("confirmed", None)
    result.pop("force", None)
    return result


def _collection(items: tuple[object, ...]) -> dict[str, object]:
    rendered = [_json_value(item) for item in items]
    return {"items": rendered, "total": len(rendered)}


def _mutation(
    resource: str,
    item: object,
    context: MutationContext,
) -> dict[str, object]:
    return {
        "resource": resource,
        "item": _json_value(item),
        "operation_id": context.operation_id,
    }


def _deleted(
    resource: str,
    name: str,
    context: MutationContext,
) -> dict[str, object]:
    return {
        "deleted": True,
        "name": name,
        "resource": resource,
        "operation_id": context.operation_id,
    }


def _json_value(item: object) -> Any:
    dump = getattr(item, "model_dump", None)
    if callable(dump):
        return dump(mode="json")
    if isinstance(item, dict):
        return item
    raise TypeError(f"admin result {type(item).__name__} is not serializable")


_T = TypeVar("_T")


def _required(item: _T | None, kind: str, name: str) -> _T:
    if item is None:
        raise AdminAPIError(
            404,
            "not_found",
            f"{kind}/{name} does not exist",
        )
    return item


def _require_name(command: AdminCommand) -> str:
    if command.name is None:
        raise AdminAPIError(
            400,
            "resource_name_required",
            f"{command.method} requires a resource name",
        )
    return command.name


def _require_confirmed(confirmed: bool, action: str) -> None:
    if not confirmed:
        raise _confirmation_error(action)


def _confirmation_error(action: str) -> AdminAPIError:
    return AdminAPIError(
        409,
        "confirmation_required",
        f"{action} requires confirmed: true",
    )


def _worker_replacement(
    current: WorkerResource,
    request: WorkerUpdateRequest,
) -> bool:
    if (
        request.runtime is not None
        and request.runtime != getattr(current, "runtime", None)
    ):
        return True
    if request.image is None:
        return False
    spec = getattr(current, "spec", {})
    current_image = spec.get("image") if isinstance(spec, dict) else None
    return request.image != current_image


def _merge_team(
    name: str,
    current: TeamResource,
    patch: _TeamPatch,
) -> TeamSpec:
    spec = getattr(current, "spec", {})
    if not isinstance(spec, dict):
        spec = {}
    admin = spec.get("admin")
    if not isinstance(admin, dict):
        admin = {}
    return TeamSpec(
        name=name,
        leader_name=(
            patch.leader_name
            if patch.leader_name is not None
            else str(current.leader)
        ),
        worker_names=(
            patch.worker_names
            if patch.worker_names is not None
            else tuple(map(str, current.workers))
        ),
        team_name=(
            patch.team_name
            if patch.team_name is not None
            else str(spec.get("teamName") or name)
        ),
        description=(
            patch.description
            if patch.description is not None
            else str(spec.get("description") or "")
        ),
        heartbeat_every=(
            patch.heartbeat_every
            if patch.heartbeat_every is not None
            else str(spec.get("heartbeatEvery") or "30m")
        ),
        admin_name=(
            patch.admin_name
            if patch.admin_name is not None
            else (str(admin.get("name")) if admin.get("name") else None)
        ),
        admin_matrix_id=(
            patch.admin_matrix_id
            if patch.admin_matrix_id is not None
            else (
                str(admin.get("matrixUserId"))
                if admin.get("matrixUserId")
                else None
            )
        ),
        peer_mentions=(
            patch.peer_mentions
            if patch.peer_mentions is not None
            else bool(spec.get("peerMentions", True))
        ),
    )
