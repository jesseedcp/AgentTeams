from __future__ import annotations

from datetime import UTC, datetime

import pytest
from agentteams_manager.admin.commands import (
    AdminAPIError,
    AdminCommand,
    AdminCommandFacade,
)
from agentteams_manager.clients.agt import (
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from agentteams_manager.domain.ids import operation_id_for
from agentteams_manager.domain.models import (
    ProjectRecord,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.workflows.resources import TeamSpec


class Resources:
    def __init__(self) -> None:
        self.workers: dict[str, WorkerResource] = {
            "alice": WorkerResource(
                name="alice",
                runtime="copaw",
                model="qwen3.6-plus",
                phase="Running",
            ),
        }
        self.teams: dict[str, TeamResource] = {
            "alpha": TeamResource(
                name="alpha",
                leader="alice",
                workers=("bob",),
                phase="Running",
                spec={"description": "Initial", "peerMentions": True},
            ),
        }
        self.calls: list[tuple[str, object, object]] = []

    async def list_workers(self):
        return tuple(self.workers.values())

    async def get_worker(self, name):
        return self.workers.get(name)

    async def create_worker(self, request, *, context):
        assert isinstance(request, WorkerCreateRequest)
        self.calls.append(("create_worker", request, context))
        item = WorkerResource(
            name=request.name,
            runtime=request.runtime,
            model=request.model,
            phase="Running",
        )
        self.workers[item.name] = item
        return item

    async def update_worker(self, request, *, context):
        assert isinstance(request, WorkerUpdateRequest)
        self.calls.append(("update_worker", request, context))
        current = self.workers[request.name]
        item = current.model_copy(
            update={
                "runtime": request.runtime or current.runtime,
                "model": request.model or current.model,
            },
        )
        self.workers[item.name] = item
        return item

    async def delete_worker(self, name, *, context):
        self.calls.append(("delete_worker", name, context))
        self.workers.pop(name)

    async def list_teams(self):
        return tuple(self.teams.values())

    async def get_team(self, name):
        return self.teams.get(name)

    async def create_team(self, spec, *, context):
        assert isinstance(spec, TeamSpec)
        self.calls.append(("create_team", spec, context))
        item = TeamResource(
            name=spec.name,
            leader=spec.leader_name,
            workers=spec.worker_names,
            phase="Running",
            spec={"description": spec.description},
        )
        self.teams[item.name] = item
        return item

    async def apply_team(self, spec, *, context):
        self.calls.append(("apply_team", spec, context))
        item = TeamResource(
            name=spec.name,
            leader=spec.leader_name,
            workers=spec.worker_names,
            phase="Running",
            spec={"description": spec.description},
        )
        self.teams[item.name] = item
        return item

    async def delete_team(self, name, *, context):
        self.calls.append(("delete_team", name, context))
        item = self.teams.pop(name)
        return (item.leader, *item.workers)


class Projects:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        self.items = {
            "project-20260728-120000-abcdef": ProjectRecord(
                project_id="project-20260728-120000-abcdef",
                name="Launch",
                room_id="!launch:example",
                status="active",
                metadata={
                    "description": "Ship it",
                    "plan": "Initial plan",
                    "participants": ["alice"],
                },
                created_at=now,
                updated_at=now,
            ),
        }

    async def list_all(self):
        return tuple(self.items.values())

    async def get(self, project_id):
        return self.items.get(project_id)


class ProjectWorkflows:
    def __init__(self, projects: Projects) -> None:
        self.projects = projects
        self.calls: list[tuple[str, object]] = []

    async def create(self, **kwargs):
        self.calls.append(("create", kwargs))
        return {
            "operation_id": kwargs["context"].operation_id,
            "project_id": "project-created",
            "status": "active",
        }

    async def revise_plan(self, **kwargs):
        self.calls.append(("revise_plan", kwargs))
        return {
            "operation_id": kwargs["context"].operation_id,
            "project_id": kwargs["project_id"],
            "status": "active",
        }

    async def update_participants(self, **kwargs):
        self.calls.append(("update_participants", kwargs))
        return {
            "operation_id": kwargs["context"].operation_id,
            "project_id": kwargs["project_id"],
            "status": "active",
        }

    async def close(self, **kwargs):
        self.calls.append(("close", kwargs))
        return {
            "operation_id": kwargs["context"].operation_id,
            "project_id": kwargs["project_id"],
            "status": "completed",
        }


def facade():
    resources = Resources()
    projects = Projects()
    workflows = ProjectWorkflows(projects)
    return (
        AdminCommandFacade(
            resources=resources,
            projects=projects,
            project_workflows=workflows,
            admin_room_id="!admin:example",
        ),
        resources,
        workflows,
    )


@pytest.mark.asyncio
async def test_worker_crud_uses_typed_workflows_and_idempotency() -> None:
    commands, resources, _ = facade()

    created = await commands.execute(
        AdminCommand(
            method="POST",
            resource="workers",
            payload={
                "name": "charlie",
                "runtime": "qwenpaw",
                "model": "qwen3.6-plus",
            },
            idempotency_key="create-charlie",
        ),
    )
    assert created["item"]["name"] == "charlie"
    context = resources.calls[-1][2]
    assert context.operation_id == operation_id_for(
        "!admin:example",
        "admin-api:create-charlie",
        "POST:workers:charlie",
    )

    listed = await commands.execute(
        AdminCommand(method="GET", resource="workers"),
    )
    assert listed["total"] == 2

    with pytest.raises(AdminAPIError, match="confirmed"):
        await commands.execute(
            AdminCommand(
                method="PATCH",
                resource="workers",
                name="charlie",
                payload={"runtime": "copaw"},
                idempotency_key="change-runtime",
            ),
        )

    updated = await commands.execute(
        AdminCommand(
            method="PATCH",
            resource="workers",
            name="charlie",
            payload={"runtime": "copaw", "confirmed": True},
            idempotency_key="change-runtime",
        ),
    )
    assert updated["item"]["runtime"] == "copaw"

    with pytest.raises(AdminAPIError, match="confirmed"):
        await commands.execute(
            AdminCommand(
                method="DELETE",
                resource="workers",
                name="charlie",
                payload={},
                idempotency_key="delete-charlie",
            ),
        )

    deleted = await commands.execute(
        AdminCommand(
            method="DELETE",
            resource="workers",
            name="charlie",
            payload={"confirmed": True},
            idempotency_key="delete-charlie",
        ),
    )
    assert deleted == {
        "deleted": True,
        "name": "charlie",
        "resource": "workers",
        "operation_id": operation_id_for(
            "!admin:example",
            "admin-api:delete-charlie",
            "DELETE:workers:charlie",
        ),
    }


@pytest.mark.asyncio
async def test_team_patch_requires_confirmation_when_roster_shrinks() -> None:
    commands, resources, _ = facade()
    with pytest.raises(AdminAPIError, match="confirmed"):
        await commands.execute(
            AdminCommand(
                method="PATCH",
                resource="teams",
                name="alpha",
                payload={"worker_names": []},
                idempotency_key="shrink-alpha",
            ),
        )

    result = await commands.execute(
        AdminCommand(
            method="PATCH",
            resource="teams",
            name="alpha",
            payload={"worker_names": [], "confirmed": True},
            idempotency_key="shrink-alpha",
        ),
    )
    assert result["item"]["workers"] == []
    assert resources.calls[-1][0] == "apply_team"


@pytest.mark.asyncio
async def test_project_crud_uses_repository_reads_and_project_workflows() -> None:
    commands, _, workflows = facade()
    project_id = "project-20260728-120000-abcdef"

    listed = await commands.execute(
        AdminCommand(method="GET", resource="projects"),
    )
    assert listed["items"][0]["project_id"] == project_id

    item = await commands.execute(
        AdminCommand(method="GET", resource="projects", name=project_id),
    )
    assert item["item"]["metadata"]["plan"] == "Initial plan"

    await commands.execute(
        AdminCommand(
            method="POST",
            resource="projects",
            payload={
                "title": "New launch",
                "description": "Deliver",
                "plan": "Build, verify, release",
                "participants": ["alice"],
            },
            idempotency_key="create-project",
        ),
    )
    assert workflows.calls[-1][0] == "create"

    with pytest.raises(AdminAPIError, match="confirmed"):
        await commands.execute(
            AdminCommand(
                method="PATCH",
                resource="projects",
                name=project_id,
                payload={
                    "plan": "Replacement plan",
                    "change_kind": "major",
                    "reason": "Scope changed",
                },
                idempotency_key="major-plan",
            ),
        )

    await commands.execute(
        AdminCommand(
            method="PATCH",
            resource="projects",
            name=project_id,
            payload={
                "plan": "Replacement plan",
                "change_kind": "major",
                "reason": "Scope changed",
                "confirmed": True,
            },
            idempotency_key="major-plan",
        ),
    )
    assert workflows.calls[-1][0] == "revise_plan"

    await commands.execute(
        AdminCommand(
            method="DELETE",
            resource="projects",
            name=project_id,
            payload={"confirmed": True, "force": True},
            idempotency_key="close-project",
        ),
    )
    assert workflows.calls[-1][0] == "close"


@pytest.mark.asyncio
async def test_writes_require_idempotency_key_and_details_require_name() -> None:
    commands, _, _ = facade()
    with pytest.raises(AdminAPIError) as missing_key:
        await commands.execute(
            AdminCommand(
                method="POST",
                resource="workers",
                payload={
                    "name": "charlie",
                    "runtime": "copaw",
                    "model": "qwen3.6-plus",
                },
            ),
        )
    assert missing_key.value.code == "idempotency_key_required"
    assert missing_key.value.status == 400

    with pytest.raises(AdminAPIError) as missing_name:
        await commands.execute(
            AdminCommand(method="GET", resource="workers", name="missing"),
        )
    assert missing_name.value.status == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "name"),
    [
        ("POST", "alice"),
        ("PATCH", None),
        ("DELETE", None),
    ],
)
async def test_command_rejects_collection_detail_shape_mismatches(
    method: str,
    name: str | None,
) -> None:
    commands, _, _ = facade()
    with pytest.raises(AdminAPIError) as rejected:
        await commands.execute(
            AdminCommand(
                method=method,
                resource="workers",
                name=name,
                payload={"confirmed": True},
                idempotency_key="invalid-shape",
            ),
        )
    assert rejected.value.code == "method_not_allowed"
    assert rejected.value.status == 405
