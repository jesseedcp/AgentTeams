from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.clients.agt import (
    HumanCreateRequest,
    HumanUpdateRequest,
)
from agentteams_manager.domain.models import (
    HumanResource,
    OperationKind,
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceHeartbeat,
    ResourceService,
)


class Supervisor:
    def __init__(self) -> None:
        self.statuses: dict[str, OperationStatus] = {}

    async def begin(self, **kwargs: object) -> object:
        operation_id = str(kwargs["operation_id"])
        return SimpleNamespace(
            operation_id=operation_id,
            kind=kwargs["kind"],
            target_key=kwargs["target_key"],
            request=kwargs["request"],
            status=self.statuses.setdefault(
                operation_id,
                OperationStatus.PLANNED,
            ),
        )

    async def before_effect(self, *args: object) -> object:
        self.statuses[str(args[0])] = OperationStatus.DISPATCHED
        return SimpleNamespace(sequence=1)

    async def effect_succeeded(self, *args: object) -> object:
        self.statuses[str(args[0])] = OperationStatus.SUCCEEDED
        return SimpleNamespace(status=OperationStatus.SUCCEEDED)

    async def effect_ambiguous(self, *args: object) -> object:
        self.statuses[str(args[0])] = OperationStatus.RECONCILING
        return SimpleNamespace(status=OperationStatus.RECONCILING)

    async def effect_failed(self, *args: object) -> object:
        self.statuses[str(args[0])] = OperationStatus.FAILED
        return SimpleNamespace(status=OperationStatus.FAILED)


class Controller:
    def __init__(self) -> None:
        self.humans: dict[str, HumanResource] = {}
        self.sequences: dict[str, list[HumanResource | None]] = {}
        self.created: list[HumanCreateRequest] = []
        self.updated: list[HumanUpdateRequest] = []
        self.deleted: list[str] = []

    async def get_human(self, name: str) -> HumanResource | None:
        sequence = self.sequences.get(name)
        if sequence:
            return sequence.pop(0)
        return self.humans.get(name)

    async def list_humans(self) -> tuple[HumanResource, ...]:
        return tuple(self.humans.values())

    async def create_human(self, request: HumanCreateRequest) -> None:
        self.created.append(request)

    async def update_human(
        self,
        request: HumanUpdateRequest,
    ) -> HumanResource:
        self.updated.append(request)
        current = self.humans[request.name]
        spec = dict(current.spec)
        if request.display_name is not None:
            spec["displayName"] = request.display_name
        if request.email is not None:
            spec["email"] = request.email
        if request.accessible_teams is not None:
            spec["accessibleTeams"] = list(request.accessible_teams)
        if request.accessible_workers is not None:
            spec["accessibleWorkers"] = list(
                request.accessible_workers,
            )
        if request.note is not None:
            spec["note"] = request.note
        updated = current.model_copy(
            update={
                "permission_level": (
                    request.permission_level
                    if request.permission_level is not None
                    else current.permission_level
                ),
                "spec": spec,
            },
        )
        self.humans[request.name] = updated
        return updated

    async def delete_human(self, name: str) -> None:
        self.deleted.append(name)
        self.humans.pop(name, None)


class Topology:
    def __init__(self) -> None:
        self.refreshes = 0

    async def refresh(self) -> object:
        self.refreshes += 1
        return object()


class Matrix:
    async def send_text(self, *args: object, **kwargs: object) -> str:
        del args, kwargs
        return "$unused"


async def no_sleep(delay: float) -> None:
    del delay


def context(call: str) -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id=call,
    )


def human() -> HumanResource:
    return HumanResource(
        name="reviewer",
        matrix_user_id="@reviewer:example",
        permission_level=2,
        allowed_rooms=("!alpha:example",),
        spec={
            "displayName": "Reviewer",
            "email": "reviewer@example.com",
            "accessibleTeams": ["alpha"],
            "accessibleWorkers": ["alpha-dev"],
            "note": "",
        },
        status={"phase": "Running"},
    )


def service(
    controller: Controller,
) -> tuple[ResourceService, Topology]:
    topology = Topology()
    return (
        ResourceService(
            controller=controller,
            supervisor=Supervisor(),
            topology=topology,
            matrix=Matrix(),
            sleeper=no_sleep,
            human_poll_delays=(0, 0),
        ),
        topology,
    )


@pytest.mark.asyncio
async def test_create_human_proves_permission_scope_from_controller() -> None:
    controller = Controller()
    controller.sequences["reviewer"] = [None, human()]
    workflow, topology = service(controller)
    request = HumanCreateRequest(
        name="reviewer",
        display_name="Reviewer",
        email="reviewer@example.com",
        permission_level=2,
        accessible_teams=("alpha",),
        accessible_workers=("alpha-dev",),
    )

    created = await workflow.create_human(
        request,
        context=context("create-reviewer"),
    )

    assert created.permission_level == 2
    assert created.spec["accessibleTeams"] == ["alpha"]
    assert controller.created == [request]
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_list_and_delete_human_use_controller_facts() -> None:
    controller = Controller()
    controller.humans["reviewer"] = human()
    workflow, topology = service(controller)

    listed = await workflow.list_humans()
    await workflow.delete_human(
        "reviewer",
        context=context("delete-reviewer"),
    )

    assert [item.name for item in listed] == ["reviewer"]
    assert controller.deleted == ["reviewer"]
    assert await workflow.get_human("reviewer") is None
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_update_human_proves_new_scope_and_allows_explicit_clear() -> None:
    controller = Controller()
    controller.humans["reviewer"] = human()
    workflow, topology = service(controller)
    request = HumanUpdateRequest(
        name="reviewer",
        permission_level=3,
        accessible_teams=(),
        accessible_workers=("release-bot",),
        note="Release administrator",
    )

    updated = await workflow.update_human(
        request,
        context=context("update-reviewer"),
    )

    assert controller.updated == [request]
    assert updated.permission_level == 3
    assert updated.spec["accessibleTeams"] == []
    assert updated.spec["accessibleWorkers"] == ["release-bot"]
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_heartbeat_recovers_ambiguous_human_update_from_controller_fact(
) -> None:
    controller = Controller()
    current = human().model_copy(
        update={
            "permission_level": 3,
            "spec": {
                **human().spec,
                "accessibleTeams": [],
                "accessibleWorkers": ["release-bot"],
            },
        },
    )
    controller.humans["reviewer"] = current
    workflow, topology = service(controller)
    now = datetime.now(UTC)
    operation = OperationRecord(
        operation_id="d" * 32,
        kind=OperationKind.UPDATE_HUMAN,
        target_key="human/reviewer",
        status=OperationStatus.RECONCILING,
        request=HumanUpdateRequest(
            name="reviewer",
            permission_level=3,
            accessible_teams=(),
            accessible_workers=("release-bot",),
        ).model_dump(mode="json"),
        created_at=now,
        updated_at=now,
    )

    class Operations:
        async def list_recoverable(
            self,
        ) -> tuple[OperationRecord, ...]:
            return (operation,)

    report = await ResourceHeartbeat(
        operations=Operations(),
        resources=workflow,
    ).reconcile_pending_resources()

    assert report.inspected == 1
    assert report.reconciled == ("d" * 32,)
    assert topology.refreshes == 1
