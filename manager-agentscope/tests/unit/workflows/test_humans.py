from __future__ import annotations

from types import SimpleNamespace

import pytest

from agentteams_manager.clients.agt import HumanCreateRequest
from agentteams_manager.domain.models import (
    HumanResource,
    OperationStatus,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
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
            "accessibleTeams": ["alpha"],
            "accessibleWorkers": ["alpha-dev"],
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
