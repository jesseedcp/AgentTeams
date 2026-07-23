from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from agentteams_manager.domain.models import (
    OperationStatus,
    TeamResource,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceService,
    TeamMemberSpec,
    TeamSpec,
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
        self.teams: dict[str, TeamResource] = {}
        self.get_sequences: dict[str, list[TeamResource | None]] = {}
        self.simple_requests: list[object] = []
        self.apply_documents: list[bytes] = []
        self.deleted: list[str] = []

    async def get_team(self, name: str) -> TeamResource | None:
        sequence = self.get_sequences.get(name)
        if sequence:
            return sequence.pop(0)
        return self.teams.get(name)

    async def list_teams(self) -> tuple[TeamResource, ...]:
        return tuple(self.teams.values())

    async def create_team(self, request: object) -> None:
        self.simple_requests.append(request)

    async def apply_team(
        self,
        name: str,
        document: bytes,
    ) -> TeamResource:
        self.apply_documents.append(document)
        return TeamResource(
            name=name,
            leader=f"{name}-lead",
            workers=(),
            phase="Pending",
        )

    async def delete_team(self, name: str) -> None:
        self.deleted.append(name)
        self.teams.pop(name, None)


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


def team_ready(
    *,
    name: str = "alpha",
    leader: str = "alpha-lead",
    workers: tuple[str, ...] = ("researcher", "coder"),
) -> TeamResource:
    return TeamResource(
        name=name,
        leader=leader,
        workers=workers,
        phase="Active",
        spec={
            "teamRoomID": "!team:example",
            "leaderDMRoomID": "!leader-dm:example",
        },
        status={
            "leaderReady": True,
            "readyWorkers": len(workers),
            "totalWorkers": len(workers),
        },
    )


def make_service(
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
            team_poll_delays=(0, 0),
        ),
        topology,
    )


def mixed_spec() -> TeamSpec:
    return TeamSpec(
        name="alpha",
        description="Mixed runtime team",
        leader=TeamMemberSpec(
            name="alpha-lead",
            runtime="qwenpaw",
            model="qwen3.6-plus",
        ),
        workers=(
            TeamMemberSpec(
                name="researcher",
                runtime="hermes",
                model="qwen3.6-plus",
            ),
            TeamMemberSpec(
                name="coder",
                runtime="openclaw",
                model="qwen3.6-plus",
                skills=("github-operations",),
            ),
        ),
    )


def test_team_apply_document_uses_current_v1beta1_contract() -> None:
    document = json.loads(mixed_spec().to_apply_document())

    assert document["apiVersion"] == "agentteams.io/v1beta1"
    assert document["kind"] == "Team"
    assert document["metadata"] == {"name": "alpha"}
    assert document["spec"]["leader"]["runtime"] == "qwenpaw"
    assert [
        worker["runtime"]
        for worker in document["spec"]["workers"]
    ] == ["hermes", "openclaw"]
    assert document["spec"]["workers"][1]["skills"] == [
        "github-operations",
    ]


@pytest.mark.parametrize(
    "path",
    (
        Path("helm/agentteams/crds/teams.agentteams.io.yaml"),
        Path(
            "agentteams-controller/config/crd/"
            "teams.agentteams.io.yaml",
        ),
    ),
)
def test_team_crds_accept_every_supported_runtime(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    leader = text.split("                leader:", 1)[1].split(
        "                workers:",
        1,
    )[0]
    workers = text.split("                workers:", 1)[1]

    assert "runtime:" in leader
    assert "qwenpaw" in leader
    assert "qwenpaw" in workers


@pytest.mark.asyncio
async def test_mixed_runtime_team_uses_apply_then_controller_get() -> None:
    controller = Controller()
    controller.get_sequences["alpha"] = [None, team_ready()]
    service, topology = make_service(controller)

    team = await service.apply_team(
        mixed_spec(),
        context=context("apply-alpha"),
    )

    assert team.name == "alpha"
    assert len(controller.apply_documents) == 1
    assert not controller.simple_requests
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_simple_team_uses_typed_create_command() -> None:
    controller = Controller()
    controller.get_sequences["simple"] = [
        None,
        team_ready(
            name="simple",
            leader="simple-lead",
            workers=("dev",),
        ),
    ]
    service, _ = make_service(controller)
    spec = TeamSpec(
        name="simple",
        leader=TeamMemberSpec(
            name="simple-lead",
            runtime="copaw",
            model="qwen3.6-plus",
        ),
        workers=(TeamMemberSpec(name="dev"),),
    )

    team = await service.create_team(
        spec,
        context=context("create-simple"),
    )

    assert team.name == "simple"
    assert len(controller.simple_requests) == 1
    assert not controller.apply_documents


@pytest.mark.asyncio
async def test_delete_team_is_proved_absent_before_topology_refresh() -> None:
    controller = Controller()
    controller.teams["alpha"] = team_ready()
    service, topology = make_service(controller)

    await service.delete_team(
        "alpha",
        context=context("delete-alpha"),
    )

    assert controller.deleted == ["alpha"]
    assert await controller.get_team("alpha") is None
    assert topology.refreshes == 1
