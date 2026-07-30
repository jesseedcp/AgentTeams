from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from agentteams_manager.domain.models import (
    OperationStatus,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
    NotFoundError,
)
from agentteams_manager.workflows.resources import (
    MutationContext,
    ResourceService,
    TeamSpec,
)


class Supervisor:
    def __init__(self) -> None:
        self.statuses: dict[str, OperationStatus] = {}
        self.records: dict[str, SimpleNamespace] = {}

    async def begin(self, **kwargs: object) -> object:
        operation_id = str(kwargs["operation_id"])
        existing = self.records.get(operation_id)
        if existing is not None:
            if (
                existing.kind != kwargs["kind"]
                or existing.target_key != kwargs["target_key"]
                or existing.request != kwargs["request"]
            ):
                raise ConflictError(
                    f"operation ID collision for {operation_id}",
                )
            existing.status = self.statuses[operation_id]
            return existing
        record = SimpleNamespace(
            operation_id=operation_id,
            kind=kwargs["kind"],
            target_key=kwargs["target_key"],
            request=kwargs["request"],
            status=self.statuses.setdefault(
                operation_id,
                OperationStatus.PLANNED,
            ),
        )
        self.records[operation_id] = record
        return record

    async def get(self, operation_id: str) -> object | None:
        record = self.records.get(operation_id)
        if record is not None:
            record.status = self.statuses[operation_id]
        return record

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
        self.workers: dict[str, WorkerResource] = {}
        self.teams: dict[str, TeamResource] = {}
        self.get_sequences: dict[str, list[TeamResource | None]] = {}
        self.simple_requests: list[object] = []
        self.apply_documents: list[bytes] = []
        self.deleted: list[str] = []
        self.defer_team_deletes = False

    async def get_worker(self, name: str) -> WorkerResource | None:
        return self.workers.get(name)

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
        if not self.defer_team_deletes:
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
    *,
    supervisor: Supervisor | None = None,
) -> tuple[ResourceService, Topology]:
    topology = Topology()
    return (
        ResourceService(
            controller=controller,
            supervisor=supervisor or Supervisor(),
            topology=topology,
            matrix=Matrix(),
            sleeper=no_sleep,
            team_poll_delays=(0, 0),
        ),
        topology,
    )


def reference_spec() -> TeamSpec:
    return TeamSpec(
        name="alpha",
        description="Reference-only team",
        leader_name="alpha-lead",
        worker_names=("researcher", "coder"),
        heartbeat_every="45m",
        admin_name="reviewer",
        admin_matrix_id="@reviewer:example.com",
        peer_mentions=False,
    )


def test_team_apply_document_uses_current_v1beta1_contract() -> None:
    document = json.loads(reference_spec().to_apply_document())

    assert document["apiVersion"] == "agentteams.io/v1beta1"
    assert document["kind"] == "Team"
    assert document["metadata"] == {"name": "alpha"}
    assert document["spec"]["workerMembers"] == [
        {"name": "alpha-lead", "role": "team_leader"},
        {"name": "researcher", "role": "worker"},
        {"name": "coder", "role": "worker"},
    ]
    assert document["spec"]["heartbeatEvery"] == "45m"
    assert document["spec"]["admin"] == {
        "name": "reviewer",
        "matrixUserId": "@reviewer:example.com",
    }
    assert document["spec"]["peerMentions"] is False
    assert "leader" not in document["spec"]
    assert "workers" not in document["spec"]


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
def test_team_crds_only_define_worker_references(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert "workerMembers:" in text
    assert "team_leader" in text
    assert "\n                leader:" not in text
    assert "\n                workers:" not in text


@pytest.mark.asyncio
async def test_reference_team_uses_apply_then_controller_get() -> None:
    controller = Controller()
    for name in ("alpha-lead", "researcher", "coder"):
        controller.workers[name] = WorkerResource(
            name=name,
            runtime="copaw",
            phase="Ready",
        )
    controller.get_sequences["alpha"] = [None, team_ready()]
    service, topology = make_service(controller)

    team = await service.apply_team(
        reference_spec(),
        context=context("apply-alpha"),
    )

    assert team.name == "alpha"
    assert len(controller.apply_documents) == 1
    assert not controller.simple_requests
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_simple_team_uses_typed_create_command() -> None:
    controller = Controller()
    for name in ("simple-lead", "dev"):
        controller.workers[name] = WorkerResource(
            name=name,
            runtime="copaw",
            phase="Ready",
        )
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
        leader_name="simple-lead",
        worker_names=("dev",),
    )

    team = await service.create_team(
        spec,
        context=context("create-simple"),
    )

    assert team.name == "simple"
    assert len(controller.simple_requests) == 1
    assert not controller.apply_documents


@pytest.mark.asyncio
async def test_create_team_default_window_covers_k8s_room_startup() -> None:
    controller = Controller()
    for name in ("alpha-lead", "researcher", "coder"):
        controller.workers[name] = WorkerResource(
            name=name,
            runtime="qwenpaw",
            phase="Running",
        )
    pending = TeamResource(
        name="alpha",
        leader="alpha-lead",
        workers=("researcher", "coder"),
        phase="Starting",
    )
    controller.get_sequences["alpha"] = [
        None,
        pending,
        pending,
        pending,
        pending,
        pending,
        pending,
        team_ready(),
    ]
    supervisor = Supervisor()
    topology = Topology()
    service = ResourceService(
        controller=controller,
        supervisor=supervisor,
        topology=topology,
        matrix=Matrix(),
        sleeper=no_sleep,
    )
    spec = TeamSpec(
        name="alpha",
        leader_name="alpha-lead",
        worker_names=("researcher", "coder"),
    )

    team = await service.create_team(
        spec,
        context=context("create-alpha-delayed"),
    )

    assert team.phase == "Active"
    assert len(controller.simple_requests) == 1
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_delete_team_is_proved_absent_before_topology_refresh() -> None:
    controller = Controller()
    controller.teams["alpha"] = team_ready()
    service, topology = make_service(controller)

    preserved_workers = await service.delete_team(
        "alpha",
        context=context("delete-alpha"),
    )

    assert controller.deleted == ["alpha"]
    assert await controller.get_team("alpha") is None
    assert preserved_workers == ("alpha-lead", "researcher", "coder")
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_delete_team_waits_for_controller_absence_to_converge() -> None:
    controller = Controller()
    current = team_ready()
    controller.teams["alpha"] = current
    controller.get_sequences["alpha"] = [
        current,
        current,
        current,
        current,
        None,
    ]
    service, topology = make_service(controller)

    preserved_workers = await service.delete_team(
        "alpha",
        context=context("delete-alpha-delayed"),
    )

    assert controller.deleted == ["alpha"]
    assert preserved_workers == ("alpha-lead", "researcher", "coder")
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_delete_team_retry_reuses_the_original_member_snapshot() -> None:
    controller = Controller()
    controller.teams["alpha"] = team_ready()
    controller.defer_team_deletes = True
    supervisor = Supervisor()
    service, topology = make_service(
        controller,
        supervisor=supervisor,
    )
    mutation = context("delete-alpha-retry")

    with pytest.raises(AmbiguousEffectError):
        await service.delete_team("alpha", context=mutation)

    controller.teams.pop("alpha")
    preserved_workers = await service.delete_team(
        "alpha",
        context=mutation,
    )

    assert preserved_workers == ("alpha-lead", "researcher", "coder")
    assert controller.deleted == ["alpha"]
    assert topology.refreshes == 1


@pytest.mark.asyncio
async def test_create_team_reports_every_missing_worker_before_mutation() -> None:
    controller = Controller()
    controller.workers["alpha-lead"] = WorkerResource(
        name="alpha-lead",
        runtime="copaw",
        phase="Ready",
    )
    service, _ = make_service(controller)
    spec = TeamSpec(
        name="alpha",
        leader_name="alpha-lead",
        worker_names=("researcher", "coder"),
    )

    with pytest.raises(
        NotFoundError,
        match=r"researcher, coder",
    ):
        await service.create_team(
            spec,
            context=context("create-missing"),
        )

    assert controller.simple_requests == []
    assert controller.apply_documents == []
