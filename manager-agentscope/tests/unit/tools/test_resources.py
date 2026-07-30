from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from pydantic import SecretStr

from agentteams_manager.clients.agt import (
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from agentteams_manager.domain.models import (
    HumanResource,
    RoomKind,
    RoomPolicy,
    TeamResource,
    WorkerResource,
)
from agentteams_manager.matrix.policy import ALL_MANAGER_TOOLS
from agentteams_manager.tools.resources import (
    RESOURCE_TOOL_NAMES,
    ResourceToolkit,
)
from agentteams_manager.tools.base import ManagerToolkit, bind_matrix_turn
from agentteams_manager.workflows.resources import MutationContext


class Resources:
    def __init__(self) -> None:
        self.created: list[tuple[WorkerCreateRequest, MutationContext]] = []
        self.updated: list[tuple[WorkerUpdateRequest, MutationContext]] = []
        self.reset: list[tuple[str, MutationContext]] = []
        self.workers = (
            WorkerResource(
                name="alice",
                runtime="qwenpaw",
                phase="Running",
                room_id="!alice:example",
            ),
            WorkerResource(
                name="bob",
                runtime="hermes",
                phase="Running",
                room_id="!bob:example",
            ),
        )

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        return self.workers

    async def get_worker(self, name: str) -> WorkerResource | None:
        return next(
            (worker for worker in self.workers if worker.name == name),
            None,
        )

    async def create_worker(
        self,
        request: WorkerCreateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        self.created.append((request, context))
        return WorkerResource(
            name=request.name,
            runtime=request.runtime,
            model=request.model,
            phase="Running",
            room_id=f"!{request.name}:example",
        )

    async def reset_worker(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        self.reset.append((name, context))
        worker = await self.get_worker(name)
        assert worker is not None
        return worker

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
        *,
        context: MutationContext,
    ) -> WorkerResource:
        self.updated.append((request, context))
        worker = await self.get_worker(request.name)
        assert worker is not None
        return worker.model_copy(
            update={
                "spec": {
                    **worker.spec,
                    "console": {
                        "enabled": request.console_enabled,
                        "port": request.console_port or 8088,
                    },
                },
            },
        )

    async def list_teams(self) -> tuple[TeamResource, ...]:
        return ()

    async def get_team(self, name: str) -> TeamResource | None:
        del name
        return None

    async def delete_team(
        self,
        name: str,
        *,
        context: MutationContext,
    ) -> tuple[str, ...]:
        del name, context
        return ("alpha-lead", "researcher", "coder")

    async def list_humans(self) -> tuple[HumanResource, ...]:
        return ()

    async def get_human(self, name: str) -> HumanResource | None:
        del name
        return None


class Matrix:
    def __init__(self) -> None:
        self.registrations: list[tuple[str, str, bool]] = []

    async def joined_rooms(self) -> tuple[str, ...]:
        return ("!admin:example",)

    async def members(self, room_id: str) -> tuple[str, ...]:
        del room_id
        return ("@admin:example", "@manager:example")

    async def register_user(
        self,
        *,
        username: str,
        password: SecretStr,
        admin: bool = False,
    ) -> dict[str, str | bool]:
        self.registrations.append(
            (username, password.get_secret_value(), admin),
        )
        return {"user_id": f"@{username}:example", "admin": admin}


class Channels:
    async def primary_channel(self, user_id: str) -> str | None:
        del user_id
        return "!admin:example"

    async def trusted_channels(
        self,
        user_id: str,
    ) -> tuple[str, ...]:
        del user_id
        return ()


class MatrixWorkflows:
    def __init__(self) -> None:
        self.created: list[tuple[str, str, tuple[str, ...], MutationContext]] = []

    async def create_channel(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        revision: int,
        context: MutationContext,
    ) -> str:
        del revision
        self.created.append((name, topic, invite, context))
        return "!created:example"


def _context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id="tool-call-7",
    )


def _policy(**changes: object) -> RoomPolicy:
    values: dict[str, object] = {
        "room_id": "!admin:example",
        "kind": RoomKind.ADMIN_DM,
        "revision": 1,
        "allowed_tools": ALL_MANAGER_TOOLS,
        "confirm_tools": frozenset(),
        "resource_scope_all": True,
    }
    values.update(changes)
    return RoomPolicy(**values)


def _toolkit(
    *,
    policy: RoomPolicy | None = None,
) -> tuple[ResourceToolkit, Resources]:
    resources = Resources()
    matrix_workflows = MatrixWorkflows()
    toolkit = ResourceToolkit(
        policy=policy or _policy(),
        resources=resources,
        matrix=Matrix(),
        matrix_workflows=matrix_workflows,
        channels=Channels(),
        manager_admin_room="!admin:example",
        context_provider=_context,
    )
    return toolkit, resources


@pytest.mark.asyncio
async def test_register_matrix_user_reads_password_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NEW_MATRIX_USER_PASSWORD", "one-time-password")
    matrix = Matrix()
    toolkit = ResourceToolkit(
        policy=_policy(),
        resources=Resources(),
        matrix=matrix,
        matrix_workflows=MatrixWorkflows(),
        channels=Channels(),
        manager_admin_room="!admin:example",
        context_provider=_context,
    )
    tool = next(
        item
        for item in toolkit.tools
        if item.name == "register_matrix_user"
    )

    chunk = await tool.call(
        username="alice",
        password_env="NEW_MATRIX_USER_PASSWORD",
        admin=False,
    )

    assert matrix.registrations == [
        ("alice", "one-time-password", False),
    ]
    assert "one-time-password" not in chunk.content[0].text
    assert "NEW_MATRIX_USER_PASSWORD" not in chunk.content[0].text


def test_register_matrix_user_is_not_exposed_outside_admin_dm() -> None:
    toolkit, _ = _toolkit(
        policy=_policy(
            kind=RoomKind.HUMAN_OR_CHANNEL_ROOM,
            room_id="!human:example",
        ),
    )

    assert "register_matrix_user" not in {
        tool.name for tool in toolkit.tools
    }


def test_resource_tools_have_closed_input_schemas() -> None:
    toolkit, _ = _toolkit()

    assert {tool.name for tool in toolkit.tools} == RESOURCE_TOOL_NAMES
    for tool in toolkit.tools:
        assert tool.input_schema["type"] == "object"
        assert tool.input_schema["additionalProperties"] is False


def test_toolkit_exposes_only_tools_allowed_by_room_policy() -> None:
    toolkit, _ = _toolkit(
        policy=_policy(
            allowed_tools=frozenset({"list_workers", "get_worker"}),
        ),
    )

    assert {tool.name for tool in toolkit.tools} == {
        "list_workers",
        "get_worker",
    }


@pytest.mark.asyncio
async def test_create_worker_uses_typed_request_and_turn_context_once() -> None:
    toolkit, resources = _toolkit()
    tool = next(
        item for item in toolkit.tools if item.name == "create_worker"
    )

    chunk = await tool.call(
        name="charlie",
        runtime="copaw",
        model="qwen3.6-plus",
        skills=["review"],
    )

    assert len(resources.created) == 1
    request, context = resources.created[0]
    assert request.name == "charlie"
    assert request.skills == ("review",)
    assert context == _context()
    receipt = json.loads(chunk.content[0].text)
    assert receipt["status"] == "accepted"
    assert receipt["result"]["background_provisioning"] is True

    with pytest.raises(Exception):
        await tool.call(
            name="charlie",
            runtime="copaw",
            model="qwen3.6-plus",
            unknown=True,
        )


@pytest.mark.asyncio
async def test_reset_worker_uses_one_confirmable_resource_operation() -> None:
    toolkit, resources = _toolkit()
    tool = next(item for item in toolkit.tools if item.name == "reset_worker")

    chunk = await tool.call(name="alice")

    assert resources.reset == [("alice", _context())]
    assert json.loads(chunk.content[0].text)["tool"] == "reset_worker"


@pytest.mark.asyncio
async def test_update_worker_console_uses_typed_request_and_returns_status() -> None:
    toolkit, resources = _toolkit()
    tool = next(item for item in toolkit.tools if item.name == "update_worker")

    chunk = await tool.call(
        name="alice",
        console_enabled=True,
        console_port=9090,
    )

    assert resources.updated == [
        (
            WorkerUpdateRequest(
                name="alice",
                console_enabled=True,
                console_port=9090,
            ),
            _context(),
        ),
    ]
    payload = json.loads(chunk.content[0].text)
    assert payload["result"]["spec"]["console"] == {
        "enabled": True,
        "port": 9090,
    }


@pytest.mark.asyncio
async def test_create_channel_calls_one_journaled_workflow() -> None:
    resources = Resources()
    workflows = MatrixWorkflows()
    toolkit = ResourceToolkit(
        policy=_policy(),
        resources=resources,
        matrix=Matrix(),
        matrix_workflows=workflows,
        channels=Channels(),
        manager_admin_room="!admin:example",
        context_provider=_context,
    )
    tool = next(
        item for item in toolkit.tools if item.name == "create_channel"
    )

    chunk = await tool.call(
        name="release",
        topic="Release coordination",
        invite=["@reviewer:example"],
    )

    assert workflows.created == [
        (
            "release",
            "Release coordination",
            ("@reviewer:example",),
            _context(),
        ),
    ]
    assert json.loads(chunk.content[0].text)["name"] == "!created:example"


@pytest.mark.asyncio
async def test_scoped_list_filters_controller_results() -> None:
    policy = _policy(
        resource_scope_all=False,
        allowed_worker_names=frozenset({"alice"}),
    )
    toolkit, _ = _toolkit(policy=policy)
    tool = next(
        item for item in toolkit.tools if item.name == "list_workers"
    )

    chunk = await tool.call()
    payload = json.loads(chunk.content[0].text)

    assert [item["name"] for item in payload["items"]] == ["alice"]


@pytest.mark.asyncio
async def test_delete_team_reports_that_worker_resources_are_preserved() -> None:
    toolkit, _ = _toolkit()
    tool = next(
        item for item in toolkit.tools if item.name == "delete_team"
    )

    chunk = await tool.call(name="alpha")
    payload = json.loads(chunk.content[0].text)

    assert payload["result"] == {
        "deleted": True,
        "preservedWorkers": ["alpha-lead", "researcher", "coder"],
    }


def test_resource_tool_source_has_no_direct_controller_transport() -> None:
    source = Path(
        "manager-agentscope/src/agentteams_manager/tools/resources.py",
    ).read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert "http://127.0.0.1" not in source
    assert "requests." not in source


@pytest.mark.asyncio
async def test_real_agentscope_tool_call_id_reaches_mutation_context() -> None:
    resources = Resources()
    resource_tools = ResourceToolkit(
        policy=_policy(),
        resources=resources,
        matrix=Matrix(),
        matrix_workflows=MatrixWorkflows(),
        channels=Channels(),
        manager_admin_room="!admin:example",
    )
    create = next(
        tool
        for tool in resource_tools.tools
        if tool.name == "create_worker"
    )
    toolkit = ManagerToolkit(tools=[create])
    call = ToolCallBlock(
        id="agentscope-call-42",
        name="create_worker",
        input=json.dumps(
            {
                "name": "charlie",
                "runtime": "copaw",
                "model": "qwen3.6-plus",
            },
        ),
    )

    with bind_matrix_turn("!admin:example", "$origin-event"):
        async for _ in toolkit.call_tool(call, AgentState()):
            pass

    assert resources.created[0][1] == MutationContext(
        room_id="!admin:example",
        event_id="$origin-event",
        tool_call_id="agentscope-call-42",
    )
