from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.domain.models import (
    HumanResource,
    InboundEvent,
    RoomKind,
)
from agentteams_manager.matrix.policy import RoomPolicyResolver
from agentteams_manager.state.topology import Actor, ActorKind


def _event(
    *,
    room_id: str,
    sender_id: str,
    is_direct: bool,
    mentions: tuple[str, ...] = (),
) -> InboundEvent:
    return InboundEvent(
        room_id=room_id,
        event_id="$event",
        sender=sender_id,
        body="hello",
        timestamp=datetime.now(UTC),
        is_direct=is_direct,
        mentions=mentions,
    )


class FakeTopology:
    def __init__(
        self,
        bindings: dict[str, object] | None = None,
        humans: dict[str, HumanResource] | None = None,
        actors: dict[str, Actor] | None = None,
    ) -> None:
        self.bindings = bindings or {}
        self.humans = humans or {}
        self.actors = actors or {}

    async def room_binding(self, room_id: str) -> object | None:
        return self.bindings.get(room_id)

    async def human_for_sender(
        self,
        matrix_user_id: str,
    ) -> HumanResource | None:
        return self.humans.get(matrix_user_id)

    async def actor_for_sender(
        self,
        matrix_user_id: str,
    ) -> Actor | None:
        return self.actors.get(matrix_user_id)


@pytest.mark.asyncio
async def test_unknown_group_sender_is_silent() -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!group:local",
            sender_id="@unknown:local",
            is_direct=False,
        ),
    )

    assert policy.silent
    assert not policy.allowed_tools


@pytest.mark.asyncio
async def test_admin_dm_gets_management_tools() -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!admin-dm:local",
            sender_id="@admin:local",
            is_direct=True,
        ),
    )

    assert policy.kind is RoomKind.ADMIN_DM
    assert "create_worker" in policy.allowed_tools


@pytest.mark.asyncio
async def test_configured_admin_room_survives_empty_direct_room_cache() -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
        admin_room_id="!admin-dm:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!admin-dm:local",
            sender_id="@admin:local",
            is_direct=False,
        ),
    )

    assert not policy.silent
    assert policy.kind is RoomKind.ADMIN_DM
    assert policy.allowed_senders == frozenset({"@admin:local"})
    assert "create_worker" in policy.allowed_tools


@pytest.mark.asyncio
async def test_worker_identity_gets_only_worker_room_tools() -> None:
    binding = SimpleNamespace(
        room_kind=RoomKind.WORKER_ROOM,
        room_id="!worker:local",
        resource_name="alice",
        matrix_user_id="@alice:local",
        payload={
            "spec": {
                "mcpServers": [
                    {
                        "name": "jira",
                        "url": "https://gateway/mcp/jira",
                    },
                ],
            },
        },
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology({"!worker:local": binding}),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!worker:local",
            sender_id="@alice:local",
            is_direct=False,
        ),
    )

    assert policy.kind is RoomKind.WORKER_ROOM
    assert policy.allowed_tools == frozenset(
        {
            "delegate_task",
            "complete_task",
            "sync_files",
            "inspect_git_request",
            "git_delegate",
            "git_delegate_high_risk",
        },
    )
    assert policy.resource_name == "alice"
    assert policy.allowed_mcp_names == frozenset({"jira"})


@pytest.mark.asyncio
async def test_leader_model_switch_is_limited_to_team_members() -> None:
    binding = SimpleNamespace(
        room_kind=RoomKind.LEADER_ROOM,
        room_id="!leader:local",
        resource_name="alpha",
        matrix_user_id="@alpha-lead:local",
        payload={
            "leader": "alpha-lead",
            "workers": ["alpha-dev", "alpha-test"],
        },
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology({"!leader:local": binding}),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!leader:local",
            sender_id="@alpha-lead:local",
            is_direct=False,
        ),
    )

    assert "switch_worker_model" in policy.allowed_tools
    assert "switch_worker_model" in policy.confirm_tools
    assert policy.allowed_worker_names == frozenset(
        {"alpha-lead", "alpha-dev", "alpha-test"},
    )
    assert "switch_model" not in policy.allowed_tools


@pytest.mark.asyncio
async def test_scoped_human_cannot_gain_resource_admin_tools() -> None:
    human = HumanResource(
        name="reviewer",
        matrix_user_id="@reviewer:local",
        permission_level=2,
        allowed_rooms=("!worker:local",),
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology(humans={"@reviewer:local": human}),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!worker:local",
            sender_id="@reviewer:local",
            is_direct=False,
            mentions=("@manager:local",),
        ),
    )

    assert not policy.silent
    assert "list_workers" in policy.allowed_tools
    assert "create_worker" not in policy.allowed_tools
    assert "switch_model" not in policy.allowed_tools


@pytest.mark.asyncio
async def test_admin_can_manage_only_bound_project_in_project_room() -> None:
    binding = SimpleNamespace(
        room_kind=RoomKind.PROJECT_ROOM,
        room_id="!project:local",
        resource_name="project-20260723-120000-abc123",
        matrix_user_id=None,
        payload={},
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology({"!project:local": binding}),
        admin_user_id="@admin:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!project:local",
            sender_id="@admin:local",
            is_direct=False,
            mentions=("@manager:local",),
        ),
    )

    assert policy.kind is RoomKind.PROJECT_ROOM
    assert policy.project_id == "project-20260723-120000-abc123"
    assert "update_project" in policy.allowed_tools
    assert "delete_project" in policy.confirm_tools
    assert {
        "request_project_revision",
        "reassign_project_task",
        "report_project_blocked",
        "revise_project_plan",
        "revise_project_plan_major",
        "update_project_participants",
    } <= policy.allowed_tools
    assert {
        "revise_project_plan_major",
        "update_project_participants",
    } <= policy.confirm_tools
    assert "revise_project_plan" not in policy.confirm_tools


@pytest.mark.asyncio
async def test_project_worker_mention_wakes_with_reporting_tools_only() -> None:
    binding = SimpleNamespace(
        room_kind=RoomKind.PROJECT_ROOM,
        room_id="!project:local",
        resource_name="project-20260723-120000-abc123",
        matrix_user_id=None,
        payload={"participants": ["alpha-dev"]},
    )
    actor = Actor(
        matrix_user_id="@alpha-dev:local",
        kind=ActorKind.TEAM_WORKER,
        resource_name="alpha-dev",
        team_name="alpha",
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology(
            {"!project:local": binding},
            actors={"@alpha-dev:local": actor},
        ),
        admin_user_id="@admin:local",
        manager_user_id="@manager:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!project:local",
            sender_id="@alpha-dev:local",
            is_direct=False,
            mentions=("@manager:local",),
        ),
    )

    assert not policy.silent
    assert policy.allowed_tools == frozenset(
        {
            "list_tasks",
            "get_task",
                "complete_task",
                "get_project",
                "report_project_blocked",
                "sync_files",
        },
    )
    assert "create_worker" not in policy.allowed_tools
    assert "update_project" not in policy.allowed_tools


@pytest.mark.asyncio
async def test_project_worker_without_manager_mention_stays_silent() -> None:
    binding = SimpleNamespace(
        room_kind=RoomKind.PROJECT_ROOM,
        room_id="!project:local",
        resource_name="project-20260723-120000-abc123",
        matrix_user_id=None,
        payload={"participants": ["alpha-dev"]},
    )
    actor = Actor(
        matrix_user_id="@alpha-dev:local",
        kind=ActorKind.TEAM_WORKER,
        resource_name="alpha-dev",
        team_name="alpha",
    )
    resolver = RoomPolicyResolver(
        topology=FakeTopology(
            {"!project:local": binding},
            actors={"@alpha-dev:local": actor},
        ),
        admin_user_id="@admin:local",
        manager_user_id="@manager:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!project:local",
            sender_id="@alpha-dev:local",
            is_direct=False,
        ),
    )

    assert policy.silent


@pytest.mark.asyncio
async def test_manager_self_event_is_always_silent() -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
        manager_user_id="@manager:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!admin:local",
            sender_id="@manager:local",
            is_direct=True,
        ),
    )

    assert policy.silent


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_type", "relation_type", "is_bot_acknowledgement"),
    [
        ("m.room.redaction", None, False),
        ("m.room.message", "m.replace", False),
        ("m.room.message", None, True),
    ],
)
async def test_non_actionable_matrix_events_never_wake_manager(
    event_type: str,
    relation_type: str | None,
    is_bot_acknowledgement: bool,
) -> None:
    resolver = RoomPolicyResolver(
        topology=FakeTopology(),
        admin_user_id="@admin:local",
        manager_user_id="@manager:local",
    )

    policy = await resolver.resolve(
        _event(
            room_id="!admin:local",
            sender_id="@admin:local",
            is_direct=True,
        ).model_copy(
            update={
                "event_type": event_type,
                "relation_type": relation_type,
                "is_bot_acknowledgement": is_bot_acknowledgement,
            },
        ),
    )

    assert policy.silent
