from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agentteams_manager.domain.models import (
    OperationKind,
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.workflows.matrix_resources import (
    MatrixResourceService,
)
from agentteams_manager.workflows.resources import MutationContext


class Supervisor:
    def __init__(self, events: list[str]) -> None:
        self.operations: dict[str, SimpleNamespace] = {}
        self.events = events

    async def begin(self, **kwargs: object) -> object:
        operation_id = str(kwargs["operation_id"])
        operation = self.operations.get(operation_id)
        if operation is None:
            operation = SimpleNamespace(
                operation_id=operation_id,
                kind=kwargs["kind"],
                target_key=kwargs["target_key"],
                request=kwargs["request"],
                result={},
                status=OperationStatus.PLANNED,
            )
            self.operations[operation_id] = operation
        return operation

    async def before_effect(self, *args: object) -> object:
        operation = self.operations[str(args[0])]
        operation.status = OperationStatus.DISPATCHED
        self.events.append("intent")
        return SimpleNamespace(sequence=1)

    async def effect_succeeded(self, *args: object) -> object:
        operation = self.operations[str(args[0])]
        operation.status = OperationStatus.SUCCEEDED
        operation.result = args[2]
        self.events.append("succeeded")
        return operation

    async def effect_ambiguous(self, *args: object) -> object:
        operation = self.operations[str(args[0])]
        operation.status = OperationStatus.RECONCILING
        self.events.append("ambiguous")
        return operation

    async def effect_failed(self, *args: object) -> object:
        operation = self.operations[str(args[0])]
        operation.status = OperationStatus.FAILED
        self.events.append("failed")
        return operation


class Matrix:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.sent: list[tuple[str, str, str, tuple[str, ...]]] = []

    async def joined_rooms(self) -> tuple[str, ...]:
        return ("!primary:example", "!admin:example")

    async def members(self, room_id: str) -> tuple[str, ...]:
        del room_id
        return ("@admin:example", "@reviewer:example")

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        del thread_id
        self.events.append("matrix")
        self.sent.append((room_id, text, txn_id, mentions))
        return "$notification"


class Channels:
    async def primary_channel(self, user_id: str) -> str | None:
        del user_id
        return "!primary:example"

    async def trusted_channels(
        self,
        user_id: str,
    ) -> tuple[str, ...]:
        del user_id
        return ()


def context() -> MutationContext:
    return MutationContext(
        room_id="!admin:example",
        event_id="$event",
        tool_call_id="notify-1",
    )


@pytest.mark.asyncio
async def test_notification_journals_before_matrix_and_replay_is_idempotent(
) -> None:
    ordered: list[str] = []
    supervisor = Supervisor(ordered)
    matrix = Matrix(ordered)
    service = MatrixResourceService(
        supervisor=supervisor,
        matrix=matrix,
        channels=Channels(),
        manager_admin_room="!admin:example",
    )

    first = await service.send_notification(
        recipient="@reviewer:example",
        text="Build completed",
        context=context(),
    )
    second = await service.send_notification(
        recipient="@reviewer:example",
        text="Build completed",
        context=context(),
    )

    assert ordered == ["intent", "matrix", "succeeded"]
    assert first == second
    assert len(matrix.sent) == 1
    assert matrix.sent[0][2].startswith("agentteams:")


class ChannelWriter(Channels):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.primary: dict[str, str] = {}

    async def set_primary_channel(
        self,
        user_id: str,
        room_id: str,
    ) -> None:
        self.events.append("sqlite")
        self.primary[user_id] = room_id


@pytest.mark.asyncio
async def test_channel_change_journals_before_local_topology_write() -> None:
    ordered: list[str] = []
    supervisor = Supervisor(ordered)
    channels = ChannelWriter(ordered)
    service = MatrixResourceService(
        supervisor=supervisor,
        matrix=Matrix(ordered),
        channels=channels,
        manager_admin_room="!admin:example",
    )

    await service.update_channel(
        action="set_primary",
        user_id="@reviewer:example",
        room_id="!primary:example",
        peer_user_id=None,
        context=context(),
    )

    assert ordered == ["intent", "sqlite", "succeeded"]
    assert channels.primary["@reviewer:example"] == "!primary:example"


@pytest.mark.asyncio
async def test_room_create_recovery_uses_unique_creation_marker() -> None:
    ordered: list[str] = []
    supervisor = Supervisor(ordered)
    now = datetime.now(UTC)
    operation = OperationRecord(
        operation_id="e" * 32,
        kind=OperationKind.MATRIX_MUTATION,
        target_key="matrix-room/release",
        status=OperationStatus.RECONCILING,
        request={
            "action": "create_channel",
            "name": "release",
            "topic": "Release coordination",
            "invite": ["@reviewer:example"],
            "revision": 7,
        },
        created_at=now,
        updated_at=now,
    )
    supervisor.operations[operation.operation_id] = operation

    class RecoveredMatrix(Matrix):
        async def joined_rooms(self) -> tuple[str, ...]:
            return ("!other:example", "!release:example")

        async def room_state(
            self,
            room_id: str,
        ) -> tuple[dict[str, object], ...]:
            marker = (
                operation.operation_id
                if room_id == "!release:example"
                else "another-operation"
            )
            return (
                {
                    "type": "io.agentteams.creation",
                    "state_key": "",
                    "content": {"operation_id": marker},
                },
            )

    service = MatrixResourceService(
        supervisor=supervisor,
        matrix=RecoveredMatrix(ordered),
        channels=Channels(),
        manager_admin_room="!admin:example",
    )

    receipt = await service.resume(operation)

    assert receipt["room_id"] == "!release:example"
    assert ordered == ["succeeded"]
