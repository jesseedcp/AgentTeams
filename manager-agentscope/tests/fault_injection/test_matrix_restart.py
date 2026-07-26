from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.event import (
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from nio.crypto.attachments import encrypt_attachment

from agentteams_manager.domain.models import (
    InboundEvent,
    MediaReference,
    RoomKind,
    RoomPolicy,
)
from agentteams_manager.matrix.crypto import CryptoStore
from agentteams_manager.matrix.media import MediaAdapter
from agentteams_manager.matrix.session_runner import MatrixSessionRunner
from agentteams_manager.runtime.session_manager import RoomSessionManager
from agentteams_manager.state.database import Database
from agentteams_manager.state.confirmations import (
    ConfirmationRepository,
    ConfirmationService,
    ConfirmationStatus,
)
from agentteams_manager.state.sessions import SessionRepository
from tests.integration.test_matrix_agent_turn import RecordingMatrix


class Agent:
    def __init__(self, room_id: str) -> None:
        self.state = AgentState(session_id=f"matrix:{room_id}")
        self.results: list[UserConfirmResultEvent] = []

    async def reply_stream(self, *, inputs: object):
        if isinstance(inputs, UserConfirmResultEvent):
            self.results.append(inputs)
            return
        yield RequireUserConfirmEvent(
            reply_id="reply-restart",
            tool_calls=[
                ToolCallBlock(
                    id="call-restart",
                    name="delete_worker",
                    input='{"name":"alice"}',
                ),
            ],
        )


class Factory:
    runtime_revision = 1

    def __init__(self) -> None:
        self.agent: Agent | None = None

    async def create(
        self,
        room_id: str,
        policy: RoomPolicy,
        state: AgentState | None = None,
    ) -> Agent:
        del policy
        self.agent = Agent(room_id)
        if state is not None:
            self.agent.state = state
        return self.agent


def _event(body: str, event_id: str) -> InboundEvent:
    return InboundEvent(
        room_id="!admin:local",
        event_id=event_id,
        sender="@admin:local",
        body=body,
        timestamp=datetime.now(UTC),
        is_direct=True,
    )


def _policy() -> RoomPolicy:
    return RoomPolicy(
        room_id="!admin:local",
        kind=RoomKind.ADMIN_DM,
        revision=1,
        allowed_senders=frozenset({"@admin:local"}),
    )


@pytest.mark.asyncio
async def test_confirmation_survives_process_reconstruction(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    first_confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    first_factory = Factory()
    first_runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=first_factory,
            sessions=repository,
        ),
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=first_confirmations,
    )
    await first_runner.handle(_event("delete alice", "$delete"), _policy())

    stored = await repository.load("!admin:local")
    assert stored is not None
    assert "agentteams.matrix.pending_confirmation" not in (
        stored.state.middle_context
    )
    approval = (await first_confirmations.pending())[0]

    second_factory = Factory()
    second_confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    second_runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=second_factory,
            sessions=repository,
        ),
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=second_confirmations,
    )
    await second_runner.handle(
        _event(f"/confirm {approval.confirmation_id}", "$confirm"),
        _policy(),
    )

    assert second_factory.agent is not None
    assert second_factory.agent.results[0].confirm_results[0].confirmed


@pytest.mark.asyncio
async def test_cross_room_confirmation_survives_process_reconstruction(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    project_policy = RoomPolicy(
        room_id="!project:local",
        kind=RoomKind.PROJECT_ROOM,
        revision=1,
        allowed_tools=frozenset({"delete_worker"}),
        confirm_tools=frozenset({"delete_worker"}),
        allowed_senders=frozenset({"@worker:local"}),
        project_id="project-1",
    )
    first_runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=Factory(),
            sessions=repository,
        ),
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=confirmations,
    )
    await first_runner.handle(
        InboundEvent(
            room_id="!project:local",
            event_id="$delete-project",
            sender="@worker:local",
            body="delete alice",
            timestamp=datetime.now(UTC),
            is_direct=False,
        ),
        project_policy,
    )
    approval = (await confirmations.pending())[0]

    restarted_confirmations = ConfirmationService(
        ConfirmationRepository(database),
    )
    second_factory = Factory()
    second_runner = MatrixSessionRunner(
        sessions=RoomSessionManager(
            factory=second_factory,
            sessions=repository,
        ),
        matrix=RecordingMatrix(),
        admin_user_id="@admin:local",
        admin_room_id="!admin:local",
        confirmations=restarted_confirmations,
    )
    await second_runner.handle(
        _event(
            f"/confirm {approval.confirmation_id}",
            "$confirm-project",
        ),
        _policy(),
    )

    assert second_factory.agent is not None
    assert second_factory.agent.results[0].confirm_results[0].confirmed
    completed = await restarted_confirmations.get(
        approval.confirmation_id,
    )
    assert completed is not None
    assert completed.status is ConfirmationStatus.APPROVED


@pytest.mark.asyncio
async def test_unclean_restart_reuses_e2ee_material(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "matrix-e2ee"
    store = CryptoStore(store_path)
    store.prepare()
    sentinel = store_path / "megolm-session"
    sentinel.write_bytes(b"session-key-material")

    plaintext = b"\x89PNG\r\nstill-readable"
    ciphertext, keys = encrypt_attachment(plaintext)

    class Nio:
        async def download(self, *, mxc: str) -> object:
            del mxc
            return type(
                "Response",
                (),
                {
                    "body": ciphertext,
                    "filename": "proof.png",
                },
            )()

    restarted = CryptoStore(store_path)
    restarted.prepare()
    block = (
        await MediaAdapter(Nio()).download(
            MediaReference(
                mxc_uri="mxc://local/proof",
                media_type="image/png",
                encryption_key=keys["key"]["k"],
                encryption_hash=keys["hashes"]["sha256"],
                encryption_iv=keys["iv"],
            ),
        )
    )[0]

    assert sentinel.read_bytes() == b"session-key-material"
    assert block.source.media_type == "image/png"
