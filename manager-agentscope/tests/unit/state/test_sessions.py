import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agentscope.state import AgentState
from agentteams_manager.state.database import Database
from agentteams_manager.state.sessions import SessionRepository


@pytest.mark.asyncio
async def test_agent_state_round_trip(tmp_path: Path) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    state = AgentState(session_id="matrix:!room:example")
    state.summary = "compressed context"

    await repository.save(
        room_id="!room:example",
        state=state,
        policy_revision=3,
        last_event_id="$event",
    )
    restored = await repository.load("!room:example")

    assert restored is not None
    assert restored.state.session_id == "matrix:!room:example"
    assert restored.state.summary == "compressed context"
    assert restored.policy_revision == 3
    assert restored.last_event_id == "$event"


@pytest.mark.asyncio
async def test_room_settings_persist_model_and_daily_reset(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)

    settings = await repository.configure(
        "!room:example",
        model_override="qwen-custom",
        timezone="Asia/Shanghai",
        now=now,
    )
    restored = await repository.settings("!room:example", now=now)

    assert restored == settings
    assert restored.model_override == "qwen-custom"
    assert restored.next_reset_at == datetime(
        2026,
        7,
        26,
        20,
        0,
        tzinfo=UTC,
    )


@pytest.mark.asyncio
async def test_room_settings_updates_do_not_overwrite_other_controls(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    repository = SessionRepository(database)
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)

    await repository.update(
        "!room:example",
        thinking_effort="high",
        reasoning_visibility="on",
        verbose_mode="full",
        elevated_mode="ask",
        queue_mode="collect",
        queue_limit=7,
        now=now,
    )
    updated = await repository.update(
        "!room:example",
        model_override="openrouter/example/model",
        now=now,
    )

    assert updated.model_override == "openrouter/example/model"
    assert updated.thinking_effort == "high"
    assert updated.reasoning_visibility == "on"
    assert updated.verbose_mode == "full"
    assert updated.elevated_mode == "ask"
    assert updated.queue_mode == "collect"
    assert updated.queue_limit == 7


@pytest.mark.asyncio
async def test_database_open_migrates_legacy_session_settings(
    tmp_path: Path,
) -> None:
    path = tmp_path / "manager.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE session_settings (
              room_id TEXT PRIMARY KEY,
              model_override TEXT,
              timezone TEXT NOT NULL,
              next_reset_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO session_settings(
              room_id, model_override, timezone, next_reset_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "!legacy:example",
                "qwen-custom",
                "UTC",
                "2026-07-27T04:00:00+00:00",
                "2026-07-26T12:00:00+00:00",
            ),
        )
        connection.execute("PRAGMA user_version=11")

    database = Database(path)
    await database.open()
    repository = SessionRepository(database)
    restored = await repository.settings(
        "!legacy:example",
        now=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )

    assert restored.model_override == "qwen-custom"
    assert restored.thinking_effort is None
    assert restored.reasoning_visibility == "off"
    assert restored.verbose_mode == "off"
    assert restored.elevated_mode == "off"
    assert restored.queue_mode == "followup"
    assert restored.queue_limit == 20
