import sqlite3
from pathlib import Path

import pytest

from agentteams_manager.state.database import Database
from agentteams_manager.state.schema import SCHEMA_VERSION


@pytest.mark.asyncio
async def test_database_enables_required_sqlite_guards(tmp_path: Path) -> None:
    database = Database(tmp_path / "state" / "manager.db")
    await database.open()

    pragmas = await database.read(
        lambda connection: {
            "journal_mode": connection.execute(
                "PRAGMA journal_mode",
            ).fetchone()[0],
            "foreign_keys": connection.execute(
                "PRAGMA foreign_keys",
            ).fetchone()[0],
            "busy_timeout": connection.execute(
                "PRAGMA busy_timeout",
            ).fetchone()[0],
            "user_version": connection.execute(
                "PRAGMA user_version",
            ).fetchone()[0],
        },
    )

    assert pragmas == {
        "journal_mode": "wal",
        "foreign_keys": 1,
        "busy_timeout": 5000,
        "user_version": SCHEMA_VERSION,
    }


@pytest.mark.asyncio
async def test_database_backup_is_a_readable_consistent_copy(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "manager.db")
    await database.open()
    await database.write(
        lambda connection: connection.execute(
            "INSERT INTO key_values(key, value, updated_at) "
            "VALUES ('revision', '3', '2026-07-23T00:00:00Z')",
        ),
    )

    backup_path = tmp_path / "backup" / "manager.db"
    await database.backup_to(backup_path)
    backup = Database(backup_path)
    await backup.open()

    value = await backup.read(
        lambda connection: connection.execute(
            "SELECT value FROM key_values WHERE key='revision'",
        ).fetchone()[0],
    )
    assert value == "3"


@pytest.mark.asyncio
async def test_matrix_event_schema_migrates_old_claims_as_completed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE processed_matrix_events (
              room_id TEXT NOT NULL,
              event_id TEXT NOT NULL,
              processed_at TEXT NOT NULL,
              PRIMARY KEY(room_id, event_id)
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO processed_matrix_events(
              room_id, event_id, processed_at
            ) VALUES ('!room:local', '$old', '2026-07-29T00:00:00+00:00')
            """,
        )
        connection.execute("PRAGMA user_version=13")

    database = Database(path)
    await database.open()
    row = await database.read(
        lambda connection: connection.execute(
            """
            SELECT status, attempt_count, event_json
              FROM processed_matrix_events
             WHERE event_id='$old'
            """,
        ).fetchone(),
    )

    assert row is not None
    assert dict(row) == {
        "status": "completed",
        "attempt_count": 1,
        "event_json": "",
    }


@pytest.mark.asyncio
async def test_legacy_project_decisions_migrate_as_private(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy-memory.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE project_decisions (
              decision_id TEXT PRIMARY KEY,
              project_id TEXT NOT NULL,
              decision TEXT NOT NULL,
              rationale TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO project_decisions(
              decision_id, project_id, decision, rationale, created_at
            ) VALUES (
              'decision-1', 'project-1', 'Keep SQLite',
              'Avoid another service', '2026-07-29T00:00:00+00:00'
            )
            """,
        )
        connection.execute("PRAGMA user_version=15")

    database = Database(path)
    await database.open()
    visibility = await database.read(
        lambda connection: connection.execute(
            """
            SELECT visibility FROM project_decisions
            WHERE decision_id='decision-1'
            """,
        ).fetchone()[0],
    )

    assert visibility == "private"
