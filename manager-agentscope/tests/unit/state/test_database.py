from pathlib import Path

import pytest

from agentteams_manager.state.database import Database


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
        "user_version": 1,
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

