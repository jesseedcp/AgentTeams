"""Secret-safe snapshots for the local operations console."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from agentteams_manager.health import ReadinessState
from agentteams_manager.state.database import Database


class AdminSnapshotService:
    """Read durable state and live Controller resources without mutation."""

    def __init__(
        self,
        *,
        database: Database,
        readiness: ReadinessState,
        controller: Any,
        runtime_registry: Any,
    ) -> None:
        self._database = database
        self._readiness = readiness
        self._controller = controller
        self._runtime_registry = runtime_registry

    async def snapshot(self, section: str) -> dict[str, object]:
        if section == "overview":
            return {
                "items": [
                    {
                        "component": name,
                        "ready": ready,
                    }
                    for name, ready in self._readiness.as_dict().items()
                ],
                "observed_at": datetime.now(UTC).isoformat(),
            }
        if section in {"sessions", "confirmations", "projects"}:
            return {"items": await self._rows(section)}
        if section == "workers":
            return {
                "items": [
                    item.model_dump(mode="json")
                    for item in await self._controller.list_workers()
                ],
            }
        if section == "teams":
            return {
                "items": [
                    item.model_dump(mode="json")
                    for item in await self._controller.list_teams()
                ],
            }
        if section == "runtime":
            document = self._runtime_registry.current.document
            return {
                "items": [
                    {
                        "manager": document.manager_name,
                        "revision": document.revision,
                        "model": document.model,
                        "skills": list(document.skills),
                        "mcp_servers": len(document.mcp_servers),
                    },
                ],
            }
        if section == "heartbeat":
            return {
                "items": [
                    {
                        "heartbeat_ready": (
                            self._readiness.heartbeat_ready
                        ),
                        "observed_at": datetime.now(UTC).isoformat(),
                    },
                ],
            }
        raise KeyError(section)

    async def _rows(self, section: str) -> list[dict[str, object]]:
        def read(connection: sqlite3.Connection) -> list[dict[str, object]]:
            if section == "sessions":
                rows = connection.execute(
                    """
                    SELECT room_id, policy_revision, last_event_id, updated_at
                    FROM sessions ORDER BY updated_at DESC
                    """,
                ).fetchall()
            elif section == "confirmations":
                rows = connection.execute(
                    """
                    SELECT confirmation_id, source_room_id, requester_id,
                           status, created_at, expires_at
                    FROM confirmation_requests ORDER BY created_at DESC
                    """,
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT project_id, name, room_id, status,
                           created_at, updated_at
                    FROM projects ORDER BY updated_at DESC
                    """,
                ).fetchall()
            return [dict(row) for row in rows]

        return await self._database.read(read)
