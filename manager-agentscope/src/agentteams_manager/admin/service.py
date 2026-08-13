"""Secret-safe snapshots for the local operations console.

为本地运维控制台生成经过脱敏的系统状态快照。

控制台需要展示 Manager、Worker、Team、Project 和运行健康度，但它不应读取或返回
Secret 的真实内容。本模块只组合允许展示的字段，并通过已有 typed client 查询权威
系统；它提供的是某一时刻的观察结果，不是新的事实来源，也不能代替 Controller、
Matrix 或 Higress 的权威状态。
"""

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
        coding_cli: Any | None = None,
    ) -> None:
        # 逻辑说明：集中注入本地数据库、就绪状态和只读外部客户端，快照服务本身不成为新的事实来源。
        self._database = database
        self._readiness = readiness
        self._controller = controller
        self._runtime_registry = runtime_registry
        self._coding_cli = coding_cli

    async def snapshot(self, section: str) -> dict[str, object]:
        # 逻辑说明：按页面 section 查询外部权威资源或本地状态，并只返回允许展示的字段。
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
                        "coding_cli": (
                            self._coding_cli.status()
                            if self._coding_cli is not None
                            else {
                                "enabled": False,
                                "providers": {},
                            }
                        ),
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
        # 逻辑说明：把受支持 section 映射为固定 SQL，未知 section 不允许拼入查询。
        def read(connection: sqlite3.Connection) -> list[dict[str, object]]:
            # 逻辑说明：在同一只读事务内查询并投影 Row，避免把连接对象带出工作线程。
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
