"""Durable project records independent from Matrix and MinIO.

保存 Project 生命周期、计划版本和参与者的本地事务记录。

Project 的对话房间属于 Matrix，artifact 属于 MinIO，资源期望状态可能来自 Controller；
本表保存 Manager 协调这些系统时所需的 project ID、planning/active 等状态和已确认计划
版本。它不是独占真相，workflow 会将本地记录与各权威系统对账后才报告成功。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from agentteams_manager.domain.models import ProjectRecord

from .database import Database


def _project_from_row(row: sqlite3.Row) -> ProjectRecord:
    # 逻辑说明：反序列化项目元数据并把一行 SQLite 状态转换为领域记录；非法 JSON 会立即报错，避免把损坏计划继续用于调度。
    return ProjectRecord(
        project_id=row["project_id"],
        name=row["name"],
        room_id=row["room_id"],
        status=row["status"],
        metadata=json.loads(row["metadata_json"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class ProjectRepository:
    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存 Project 快照的数据库依赖；房间、计划和生命周期字段仍由显式 repository 方法在单个事务中读写。
        self._database = database

    async def create(self, project: ProjectRecord) -> ProjectRecord:
        # 逻辑说明：在写事务中插入完整项目快照并返回输入记录；重复 project ID 触发唯一键错误，由创建 workflow 按幂等操作记录处理。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：把 metadata 稳定序列化后与生命周期字段一起原子写入，避免项目存在但计划元数据缺失。
            connection.execute(
                """
                INSERT INTO projects(
                    project_id, name, room_id, status, metadata_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.project_id,
                    project.name,
                    project.room_id,
                    project.status,
                    json.dumps(
                        project.metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    project.created_at.isoformat(),
                    project.updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return project

    async def get(self, project_id: str) -> ProjectRecord | None:
        # 逻辑说明：按 project ID 查询持久记录并转换为领域对象；不存在返回 None，不会隐式创建项目。
        def read(connection: sqlite3.Connection) -> ProjectRecord | None:
            # 逻辑说明：执行参数化单行查询，保证用户输入不进入 SQL 结构。
            row = connection.execute(
                "SELECT * FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            return _project_from_row(row) if row else None

        return await self._database.read(read)

    async def list_all(self) -> tuple[ProjectRecord, ...]:
        """Return every durable project in stable creation order."""
        # 逻辑说明：在读事务获取全部项目并按创建时间与 ID 稳定排序，返回不可变元组供管理界面或恢复流程使用。

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectRecord, ...]:
            # 逻辑说明：一次查询取得一致快照并逐行完成领域转换，不修改项目状态。
            rows = connection.execute(
                """
                SELECT * FROM projects
                 ORDER BY created_at, project_id
                """,
            ).fetchall()
            return tuple(_project_from_row(row) for row in rows)

        return await self._database.read(read)

    async def update(
        self,
        project_id: str,
        *,
        expected: set[str],
        status: str,
        room_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ProjectRecord | None:
        # 逻辑说明：校验允许的旧状态集合，并以 compare-and-set 方式更新状态、房间或元数据；状态已变化时返回 None，避免并发请求越过生命周期规则。
        if not expected:
            raise ValueError("expected project statuses must not be empty")

        def write(connection: sqlite3.Connection) -> ProjectRecord | None:
            # 逻辑说明：在同一事务完成条件 UPDATE 和回读；影响零行表示项目不存在或实际状态与 expected 不符，调用方据此对账或报告冲突。
            placeholders = ",".join("?" for _ in expected)
            cursor = connection.execute(
                f"""
                UPDATE projects
                   SET status=?,
                       room_id=COALESCE(?, room_id),
                       metadata_json=COALESCE(?, metadata_json),
                       updated_at=?
                 WHERE project_id=?
                   AND status IN ({placeholders})
                """,
                (
                    status,
                    room_id,
                    (
                        json.dumps(
                            metadata,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                        if metadata is not None
                        else None
                    ),
                    datetime.now(UTC).isoformat(),
                    project_id,
                    *sorted(expected),
                ),
            )
            if cursor.rowcount == 0:
                return None
            row = connection.execute(
                "SELECT * FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            return _project_from_row(row)

        return await self._database.write(write)

    async def list_active(self) -> tuple[ProjectRecord, ...]:
        # 逻辑说明：查询仍需 Manager 协调的 planning/active 项目并稳定排序；结束或删除项目不会进入结果。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectRecord, ...]:
            # 逻辑说明：在同一读快照筛选活跃生命周期状态并转换为不可变记录。
            rows = connection.execute(
                """
                SELECT * FROM projects
                 WHERE status IN ('planning', 'active')
                 ORDER BY created_at, project_id
                """,
            ).fetchall()
            return tuple(_project_from_row(row) for row in rows)

        return await self._database.read(read)
