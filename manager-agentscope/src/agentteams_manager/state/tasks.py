"""Durable finite and recurring task records.

保存有限任务、周期任务及 Project DAG 的本地事务状态。

有限任务从 prepared、dispatched、submitted 到 accepted/revision 等状态；Project DAG
还记录依赖节点何时解锁。周期任务保存 cron 和每次 occurrence 的稳定标识。仓库使用
条件转换与唯一约束防止重复派发，但 artifact 内容仍由 MinIO 保存，Worker 实际存在性
仍需向 Controller 查询。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from agentteams_manager.domain.errors import ConflictError, NotFoundError
from agentteams_manager.domain.models import TaskRecord

from .database import Database


def _task_from_row(row: sqlite3.Row) -> TaskRecord:
    # 逻辑说明：解析 metadata JSON 与时间/路由字段，把数据库行统一恢复为 TaskRecord；损坏记录显式失败，不以默认值掩盖调度状态。
    return TaskRecord(
        task_id=row["task_id"],
        task_type=row["task_type"],
        status=row["status"],
        title=row["title"],
        assigned_to=row["assigned_to"],
        room_id=row["room_id"],
        project_id=row["project_id"],
        delegated_to_team=row["delegated_to_team"],
        schedule=row["schedule"],
        timezone=row["timezone"],
        last_executed_at=row["last_executed_at"],
        next_scheduled_at=row["next_scheduled_at"],
        metadata=json.loads(row["metadata_json"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class TaskRepository:
    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存 Task 生命周期快照的事务入口；构造时不创建任务，所有状态变更仍由显式方法校验旧状态后提交。
        self._database = database

    async def create(self, task: TaskRecord) -> TaskRecord:
        # 逻辑说明：在写事务插入完整任务快照并返回输入记录；重复 task ID 由唯一约束拒绝，创建 workflow 再按 operation 幂等处理。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：稳定序列化 metadata，并把生命周期、调度与路由字段一次原子落库。
            connection.execute(
                """
                INSERT INTO tasks(
                    task_id, task_type, status, title, assigned_to,
                    room_id, project_id, delegated_to_team, schedule,
                    timezone, last_executed_at, next_scheduled_at,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    task.task_type,
                    task.status,
                    task.title,
                    task.assigned_to,
                    task.room_id,
                    task.project_id,
                    task.delegated_to_team,
                    task.schedule,
                    task.timezone,
                    (
                        task.last_executed_at.isoformat()
                        if task.last_executed_at
                        else None
                    ),
                    (
                        task.next_scheduled_at.isoformat()
                        if task.next_scheduled_at
                        else None
                    ),
                    json.dumps(
                        task.metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    task.created_at.isoformat(),
                    task.updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return task

    async def get(self, task_id: str) -> TaskRecord | None:
        # 逻辑说明：按 task ID 查询当前持久快照，不存在返回 None，不隐式创建或改变状态。
        def read(connection: sqlite3.Connection) -> TaskRecord | None:
            # 逻辑说明：执行参数化单行查询并通过统一转换器校验记录。
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(row) if row else None

        return await self._database.read(read)

    async def list_all(self) -> tuple[TaskRecord, ...]:
        """Return every durable task in stable creation order."""
        # 逻辑说明：在一致读快照返回全部任务，按创建时间和 ID 稳定排序供管理页面与恢复使用。

        def read(connection: sqlite3.Connection) -> tuple[TaskRecord, ...]:
            # 逻辑说明：一次查询批量转换任务记录，不产生调度副作用。
            rows = connection.execute(
                """
                SELECT * FROM tasks
                 ORDER BY created_at, task_id
                """,
            ).fetchall()
            return tuple(_task_from_row(row) for row in rows)

        return await self._database.read(read)

    async def transition(
        self,
        task_id: str,
        *,
        expected: set[str],
        target: str,
        last_executed_at: datetime | None = None,
        next_scheduled_at: datetime | None = None,
        metadata: dict[str, object] | None = None,
    ) -> TaskRecord | None:
        # 逻辑说明：要求非空 expected 状态集合，并以 compare-and-set 同时更新生命周期、调度时间和可选 metadata；并发状态已变则返回 None。
        if not expected:
            raise ValueError("expected statuses must not be empty")

        def write(connection: sqlite3.Connection) -> TaskRecord | None:
            # 逻辑说明：条件 UPDATE 与回读在同一事务，影响零行表示不存在或 CAS 失败，不覆盖获胜者的新状态。
            placeholders = ",".join("?" for _ in expected)
            cursor = connection.execute(
                f"""
                UPDATE tasks
                   SET status=?,
                       last_executed_at=COALESCE(?, last_executed_at),
                       next_scheduled_at=COALESCE(?, next_scheduled_at),
                       metadata_json=COALESCE(?, metadata_json),
                       updated_at=?
                 WHERE task_id=?
                   AND status IN ({placeholders})
                """,
                (
                    target,
                    (
                        last_executed_at.isoformat()
                        if last_executed_at
                        else None
                    ),
                    (
                        next_scheduled_at.isoformat()
                        if next_scheduled_at
                        else None
                    ),
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
                    task_id,
                    *sorted(expected),
                ),
            )
            if cursor.rowcount == 0:
                return None
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(row)

        return await self._database.write(write)

    async def update_routing(
        self,
        task_id: str,
        *,
        room_id: str,
        metadata: dict[str, object],
    ) -> TaskRecord:
        """Move a task to its authoritative room without changing status."""
        # 逻辑说明：在事务中只替换权威 room 和 metadata 并更新时间，保持任务状态不变；任务不存在时报错，不产生孤立路由。

        def write(connection: sqlite3.Connection) -> TaskRecord:
            # 逻辑说明：执行更新、检查影响行数并回读实际记录，保证返回路由已提交。
            now = datetime.now(UTC)
            cursor = connection.execute(
                """
                UPDATE tasks
                   SET room_id=?, metadata_json=?, updated_at=?
                 WHERE task_id=?
                """,
                (
                    room_id,
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    now.isoformat(),
                    task_id,
                ),
            )
            if cursor.rowcount != 1:
                raise NotFoundError(f"task/{task_id} does not exist")
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(row)

        return await self._database.write(write)

    async def due_schedules(self, now: datetime) -> tuple[TaskRecord, ...]:
        # 逻辑说明：查询 active 且到期、尚未执行当前周期的 recurring/infinite 任务，并按下一次时间稳定返回；这里只选候选，不抢占执行。
        def read(connection: sqlite3.Connection) -> tuple[TaskRecord, ...]:
            # 逻辑说明：在单一读快照比较 ISO 时间与 last_executed 游标，避免同一查询内看到混合状态。
            rows = connection.execute(
                """
                SELECT * FROM tasks
                 WHERE task_type IN ('infinite', 'recurring')
                   AND status='active'
                   AND next_scheduled_at IS NOT NULL
                   AND next_scheduled_at <= ?
                   AND (
                       last_executed_at IS NULL
                       OR last_executed_at < next_scheduled_at
                   )
                 ORDER BY next_scheduled_at, task_id
                """,
                (now.isoformat(),),
            ).fetchall()
            return tuple(_task_from_row(row) for row in rows)

        return await self._database.read(read)

    async def list_by_project(
        self,
        project_id: str,
    ) -> tuple[TaskRecord, ...]:
        # 逻辑说明：按 project ID 读取全部任务并按创建顺序返回，用于构建项目图与验收；不改变 ready 状态。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[TaskRecord, ...]:
            # 逻辑说明：参数化查询并批量转换项目任务。
            rows = connection.execute(
                """
                SELECT * FROM tasks
                 WHERE project_id=?
                 ORDER BY created_at, task_id
                """,
                (project_id,),
            ).fetchall()
            return tuple(_task_from_row(row) for row in rows)

        return await self._database.read(read)


class ProjectTaskState(StrEnum):
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    REVISION_NEEDED = "revision_needed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class ProjectTaskTransition:
    task_id: str
    sequence: int
    from_status: ProjectTaskState | None
    to_status: ProjectTaskState
    actor_id: str
    reason: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectPlanRevision:
    project_id: str
    revision: int
    body: str
    change_kind: str
    created_by: str
    created_at: datetime


class ProjectGraphRepository:
    """Normalized project DAG, actor transitions, people, and plans."""

    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存 Project DAG、参与者、计划版本和事件表共用的数据库边界；图边与任务归属只在后续事务中一起校验和更新。
        self._database = database

    async def set_dependencies(
        self,
        task_id: str,
        dependencies: tuple[str, ...],
    ) -> None:
        # 逻辑说明：先去重依赖顺序，再在事务中确认任务属于项目、替换全部边并验证同项目、非自依赖和无环；失败时旧依赖整体保留。
        normalized = tuple(dict.fromkeys(dependencies))

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：删除旧边、逐条校验和插入新边均在同一事务，并用递归路径查询阻止形成环。
            task = connection.execute(
                "SELECT project_id FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if task is None:
                raise NotFoundError(f"task/{task_id} does not exist")
            project_id = task["project_id"]
            if not project_id:
                raise ConflictError(
                    f"task/{task_id} is not a project task",
                )
            connection.execute(
                """
                DELETE FROM project_task_dependencies
                 WHERE task_id=?
                """,
                (task_id,),
            )
            now = datetime.now(UTC).isoformat()
            for dependency_id in normalized:
                dependency = connection.execute(
                    """
                    SELECT project_id FROM tasks WHERE task_id=?
                    """,
                    (dependency_id,),
                ).fetchone()
                if dependency is None:
                    raise NotFoundError(
                        f"task/{dependency_id} does not exist",
                    )
                if dependency["project_id"] != project_id:
                    raise ConflictError(
                        "project task dependencies must stay "
                        "inside one project",
                    )
                if dependency_id == task_id or _path_exists(
                    connection,
                    start=dependency_id,
                    target=task_id,
                ):
                    raise ConflictError(
                        f"dependency cycle involving task/{task_id}",
                    )
                connection.execute(
                    """
                    INSERT INTO project_task_dependencies(
                        project_id, task_id, depends_on_task_id, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (project_id, task_id, dependency_id, now),
                )

        await self._database.write(write)

    async def dependencies(self, task_id: str) -> tuple[str, ...]:
        # 逻辑说明：读取任务的直接依赖并按 ID 稳定返回；不展开传递闭包，也不推进任务状态。
        def read(connection: sqlite3.Connection) -> tuple[str, ...]:
            # 逻辑说明：在一致快照查询依赖边的目标列。
            rows = connection.execute(
                """
                SELECT depends_on_task_id
                  FROM project_task_dependencies
                 WHERE task_id=?
                 ORDER BY depends_on_task_id
                """,
                (task_id,),
            ).fetchall()
            return tuple(row["depends_on_task_id"] for row in rows)

        return await self._database.read(read)

    async def transition(
        self,
        task_id: str,
        *,
        expected: set[ProjectTaskState],
        target: ProjectTaskState,
        actor_id: str,
        reason: str | None = None,
    ) -> TaskRecord:
        # 逻辑说明：要求 expected 集合，在事务内检查当前 project task 状态和允许转移图，条件更新并记录审计 transition；非法或并发变化报冲突。
        if not expected:
            raise ValueError("expected project task states cannot be empty")

        def write(connection: sqlite3.Connection) -> TaskRecord:
            # 逻辑说明：读取、领域校验、状态写入、transition sequence 追加和回读作为一个原子事务完成。
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"task/{task_id} does not exist")
            current = ProjectTaskState(row["status"])
            if current not in expected:
                raise ConflictError(
                    f"task/{task_id} cannot transition from "
                    f"{current.value} to {target.value}",
                )
            if target not in _PROJECT_TRANSITIONS[current]:
                raise ConflictError(
                    f"project task transition {current.value} -> "
                    f"{target.value} is not allowed",
                )
            now = datetime.now(UTC)
            connection.execute(
                """
                UPDATE tasks SET status=?, updated_at=?
                 WHERE task_id=?
                """,
                (target.value, now.isoformat(), task_id),
            )
            _record_transition(
                connection,
                task_id=task_id,
                from_status=current,
                to_status=target,
                actor_id=actor_id,
                reason=reason,
                now=now,
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(updated)

        return await self._database.write(write)

    async def reassign(
        self,
        task_id: str,
        *,
        assigned_to: str,
        room_id: str,
        matrix_user_id: str,
        storage_team_name: str | None,
        actor_id: str,
        reason: str,
        operation_id: str,
    ) -> TaskRecord:
        """Atomically revoke the old assignment and prepare a new dispatch."""
        # 逻辑说明：在一个事务确认可重分配状态、撤销旧指派、写入新 Worker/room/storage 路由与 operation ID，并记录回到 ready 的审计转移。

        def write(connection: sqlite3.Connection) -> TaskRecord:
            # 逻辑说明：所有身份、状态和 metadata 修改要么一起提交，要么失败回滚，防止 Worker 房间已换但任务仍指向旧负责人。
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"task/{task_id} does not exist")
            current = ProjectTaskState(row["status"])
            allowed = {
                ProjectTaskState.PENDING,
                ProjectTaskState.READY,
                ProjectTaskState.DISPATCHED,
                ProjectTaskState.IN_PROGRESS,
                ProjectTaskState.BLOCKED,
                ProjectTaskState.REVISION_NEEDED,
            }
            if current not in allowed:
                raise ConflictError(
                    f"task/{task_id} cannot be reassigned from "
                    f"{current.value}",
                )
            target = (
                ProjectTaskState.PENDING
                if current is ProjectTaskState.PENDING
                else ProjectTaskState.READY
            )
            metadata = json.loads(row["metadata_json"])
            metadata.update(
                {
                    "matrix_user_id": matrix_user_id,
                    "reassigned_by": actor_id,
                    "reassignment_reason": reason,
                    "reassignment_operation_id": operation_id,
                },
            )
            if storage_team_name is None:
                metadata.pop("storage_team_name", None)
            else:
                metadata["storage_team_name"] = storage_team_name
            metadata.pop("assignment_event_id", None)
            metadata.pop("assignment_txn_id", None)
            now = datetime.now(UTC)
            connection.execute(
                """
                UPDATE tasks
                   SET assigned_to=?, room_id=?, delegated_to_team=NULL,
                       status=?,
                       metadata_json=?, updated_at=?
                 WHERE task_id=?
                """,
                (
                    assigned_to,
                    room_id,
                    target.value,
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    now.isoformat(),
                    task_id,
                ),
            )
            if target is not current:
                _record_transition(
                    connection,
                    task_id=task_id,
                    from_status=current,
                    to_status=target,
                    actor_id=actor_id,
                    reason=reason,
                    now=now,
                )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(updated)

        return await self._database.write(write)

    async def promote_ready(
        self,
        project_id: str,
    ) -> tuple[TaskRecord, ...]:
        # 逻辑说明：找出 project 中所有依赖已 completed 的 pending 任务，在事务中批量提升为 ready 并为每项写 transition，返回刚提升的记录。
        def write(
            connection: sqlite3.Connection,
        ) -> tuple[TaskRecord, ...]:
            # 逻辑说明：候选查询、状态更新和审计追加使用同一事务，避免依赖状态与 ready 结果分离。
            rows = connection.execute(
                """
                SELECT task.* FROM tasks AS task
                 WHERE task.project_id=?
                   AND task.status='pending'
                   AND NOT EXISTS (
                       SELECT 1
                         FROM project_task_dependencies AS edge
                         JOIN tasks AS dependency
                           ON dependency.task_id=edge.depends_on_task_id
                        WHERE edge.task_id=task.task_id
                          AND dependency.status <> 'completed'
                   )
                 ORDER BY task.created_at, task.task_id
                """,
                (project_id,),
            ).fetchall()
            promoted: list[TaskRecord] = []
            for row in rows:
                now = datetime.now(UTC)
                cursor = connection.execute(
                    """
                    UPDATE tasks SET status='ready', updated_at=?
                     WHERE task_id=? AND status='pending'
                    """,
                    (now.isoformat(), row["task_id"]),
                )
                if cursor.rowcount != 1:
                    continue
                _record_transition(
                    connection,
                    task_id=row["task_id"],
                    from_status=ProjectTaskState.PENDING,
                    to_status=ProjectTaskState.READY,
                    actor_id="@manager:system",
                    reason="all dependencies completed",
                    now=now,
                )
                updated = connection.execute(
                    "SELECT * FROM tasks WHERE task_id=?",
                    (row["task_id"],),
                ).fetchone()
                promoted.append(_task_from_row(updated))
            return tuple(promoted)

        return await self._database.write(write)

    async def transitions(
        self,
        task_id: str,
    ) -> tuple[ProjectTaskTransition, ...]:
        # 逻辑说明：按 task ID 读取完整状态迁移历史并依 sequence 排序，恢复可选旧状态和时间供审计展示。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectTaskTransition, ...]:
            # 逻辑说明：在一致读快照逐行转换状态枚举与时间。
            rows = connection.execute(
                """
                SELECT * FROM project_task_transitions
                 WHERE task_id=? ORDER BY sequence
                """,
                (task_id,),
            ).fetchall()
            return tuple(
                ProjectTaskTransition(
                    task_id=row["task_id"],
                    sequence=row["sequence"],
                    from_status=(
                        ProjectTaskState(row["from_status"])
                        if row["from_status"]
                        else None
                    ),
                    to_status=ProjectTaskState(row["to_status"]),
                    actor_id=row["actor_id"],
                    reason=row["reason"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                for row in rows
            )

        return await self._database.read(read)

    async def add_participant(
        self,
        project_id: str,
        worker_name: str,
        *,
        now: datetime,
    ) -> None:
        # 逻辑说明：在事务中 upsert 项目参与者并清除 removed_at；重复添加刷新 joined_at，保证当前集合只有一条有效关系。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：单条 upsert 原子恢复或创建参与关系。
            connection.execute(
                """
                INSERT INTO project_participants(
                    project_id, worker_name, joined_at, removed_at
                ) VALUES (?, ?, ?, NULL)
                ON CONFLICT(project_id, worker_name) DO UPDATE SET
                    joined_at=excluded.joined_at,
                    removed_at=NULL
                """,
                (project_id, worker_name, now.isoformat()),
            )

        await self._database.write(write)

    async def remove_participant(
        self,
        project_id: str,
        worker_name: str,
        *,
        now: datetime,
    ) -> bool:
        # 逻辑说明：仅把当前仍活跃的参与关系标记 removed_at，并返回是否真正移除；重复调用幂等返回 False。
        def write(connection: sqlite3.Connection) -> bool:
            # 逻辑说明：条件 UPDATE 与影响行数共同实现 compare-and-set 移除。
            cursor = connection.execute(
                """
                UPDATE project_participants SET removed_at=?
                 WHERE project_id=? AND worker_name=?
                   AND removed_at IS NULL
                """,
                (now.isoformat(), project_id, worker_name),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)

    async def participants(self, project_id: str) -> tuple[str, ...]:
        # 逻辑说明：查询 removed_at 为空的当前参与者并按名称稳定返回；历史移除记录不会泄漏到调度集合。
        def read(connection: sqlite3.Connection) -> tuple[str, ...]:
            # 逻辑说明：在同一快照读取活跃 participant rows。
            rows = connection.execute(
                """
                SELECT worker_name FROM project_participants
                 WHERE project_id=? AND removed_at IS NULL
                 ORDER BY worker_name
                """,
                (project_id,),
            ).fetchall()
            return tuple(row["worker_name"] for row in rows)

        return await self._database.read(read)

    async def update_participants(
        self,
        project_id: str,
        *,
        add: tuple[str, ...],
        remove: tuple[str, ...],
        worker_users: dict[str, str],
        now: datetime,
    ) -> tuple[str, ...]:
        """Update participant rows and project metadata in one transaction."""
        # 逻辑说明：在单事务核对项目、阻止移除仍有活跃任务的 Worker、应用增删，并同步 project metadata 的成员与用户映射；返回最终有序集合。

        def write(connection: sqlite3.Connection) -> tuple[str, ...]:
            # 逻辑说明：参与关系和项目 metadata 共用事务，任何权限/任务占用冲突都回滚全部变化，避免 Matrix 成员与持久计划依据分裂。
            project = connection.execute(
                "SELECT metadata_json FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            if project is None:
                raise NotFoundError(f"project/{project_id} does not exist")
            if remove:
                placeholders = ",".join("?" for _ in remove)
                active = connection.execute(
                    f"""
                    SELECT task_id, assigned_to FROM tasks
                     WHERE project_id=?
                       AND assigned_to IN ({placeholders})
                       AND status NOT IN ('completed', 'failed', 'cancelled')
                     ORDER BY task_id
                    """,
                    (project_id, *remove),
                ).fetchall()
                if active:
                    assignments = ", ".join(
                        f"{row['task_id']}->{row['assigned_to']}"
                        for row in active
                    )
                    raise ConflictError(
                        "cannot remove participant with active task: "
                        + assignments,
                    )
            timestamp = now.isoformat()
            for worker_name in add:
                connection.execute(
                    """
                    INSERT INTO project_participants(
                        project_id, worker_name, joined_at, removed_at
                    ) VALUES (?, ?, ?, NULL)
                    ON CONFLICT(project_id, worker_name) DO UPDATE SET
                        joined_at=excluded.joined_at,
                        removed_at=NULL
                    """,
                    (project_id, worker_name, timestamp),
                )
            for worker_name in remove:
                connection.execute(
                    """
                    UPDATE project_participants SET removed_at=?
                     WHERE project_id=? AND worker_name=?
                       AND removed_at IS NULL
                    """,
                    (timestamp, project_id, worker_name),
                )
            rows = connection.execute(
                """
                SELECT worker_name FROM project_participants
                 WHERE project_id=? AND removed_at IS NULL
                 ORDER BY worker_name
                """,
                (project_id,),
            ).fetchall()
            participants = tuple(row["worker_name"] for row in rows)
            if not participants:
                raise ConflictError(
                    "project must retain at least one participant",
                )
            metadata = json.loads(project["metadata_json"])
            metadata["participants"] = list(participants)
            metadata["worker_users"] = {
                name: worker_users[name]
                for name in participants
                if name in worker_users
            }
            connection.execute(
                """
                UPDATE projects
                   SET metadata_json=?, updated_at=?
                 WHERE project_id=?
                """,
                (
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    timestamp,
                    project_id,
                ),
            )
            return participants

        return await self._database.write(write)

    async def append_plan_revision(
        self,
        project_id: str,
        *,
        body: str,
        change_kind: str,
        created_by: str,
        now: datetime,
    ) -> ProjectPlanRevision:
        # 逻辑说明：读取最新 revision；若正文、类型和作者完全相同则幂等返回，否则创建下一版本，保证并发写在 SQLite 事务内串行编号。
        def write(connection: sqlite3.Connection) -> ProjectPlanRevision:
            # 逻辑说明：最新检查、revision+1 插入和回读作为一个事务，避免重复或跳号。
            latest = connection.execute(
                """
                SELECT * FROM project_plan_revisions
                 WHERE project_id=? ORDER BY revision DESC LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            if (
                latest is not None
                and latest["body"] == body
                and latest["change_kind"] == change_kind
                and latest["created_by"] == created_by
            ):
                return _plan_revision_from_row(latest)
            row = connection.execute(
                """
                SELECT COALESCE(MAX(revision), 0) + 1 AS revision
                  FROM project_plan_revisions WHERE project_id=?
                """,
                (project_id,),
            ).fetchone()
            revision = int(row["revision"])
            connection.execute(
                """
                INSERT INTO project_plan_revisions(
                    project_id, revision, body, change_kind,
                    created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    revision,
                    body,
                    change_kind,
                    created_by,
                    now.isoformat(),
                ),
            )
            return ProjectPlanRevision(
                project_id=project_id,
                revision=revision,
                body=body,
                change_kind=change_kind,
                created_by=created_by,
                created_at=now,
            )

        return await self._database.write(write)

    async def revise_plan(
        self,
        project_id: str,
        *,
        body: str,
        change_kind: str,
        reason: str,
        created_by: str,
        now: datetime,
    ) -> ProjectPlanRevision:
        """Version a plan and make it current in one SQLite transaction."""
        # 逻辑说明：确认项目存在，比较最新计划实现幂等；新版本插入后同步更新 project metadata 当前计划、原因与 revision，最后返回已提交版本。

        def write(connection: sqlite3.Connection) -> ProjectPlanRevision:
            # 逻辑说明：版本表和项目当前指针在同一事务更新，任何 JSON/SQL 失败都不会出现指针指向不存在版本。
            project = connection.execute(
                "SELECT metadata_json FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            if project is None:
                raise NotFoundError(f"project/{project_id} does not exist")
            metadata = json.loads(project["metadata_json"])
            latest = connection.execute(
                """
                SELECT * FROM project_plan_revisions
                 WHERE project_id=? ORDER BY revision DESC LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            if (
                latest is not None
                and latest["body"] == body
                and latest["change_kind"] == change_kind
                and latest["created_by"] == created_by
                and metadata.get("plan_change_reason") == reason
            ):
                return _plan_revision_from_row(latest)
            row = connection.execute(
                """
                SELECT COALESCE(MAX(revision), 0) + 1 AS revision
                  FROM project_plan_revisions WHERE project_id=?
                """,
                (project_id,),
            ).fetchone()
            revision = int(row["revision"])
            connection.execute(
                """
                INSERT INTO project_plan_revisions(
                    project_id, revision, body, change_kind,
                    created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    revision,
                    body,
                    change_kind,
                    created_by,
                    now.isoformat(),
                ),
            )
            metadata.update(
                {
                    "plan": body,
                    "plan_revision": revision,
                    "plan_change_kind": change_kind,
                    "plan_change_reason": reason,
                },
            )
            connection.execute(
                """
                UPDATE projects
                   SET metadata_json=?, updated_at=?
                 WHERE project_id=?
                """,
                (
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    now.isoformat(),
                    project_id,
                ),
            )
            return ProjectPlanRevision(
                project_id=project_id,
                revision=revision,
                body=body,
                change_kind=change_kind,
                created_by=created_by,
                created_at=now,
            )

        return await self._database.write(write)

    async def plan_revisions(
        self,
        project_id: str,
    ) -> tuple[ProjectPlanRevision, ...]:
        # 逻辑说明：按 project ID 读取所有计划版本并依 revision 升序返回，供计划审计和回滚决策使用。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectPlanRevision, ...]:
            # 逻辑说明：一致快照查询并统一转换时间与版本字段。
            rows = connection.execute(
                """
                SELECT * FROM project_plan_revisions
                 WHERE project_id=? ORDER BY revision
                """,
                (project_id,),
            ).fetchall()
            return tuple(_plan_revision_from_row(row) for row in rows)

        return await self._database.read(read)


_PROJECT_TRANSITIONS: dict[
    ProjectTaskState,
    frozenset[ProjectTaskState],
] = {
    ProjectTaskState.PENDING: frozenset(
        {ProjectTaskState.READY, ProjectTaskState.CANCELLED},
    ),
    ProjectTaskState.READY: frozenset(
        {ProjectTaskState.DISPATCHED, ProjectTaskState.CANCELLED},
    ),
    ProjectTaskState.DISPATCHED: frozenset(
        {
            ProjectTaskState.READY,
            ProjectTaskState.IN_PROGRESS,
            ProjectTaskState.BLOCKED,
            ProjectTaskState.REVISION_NEEDED,
            ProjectTaskState.COMPLETED,
            ProjectTaskState.CANCELLED,
        },
    ),
    ProjectTaskState.IN_PROGRESS: frozenset(
        {
            ProjectTaskState.READY,
            ProjectTaskState.BLOCKED,
            ProjectTaskState.REVISION_NEEDED,
            ProjectTaskState.COMPLETED,
            ProjectTaskState.CANCELLED,
        },
    ),
    ProjectTaskState.BLOCKED: frozenset(
        {
            ProjectTaskState.READY,
            ProjectTaskState.IN_PROGRESS,
            ProjectTaskState.COMPLETED,
            ProjectTaskState.CANCELLED,
        },
    ),
    ProjectTaskState.REVISION_NEEDED: frozenset(
        {
            ProjectTaskState.READY,
            ProjectTaskState.DISPATCHED,
            ProjectTaskState.COMPLETED,
            ProjectTaskState.CANCELLED,
        },
    ),
    ProjectTaskState.COMPLETED: frozenset(),
    ProjectTaskState.CANCELLED: frozenset(),
}


def _path_exists(
    connection: sqlite3.Connection,
    *,
    start: str,
    target: str,
) -> bool:
    # 逻辑说明：用递归 CTE 判断从 start 沿依赖边能否到达 target，供插边前检测环；只查询当前事务内可见图。
    row = connection.execute(
        """
        WITH RECURSIVE path(task_id) AS (
            SELECT depends_on_task_id
              FROM project_task_dependencies
             WHERE task_id=?
            UNION
            SELECT edge.depends_on_task_id
              FROM project_task_dependencies AS edge
              JOIN path ON edge.task_id=path.task_id
        )
        SELECT 1 FROM path WHERE task_id=? LIMIT 1
        """,
        (start, target),
    ).fetchone()
    return row is not None


def _record_transition(
    connection: sqlite3.Connection,
    *,
    task_id: str,
    from_status: ProjectTaskState | None,
    to_status: ProjectTaskState,
    actor_id: str,
    reason: str | None,
    now: datetime,
) -> None:
    # 逻辑说明：在当前事务计算该任务下一审计 sequence 并插入 from/to、actor、reason 和时间；与调用方状态更新共同提交。
    row = connection.execute(
        """
        SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
          FROM project_task_transitions WHERE task_id=?
        """,
        (task_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO project_task_transitions(
            task_id, sequence, from_status, to_status,
            actor_id, reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            int(row["sequence"]),
            from_status.value if from_status else None,
            to_status.value,
            actor_id,
            reason,
            now.isoformat(),
        ),
    )


def _plan_revision_from_row(row: sqlite3.Row) -> ProjectPlanRevision:
    # 逻辑说明：把计划版本行转换为不可变领域对象并解析创建时间，统一所有计划历史查询的字段语义。
    return ProjectPlanRevision(
        project_id=row["project_id"],
        revision=row["revision"],
        body=row["body"],
        change_kind=row["change_kind"],
        created_by=row["created_by"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )
