"""Durable finite and recurring task records."""

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
        self._database = database

    async def create(self, task: TaskRecord) -> TaskRecord:
        def write(connection: sqlite3.Connection) -> None:
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
        def read(connection: sqlite3.Connection) -> TaskRecord | None:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return _task_from_row(row) if row else None

        return await self._database.read(read)

    async def list_all(self) -> tuple[TaskRecord, ...]:
        """Return every durable task in stable creation order."""

        def read(connection: sqlite3.Connection) -> tuple[TaskRecord, ...]:
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
        if not expected:
            raise ValueError("expected statuses must not be empty")

        def write(connection: sqlite3.Connection) -> TaskRecord | None:
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

        def write(connection: sqlite3.Connection) -> TaskRecord:
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
        def read(connection: sqlite3.Connection) -> tuple[TaskRecord, ...]:
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
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[TaskRecord, ...]:
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
        self._database = database

    async def set_dependencies(
        self,
        task_id: str,
        dependencies: tuple[str, ...],
    ) -> None:
        normalized = tuple(dict.fromkeys(dependencies))

        def write(connection: sqlite3.Connection) -> None:
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
        def read(connection: sqlite3.Connection) -> tuple[str, ...]:
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
        if not expected:
            raise ValueError("expected project task states cannot be empty")

        def write(connection: sqlite3.Connection) -> TaskRecord:
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

        def write(connection: sqlite3.Connection) -> TaskRecord:
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
        def write(
            connection: sqlite3.Connection,
        ) -> tuple[TaskRecord, ...]:
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
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectTaskTransition, ...]:
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
        def write(connection: sqlite3.Connection) -> None:
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
        def write(connection: sqlite3.Connection) -> bool:
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
        def read(connection: sqlite3.Connection) -> tuple[str, ...]:
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

        def write(connection: sqlite3.Connection) -> tuple[str, ...]:
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
        def write(connection: sqlite3.Connection) -> ProjectPlanRevision:
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

        def write(connection: sqlite3.Connection) -> ProjectPlanRevision:
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
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectPlanRevision, ...]:
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
    return ProjectPlanRevision(
        project_id=row["project_id"],
        revision=row["revision"],
        body=row["body"],
        change_kind=row["change_kind"],
        created_by=row["created_by"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )
