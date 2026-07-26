"""Bounded SQLite memory for sessions, projects, and Worker evidence."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime

from .database import Database


@dataclass(frozen=True, slots=True)
class DailyMemory:
    memory_id: str
    room_id: str
    memory_day: date
    content: str
    source_event_id: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class LongTermMemory:
    memory_id: str
    scope: str
    category: str
    content: str
    importance: float
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectDecision:
    decision_id: str
    project_id: str
    decision: str
    rationale: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkerAssessment:
    worker_name: str
    capability: str
    score: float
    evidence: str
    updated_at: datetime


class MemoryRepository:
    """Persist curated memory while pruning each scope deterministically."""

    def __init__(
        self,
        database: Database,
        *,
        per_scope_limit: int = 200,
    ) -> None:
        if per_scope_limit <= 0:
            raise ValueError("memory scope limit must be positive")
        self._database = database
        self._limit = per_scope_limit

    async def append_daily(
        self,
        *,
        room_id: str,
        content: str,
        source_event_id: str,
        now: datetime,
    ) -> DailyMemory:
        body = _required(content, "daily memory")
        created = now.astimezone(UTC)
        memory_id = _stable_id("daily", room_id, source_event_id)

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO daily_memories(
                    memory_id, room_id, memory_day, content,
                    source_event_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(room_id, source_event_id) DO NOTHING
                """,
                (
                    memory_id,
                    room_id,
                    created.date().isoformat(),
                    body,
                    source_event_id,
                    created.isoformat(),
                ),
            )
            connection.execute(
                """
                DELETE FROM daily_memories
                WHERE room_id=? AND memory_id NOT IN (
                    SELECT memory_id FROM daily_memories
                    WHERE room_id=?
                    ORDER BY created_at DESC, memory_id
                    LIMIT ?
                )
                """,
                (room_id, room_id, self._limit),
            )

        await self._database.write(write)
        items = await self.daily(room_id, created.date())
        return next(item for item in items if item.memory_id == memory_id)

    async def daily(
        self,
        room_id: str,
        day: date,
    ) -> tuple[DailyMemory, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[DailyMemory, ...]:
            rows = connection.execute(
                """
                SELECT * FROM daily_memories
                WHERE room_id=? AND memory_day=?
                ORDER BY created_at DESC, memory_id
                """,
                (room_id, day.isoformat()),
            ).fetchall()
            return tuple(
                DailyMemory(
                    memory_id=row["memory_id"],
                    room_id=row["room_id"],
                    memory_day=date.fromisoformat(row["memory_day"]),
                    content=row["content"],
                    source_event_id=row["source_event_id"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                for row in rows
            )

        return await self._database.read(read)

    async def curate_long_term(
        self,
        *,
        scope: str,
        category: str,
        content: str,
        importance: float,
        now: datetime,
    ) -> LongTermMemory:
        body = _required(content, "long-term memory")
        if not 0 <= importance <= 10:
            raise ValueError("memory importance must be between 0 and 10")
        timestamp = now.astimezone(UTC)
        memory_id = _stable_id("long-term", scope, category, body)

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO long_term_memories(
                    memory_id, scope, category, content, importance,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(memory_id) DO UPDATE SET
                    importance=excluded.importance,
                    updated_at=excluded.updated_at
                """,
                (
                    memory_id,
                    scope,
                    category,
                    body,
                    importance,
                    timestamp.isoformat(),
                    timestamp.isoformat(),
                ),
            )
            connection.execute(
                """
                DELETE FROM long_term_memories
                WHERE scope=? AND memory_id NOT IN (
                    SELECT memory_id FROM long_term_memories
                    WHERE scope=?
                    ORDER BY importance DESC, updated_at DESC, memory_id
                    LIMIT ?
                )
                """,
                (scope, scope, self._limit),
            )

        await self._database.write(write)
        retained = next(
            (
                item
                for item in await self.long_term(scope)
                if item.memory_id == memory_id
            ),
            None,
        )
        return retained or LongTermMemory(
            memory_id=memory_id,
            scope=scope,
            category=category,
            content=body,
            importance=importance,
            created_at=timestamp,
            updated_at=timestamp,
        )

    async def long_term(
        self,
        scope: str,
    ) -> tuple[LongTermMemory, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[LongTermMemory, ...]:
            rows = connection.execute(
                """
                SELECT * FROM long_term_memories WHERE scope=?
                ORDER BY importance DESC, updated_at DESC, memory_id
                """,
                (scope,),
            ).fetchall()
            return tuple(
                LongTermMemory(
                    memory_id=row["memory_id"],
                    scope=row["scope"],
                    category=row["category"],
                    content=row["content"],
                    importance=float(row["importance"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )
                for row in rows
            )

        return await self._database.read(read)

    async def record_project_decision(
        self,
        *,
        project_id: str,
        decision: str,
        rationale: str,
        now: datetime,
    ) -> ProjectDecision:
        decision_text = _required(decision, "project decision")
        rationale_text = _required(rationale, "decision rationale")
        created = now.astimezone(UTC)
        decision_id = _stable_id(
            "project-decision",
            project_id,
            decision_text,
            rationale_text,
        )

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO project_decisions(
                    decision_id, project_id, decision, rationale, created_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(decision_id) DO NOTHING
                """,
                (
                    decision_id,
                    project_id,
                    decision_text,
                    rationale_text,
                    created.isoformat(),
                ),
            )
            connection.execute(
                """
                DELETE FROM project_decisions
                WHERE project_id=? AND decision_id NOT IN (
                    SELECT decision_id FROM project_decisions
                    WHERE project_id=?
                    ORDER BY created_at DESC, decision_id
                    LIMIT ?
                )
                """,
                (project_id, project_id, self._limit),
            )

        await self._database.write(write)
        return next(
            item
            for item in await self.project_decisions(project_id)
            if item.decision_id == decision_id
        )

    async def project_decisions(
        self,
        project_id: str,
    ) -> tuple[ProjectDecision, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectDecision, ...]:
            rows = connection.execute(
                """
                SELECT * FROM project_decisions WHERE project_id=?
                ORDER BY created_at DESC, decision_id
                LIMIT ?
                """,
                (project_id, self._limit),
            ).fetchall()
            return tuple(
                ProjectDecision(
                    decision_id=row["decision_id"],
                    project_id=row["project_id"],
                    decision=row["decision"],
                    rationale=row["rationale"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                for row in rows
            )

        return await self._database.read(read)

    async def assess_worker(
        self,
        *,
        worker_name: str,
        capability: str,
        score: float,
        evidence: str,
        now: datetime,
    ) -> WorkerAssessment:
        if not 0 <= score <= 1:
            raise ValueError("Worker capability score must be between 0 and 1")
        evidence_text = _required(evidence, "Worker assessment evidence")
        timestamp = now.astimezone(UTC)

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO worker_capability_assessments(
                    worker_name, capability, score, evidence, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(worker_name, capability) DO UPDATE SET
                    score=excluded.score,
                    evidence=excluded.evidence,
                    updated_at=excluded.updated_at
                """,
                (
                    worker_name,
                    capability,
                    score,
                    evidence_text,
                    timestamp.isoformat(),
                ),
            )
            connection.execute(
                """
                DELETE FROM worker_capability_assessments
                WHERE worker_name=? AND capability NOT IN (
                    SELECT capability
                    FROM worker_capability_assessments
                    WHERE worker_name=?
                    ORDER BY score DESC, updated_at DESC, capability
                    LIMIT ?
                )
                """,
                (worker_name, worker_name, self._limit),
            )

        await self._database.write(write)
        return next(
            item
            for item in await self.worker_assessments(worker_name)
            if item.capability == capability
        )

    async def worker_assessments(
        self,
        worker_name: str,
    ) -> tuple[WorkerAssessment, ...]:
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[WorkerAssessment, ...]:
            rows = connection.execute(
                """
                SELECT * FROM worker_capability_assessments
                WHERE worker_name=?
                ORDER BY score DESC, updated_at DESC, capability
                LIMIT ?
                """,
                (worker_name, self._limit),
            ).fetchall()
            return tuple(
                WorkerAssessment(
                    worker_name=row["worker_name"],
                    capability=row["capability"],
                    score=float(row["score"]),
                    evidence=row["evidence"],
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )
                for row in rows
            )

        return await self._database.read(read)


def _required(value: str, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} must not be empty")
    return normalized


def _stable_id(*parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode()).hexdigest()
    return digest[:32]
