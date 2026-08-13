"""Bounded SQLite memory for sessions, projects, and Worker evidence.

在 SQLite 中保存有界的 Manager 记忆、项目决策和 Worker 证据。

记忆用于补充下一次模型上下文，但不是实时权威状态。记录按 scope 与来源建立稳定 ID，
限制正文长度和召回数量，并把管理员私有记忆与 Project Room 可见决策分开。任何会变化
的 Worker、Task 或 Project 状态在执行前仍必须重新查询对应 typed tool。
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal

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
    visibility: Literal["private", "project"]
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
        # 逻辑说明：拒绝非正的每 scope 容量，并保存数据库与确定性裁剪上限；后续所有写入在同一事务内插入并修剪。
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
        # 逻辑说明：规范正文、统一 UTC 时间并从房间和来源生成稳定 ID；事务内幂等插入并裁剪旧项，随后回读返回实际保留记录。
        body = _required(content, "daily memory")
        created = now.astimezone(UTC)
        memory_id = _stable_id("daily", room_id, source_event_id)

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：按 room/source 去重写入后，以稳定排序只保留该房间最新 limit 条，保证重复事件不扩增记忆。
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
        # 逻辑说明：按房间与日期读取日记忆，转换日期/时间并按最新优先返回；只读不改变裁剪状态。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[DailyMemory, ...]:
            # 逻辑说明：在同一快照查询并逐行构造不可变 DailyMemory。
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

    async def recent_daily(
        self,
        room_id: str,
        *,
        through: date,
        days: int = 2,
        limit: int = 50,
    ) -> tuple[DailyMemory, ...]:
        # 逻辑说明：先验证天数窗口和结果上限，再查询截至指定日期的有界近期记忆；非法边界在数据库访问前拒绝。
        if days <= 0:
            raise ValueError("memory day window must be positive")
        if limit <= 0:
            raise ValueError("memory result limit must be positive")

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[DailyMemory, ...]:
            # 逻辑说明：用 SQLite 日期运算筛选窗口并按最新时间限制结果，统一转换为领域对象。
            rows = connection.execute(
                """
                SELECT * FROM daily_memories
                WHERE room_id=?
                  AND memory_day <= ?
                  AND memory_day >= date(?, ?)
                ORDER BY created_at DESC, memory_id
                LIMIT ?
                """,
                (
                    room_id,
                    through.isoformat(),
                    through.isoformat(),
                    f"-{days - 1} day",
                    limit,
                ),
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
        # 逻辑说明：规范正文与重要度，生成内容寻址 ID 后在事务中 upsert 并按重要度裁剪；回读保留项，若刚写项被容量策略淘汰则返回所请求快照作回执。
        body = _required(content, "long-term memory")
        if not 0 <= importance <= 10:
            raise ValueError("memory importance must be between 0 and 10")
        timestamp = now.astimezone(UTC)
        memory_id = _stable_id("long-term", scope, category, body)

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：相同内容只更新时间和重要度，再以重要度/更新时间稳定保留每 scope 前 limit 条，避免无界增长。
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
        # 逻辑说明：读取指定 scope 的长期记忆并按重要度、更新时间稳定排序，供上下文召回使用。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[LongTermMemory, ...]:
            # 逻辑说明：在一致读快照转换浮点数与 ISO 时间，返回不可变元组。
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
        visibility: Literal["private", "project"] = "private",
    ) -> ProjectDecision:
        # 逻辑说明：规范决策与理由、校验可见性并生成稳定 ID；事务中去重插入和按项目裁剪，随后回读实际记录。
        decision_text = _required(decision, "project decision")
        rationale_text = _required(rationale, "decision rationale")
        if visibility not in {"private", "project"}:
            raise ValueError("project decision visibility is invalid")
        created = now.astimezone(UTC)
        decision_id = _stable_id(
            "project-decision",
            project_id,
            decision_text,
            rationale_text,
            visibility,
        )

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：同一内容决策不重复写入，并只保留项目最近 limit 条，使来源重试幂等且空间有界。
            connection.execute(
                """
                INSERT INTO project_decisions(
                    decision_id, project_id, decision, rationale,
                    visibility, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(decision_id) DO NOTHING
                """,
                (
                    decision_id,
                    project_id,
                    decision_text,
                    rationale_text,
                    visibility,
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
        *,
        include_private: bool = True,
    ) -> tuple[ProjectDecision, ...]:
        # 逻辑说明：按项目读取决策，并根据 include_private 决定是否只投影 project 可见项；始终限制数量与稳定排序。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectDecision, ...]:
            # 逻辑说明：在同一读事务选择私有或项目公开查询，再统一转换成强类型决策。
            if include_private:
                rows = connection.execute(
                    """
                    SELECT * FROM project_decisions WHERE project_id=?
                    ORDER BY created_at DESC, decision_id
                    LIMIT ?
                    """,
                    (project_id, self._limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM project_decisions
                    WHERE project_id=? AND visibility='project'
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
                    visibility=row["visibility"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                for row in rows
            )

        return await self._database.read(read)

    async def recent_project_decisions(
        self,
        *,
        limit: int = 50,
    ) -> tuple[ProjectDecision, ...]:
        # 逻辑说明：验证全局结果上限后读取最近项目决策，供 Admin 召回；这里只查询，不改变可见性。
        if limit <= 0:
            raise ValueError("memory result limit must be positive")

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ProjectDecision, ...]:
            # 逻辑说明：按时间和稳定 ID 排序限制结果，并反序列化为领域对象。
            rows = connection.execute(
                """
                SELECT * FROM project_decisions
                ORDER BY created_at DESC, decision_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return tuple(
                ProjectDecision(
                    decision_id=row["decision_id"],
                    project_id=row["project_id"],
                    decision=row["decision"],
                    rationale=row["rationale"],
                    visibility=row["visibility"],
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
        # 逻辑说明：校验 0..1 分数和非空证据，事务中按 worker/capability upsert 并裁剪较弱旧项，随后回读返回实际评估。
        if not 0 <= score <= 1:
            raise ValueError("Worker capability score must be between 0 and 1")
        evidence_text = _required(evidence, "Worker assessment evidence")
        timestamp = now.astimezone(UTC)

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：原子更新同一能力的分数、证据与时间，再按分数/时间只保留该 Worker 前 limit 项。
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
        # 逻辑说明：读取某 Worker 的有界能力评估并按分数和更新时间排序，结果仅是证据记忆而非实时 Worker 状态。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[WorkerAssessment, ...]:
            # 逻辑说明：在一致快照查询并转换分数和时间字段。
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

    async def recent_worker_assessments(
        self,
        *,
        limit: int = 50,
    ) -> tuple[WorkerAssessment, ...]:
        # 逻辑说明：验证结果上限并跨 Worker 读取最近评估，供 Admin 记忆召回；不会据此直接调度任务。
        if limit <= 0:
            raise ValueError("memory result limit must be positive")

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[WorkerAssessment, ...]:
            # 逻辑说明：按更新时间稳定排序并限制数量，逐行恢复不可变评估对象。
            rows = connection.execute(
                """
                SELECT * FROM worker_capability_assessments
                ORDER BY updated_at DESC, worker_name, capability
                LIMIT ?
                """,
                (limit,),
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
    # 逻辑说明：去掉首尾空白并拒绝空内容，返回规范文本供稳定 ID 和持久化共同使用。
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} must not be empty")
    return normalized


def _stable_id(*parts: str) -> str:
    # 逻辑说明：用不可混淆分隔符连接身份字段并计算 SHA-256，截取稳定 128-bit 十六进制 ID，实现内容级幂等而不保存原文到键中。
    digest = hashlib.sha256("\x1f".join(parts).encode()).hexdigest()
    return digest[:32]
