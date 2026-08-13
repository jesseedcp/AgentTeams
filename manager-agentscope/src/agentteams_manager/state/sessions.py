"""AgentScope session persistence keyed by Matrix room.

按 Matrix room 持久化 AgentScope state 与会话设置。

每个房间拥有独立上下文、摘要、模型覆盖、thinking、queue 与 elevated 模式。AgentState
序列化后写入 SQLite，进程重启可继续对话；每日 reset 边界存为绝对时间，避免重启后
重复或漏掉清理。这里存储设置，不决定设置是否有权限，授权仍由 Admin 命令和 policy
处理。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agentscope.state import AgentState

from .database import Database

THINKING_EFFORTS = frozenset(
    {"off", "minimal", "low", "medium", "high", "xhigh"},
)
REASONING_VISIBILITIES = frozenset({"off", "on", "stream"})
VERBOSE_MODES = frozenset({"off", "on", "full"})
ELEVATED_MODES = frozenset({"off", "ask", "full"})
QUEUE_MODES = frozenset({"followup", "collect", "interrupt"})


class _Unset:
    pass


_UNSET = _Unset()
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class StoredSession:
    room_id: str
    state: AgentState
    policy_revision: int
    last_event_id: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SessionSettings:
    room_id: str
    model_override: str | None
    thinking_effort: str | None
    reasoning_visibility: str
    verbose_mode: str
    elevated_mode: str
    queue_mode: str
    queue_limit: int
    timezone: str
    next_reset_at: datetime
    updated_at: datetime


class SessionRepository:
    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存 room-scoped AgentState 的持久化入口；构造时不加载任何房间，避免应用启动一次性反序列化全部会话。
        self._database = database

    async def load(self, room_id: str) -> StoredSession | None:
        # 逻辑说明：按 Matrix 房间加载序列化 AgentScope 状态和最后事件游标；不存在返回 None，解析失败向上抛出以避免静默丢失上下文。
        def read(connection: sqlite3.Connection) -> StoredSession | None:
            # 逻辑说明：在读事务查询单个房间，并把 JSON state 与时间字段恢复为强类型对象。
            row = connection.execute(
                "SELECT * FROM sessions WHERE room_id=?",
                (room_id,),
            ).fetchone()
            if row is None:
                return None
            return StoredSession(
                room_id=row["room_id"],
                state=AgentState.model_validate_json(
                    row["agent_state_json"],
                ),
                policy_revision=row["policy_revision"],
                last_event_id=row["last_event_id"],
                updated_at=datetime.fromisoformat(row["updated_at"]),
            )

        return await self._database.read(read)

    async def save(
        self,
        *,
        room_id: str,
        state: AgentState,
        policy_revision: int,
        last_event_id: str | None,
    ) -> StoredSession:
        # 逻辑说明：先在事务外生成统一更新时间并序列化 AgentState，再按 room ID 原子 upsert；成功后返回与持久内容一致的会话快照。
        updated_at = datetime.now(UTC)
        serialized = state.model_dump_json()

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：一次 upsert 同步替换状态、策略 revision 和最后事件 ID，防止这些恢复游标分开提交。
            connection.execute(
                """
                INSERT INTO sessions(
                    room_id, agent_state_json, policy_revision,
                    last_event_id, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(room_id) DO UPDATE SET
                    agent_state_json=excluded.agent_state_json,
                    policy_revision=excluded.policy_revision,
                    last_event_id=excluded.last_event_id,
                    updated_at=excluded.updated_at
                """,
                (
                    room_id,
                    serialized,
                    policy_revision,
                    last_event_id,
                    updated_at.isoformat(),
                ),
            )

        await self._database.write(write)
        return StoredSession(
            room_id=room_id,
            state=state,
            policy_revision=policy_revision,
            last_event_id=last_event_id,
            updated_at=updated_at,
        )

    async def delete(self, room_id: str) -> bool:
        # 逻辑说明：在写事务删除指定房间的会话状态并返回是否命中；设置表由独立生命周期管理，不在这里连带清除。
        def write(connection: sqlite3.Connection) -> bool:
            # 逻辑说明：用影响行数区分成功删除和本来不存在，重复 reset 因而可以安全重试。
            cursor = connection.execute(
                "DELETE FROM sessions WHERE room_id=?",
                (room_id,),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)

    async def settings(
        self,
        room_id: str,
        *,
        now: datetime,
        timezone: str = "UTC",
    ) -> SessionSettings:
        # 逻辑说明：先读取房间设置，存在则原样返回；不存在才按默认模型和时区创建，避免每次访问重算每日 reset 边界。
        existing = await self._read_settings(room_id)
        if existing is not None:
            return existing
        return await self.configure(
            room_id,
            model_override=None,
            timezone=timezone,
            now=now,
        )

    async def configure(
        self,
        room_id: str,
        *,
        model_override: str | None,
        timezone: str,
        now: datetime,
    ) -> SessionSettings:
        # 逻辑说明：通过统一存储路径设置模型与时区并要求重算 reset 日程；校验或数据库失败直接传播，不返回未持久化设置。
        return await self._store_settings(
            room_id,
            model_override=model_override,
            timezone=timezone,
            now=now,
            reset_schedule=True,
        )

    async def update(
        self,
        room_id: str,
        *,
        now: datetime,
        model_override: str | None | _Unset = _UNSET,
        thinking_effort: str | None | _Unset = _UNSET,
        reasoning_visibility: str | _Unset = _UNSET,
        verbose_mode: str | _Unset = _UNSET,
        elevated_mode: str | _Unset = _UNSET,
        queue_mode: str | _Unset = _UNSET,
        queue_limit: int | _Unset = _UNSET,
    ) -> SessionSettings:
        """Update selected controls without resetting unrelated settings."""
        # 逻辑说明：把未提供字段保留为 _UNSET，委托统一事务只更新显式选择的控制项；不会因为普通设置修改而重置每日清理时间。
        return await self._store_settings(
            room_id,
            model_override=model_override,
            thinking_effort=thinking_effort,
            reasoning_visibility=reasoning_visibility,
            verbose_mode=verbose_mode,
            elevated_mode=elevated_mode,
            queue_mode=queue_mode,
            queue_limit=queue_limit,
            now=now,
            reset_schedule=False,
        )

    async def _store_settings(
        self,
        room_id: str,
        *,
        now: datetime,
        model_override: str | None | _Unset = _UNSET,
        thinking_effort: str | None | _Unset = _UNSET,
        reasoning_visibility: str | _Unset = _UNSET,
        verbose_mode: str | _Unset = _UNSET,
        elevated_mode: str | _Unset = _UNSET,
        queue_mode: str | _Unset = _UNSET,
        queue_limit: int | _Unset = _UNSET,
        timezone: str | _Unset = _UNSET,
        reset_schedule: bool,
    ) -> SessionSettings:
        # 逻辑说明：在一个事务中读取当前设置、合并显式变更、校验枚举和队列上限，再 upsert 完整快照；任何校验或 SQL 失败都不会留下部分设置。
        normalized_now = now.astimezone(UTC)

        def write(connection: sqlite3.Connection) -> SessionSettings:
            # 逻辑说明：以当前记录为默认值解析所有 _UNSET 字段，必要时重算 reset，然后一次写入并返回同值对象，保证返回值就是实际提交内容。
            row = connection.execute(
                "SELECT * FROM session_settings WHERE room_id=?",
                (room_id,),
            ).fetchone()
            current = (
                _settings_from_row(row) if row is not None else None
            )
            selected_model = _selected(
                model_override,
                current.model_override if current is not None else None,
            )
            selected_thinking = _selected(
                thinking_effort,
                current.thinking_effort if current is not None else None,
            )
            selected_reasoning = _selected(
                reasoning_visibility,
                (
                    current.reasoning_visibility
                    if current is not None
                    else "off"
                ),
            )
            selected_verbose = _selected(
                verbose_mode,
                current.verbose_mode if current is not None else "off",
            )
            selected_elevated = _selected(
                elevated_mode,
                current.elevated_mode if current is not None else "off",
            )
            selected_queue_mode = _selected(
                queue_mode,
                current.queue_mode if current is not None else "followup",
            )
            selected_queue_limit = _selected(
                queue_limit,
                current.queue_limit if current is not None else 20,
            )
            selected_timezone = _selected(
                timezone,
                current.timezone if current is not None else "UTC",
            )
            _validate_settings(
                thinking_effort=selected_thinking,
                reasoning_visibility=selected_reasoning,
                verbose_mode=selected_verbose,
                elevated_mode=selected_elevated,
                queue_mode=selected_queue_mode,
                queue_limit=selected_queue_limit,
            )
            next_reset = (
                _next_daily_reset(normalized_now, selected_timezone)
                if current is None or reset_schedule
                else current.next_reset_at
            )
            connection.execute(
                """
                INSERT INTO session_settings(
                    room_id, model_override, thinking_effort,
                    reasoning_visibility, verbose_mode, elevated_mode,
                    queue_mode, queue_limit, timezone,
                    next_reset_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(room_id) DO UPDATE SET
                    model_override=excluded.model_override,
                    thinking_effort=excluded.thinking_effort,
                    reasoning_visibility=excluded.reasoning_visibility,
                    verbose_mode=excluded.verbose_mode,
                    elevated_mode=excluded.elevated_mode,
                    queue_mode=excluded.queue_mode,
                    queue_limit=excluded.queue_limit,
                    timezone=excluded.timezone,
                    next_reset_at=excluded.next_reset_at,
                    updated_at=excluded.updated_at
                """,
                (
                    room_id,
                    selected_model,
                    selected_thinking,
                    selected_reasoning,
                    selected_verbose,
                    selected_elevated,
                    selected_queue_mode,
                    selected_queue_limit,
                    selected_timezone,
                    next_reset.isoformat(),
                    normalized_now.isoformat(),
                ),
            )
            return SessionSettings(
                room_id=room_id,
                model_override=selected_model,
                thinking_effort=selected_thinking,
                reasoning_visibility=selected_reasoning,
                verbose_mode=selected_verbose,
                elevated_mode=selected_elevated,
                queue_mode=selected_queue_mode,
                queue_limit=selected_queue_limit,
                timezone=selected_timezone,
                next_reset_at=next_reset,
                updated_at=normalized_now,
            )

        return await self._database.write(write)

    async def due_for_reset(
        self,
        now: datetime,
    ) -> tuple[SessionSettings, ...]:
        # 逻辑说明：把输入时间规范到 UTC 后查询到期房间，按 reset 时间和 room ID 稳定排序；这里只识别候选，不执行会话删除。
        normalized_now = now.astimezone(UTC)

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[SessionSettings, ...]:
            # 逻辑说明：在同一读快照筛选已到每日边界的设置并批量转换。
            rows = connection.execute(
                """
                SELECT * FROM session_settings
                WHERE next_reset_at <= ?
                ORDER BY next_reset_at, room_id
                """,
                (normalized_now.isoformat(),),
            ).fetchall()
            return tuple(_settings_from_row(row) for row in rows)

        return await self._database.read(read)

    async def advance_reset(
        self,
        room_id: str,
        *,
        now: datetime,
    ) -> SessionSettings:
        # 逻辑说明：读取或创建当前设置，再保留模型与时区调用 configure 重算下一次边界；两个 await 之间若失败，旧边界仍保留以便稍后重试。
        current = await self.settings(room_id, now=now)
        return await self.configure(
            room_id,
            model_override=current.model_override,
            timezone=current.timezone,
            now=now,
        )

    async def _read_settings(
        self,
        room_id: str,
    ) -> SessionSettings | None:
        # 逻辑说明：在只读事务按 room ID 获取设置；不存在返回 None，供 settings 决定是否初始化默认值。
        def read(
            connection: sqlite3.Connection,
        ) -> SessionSettings | None:
            # 逻辑说明：执行单行参数化查询并统一反序列化时间和控制项字段。
            row = connection.execute(
                "SELECT * FROM session_settings WHERE room_id=?",
                (room_id,),
            ).fetchone()
            return _settings_from_row(row) if row is not None else None

        return await self._database.read(read)


def _settings_from_row(row: sqlite3.Row) -> SessionSettings:
    # 逻辑说明：把数据库行转换为会话设置对象，并把 ISO 时间恢复为 datetime；字段或时间损坏时主动失败而非套用默认值。
    return SessionSettings(
        room_id=row["room_id"],
        model_override=row["model_override"],
        thinking_effort=row["thinking_effort"],
        reasoning_visibility=row["reasoning_visibility"],
        verbose_mode=row["verbose_mode"],
        elevated_mode=row["elevated_mode"],
        queue_mode=row["queue_mode"],
        queue_limit=row["queue_limit"],
        timezone=row["timezone"],
        next_reset_at=datetime.fromisoformat(row["next_reset_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def _next_daily_reset(now: datetime, timezone: str) -> datetime:
    # 逻辑说明：解析 IANA 时区，在当地时间计算下一次凌晨 4 点并转换回 UTC；未知时区明确报错，已过今日边界则顺延一天。
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"unknown session timezone {timezone!r}") from error
    local_now = now.astimezone(zone)
    candidate = datetime.combine(
        local_now.date(),
        time(hour=4),
        tzinfo=zone,
    )
    if candidate <= local_now:
        candidate += timedelta(days=1)
    return candidate.astimezone(UTC)


def _selected(value: T | _Unset, current: T) -> T:
    return current if isinstance(value, _Unset) else value


def _validate_settings(
    *,
    thinking_effort: str | None,
    reasoning_visibility: str,
    verbose_mode: str,
    elevated_mode: str,
    queue_mode: str,
    queue_limit: int,
) -> None:
    # 逻辑说明：集中校验 thinking、reasoning、verbose、elevated、queue 枚举和队列范围；发现非法值立即报错，使写事务在落库前整体失败。
    if (
        thinking_effort is not None
        and thinking_effort not in THINKING_EFFORTS
    ):
        raise ValueError(f"unsupported thinking effort {thinking_effort!r}")
    if reasoning_visibility not in REASONING_VISIBILITIES:
        raise ValueError(
            f"unsupported reasoning visibility {reasoning_visibility!r}",
        )
    if verbose_mode not in VERBOSE_MODES:
        raise ValueError(f"unsupported verbose mode {verbose_mode!r}")
    if elevated_mode not in ELEVATED_MODES:
        raise ValueError(f"unsupported elevated mode {elevated_mode!r}")
    if queue_mode not in QUEUE_MODES:
        raise ValueError(f"unsupported queue mode {queue_mode!r}")
    if not 1 <= queue_limit <= 100:
        raise ValueError("queue limit must be between 1 and 100")
