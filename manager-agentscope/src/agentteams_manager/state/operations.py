"""Durable operation and idempotency repositories.

持久化 Operation 状态机、外部效果事件和 Matrix 消费 claim。

每个 mutation 都用稳定 operation ID 从 planned 走向 prepared、dispatched、running，
最终 succeeded/failed 或进入 reconciling。条件 UPDATE 相当于 compare-and-set：只有
预期旧状态匹配才转换，防止并发恢复覆盖新结果。Matrix event claim 也存在这里，保证
同步重放不会再次启动同一 turn。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from agentteams_manager.domain.errors import (
    ConflictError,
    InvalidTransitionError,
    RecoveryError,
)
from agentteams_manager.domain.models import (
    InboundEvent,
    JournalEvent,
    OperationRecord,
    OperationStatus,
)

from .database import Database


def _json(value: object) -> str:
    # 逻辑说明：用固定键序和紧凑分隔符序列化 operation 请求/结果，使同一结构得到稳定文本，便于幂等记录、审计与恢复时比较。
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _operation_from_row(row: sqlite3.Row) -> OperationRecord:
    # 逻辑说明：解析 request/result JSON 与状态枚举，把数据库行恢复为 OperationRecord；损坏数据立即失败，避免错误恢复外部副作用。
    return OperationRecord(
        operation_id=row["operation_id"],
        kind=row["kind"],
        target_key=row["target_key"],
        status=row["status"],
        request=json.loads(row["request_json"]),
        result=json.loads(row["result_json"]),
        retry_count=row["retry_count"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class OperationRepository:
    """用 compare-and-swap 持久化 Operation、事件及消费进度。

    所有状态转换都带 ``expected`` 集合；返回 ``None`` 表示另一个执行者已推进状态，调用方
    必须重新读取，而不能强行覆盖。journal sequence 与 applied sequence 分开记录，允许
    崩溃后找出“已远端记录、尚未本地应用”的缺口。
    """

    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存 operation journal 的事务入口；只有 create/transition 等方法才写状态机，构造阶段不会生成 operation ID 或恢复记录。
        self._database = database

    async def create(self, record: OperationRecord) -> OperationRecord:
        # 逻辑说明：在单一写事务保存完整 planned operation 并返回原记录；重复稳定 ID 由唯一约束拒绝，调用 workflow 再读现有状态对账。
        def write(connection: sqlite3.Connection) -> OperationRecord:
            # 逻辑说明：稳定序列化请求与结果并原子插入所有状态机字段，避免只有意图没有恢复元数据。
            connection.execute(
                """
                INSERT INTO operations(
                    operation_id, kind, target_key, status, request_json,
                    result_json, retry_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.operation_id,
                    record.kind.value,
                    record.target_key,
                    record.status.value,
                    _json(record.request),
                    _json(record.result),
                    record.retry_count,
                    record.created_at.isoformat(),
                    record.updated_at.isoformat(),
                ),
            )
            return record

        return await self._database.write(write)

    async def get(self, operation_id: str) -> OperationRecord | None:
        # 逻辑说明：按 operation ID 查询当前状态快照，不存在返回 None；读取不推进状态机。
        def read(connection: sqlite3.Connection) -> OperationRecord | None:
            # 逻辑说明：执行参数化单行查询并统一反序列化记录。
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            return _operation_from_row(row) if row else None

        return await self._database.read(read)

    async def transition(
        self,
        operation_id: str,
        *,
        expected: set[OperationStatus],
        target: OperationStatus,
        result: dict[str, object] | None = None,
        increment_retry: bool = False,
    ) -> OperationRecord | None:
        """Atomically move an operation when its current status matches."""
        # 逻辑说明：要求非空 expected 集合，在事务中验证当前状态与领域转移规则，再 compare-and-set 更新结果和重试数；并发已推进时返回 None。
        if not expected:
            raise ValueError("expected statuses must not be empty")

        def write(
            connection: sqlite3.Connection,
        ) -> OperationRecord | None:
            # 逻辑说明：读取、合法性检查、条件 UPDATE 和回读都在同一事务；非法边报错，CAS 丢失则不覆盖获胜者状态。
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            current = _operation_from_row(row)
            if current.status not in expected:
                return None
            if not current.can_transition_to(target):
                raise InvalidTransitionError(
                    f"{current.status} cannot transition to {target}",
                )

            updated_at = datetime.now(UTC).isoformat()
            result_json = _json(result if result is not None else current.result)
            retry_delta = 1 if increment_retry else 0
            placeholders = ",".join("?" for _ in expected)
            cursor = connection.execute(
                f"""
                UPDATE operations
                   SET status=?, result_json=?,
                       retry_count=retry_count+?, updated_at=?
                 WHERE operation_id=?
                   AND status IN ({placeholders})
                """,
                (
                    target.value,
                    result_json,
                    retry_delta,
                    updated_at,
                    operation_id,
                    *(status.value for status in sorted(expected)),
                ),
            )
            if cursor.rowcount == 0:
                return None
            changed = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            return _operation_from_row(changed)

        return await self._database.write(write)

    async def list_recoverable(self) -> tuple[OperationRecord, ...]:
        # 逻辑说明：定义所有非终态可恢复状态，查询并按更新时间稳定返回，供启动对账；这里只列举，不自行重试外部效果。
        recoverable = (
            OperationStatus.PREPARED,
            OperationStatus.DISPATCHED,
            OperationStatus.RUNNING,
            OperationStatus.RETRY_WAIT,
            OperationStatus.RECONCILING,
        )

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[OperationRecord, ...]:
            # 逻辑说明：在一致快照用动态占位符筛选可恢复状态并批量反序列化。
            placeholders = ",".join("?" for _ in recoverable)
            rows = connection.execute(
                f"""
                SELECT * FROM operations
                 WHERE status IN ({placeholders})
                 ORDER BY updated_at, operation_id
                """,
                tuple(status.value for status in recoverable),
            ).fetchall()
            return tuple(_operation_from_row(row) for row in rows)

        return await self._database.read(read)

    async def claim_matrix_event(self, room_id: str, event_id: str) -> bool:
        """Legacy claim-once API retained for transport/test compatibility."""
        # 逻辑说明：用房间/事件唯一键尝试插入已完成 claim，返回是否首次取得；重复 Matrix 投递得到 False，不重复启动 turn。

        def write(connection: sqlite3.Connection) -> bool:
            # 逻辑说明：INSERT OR IGNORE 将查重与写入合为一条原子 SQL，影响行数即 claim 结果。
            now = datetime.now(UTC).isoformat()
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO processed_matrix_events(
                    room_id, event_id, processed_at, status,
                    attempt_count, updated_at
                ) VALUES (?, ?, ?, 'completed', 1, ?)
                """,
                (room_id, event_id, now, now),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)

    async def claim_inbound_event(self, event: InboundEvent) -> bool:
        """Durably store an inbound event before admitting it to the router."""
        # 逻辑说明：先在事务中查重，再把完整入站事件以 processing 状态持久化；只有成功落库才返回 True 允许路由，保证崩溃后可恢复。

        def write(connection: sqlite3.Connection) -> bool:
            # 逻辑说明：在同一事务完成存在检查与事件 JSON 插入，重复事件返回 False 不覆盖原尝试计数。
            row = connection.execute(
                """
                SELECT status
                  FROM processed_matrix_events
                 WHERE room_id=? AND event_id=?
                """,
                (event.room_id, event.event_id),
            ).fetchone()
            if row is not None:
                return False
            now = datetime.now(UTC).isoformat()
            connection.execute(
                """
                INSERT INTO processed_matrix_events(
                    room_id, event_id, processed_at, event_json,
                    status, attempt_count, updated_at
                ) VALUES (?, ?, ?, ?, 'processing', 1, ?)
                """,
                (
                    event.room_id,
                    event.event_id,
                    now,
                    event.model_dump_json(),
                    now,
                ),
            )
            return True

        return await self._database.write(write)

    async def complete_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> None:
        """Mark an event terminal only after its handler returns successfully."""
        # 逻辑说明：handler 成功后才把 claim 标为 completed，并清除错误与重试时间；写失败向上抛出，使事件仍可在恢复时对账。

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：以房间/事件键原子写入终态和更新时间，不触碰持久事件正文。
            connection.execute(
                """
                UPDATE processed_matrix_events
                   SET status='completed',
                       last_error=NULL,
                       next_attempt_at=NULL,
                       updated_at=?
                 WHERE room_id=? AND event_id=?
                """,
                (datetime.now(UTC).isoformat(), room_id, event_id),
            )

        await self._database.write(write)

    async def fail_matrix_event(
        self,
        room_id: str,
        event_id: str,
        *,
        error: str,
        max_attempts: int,
    ) -> bool:
        """Persist a retryable failure, returning True for a dead letter."""
        # 逻辑说明：校验最大尝试数，在事务中读取当前次数并选择 retry_wait 或 dead_letter，截断错误文本后持久化并返回是否终止重试。

        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")

        def write(connection: sqlite3.Connection) -> bool:
            # 逻辑说明：读取尝试数、计算终态并更新错误/下一次时间在同一事务；丢失 claim 保守视为 dead letter，避免无身份重试。
            row = connection.execute(
                """
                SELECT attempt_count
                  FROM processed_matrix_events
                 WHERE room_id=? AND event_id=?
                """,
                (room_id, event_id),
            ).fetchone()
            if row is None:
                return True
            dead_letter = int(row["attempt_count"]) >= max_attempts
            now = datetime.now(UTC).isoformat()
            connection.execute(
                """
                UPDATE processed_matrix_events
                   SET status=?,
                       last_error=?,
                       next_attempt_at=?,
                       updated_at=?
                 WHERE room_id=? AND event_id=?
                """,
                (
                    "dead_letter" if dead_letter else "retry_wait",
                    error[:4_000],
                    None if dead_letter else now,
                    now,
                    room_id,
                    event_id,
                ),
            )
            return dead_letter

        return await self._database.write(write)

    async def retry_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> None:
        """Start the next durable delivery attempt."""
        # 逻辑说明：仅将 retry_wait 事件切回 processing、递增尝试并清除等待时间；条件不匹配时不覆盖已完成或取消状态。

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：单条条件 UPDATE 原子取得下一次 delivery 尝试。
            connection.execute(
                """
                UPDATE processed_matrix_events
                   SET status='processing',
                       attempt_count=attempt_count + 1,
                       next_attempt_at=NULL,
                       updated_at=?
                 WHERE room_id=? AND event_id=?
                   AND status='retry_wait'
                """,
                (datetime.now(UTC).isoformat(), room_id, event_id),
            )

        await self._database.write(write)

    async def cancel_matrix_event(
        self,
        room_id: str,
        event_id: str,
        *,
        reason: str,
    ) -> None:
        """Make an intentionally discarded queued event terminal."""
        # 逻辑说明：把主动丢弃的事件标成 cancelled、保存有界原因并清除重试时间；恢复扫描因终态不会再次处理它。

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：在一个事务按事件身份写终态和审计原因。
            connection.execute(
                """
                UPDATE processed_matrix_events
                   SET status='cancelled',
                       last_error=?,
                       next_attempt_at=NULL,
                       updated_at=?
                 WHERE room_id=? AND event_id=?
                """,
                (
                    reason[:4_000],
                    datetime.now(UTC).isoformat(),
                    room_id,
                    event_id,
                ),
            )

        await self._database.write(write)

    async def recoverable_matrix_events(
        self,
        *,
        limit: int = 1_000,
    ) -> tuple[InboundEvent, ...]:
        """Reclaim processing/retry rows left by a prior Manager process."""
        # 逻辑说明：非正 limit 直接返回；否则事务中读取遗留 processing/retry 事件，坏 JSON 转 dead_letter，合法项递增尝试并返回重放队列。

        if limit < 1:
            return ()

        def write(
            connection: sqlite3.Connection,
        ) -> tuple[InboundEvent, ...]:
            # 逻辑说明：解析、坏记录隔离、claim 重置与 attempt 递增在同一事务完成，防止返回了事件却未持久化新的处理权。
            rows = connection.execute(
                """
                SELECT room_id, event_id, event_json
                  FROM processed_matrix_events
                 WHERE status IN ('processing', 'retry_wait')
                   AND event_json <> ''
                 ORDER BY processed_at, room_id, event_id
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
            recovered: list[InboundEvent] = []
            now = datetime.now(UTC).isoformat()
            for row in rows:
                try:
                    event = InboundEvent.model_validate_json(
                        row["event_json"],
                    )
                except (TypeError, ValueError) as exc:
                    connection.execute(
                        """
                        UPDATE processed_matrix_events
                           SET status='dead_letter',
                               last_error=?,
                               updated_at=?
                         WHERE room_id=? AND event_id=?
                        """,
                        (
                            f"invalid durable event: {exc}"[:4_000],
                            now,
                            row["room_id"],
                            row["event_id"],
                        ),
                    )
                    continue
                connection.execute(
                    """
                    UPDATE processed_matrix_events
                       SET status='processing',
                           attempt_count=attempt_count + 1,
                           next_attempt_at=NULL,
                           updated_at=?
                     WHERE room_id=? AND event_id=?
                    """,
                    (now, row["room_id"], row["event_id"]),
                )
                recovered.append(event)
            return tuple(recovered)

        return await self._database.write(write)

    async def get_value(self, key: str) -> str | None:
        """Read a durable process cursor or transport value."""
        # 逻辑说明：在只读事务按 key 获取通用持久游标；不存在返回 None，不隐式创建默认值。
        def read(connection: sqlite3.Connection) -> str | None:
            # 逻辑说明：参数化查询单个值并规范为字符串返回。
            row = connection.execute(
                "SELECT value FROM key_values WHERE key=?",
                (key,),
            ).fetchone()
            return str(row["value"]) if row is not None else None

        return await self._database.read(read)

    async def set_value(self, key: str, value: str) -> None:
        """Atomically create or replace a durable process value."""
        # 逻辑说明：以当前 UTC 时间原子 upsert 通用游标，使重启后读取最新 transport/recovery 值。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：单条 upsert 同步替换 value 与更新时间，失败整体回滚。
            connection.execute(
                """
                INSERT INTO key_values(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, value, datetime.now(UTC).isoformat()),
            )

        await self._database.write(write)

    async def next_sequence(self, operation_id: str) -> int:
        # 逻辑说明：operation ID 不参与全局计数；事务中原子创建或递增 journal_sequence 并返回新整数，确保并发事件序号唯一。
        del operation_id

        def write(connection: sqlite3.Connection) -> int:
            # 逻辑说明：使用 SQLite upsert RETURNING 把读取与递增合成一次原子操作。
            row = connection.execute(
                """
                INSERT INTO key_values(key, value, updated_at)
                VALUES ('journal_sequence', '1', ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=CAST(key_values.value AS INTEGER) + 1,
                    updated_at=excluded.updated_at
                RETURNING CAST(value AS INTEGER)
                """,
                (datetime.now(UTC).isoformat(),),
            ).fetchone()
            return int(row[0])

        return await self._database.write(write)

    async def current_sequence(self) -> int:
        # 逻辑说明：读取已分配的最高 journal sequence，键不存在时返回 0；不推进任何水位。
        def read(connection: sqlite3.Connection) -> int:
            # 逻辑说明：在当前快照读取字符串游标并转换整数。
            row = connection.execute(
                "SELECT value FROM key_values WHERE key='journal_sequence'",
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)

    async def current_applied_sequence(self) -> int:
        """Return the highest journal event fully reflected in local state."""
        # 逻辑说明：读取已完整物化的最高 sequence，缺键返回 0；它与已分配 sequence 分离，用于发现恢复缺口。
        def read(connection: sqlite3.Connection) -> int:
            # 逻辑说明：查询 snapshot-safe 水位并安全转换为整数。
            row = connection.execute(
                """
                SELECT value FROM key_values
                 WHERE key='journal_applied_sequence'
                """,
            ).fetchone()
            return int(row["value"]) if row is not None else 0

        return await self._database.read(read)

    async def mark_event_applied(self, sequence: int) -> None:
        """Advance the snapshot-safe watermark after a state transition."""
        # 逻辑说明：校验正序号，并在事务中先确认本地事件存在再单调推进 applied 水位；缺事件时报恢复错误，避免快照跳过未物化内容。
        if sequence < 1:
            raise ValueError("applied journal sequence must be positive")

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：存在检查与水位推进使用同一事务，确保不会看到事件后在其提交前更新游标。
            row = connection.execute(
                "SELECT 1 FROM operation_events WHERE sequence=?",
                (sequence,),
            ).fetchone()
            if row is None:
                raise RecoveryError(
                    f"cannot apply missing journal event {sequence}",
                )
            _advance_applied_sequence(
                connection,
                sequence=sequence,
                updated_at=datetime.now(UTC).isoformat(),
            )

        await self._database.write(write)

    async def append_event(self, event: JournalEvent) -> None:
        # 逻辑说明：在本地事务追加不可变 operation event 的结构化副本；sequence 冲突由唯一约束拒绝，不覆盖历史。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：稳定序列化 payload 并一次插入事件身份、类型和时间。
            connection.execute(
                """
                INSERT INTO operation_events(
                    operation_id, sequence, event_type,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.operation_id,
                    event.sequence,
                    event.event_type,
                    _json(event.payload),
                    event.created_at.isoformat(),
                ),
            )

        await self._database.write(write)

    async def replay_event(self, event: JournalEvent) -> None:
        """Materialize one immutable remote event into restored SQLite state."""
        # 逻辑说明：解析可能的 operation_started 意图；事务中校验重复 sequence 内容一致、建立或核对 Operation、追加事件、应用结果并单调推进两个水位。
        started = _started_operation(event)

        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：整个重放是一个 SQLite 事务；重复相同事件幂等补齐 outcome/水位，碰撞或缺少 started 意图则回滚并终止恢复。
            duplicate = connection.execute(
                "SELECT * FROM operation_events WHERE sequence=?",
                (event.sequence,),
            ).fetchone()
            if duplicate is not None:
                persisted = JournalEvent(
                    operation_id=duplicate["operation_id"],
                    sequence=duplicate["sequence"],
                    event_type=duplicate["event_type"],
                    payload=json.loads(duplicate["payload_json"]),
                    created_at=duplicate["created_at"],
                )
                if persisted != event:
                    raise RecoveryError(
                        "journal sequence collision at "
                        f"{event.sequence}",
                    )
                _apply_replayed_outcome(connection, event)
                _advance_journal_sequence(connection, event)
                _advance_applied_sequence(
                    connection,
                    sequence=event.sequence,
                    updated_at=event.created_at.isoformat(),
                )
                return

            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?",
                (event.operation_id,),
            ).fetchone()
            if started is not None:
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO operations(
                            operation_id, kind, target_key, status,
                            request_json, result_json, retry_count,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            started.operation_id,
                            started.kind.value,
                            started.target_key,
                            started.status.value,
                            _json(started.request),
                            _json(started.result),
                            started.retry_count,
                            started.created_at.isoformat(),
                            started.updated_at.isoformat(),
                        ),
                    )
                else:
                    existing = _operation_from_row(row)
                    if (
                        existing.kind is not started.kind
                        or existing.target_key != started.target_key
                        or existing.request != started.request
                    ):
                        raise ConflictError(
                            "restored operation identity conflicts with "
                            f"journal event {event.sequence}",
                        )
            elif row is None:
                raise RecoveryError(
                    f"journal event {event.sequence} for "
                    f"{event.operation_id} has no operation_started intent",
                )

            connection.execute(
                """
                INSERT INTO operation_events(
                    operation_id, sequence, event_type,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.operation_id,
                    event.sequence,
                    event.event_type,
                    _json(event.payload),
                    event.created_at.isoformat(),
                ),
            )
            _apply_replayed_outcome(connection, event)
            _advance_journal_sequence(connection, event)
            _advance_applied_sequence(
                connection,
                sequence=event.sequence,
                updated_at=event.created_at.isoformat(),
            )

        await self._database.write(write)

    async def events_for(
        self,
        operation_id: str,
    ) -> tuple[JournalEvent, ...]:
        # 逻辑说明：按 operation ID 读取本地事件并按 sequence 排序，解析 payload 后返回不可变历史；不修改恢复水位。
        def read(connection: sqlite3.Connection) -> tuple[JournalEvent, ...]:
            # 逻辑说明：在一致快照批量构造 JournalEvent，JSON 损坏会显式失败。
            rows = connection.execute(
                """
                SELECT * FROM operation_events
                 WHERE operation_id=?
                 ORDER BY sequence
                """,
                (operation_id,),
            ).fetchall()
            return tuple(
                JournalEvent(
                    operation_id=row["operation_id"],
                    sequence=row["sequence"],
                    event_type=row["event_type"],
                    payload=json.loads(row["payload_json"]),
                    created_at=row["created_at"],
                )
                for row in rows
            )

        return await self._database.read(read)


def _started_operation(event: JournalEvent) -> OperationRecord | None:
    # 逻辑说明：仅处理 operation_started，验证 payload 中存在同 ID 且状态为 planned 的 Operation；不满足即拒绝远端 journal，避免伪造恢复身份。
    if event.event_type != "operation_started":
        return None
    raw = event.payload.get("operation")
    if not isinstance(raw, dict):
        raise RecoveryError(
            f"operation_started event {event.sequence} has no operation",
        )
    operation = OperationRecord.model_validate(raw)
    if operation.operation_id != event.operation_id:
        raise RecoveryError(
            f"operation_started event {event.sequence} ID mismatch",
        )
    if operation.status is not OperationStatus.PLANNED:
        raise RecoveryError(
            f"operation_started event {event.sequence} is not planned",
        )
    return operation


def _advance_journal_sequence(
    connection: sqlite3.Connection,
    event: JournalEvent,
) -> None:
    # 逻辑说明：把 journal_sequence 水位单调推进到 max(当前,event)，事件乱序或重复都不能让水位倒退。
    connection.execute(
        """
        INSERT INTO key_values(key, value, updated_at)
        VALUES ('journal_sequence', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=CAST(
                MAX(
                    CAST(key_values.value AS INTEGER),
                    CAST(excluded.value AS INTEGER)
                )
                AS TEXT
            ),
            updated_at=excluded.updated_at
        """,
        (
            str(event.sequence),
            event.created_at.isoformat(),
        ),
    )


def _advance_applied_sequence(
    connection: sqlite3.Connection,
    *,
    sequence: int,
    updated_at: str,
) -> None:
    # 逻辑说明：把已物化水位原子推进到给定 sequence 的最大值，供安全快照与恢复起点判断。
    connection.execute(
        """
        INSERT INTO key_values(key, value, updated_at)
        VALUES ('journal_applied_sequence', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=CAST(
                MAX(
                    CAST(key_values.value AS INTEGER),
                    CAST(excluded.value AS INTEGER)
                )
                AS TEXT
            ),
            updated_at=excluded.updated_at
        """,
        (str(sequence), updated_at),
    )


def _apply_replayed_outcome(
    connection: sqlite3.Connection,
    event: JournalEvent,
) -> None:
    # 逻辑说明：按不可变事件类型把 Operation 映射到 dispatched/running/succeeded/failed/reconciling，并校验终态不矛盾；未知事件保持状态不变。
    if event.event_type == "operation_started":
        return
    row = connection.execute(
        "SELECT * FROM operations WHERE operation_id=?",
        (event.operation_id,),
    ).fetchone()
    if row is None:
        raise RecoveryError(event.operation_id)
    operation = _operation_from_row(row)
    status = operation.status
    result = operation.result

    if event.event_type == "effect_planned":
        if status in {OperationStatus.PLANNED, OperationStatus.PREPARED}:
            status = OperationStatus.DISPATCHED
    elif event.event_type == "effect_acknowledged":
        if status not in {OperationStatus.SUCCEEDED, OperationStatus.FAILED}:
            status = OperationStatus.RUNNING
            result = _receipt(event)
    elif event.event_type == "effect_succeeded":
        if status is OperationStatus.FAILED:
            raise RecoveryError(
                f"operation {event.operation_id} has conflicting terminal "
                "journal outcomes",
            )
        status = OperationStatus.SUCCEEDED
        result = _receipt(event)
    elif event.event_type == "effect_failed":
        if status is OperationStatus.SUCCEEDED:
            raise RecoveryError(
                f"operation {event.operation_id} has conflicting terminal "
                "journal outcomes",
            )
        status = OperationStatus.FAILED
        result = {
            "effect": event.payload.get("effect"),
            "reason": event.payload.get("reason", ""),
        }
    elif event.event_type == "effect_ambiguous":
        if status not in {OperationStatus.SUCCEEDED, OperationStatus.FAILED}:
            status = OperationStatus.RECONCILING
            result = {
                "effect": event.payload.get("effect"),
                "ambiguous_reason": event.payload.get("reason", ""),
            }
    else:
        return

    connection.execute(
        """
        UPDATE operations
           SET status=?, result_json=?, updated_at=?
         WHERE operation_id=?
        """,
        (
            status.value,
            _json(result),
            event.created_at.isoformat(),
            event.operation_id,
        ),
    )


def _receipt(event: JournalEvent) -> dict[str, object]:
    # 逻辑说明：从事件 payload 提取结构化 receipt 并验证必须是字典；非法远端回执触发恢复错误，不写入 Operation result。
    receipt = event.payload.get("receipt", {})
    if not isinstance(receipt, dict):
        raise RecoveryError(
            f"journal event {event.sequence} has an invalid receipt",
        )
    return receipt
