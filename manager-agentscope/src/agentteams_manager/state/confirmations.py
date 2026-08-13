"""Durable, cross-room approval requests for AgentScope continuations.

持久化需要管理员批准的 AgentScope continuation。

高风险 tool call 可能源自 Project Room，却必须到 Admin DM 审批。本模块保存原房间、
原事件、结构化 tool call、policy 快照、有效期和决策状态；``/confirm`` 后 session runner
恢复原 continuation，而不是重新向模型提问。状态转换使用条件更新，避免双击确认或两个
管理员并发响应导致同一个操作执行两次。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from pydantic import BaseModel, ConfigDict

from agentteams_manager.domain.errors import ConflictError, NotFoundError
from agentteams_manager.domain.ids import operation_id_for
from agentteams_manager.domain.models import RoomPolicy

from .database import Database


class ConfirmationStatus(StrEnum):
    AWAITING = "awaiting"
    RESOLVING = "resolving"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class ConfirmationRequest(BaseModel):
    """One globally addressable approval and its source continuation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    confirmation_id: str
    source_room_id: str
    source_event_id: str
    source_reply_id: str
    requester_id: str
    tool_calls: tuple[ToolCallBlock, ...]
    source_policy: RoomPolicy
    status: ConfirmationStatus = ConfirmationStatus.AWAITING
    decision: bool | None = None
    resolver_id: str | None = None
    created_at: datetime
    expires_at: datetime
    resolved_at: datetime | None = None


def _from_row(row: sqlite3.Row) -> ConfirmationRequest:
    # 逻辑说明：把 JSON tool calls、policy、枚举、布尔决策与 ISO 时间完整还原为审批请求；解析失败暴露损坏状态，不静默降级为待审批。
    return ConfirmationRequest(
        confirmation_id=row["confirmation_id"],
        source_room_id=row["source_room_id"],
        source_event_id=row["source_event_id"],
        source_reply_id=row["source_reply_id"],
        requester_id=row["requester_id"],
        tool_calls=tuple(
            ToolCallBlock.model_validate(item)
            for item in json.loads(row["tool_calls_json"])
        ),
        source_policy=RoomPolicy.model_validate_json(
            row["source_policy_json"],
        ),
        status=ConfirmationStatus(row["status"]),
        decision=(
            bool(row["decision"])
            if row["decision"] is not None
            else None
        ),
        resolver_id=row["resolver_id"],
        created_at=datetime.fromisoformat(row["created_at"]),
        expires_at=datetime.fromisoformat(row["expires_at"]),
        resolved_at=(
            datetime.fromisoformat(row["resolved_at"])
            if row["resolved_at"]
            else None
        ),
    )


class ConfirmationRepository:
    """用条件事务保存 approval request 的唯一生命周期。

    pending 请求只能由一个 resolver 转入 resolving，之后 complete/cancel/expire 也要求
    预期旧状态匹配。即使 Matrix 重复投递 ``/confirm``，第二次更新也不会再次取得执行权。
    """
    def __init__(self, database: Database) -> None:
        # 逻辑说明：保存统一 SQLite 事务入口，后续审批状态迁移都通过它获得短连接；构造 repository 本身不会查询或修改 pending 请求。
        self._database = database

    async def create(
        self,
        request: ConfirmationRequest,
    ) -> ConfirmationRequest:
        # 逻辑说明：先以 confirmation ID 幂等插入，再回读并核对来源三元组；相同重试返回原请求，不同来源复用同 ID 则报冲突。
        def write(connection: sqlite3.Connection) -> None:
            # 逻辑说明：在数据库写事务调用统一插入器，保证结构化 continuation 与 policy 快照一起落库。
            _insert_request(connection, request)

        await self._database.write(write)
        stored = await self.get(request.confirmation_id)
        if stored is None:
            raise RuntimeError("confirmation insert did not persist")
        if (
            stored.source_room_id,
            stored.source_event_id,
            stored.source_reply_id,
        ) != (
            request.source_room_id,
            request.source_event_id,
            request.source_reply_id,
        ):
            raise ConflictError(
                f"confirmation/{request.confirmation_id} already exists",
            )
        return stored

    async def migrate_legacy_sessions(
        self,
        *,
        admin_room_id: str,
        admin_user_id: str,
        admin_policy: RoomPolicy,
        now: datetime,
        ttl: timedelta,
    ) -> ConfirmationRequest | None:
        """Move the old room-local admin approval and reset other parks."""
        # 逻辑说明：在单一事务扫描旧 session pending confirmation，只迁移 Admin 房间的可解析 continuation，清理其他旧 park；异常旧数据移除标记后保存，避免启动反复卡住。

        def write(
            connection: sqlite3.Connection,
        ) -> ConfirmationRequest | None:
            # 逻辑说明：逐会话解析旧中间上下文、生成稳定审批 ID 并插入新表，同时更新或删除原 session，确保迁移不会留下两份可执行 continuation。
            rows = connection.execute(
                "SELECT * FROM sessions",
            ).fetchall()
            migrated: ConfirmationRequest | None = None
            legacy_key = "agentteams.matrix.pending_confirmation"
            for row in rows:
                state = AgentState.model_validate_json(
                    row["agent_state_json"],
                )
                raw = state.middle_context.pop(legacy_key, None)
                if raw is None:
                    continue
                if row["room_id"] != admin_room_id:
                    connection.execute(
                        "DELETE FROM sessions WHERE room_id=?",
                        (row["room_id"],),
                    )
                    continue
                try:
                    reply_id = str(raw["reply_id"])
                    event_id = str(raw["event_id"])
                    tool_calls = tuple(
                        ToolCallBlock.model_validate(item)
                        for item in raw["tool_calls"]
                    )
                except (KeyError, TypeError, ValueError):
                    connection.execute(
                        """
                        UPDATE sessions SET agent_state_json=?
                         WHERE room_id=?
                        """,
                        (state.model_dump_json(), row["room_id"]),
                    )
                    continue
                migrated = ConfirmationRequest(
                    confirmation_id=operation_id_for(
                        admin_room_id,
                        event_id,
                        reply_id,
                    ),
                    source_room_id=admin_room_id,
                    source_event_id=event_id,
                    source_reply_id=reply_id,
                    requester_id=admin_user_id,
                    tool_calls=tool_calls,
                    source_policy=admin_policy,
                    created_at=now,
                    expires_at=now + ttl,
                )
                _insert_request(connection, migrated)
                connection.execute(
                    """
                    UPDATE sessions SET agent_state_json=?
                     WHERE room_id=?
                    """,
                    (state.model_dump_json(), row["room_id"]),
                )
            return migrated

        return await self._database.write(write)

    async def get(
        self,
        confirmation_id: str,
    ) -> ConfirmationRequest | None:
        # 逻辑说明：按全局 confirmation ID 读取请求并恢复结构化 continuation；不存在返回 None，不改变审批生命周期。
        def read(
            connection: sqlite3.Connection,
        ) -> ConfirmationRequest | None:
            # 逻辑说明：执行参数化单行查询并使用统一行转换器校验持久内容。
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(row) if row is not None else None

        return await self._database.read(read)

    async def pending(self) -> tuple[ConfirmationRequest, ...]:
        # 逻辑说明：查询 awaiting 与 resolving 请求并按创建顺序稳定返回，供恢复与管理界面展示；不在此方法判断过期。
        def read(
            connection: sqlite3.Connection,
        ) -> tuple[ConfirmationRequest, ...]:
            # 逻辑说明：在同一读快照批量转换所有尚未终结的审批记录。
            rows = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE status IN ('awaiting', 'resolving')
                 ORDER BY created_at, confirmation_id
                """,
            ).fetchall()
            return tuple(_from_row(row) for row in rows)

        return await self._database.read(read)

    async def pending_for_room(
        self,
        room_id: str,
    ) -> ConfirmationRequest | None:
        # 逻辑说明：查找某来源房间最早的未终结审批，供 session runner 恢复唯一 continuation；没有则返回 None。
        def read(
            connection: sqlite3.Connection,
        ) -> ConfirmationRequest | None:
            # 逻辑说明：用房间和状态条件选择最早一条记录并统一反序列化。
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE source_room_id=?
                   AND status IN ('awaiting', 'resolving')
                 ORDER BY created_at
                 LIMIT 1
                """,
                (room_id,),
            ).fetchone()
            return _from_row(row) if row is not None else None

        return await self._database.read(read)

    async def begin_resolution(
        self,
        confirmation_id: str,
        *,
        resolver_id: str,
        decision: bool,
        now: datetime,
    ) -> ConfirmationRequest:
        # 逻辑说明：在事务中核对存在、有效期和当前状态，再把 awaiting 原子变为 resolving；相同决定的重复确认返回现状，冲突决定或终态拒绝执行。
        def write(connection: sqlite3.Connection) -> ConfirmationRequest:
            # 逻辑说明：完成读取校验、条件状态迁移和回读，确保并发管理员中只有一个 continuation 获得执行权。
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"confirmation/{confirmation_id} does not exist",
                )
            current = _from_row(row)
            if current.expires_at <= now:
                raise ConflictError(
                    f"confirmation/{confirmation_id} has expired",
                )
            if current.status is ConfirmationStatus.RESOLVING:
                if current.decision is decision:
                    return current
                raise ConflictError(
                    f"confirmation/{confirmation_id} is already resolving",
                )
            if current.status is not ConfirmationStatus.AWAITING:
                raise ConflictError(
                    f"confirmation/{confirmation_id} is "
                    f"{current.status.value}",
                )
            connection.execute(
                """
                UPDATE confirmation_requests
                   SET status='resolving', decision=?, resolver_id=?,
                       resolved_at=?
                 WHERE confirmation_id=?
                """,
                (
                    int(decision),
                    resolver_id,
                    now.isoformat(),
                    confirmation_id,
                ),
            )
            updated = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(updated)

        return await self._database.write(write)

    async def complete(
        self,
        confirmation_id: str,
        *,
        now: datetime,
    ) -> ConfirmationRequest:
        # 逻辑说明：仅把 resolving 请求按已保存 decision 终结为 approved/denied，并在同一事务回读；非 resolving 表示执行权不明确，直接冲突。
        def write(connection: sqlite3.Connection) -> ConfirmationRequest:
            # 逻辑说明：验证解析阶段后写入不可逆终态与完成时间，防止跳过 begin_resolution 直接批准。
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"confirmation/{confirmation_id} does not exist",
                )
            current = _from_row(row)
            if current.status is not ConfirmationStatus.RESOLVING:
                raise ConflictError(
                    f"confirmation/{confirmation_id} is not resolving",
                )
            status = (
                ConfirmationStatus.APPROVED
                if current.decision
                else ConfirmationStatus.DENIED
            )
            connection.execute(
                """
                UPDATE confirmation_requests
                   SET status=?, resolved_at=?
                 WHERE confirmation_id=?
                """,
                (status.value, now.isoformat(), confirmation_id),
            )
            updated = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(updated)

        return await self._database.write(write)

    async def cancel(
        self,
        confirmation_id: str,
        *,
        resolver_id: str,
        now: datetime,
    ) -> ConfirmationRequest:
        # 逻辑说明：委托统一终态 compare-and-set 将未完成审批取消，并记录管理员和时间；重复取消会冲突而不会重新触发 continuation。
        return await self._terminal_update(
            confirmation_id,
            status=ConfirmationStatus.CANCELLED,
            resolver_id=resolver_id,
            now=now,
        )

    async def expire_due(
        self,
        *,
        now: datetime,
    ) -> tuple[ConfirmationRequest, ...]:
        # 逻辑说明：在一个事务选出已过期未终结请求并批量标为 expired，返回对应终态副本；并发完成的记录由状态条件保护，不会被错误覆盖。
        def write(
            connection: sqlite3.Connection,
        ) -> tuple[ConfirmationRequest, ...]:
            # 逻辑说明：查询与条件 UPDATE 使用同一时间界限和事务，保证返回集合与实际过期迁移一致。
            rows = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE status IN ('awaiting', 'resolving')
                   AND expires_at <= ?
                 ORDER BY created_at, confirmation_id
                """,
                (now.isoformat(),),
            ).fetchall()
            if rows:
                connection.execute(
                    """
                    UPDATE confirmation_requests
                       SET status='expired', resolved_at=?
                     WHERE status IN ('awaiting', 'resolving')
                       AND expires_at <= ?
                    """,
                    (now.isoformat(), now.isoformat()),
                )
            return tuple(
                _from_row(row).model_copy(
                    update={
                        "status": ConfirmationStatus.EXPIRED,
                        "resolved_at": now,
                    },
                )
                for row in rows
            )

        return await self._database.write(write)

    async def _terminal_update(
        self,
        confirmation_id: str,
        *,
        status: ConfirmationStatus,
        resolver_id: str,
        now: datetime,
    ) -> ConfirmationRequest:
        # 逻辑说明：统一执行 cancel 等终态迁移，只允许 awaiting/resolving 旧状态，并保存 resolver 与时间；零影响行视为并发冲突。
        def write(connection: sqlite3.Connection) -> ConfirmationRequest:
            # 逻辑说明：用条件 UPDATE 抢占终态并立即回读，使检查与修改原子化。
            cursor = connection.execute(
                """
                UPDATE confirmation_requests
                   SET status=?, resolver_id=?, resolved_at=?
                 WHERE confirmation_id=?
                   AND status IN ('awaiting', 'resolving')
                """,
                (
                    status.value,
                    resolver_id,
                    now.isoformat(),
                    confirmation_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ConflictError(
                    f"confirmation/{confirmation_id} is not pending",
                )
            row = connection.execute(
                """
                SELECT * FROM confirmation_requests
                 WHERE confirmation_id=?
                """,
                (confirmation_id,),
            ).fetchone()
            return _from_row(row)

        return await self._database.write(write)


class ConfirmationService:
    """在 repository 之外统一应用过期、解析与旧会话迁移规则。"""

    def __init__(
        self,
        repository: ConfirmationRepository,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        # 逻辑说明：保存 confirmation repository 与可替换时钟；构造阶段不读写数据库，测试可注入稳定时间验证过期边界。
        self._repository = repository
        self._now = now or (lambda: datetime.now(UTC))

    async def create(
        self,
        request: ConfirmationRequest,
    ) -> ConfirmationRequest:
        # 逻辑说明：把已包含来源 continuation 与 policy 快照的请求交给仓库幂等保存；数据库冲突或失败原样传播，不生成仅存在内存中的审批。
        return await self._repository.create(request)

    async def get(
        self,
        confirmation_id: str,
    ) -> ConfirmationRequest | None:
        # 逻辑说明：按全局审批 ID 读取当前生命周期快照；不存在返回 None，不隐式执行过期或状态迁移。
        return await self._repository.get(confirmation_id)

    async def pending(self) -> tuple[ConfirmationRequest, ...]:
        # 逻辑说明：先持久化所有到期迁移，再返回仓库中的未终结请求，避免调用方看到逻辑上已失效的审批。
        await self.expire_due()
        return await self._repository.pending()

    async def pending_for_room(
        self,
        room_id: str,
    ) -> ConfirmationRequest | None:
        # 逻辑说明：先统一过期，再查询该房间的 pending continuation；两个步骤失败时不伪造可恢复请求。
        await self.expire_due()
        return await self._repository.pending_for_room(room_id)

    async def resolve(
        self,
        confirmation_id: str,
        *,
        admin_id: str,
        decision: bool,
    ) -> ConfirmationRequest:
        # 逻辑说明：先清理过期审批，再以当前管理员、决定和统一时钟原子取得解析权；仓库负责幂等与并发冲突。
        await self.expire_due()
        return await self._repository.begin_resolution(
            confirmation_id,
            resolver_id=admin_id,
            decision=decision,
            now=self._now(),
        )

    async def complete(
        self,
        confirmation_id: str,
    ) -> ConfirmationRequest:
        # 逻辑说明：用统一时钟把已取得 resolving 权的请求终结为 approved/denied；仓库检查状态，重复完成不会再次执行 continuation。
        return await self._repository.complete(
            confirmation_id,
            now=self._now(),
        )

    async def cancel(
        self,
        confirmation_id: str,
        *,
        admin_id: str,
    ) -> ConfirmationRequest:
        # 逻辑说明：用管理员身份和统一时钟取消仍未终结的审批；仓库通过条件更新防止覆盖并发批准或拒绝结果。
        return await self._repository.cancel(
            confirmation_id,
            resolver_id=admin_id,
            now=self._now(),
        )

    async def expire_due(self) -> tuple[ConfirmationRequest, ...]:
        # 逻辑说明：以注入时钟批量迁移所有到期请求并返回刚过期记录，便于上层通知来源房间；仓库保证查询与更新原子。
        return await self._repository.expire_due(now=self._now())

    async def migrate_legacy_sessions(
        self,
        *,
        admin_room_id: str,
        admin_user_id: str,
        admin_policy: RoomPolicy,
        ttl: timedelta,
    ) -> ConfirmationRequest | None:
        # 逻辑说明：补齐当前时间和 TTL 后调用仓库原子迁移旧 room-local park；返回唯一迁移请求或 None，失败则保留事务前状态供下次启动重试。
        return await self._repository.migrate_legacy_sessions(
            admin_room_id=admin_room_id,
            admin_user_id=admin_user_id,
            admin_policy=admin_policy,
            now=self._now(),
            ttl=ttl,
        )


def _insert_request(
    connection: sqlite3.Connection,
    request: ConfirmationRequest,
) -> None:
    # 逻辑说明：稳定序列化 tool calls、policy 与时间，在当前事务按 confirmation ID 插入且冲突不覆盖；调用方随后回读核对是否为同一来源。
    connection.execute(
        """
        INSERT INTO confirmation_requests(
            confirmation_id, source_room_id, source_event_id,
            source_reply_id, requester_id, tool_calls_json,
            source_policy_json, status, decision, resolver_id,
            created_at, expires_at, resolved_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(confirmation_id) DO NOTHING
        """,
        (
            request.confirmation_id,
            request.source_room_id,
            request.source_event_id,
            request.source_reply_id,
            request.requester_id,
            json.dumps(
                [
                    call.model_dump(mode="json")
                    for call in request.tool_calls
                ],
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            request.source_policy.model_dump_json(),
            request.status.value,
            request.decision,
            request.resolver_id,
            request.created_at.isoformat(),
            request.expires_at.isoformat(),
            (
                request.resolved_at.isoformat()
                if request.resolved_at
                else None
            ),
        ),
    )
