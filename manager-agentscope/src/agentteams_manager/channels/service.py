"""Durable first-contact and trusted-channel policy.

持久化外部联系人的首次接触状态，并执行可信渠道策略。

一个陌生账号第一次发来消息时，系统必须先记录来源和待审核状态，不能立即把内容当作
管理员指令。管理员确认后，同一稳定联系人才能进入绑定的 Matrix 房间。SQLite 记录
使重启不会忘记信任决定；平台显示名等可变字段不能代替稳定 provider ID 作为身份。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Literal

from agentteams_manager.state.database import Database

from .base import (
    ChannelAdapter,
    ChannelContact,
    ChannelMessage,
    ChannelReceipt,
    ChannelWebhookRequest,
    ChannelWebhookResponse,
    Provider,
)
from .matrix import MatrixChannelEscalation


class ExternalContactRepository:
    def __init__(self, database: Database) -> None:
        # 逻辑说明：联系人信任与 webhook claim 共用 Manager SQLite，重启后仍保留审核和去重结果。
        self._database = database

    async def record_first_contact(
        self,
        *,
        provider: Provider,
        external_user_id: str,
        display_name: str,
        destination_id: str,
        now: datetime,
    ) -> tuple[ChannelContact, bool]:
        # 逻辑说明：事务内创建或更新联系人并保留信任状态，重复首次消息不会把 trusted 降级。
        def write(connection):
            # 逻辑说明：先查组合身份键；已有记录只刷新展示信息，新记录固定从 pending 开始。
            existing = connection.execute(
                """
                SELECT * FROM external_channel_contacts
                 WHERE provider=? AND external_user_id=?
                """,
                (provider, external_user_id),
            ).fetchone()
            if existing is not None:
                connection.execute(
                    """
                    UPDATE external_channel_contacts
                       SET display_name=?, destination_id=?, updated_at=?
                     WHERE provider=? AND external_user_id=?
                    """,
                    (
                        display_name,
                        destination_id,
                        now.isoformat(),
                        provider,
                        external_user_id,
                    ),
                )
                row = connection.execute(
                    """
                    SELECT * FROM external_channel_contacts
                     WHERE provider=? AND external_user_id=?
                    """,
                    (provider, external_user_id),
                ).fetchone()
                return _contact(row), False
            connection.execute(
                """
                INSERT INTO external_channel_contacts(
                  provider, external_user_id, display_name, destination_id,
                  status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    provider,
                    external_user_id,
                    display_name,
                    destination_id,
                    now.isoformat(),
                    now.isoformat(),
                ),
            )
            row = connection.execute(
                """
                SELECT * FROM external_channel_contacts
                 WHERE provider=? AND external_user_id=?
                """,
                (provider, external_user_id),
            ).fetchone()
            return _contact(row), True

        return await self._database.write(write)

    async def list(self) -> tuple[ChannelContact, ...]:
        # 逻辑说明：返回按 primary/更新时间排序的快照，调用方不能据可变显示名推断身份。
        def read(connection):
            # 逻辑说明：在数据库提供的同一只读连接上查询并转换全部行，返回一个一致快照。
            return tuple(
                _contact(row)
                for row in connection.execute(
                    """
                    SELECT * FROM external_channel_contacts
                     ORDER BY is_primary DESC, updated_at DESC
                    """,
                ).fetchall()
            )

        return await self._database.read(read)

    async def get(
        self,
        provider: Provider,
        external_user_id: str,
    ) -> ChannelContact | None:
        # 逻辑说明：provider 与外部 ID 组成身份键，防止不同平台的同名账号发生碰撞。
        def read(connection):
            # 逻辑说明：只以稳定组合身份键查找并转换模型，不用可变显示名作为身份兜底。
            row = connection.execute(
                """
                SELECT * FROM external_channel_contacts
                 WHERE provider=? AND external_user_id=?
                """,
                (provider, external_user_id),
            ).fetchone()
            return _contact(row) if row is not None else None

        return await self._database.read(read)

    async def set_status(
        self,
        provider: Provider,
        external_user_id: str,
        status: Literal["trusted", "blocked"],
        *,
        now: datetime,
    ) -> ChannelContact:
        # 逻辑说明：条件事务更新审核状态；不存在的联系人不会被隐式创建成可信身份。
        def write(connection):
            # 逻辑说明：条件更新和回读处在同一事务；目标缺失会抛错并回滚，而不是隐式新增联系人。
            cursor = connection.execute(
                """
                UPDATE external_channel_contacts
                   SET status=?, approved_at=?, updated_at=?
                 WHERE provider=? AND external_user_id=?
                """,
                (
                    status,
                    now.isoformat() if status == "trusted" else None,
                    now.isoformat(),
                    provider,
                    external_user_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KeyError(
                    f"{provider}/{external_user_id} does not exist",
                )
            row = connection.execute(
                """
                SELECT * FROM external_channel_contacts
                 WHERE provider=? AND external_user_id=?
                """,
                (provider, external_user_id),
            ).fetchone()
            return _contact(row)

        return await self._database.write(write)

    async def set_primary(
        self,
        provider: Provider,
        external_user_id: str,
        *,
        now: datetime,
    ) -> ChannelContact:
        # 逻辑说明：同一事务清除旧 primary 并设置目标，确保全局只有一个主联系人。
        def write(connection):
            # 逻辑说明：可信校验、清除旧 primary、设置新 primary 与回读必须在同一事务完成。
            row = connection.execute(
                """
                SELECT * FROM external_channel_contacts
                 WHERE provider=? AND external_user_id=? AND status='trusted'
                """,
                (provider, external_user_id),
            ).fetchone()
            if row is None:
                raise KeyError("only a trusted contact can be primary")
            connection.execute(
                "UPDATE external_channel_contacts SET is_primary=0",
            )
            connection.execute(
                """
                UPDATE external_channel_contacts
                   SET is_primary=1, updated_at=?
                 WHERE provider=? AND external_user_id=?
                """,
                (now.isoformat(), provider, external_user_id),
            )
            row = connection.execute(
                """
                SELECT * FROM external_channel_contacts
                 WHERE provider=? AND external_user_id=?
                """,
                (provider, external_user_id),
            ).fetchone()
            return _contact(row)

        return await self._database.write(write)

    async def claim_event(
        self,
        *,
        provider: Provider,
        message_id: str,
        now: datetime,
    ) -> bool:
        """Atomically claim a provider event before dispatching it."""
        # 逻辑说明：唯一键插入成功才取得处理权；平台重试返回 False，不再次升级或发送。

        def write(connection):
            # 逻辑说明：依靠唯一键 INSERT OR IGNORE 原子争抢处理权，并发重试只有一个返回 True。
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO external_channel_events(
                  provider, message_id, processed_at
                ) VALUES (?, ?, ?)
                """,
                (provider, message_id, now.isoformat()),
            )
            return cursor.rowcount == 1

        return await self._database.write(write)


class ChannelService:
    """Keep external identities outside privileged AgentScope turns."""

    def __init__(
        self,
        *,
        contacts: ExternalContactRepository,
        adapters: tuple[ChannelAdapter, ...],
        escalation: MatrixChannelEscalation,
    ) -> None:
        # 逻辑说明：把 adapter 建成 provider 索引，并组合持久化仓库与 Matrix 升级器供统一分流。
        self._contacts = contacts
        self._adapters: dict[str, ChannelAdapter] = {
            adapter.provider: adapter for adapter in adapters
        }
        self._escalation = escalation

    @property
    def providers(self) -> frozenset[str]:
        return frozenset(self._adapters)

    async def ingest(
        self,
        provider: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> ChannelReceipt:
        # 逻辑说明：兼容旧入口，将原始 body 解析为标准消息后复用统一信任策略。
        adapter = self._adapters.get(provider)
        if adapter is None:
            raise KeyError(f"channel provider {provider!r} is disabled")
        result = adapter.handle_webhook(
            ChannelWebhookRequest(
                method="POST",
                headers=headers,
                query={},
                body=body,
            ),
        )
        if result.message is None:
            raise ValueError("webhook did not contain a message")
        return await self._ingest_message(result.message)

    async def handle_webhook(
        self,
        provider: str,
        request: ChannelWebhookRequest,
    ) -> ChannelWebhookResponse:
        """Verify, deduplicate, dispatch, then return the native acknowledgement."""
        # 逻辑说明：先由 adapter 验签/解析和 claim；challenge 直接返回，重复消息只回 duplicate。
        adapter = self._adapters.get(provider)
        if adapter is None:
            raise KeyError(f"channel provider {provider!r} is disabled")
        parsed = adapter.handle_webhook(request)
        message = parsed.message
        if message is None:
            return parsed
        claimed = await self._contacts.claim_event(
            provider=message.provider,
            message_id=message.message_id,
            now=datetime.now(UTC),
        )
        receipt = (
            await self._ingest_message(message)
            if claimed
            else ChannelReceipt(
                provider=message.provider,
                external_user_id=message.external_user_id,
                status="duplicate",
            )
        )
        if parsed.body or parsed.status_code == 204:
            return parsed
        return ChannelWebhookResponse(
            status_code=200,
            body=json.dumps(
                receipt.model_dump(mode="json"),
                separators=(",", ":"),
                sort_keys=True,
            ).encode(),
            content_type="application/json",
        )

    async def _ingest_message(
        self,
        message: ChannelMessage,
    ) -> ChannelReceipt:
        # 逻辑说明：记录联系人后按状态分流：blocked 拒绝、pending 只首次升级、trusted 转 Matrix。
        contact, created = await self._contacts.record_first_contact(
            provider=message.provider,
            external_user_id=message.external_user_id,
            display_name=message.display_name,
            destination_id=message.destination_id,
            now=datetime.now(UTC),
        )
        if contact.status == "blocked":
            return ChannelReceipt(
                provider=message.provider,
                external_user_id=message.external_user_id,
                status="blocked",
            )
        if contact.status != "trusted":
            if created:
                await self._escalation.first_contact(message)
            return ChannelReceipt(
                provider=message.provider,
                external_user_id=message.external_user_id,
                status="pending_approval",
            )
        await self._escalation.trusted_message(message)
        return ChannelReceipt(
            provider=message.provider,
            external_user_id=message.external_user_id,
            status="accepted",
        )

    async def list_contacts(self) -> tuple[ChannelContact, ...]:
        # 逻辑说明：service 仅返回联系人快照，不暴露数据库连接或 adapter 凭据。
        return await self._contacts.list()

    async def approve(
        self,
        provider: Provider,
        external_user_id: str,
    ) -> ChannelContact:
        # 逻辑说明：管理员明确批准后才转 trusted，首次消息本身不能完成自我授权。
        return await self._contacts.set_status(
            provider,
            external_user_id,
            "trusted",
            now=datetime.now(UTC),
        )

    async def block(
        self,
        provider: Provider,
        external_user_id: str,
    ) -> ChannelContact:
        # 逻辑说明：保留审计记录并阻止后续进入 Matrix，而不是删除联系人历史。
        return await self._contacts.set_status(
            provider,
            external_user_id,
            "blocked",
            now=datetime.now(UTC),
        )

    async def set_primary(
        self,
        provider: Provider,
        external_user_id: str,
    ) -> ChannelContact:
        # 逻辑说明：委托 repository 原子切换主联系人，避免并发留下两个 primary。
        return await self._contacts.set_primary(
            provider,
            external_user_id,
            now=datetime.now(UTC),
        )

    async def send(
        self,
        provider: Provider,
        external_user_id: str,
        text: str,
    ) -> dict[str, str]:
        # 逻辑说明：仅 trusted 联系人可出站；平台回执被转换为不含凭据的稳定字段。
        contact = await self._contacts.get(provider, external_user_id)
        if contact is None or contact.status != "trusted":
            raise PermissionError("external contact is not trusted")
        adapter = self._adapters.get(provider)
        if adapter is None:
            raise KeyError(f"channel provider {provider!r} is disabled")
        message_id = await adapter.send(contact.destination_id, text)
        return {
            "provider": provider,
            "external_user_id": external_user_id,
            "message_id": message_id,
        }

    async def close(self) -> None:
        # 逻辑说明：逐个关闭 adapter 的共享 HTTP client；生命周期层负责记录关闭异常。
        for adapter in self._adapters.values():
            await adapter.close()


def _contact(row) -> ChannelContact:
    # 逻辑说明：只投影允许持久化的身份字段，SQLite 整数显式转换为布尔值。
    return ChannelContact(
        provider=row["provider"],
        external_user_id=row["external_user_id"],
        display_name=row["display_name"],
        destination_id=row["destination_id"],
        status=row["status"],
        is_primary=bool(row["is_primary"]),
    )
