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
        def write(connection):
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
        def read(connection):
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
        def read(connection):
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
        def write(connection):
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
        def write(connection):
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

        def write(connection):
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
        return await self._contacts.list()

    async def approve(
        self,
        provider: Provider,
        external_user_id: str,
    ) -> ChannelContact:
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
        for adapter in self._adapters.values():
            await adapter.close()


def _contact(row) -> ChannelContact:
    return ChannelContact(
        provider=row["provider"],
        external_user_id=row["external_user_id"],
        display_name=row["display_name"],
        destination_id=row["destination_id"],
        status=row["status"],
        is_primary=bool(row["is_primary"]),
    )
