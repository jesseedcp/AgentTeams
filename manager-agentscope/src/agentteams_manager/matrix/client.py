"""Matrix client ownership and transport boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from nio import AsyncClient
from pydantic import SecretStr

from agentteams_manager.config import ManagerConfig
from agentteams_manager.domain.models import InboundEvent

InboundHandler = Callable[[InboundEvent], Awaitable[None]]


class MatrixState(Protocol):
    """Small durable state surface needed by Matrix transport."""

    async def get_value(self, key: str) -> str | None: ...

    async def set_value(self, key: str, value: str) -> None: ...

    async def claim_matrix_event(
        self,
        room_id: str,
        event_id: str,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class MatrixClientConfig:
    """Validated values required by the Matrix adapter."""

    homeserver: str
    user_id: str
    access_token: SecretStr
    device_name: str
    crypto_store: Path
    media_dir: Path
    sync_timeout_ms: int = 30_000
    history_limit: int = 50
    encryption: bool = True
    vision_enabled: bool = True
    mention_pill_in_body: bool = False
    outbound_structured_mentions: bool = True

    @classmethod
    def from_manager_config(
        cls,
        config: ManagerConfig,
    ) -> MatrixClientConfig:
        return cls(
            homeserver=config.matrix_url,
            user_id=config.manager_user_id,
            access_token=config.matrix_access_token,
            device_name="agentteams-manager",
            crypto_store=config.workspace / "matrix-e2ee",
            media_dir=config.workspace / "media",
        )


class MatrixClient:
    """Own the sole ``nio.AsyncClient`` instance used by the Manager.

    Construction is side-effect free. Network activity starts only from
    :meth:`start`, which makes the class straightforward to test with an
    injected nio-compatible client.
    """

    def __init__(
        self,
        config: MatrixClientConfig,
        state: MatrixState,
        *,
        nio_client: AsyncClient | Any | None = None,
    ) -> None:
        self.config = config
        self._state = state
        self._client = nio_client
        self._handler: InboundHandler | None = None
        self._sync_task: asyncio.Task[None] | None = None
        self.ready = asyncio.Event()

    async def start(self, handler: InboundHandler) -> None:
        """Start transport processing.

        Full client construction, authentication, callbacks, and the sync
        loop are implemented at the protocol layer in the next slice.
        """
        self._handler = handler

    async def stop(self) -> None:
        """Stop owned background work and close the injected client."""
        if self._sync_task is not None:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None
        close = getattr(self._client, "close", None)
        if close is not None:
            await close()
        self.ready.clear()

    async def send_text(
        self,
        room_id: str,
        text: str,
        *,
        txn_id: str,
        thread_id: str | None = None,
        mentions: tuple[str, ...] = (),
    ) -> str:
        """Send text through the Matrix port.

        The concrete content mapping is added with the transport contract.
        """
        del room_id, text, txn_id, thread_id, mentions
        raise NotImplementedError("Matrix outbound transport is not ready")
