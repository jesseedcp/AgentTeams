"""Persistent Matrix end-to-end encryption support."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class CryptoStore:
    """Prepare, but never reset, the nio Olm/Megolm store."""

    path: Path

    def prepare(self) -> Path:
        self.path.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            self.path.chmod(0o700)
        return self.path


async def maintain_e2ee(client: Any, *, enabled: bool) -> None:
    """Perform the maintenance normally owned by ``sync_forever``."""
    if not enabled or not getattr(client, "olm", None):
        return
    if getattr(client, "should_upload_keys", False):
        await client.keys_upload()
    if getattr(client, "should_query_keys", False):
        await client.keys_query()
    if getattr(client, "should_claim_keys", False):
        users = client.get_users_for_key_claiming()
        await client.keys_claim(users)
    await client.send_to_device_messages()
