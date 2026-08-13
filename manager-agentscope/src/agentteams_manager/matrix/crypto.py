"""Persistent Matrix end-to-end encryption support.

维护 Matrix 端到端加密设备状态和密钥存储。

加密房间中的事件只有在设备完成验证并取得会话密钥后才能变成明文。相关 crypto store
必须跨重启保留，否则 Manager 会像一台全新设备一样无法读取历史消息。本模块只处理
传输加密生命周期；解密后的消息仍需经过 sender 与 room policy 授权。
"""

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
        # 逻辑说明：递归创建指定 crypto store 且绝不清空已有密钥，非 Windows 上把目录权限收紧为 0700；文件系统失败直接传播，成功返回同一个 Path。
        self.path.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            self.path.chmod(0o700)
        return self.path


async def maintain_e2ee(client: Any, *, enabled: bool) -> None:
    """Perform the maintenance normally owned by ``sync_forever``."""
    # 逻辑说明：加密关闭或客户端尚无 Olm 状态时立即返回；否则依照 nio 标志上传、查询、按待申领用户领取密钥，最后发送待处理的 to-device 消息，任一步失败即中止并传播。
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
