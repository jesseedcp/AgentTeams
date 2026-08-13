"""Validated Matrix media download, decryption, and upload.

在大小和类型边界内下载、解密与上传 Matrix 媒体。

媒体事件只提供 MXC 地址和元数据。本模块先限制数量与声明大小，再下载并校验真实字节；
加密附件还要验证密钥和哈希。经过验证的媒体才会变成 AgentScope block，避免一个伪造
附件耗尽内存或把未经校验的内容送进模型。
"""

from __future__ import annotations

import asyncio
import base64
import mimetypes
from pathlib import Path
from typing import Any

from agentscope.message import Base64Source, DataBlock
from nio.crypto.attachments import decrypt_attachment

from agentteams_manager.domain.models import InboundEvent, MediaReference

MAX_DECODED_MEDIA_BYTES = 10 * 1024 * 1024


class MediaError(RuntimeError):
    """Base error for rejected or failed Matrix media operations."""


class MediaTooLargeError(MediaError):
    """Decoded media exceeds the Manager's bounded input limit."""


class MediaValidationError(MediaError):
    """Matrix media metadata or response data is invalid."""


class MediaAdapter:
    """Convert Matrix content repository objects to AgentScope blocks."""

    def __init__(
        self,
        nio_client: Any,
        *,
        max_decoded_bytes: int = MAX_DECODED_MEDIA_BYTES,
    ) -> None:
        self._client = nio_client
        self._max_decoded_bytes = max_decoded_bytes

    async def download(
        self,
        source: InboundEvent | MediaReference,
    ) -> tuple[DataBlock, ...]:
        references = (
            source.media
            if isinstance(source, InboundEvent)
            else (source,)
        )
        blocks: list[DataBlock] = []
        for reference in references:
            blocks.append(await self._download_one(reference))
        return tuple(blocks)

    async def _download_one(self, reference: MediaReference) -> DataBlock:
        if (
            reference.size is not None
            and reference.size > self._max_decoded_bytes
        ):
            raise MediaTooLargeError("decoded media exceeds 10 MiB limit")
        if not reference.mxc_uri.startswith("mxc://"):
            raise MediaValidationError("media URI must use mxc://")
        response = await self._client.download(mxc=reference.mxc_uri)
        body = getattr(response, "body", None)
        if isinstance(body, bytes):
            data = body
        elif isinstance(body, bytearray):
            data = bytes(body)
        elif isinstance(body, (str, Path)):
            data = await asyncio.to_thread(Path(body).read_bytes)
        else:
            raise MediaValidationError("Matrix media download has no body")

        encrypted = any(
            (
                reference.encryption_key,
                reference.encryption_hash,
                reference.encryption_iv,
            ),
        )
        if encrypted:
            if not all(
                (
                    reference.encryption_key,
                    reference.encryption_hash,
                    reference.encryption_iv,
                ),
            ):
                raise MediaValidationError(
                    "encrypted media metadata is incomplete",
                )
            data = decrypt_attachment(
                data,
                reference.encryption_key,
                reference.encryption_hash,
                reference.encryption_iv,
            )

        if len(data) > self._max_decoded_bytes:
            raise MediaTooLargeError("decoded media exceeds 10 MiB limit")
        media_type = reference.media_type
        if not media_type or "/" not in media_type:
            raise MediaValidationError("invalid media type")
        return DataBlock(
            source=Base64Source(
                data=base64.b64encode(data).decode("ascii"),
                media_type=media_type,
            ),
            name=reference.filename or getattr(response, "filename", None),
        )

    async def upload(self, path: Path) -> str:
        path = path.resolve()
        if not path.is_file():
            raise MediaValidationError(f"media file does not exist: {path}")
        size = path.stat().st_size
        media_type = (
            mimetypes.guess_type(path.name)[0]
            or "application/octet-stream"
        )
        response, _encryption = await self._client.upload(
            data_provider=lambda _rate_limits, _timeouts: path,
            content_type=media_type,
            filename=path.name,
            filesize=size,
        )
        uri = getattr(response, "content_uri", None)
        if not isinstance(uri, str) or not uri.startswith("mxc://"):
            raise MediaValidationError("Matrix upload returned no mxc URI")
        return uri
