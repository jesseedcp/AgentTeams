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
        # 逻辑说明：保存 nio 媒体客户端与允许的解码后最大字节数，供后续每个附件统一校验；初始化本身既不读取文件也不访问 homeserver。
        self._client = nio_client
        self._max_decoded_bytes = max_decoded_bytes

    async def download(
        self,
        source: InboundEvent | MediaReference,
    ) -> tuple[DataBlock, ...]:
        # 逻辑说明：若输入是 InboundEvent 就按其 media 顺序逐个下载，否则包装单个引用；只有所有附件都转换成功才返回 DataBlock 元组，任一异常会终止且不返回部分集合。
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
        # 逻辑说明：先拒绝超限声明和非 mxc URI，再从响应字节或文件路径取内容；加密附件须元数据齐全并通过解密校验，真实大小及 MIME 合法后才 Base64 编成 DataBlock。
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
        # 逻辑说明：解析绝对路径并验证普通文件，读取大小、按扩展名推断 MIME 后将 Path 作为 nio data provider 上传；仅返回以 mxc:// 开头的 content_uri，否则抛媒体校验错误。
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
