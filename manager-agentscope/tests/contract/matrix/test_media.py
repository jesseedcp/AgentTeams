from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from nio.crypto.attachments import encrypt_attachment

from agentteams_manager.domain.models import MediaReference
from agentteams_manager.matrix.media import (
    MAX_DECODED_MEDIA_BYTES,
    MediaAdapter,
    MediaTooLargeError,
)


class DownloadNio:
    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies = bodies
        self.downloads: list[str] = []

    async def download(self, *, mxc: str) -> object:
        self.downloads.append(mxc)
        return SimpleNamespace(
            body=self.bodies[mxc],
            content_type="application/octet-stream",
            filename="image.png",
        )


@pytest.mark.asyncio
async def test_plain_and_encrypted_images_have_same_data_block_type() -> None:
    plaintext = b"\x89PNG\r\npayload"
    ciphertext, keys = encrypt_attachment(plaintext)
    nio = DownloadNio(
        {
            "mxc://local/plain": plaintext,
            "mxc://local/encrypted": ciphertext,
        },
    )
    adapter = MediaAdapter(nio)
    plain = MediaReference(
        mxc_uri="mxc://local/plain",
        media_type="image/png",
        filename="plain.png",
    )
    encrypted = MediaReference(
        mxc_uri="mxc://local/encrypted",
        media_type="image/png",
        filename="encrypted.png",
        encryption_key=keys["key"]["k"],
        encryption_hash=keys["hashes"]["sha256"],
        encryption_iv=keys["iv"],
    )

    plain_block = (await adapter.download(plain))[0]
    encrypted_block = (await adapter.download(encrypted))[0]

    assert plain_block.source.media_type == "image/png"
    assert encrypted_block.source.media_type == "image/png"
    assert plain_block.source.data == encrypted_block.source.data


@pytest.mark.asyncio
async def test_decoded_media_limit_rejects_oversized_payload() -> None:
    nio = DownloadNio({})
    adapter = MediaAdapter(nio)
    reference = MediaReference(
        mxc_uri="mxc://local/huge",
        media_type="image/png",
        size=MAX_DECODED_MEDIA_BYTES + 1,
    )

    with pytest.raises(MediaTooLargeError, match="10 MiB"):
        await adapter.download(reference)

    assert nio.downloads == []


@pytest.mark.asyncio
async def test_upload_returns_mxc_uri(tmp_path: Path) -> None:
    path = tmp_path / "report.txt"
    path.write_text("complete", encoding="utf-8")

    class UploadNio:
        async def upload(self, **kwargs: Any) -> tuple[object, None]:
            provider = kwargs["data_provider"]
            assert provider(0, 0) == path
            assert kwargs["filename"] == "report.txt"
            return SimpleNamespace(
                content_uri="mxc://local/report",
            ), None

    adapter = MediaAdapter(UploadNio())

    assert await adapter.upload(path) == "mxc://local/report"
