from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from agentteams_manager.clients.minio import (
    MinioClient,
    ObjectIntegrityError,
    ObjectVersionConflict,
)
from tests.fixtures.fake_s3 import FakeS3


@pytest.mark.asyncio
async def test_put_bytes_returns_verified_receipt() -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    data = b"requirements"

    receipt = await client.put_bytes(
        "shared/tasks/task-1/spec.md",
        data,
        content_type="text/markdown",
    )

    assert receipt.sha256 == hashlib.sha256(data).hexdigest()
    assert receipt.size == len(data)
    assert receipt.version_id == "1"
    assert s3.head(receipt.key).metadata["sha256"] == receipt.sha256


@pytest.mark.asyncio
async def test_version_mismatch_refuses_overwrite() -> None:
    client = MinioClient(FakeS3(), bucket="agentteams")
    first = await client.put_json_if_version(
        "shared/tasks/task-1/meta.json",
        {"status": "assigned"},
        expected_etag=None,
    )

    with pytest.raises(ObjectVersionConflict):
        await client.put_json_if_version(
            first.key,
            {"status": "completed"},
            expected_etag='"wrong"',
        )


@pytest.mark.asyncio
async def test_get_rejects_object_whose_checksum_metadata_is_wrong() -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    receipt = await client.put_bytes(
        "shared/tasks/task-1/spec.md",
        b"trusted",
        content_type="text/markdown",
    )
    s3.objects[receipt.key].data = b"tampered"

    with pytest.raises(ObjectIntegrityError, match="checksum"):
        await client.get_bytes(receipt.key)


@pytest.mark.asyncio
async def test_get_accepts_standard_single_part_object_without_sha256() -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    data = b"published by TeamHarness"
    receipt = await client.put_bytes(
        "teams/manual-qa/shared/tasks/task-1/result.md",
        data,
        content_type="text/markdown",
    )
    s3.objects[receipt.key].metadata.clear()

    assert await client.get_bytes(receipt.key) == data
    legacy_receipt = await client.head(receipt.key)

    assert legacy_receipt is not None
    assert legacy_receipt.sha256 == hashlib.sha256(data).hexdigest()
    assert legacy_receipt.size == len(data)


@pytest.mark.asyncio
async def test_get_rejects_legacy_object_with_wrong_etag() -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    receipt = await client.put_bytes(
        "teams/manual-qa/shared/tasks/task-1/result.md",
        b"published by TeamHarness",
        content_type="text/markdown",
    )
    item = s3.objects[receipt.key]
    item.metadata.clear()
    item.etag = '"' + ("0" * 32) + '"'

    with pytest.raises(ObjectIntegrityError, match="ETag checksum"):
        await client.get_bytes(receipt.key)


@pytest.mark.asyncio
async def test_get_rejects_unverifiable_legacy_multipart_object() -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    receipt = await client.put_bytes(
        "teams/manual-qa/shared/tasks/task-1/result.md",
        b"published by TeamHarness",
        content_type="text/markdown",
    )
    item = s3.objects[receipt.key]
    item.metadata.clear()
    item.etag = '"' + ("0" * 32) + '-2"'

    with pytest.raises(ObjectIntegrityError, match="no verifiable checksum"):
        await client.get_bytes(receipt.key)


@pytest.mark.asyncio
async def test_get_rejects_invalid_sha256_even_when_etag_is_valid() -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    receipt = await client.put_bytes(
        "teams/manual-qa/shared/tasks/task-1/result.md",
        b"published by TeamHarness",
        content_type="text/markdown",
    )
    s3.objects[receipt.key].metadata["sha256"] = "invalid"

    with pytest.raises(ObjectIntegrityError, match="invalid checksum metadata"):
        await client.get_bytes(receipt.key)


@pytest.mark.asyncio
async def test_mirror_down_replaces_only_requested_cache_directory(
    tmp_path: Path,
) -> None:
    client = MinioClient(FakeS3(), bucket="agentteams")
    await client.put_bytes(
        "shared/tasks/task-1/workspace/fresh.txt",
        b"fresh",
        content_type="text/plain",
    )
    destination = tmp_path / "task-1"
    destination.mkdir()
    (destination / "stale.txt").write_text("stale", encoding="utf-8")
    sibling = tmp_path / "keep.txt"
    sibling.write_text("keep", encoding="utf-8")

    receipt = await client.mirror_down(
        "shared/tasks/task-1/",
        destination,
    )

    assert receipt.files == 1
    assert (destination / "workspace" / "fresh.txt").read_bytes() == b"fresh"
    assert not (destination / "stale.txt").exists()
    assert sibling.read_text(encoding="utf-8") == "keep"


@pytest.mark.asyncio
async def test_mirror_up_skips_unchanged_objects(tmp_path: Path) -> None:
    s3 = FakeS3()
    client = MinioClient(s3, bucket="agentteams")
    source = tmp_path / "workspace"
    source.mkdir()
    (source / "result.md").write_text("done", encoding="utf-8")

    first = await client.mirror_up(
        source,
        "shared/tasks/task-1/workspace/",
    )
    second = await client.mirror_up(
        source,
        "shared/tasks/task-1/workspace/",
    )

    assert first.files == 1
    assert first.bytes_transferred == 4
    assert second.files == 1
    assert second.bytes_transferred == 0
    assert s3.puts == ["shared/tasks/task-1/workspace/result.md"]
