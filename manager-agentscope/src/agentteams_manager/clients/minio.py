"""Verified, version-aware access to AgentTeams' S3-compatible storage."""

from __future__ import annotations

import hashlib
import inspect
import json
import shutil
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any, Self

from agentteams_manager.domain.models import MirrorReceipt, ObjectReceipt


class MinioError(RuntimeError):
    """Base error for object-storage operations."""


class ObjectNotFound(MinioError):
    """The requested object does not exist."""


class ObjectIntegrityError(MinioError):
    """The stored bytes do not match their recorded digest or length."""


class ObjectVersionConflict(MinioError):
    """An object changed between observation and mutation."""


class MinioClient:
    """Small typed wrapper around the async S3 API used by AgentTeams.

    Every upload records a SHA-256 digest and is read back through ``HEAD``.
    Conditional methods expose S3's compare-and-swap behavior so workflow
    state cannot be silently overwritten by another Manager process.
    """

    def __init__(
        self,
        s3: Any,
        *,
        bucket: str,
        _client_context: Any | None = None,
    ) -> None:
        if not bucket.strip():
            raise ValueError("bucket must not be empty")
        self._s3 = s3
        self._bucket = bucket
        self._client_context = _client_context

    @classmethod
    async def connect(
        cls,
        *,
        endpoint_url: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region_name: str = "us-east-1",
    ) -> Self:
        """Open an aioboto3 client without importing it during unit tests."""

        import aioboto3

        context = aioboto3.Session().client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )
        s3 = await context.__aenter__()
        return cls(s3, bucket=bucket, _client_context=context)

    async def close(self) -> None:
        if self._client_context is not None:
            context, self._client_context = self._client_context, None
            await context.__aexit__(None, None, None)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any | None,
    ) -> None:
        await self.close()

    async def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> ObjectReceipt:
        """Upload bytes and verify the persisted object metadata."""

        return await self._put_bytes(
            key,
            data,
            content_type=content_type,
        )

    async def put_bytes_if_version(
        self,
        key: str,
        data: bytes,
        *,
        expected_etag: str | None,
        content_type: str = "application/octet-stream",
    ) -> ObjectReceipt:
        """Create only when absent, or replace only the observed ETag."""

        condition = (
            {"IfNoneMatch": "*"}
            if expected_etag is None
            else {"IfMatch": expected_etag}
        )
        return await self._put_bytes(
            key,
            data,
            content_type=content_type,
            condition=condition,
        )

    async def put_json_if_version(
        self,
        key: str,
        value: Any,
        *,
        expected_etag: str | None,
    ) -> ObjectReceipt:
        try:
            encoded = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("value is not canonical JSON") from exc
        return await self.put_bytes_if_version(
            key,
            encoded,
            expected_etag=expected_etag,
            content_type="application/json",
        )

    async def _put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        condition: Mapping[str, str] | None = None,
    ) -> ObjectReceipt:
        normalized = _object_key(key)
        digest = hashlib.sha256(data).hexdigest()
        request: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": normalized,
            "Body": data,
            "ContentType": content_type,
            "Metadata": {"sha256": digest},
        }
        request.update(condition or {})
        try:
            await self._s3.put_object(**request)
        except Exception as exc:
            if _is_precondition_failure(exc):
                raise ObjectVersionConflict(
                    f"object version conflict: {normalized}",
                ) from exc
            raise MinioError(f"failed to upload {normalized}") from exc

        receipt = await self.head(normalized)
        if receipt is None:
            raise ObjectIntegrityError(
                f"uploaded object disappeared before verification: {normalized}",
            )
        if receipt.sha256 != digest or receipt.size != len(data):
            raise ObjectIntegrityError(
                f"uploaded object failed checksum verification: {normalized}",
            )
        return receipt

    async def head(self, key: str) -> ObjectReceipt | None:
        normalized = _object_key(key)
        try:
            response = await self._s3.head_object(
                Bucket=self._bucket,
                Key=normalized,
            )
        except Exception as exc:
            if _is_missing(exc):
                return None
            raise MinioError(f"failed to inspect {normalized}") from exc
        return _receipt(normalized, response)

    async def get_bytes(self, key: str) -> bytes:
        normalized = _object_key(key)
        try:
            response = await self._s3.get_object(
                Bucket=self._bucket,
                Key=normalized,
            )
        except Exception as exc:
            if _is_missing(exc):
                raise ObjectNotFound(normalized) from exc
            raise MinioError(f"failed to download {normalized}") from exc

        body = response["Body"]
        try:
            data = await body.read()
        finally:
            close = getattr(body, "close", None)
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    await result

        receipt = _receipt(normalized, response)
        digest = hashlib.sha256(data).hexdigest()
        if receipt.sha256 != digest or receipt.size != len(data):
            raise ObjectIntegrityError(
                f"object checksum or length mismatch: {normalized}",
            )
        return data

    async def get_json(self, key: str) -> Any:
        data = await self.get_bytes(key)
        try:
            return json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObjectIntegrityError(f"object is not valid JSON: {key}") from exc

    async def list_prefix(self, prefix: str) -> tuple[ObjectReceipt, ...]:
        normalized = _object_prefix(prefix)
        continuation: str | None = None
        receipts: list[ObjectReceipt] = []
        while True:
            request: dict[str, Any] = {
                "Bucket": self._bucket,
                "Prefix": normalized,
            }
            if continuation is not None:
                request["ContinuationToken"] = continuation
            try:
                response = await self._s3.list_objects_v2(**request)
            except Exception as exc:
                raise MinioError(f"failed to list {normalized}") from exc
            for item in response.get("Contents", ()):
                key = _object_key(str(item["Key"]))
                head = await self.head(key)
                if head is not None:
                    receipts.append(head)
            if not response.get("IsTruncated"):
                break
            continuation = response.get("NextContinuationToken")
            if not continuation:
                raise MinioError("truncated object listing omitted continuation")
        return tuple(receipts)

    async def delete_if_version(
        self,
        key: str,
        *,
        expected_etag: str,
    ) -> None:
        normalized = _object_key(key)
        try:
            await self._s3.delete_object(
                Bucket=self._bucket,
                Key=normalized,
                IfMatch=expected_etag,
            )
        except Exception as exc:
            if _is_precondition_failure(exc) or _is_missing(exc):
                raise ObjectVersionConflict(
                    f"object version conflict: {normalized}",
                ) from exc
            raise MinioError(f"failed to delete {normalized}") from exc

    async def mirror_down(
        self,
        prefix: str,
        destination: Path,
    ) -> MirrorReceipt:
        """Download a prefix, then atomically replace only its cache root."""

        normalized_prefix = _object_prefix(prefix)
        destination = destination.resolve()
        if destination.is_symlink():
            raise ValueError("destination must not be a symlink")
        destination.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(
            tempfile.mkdtemp(
                prefix=f".{destination.name}.stage-",
                dir=destination.parent,
            ),
        )
        backup: Path | None = None
        manifest: list[dict[str, Any]] = []
        transferred = 0
        try:
            for receipt in await self.list_prefix(normalized_prefix):
                relative = receipt.key[len(normalized_prefix) :]
                relative_key = _object_key(relative)
                target = stage.joinpath(*PurePosixPath(relative_key).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                data = await self.get_bytes(receipt.key)
                target.write_bytes(data)
                transferred += len(data)
                manifest.append(
                    {
                        "key": receipt.key,
                        "sha256": receipt.sha256,
                        "size": receipt.size,
                    },
                )

            if destination.exists():
                backup = destination.with_name(
                    f".{destination.name}.backup-"
                    f"{uuid.uuid4().hex}",
                )
                destination.replace(backup)
            stage.replace(destination)
            if backup is not None:
                shutil.rmtree(backup)
                backup = None
        except Exception:
            if destination.exists() and destination != stage:
                # A fully installed destination is left intact. A stage that
                # failed before installation is removed below.
                pass
            elif backup is not None and backup.exists():
                backup.replace(destination)
                backup = None
            raise
        finally:
            if stage.exists():
                shutil.rmtree(stage)
            if backup is not None and backup.exists():
                if not destination.exists():
                    backup.replace(destination)
                else:
                    shutil.rmtree(backup)

        return MirrorReceipt(
            prefix=normalized_prefix,
            files=len(manifest),
            bytes_transferred=transferred,
            manifest_sha256=_manifest_digest(manifest),
        )

    async def mirror_up(
        self,
        source: Path,
        prefix: str,
    ) -> MirrorReceipt:
        """Upload changed files using compare-and-swap per object."""

        source = source.resolve(strict=True)
        if not source.is_dir():
            raise ValueError("source must be a directory")
        normalized_prefix = _object_prefix(prefix)
        manifest: list[dict[str, Any]] = []
        transferred = 0
        for path in sorted(source.rglob("*")):
            if path.is_symlink():
                resolved = path.resolve(strict=True)
                if not resolved.is_relative_to(source):
                    raise ValueError(f"symlink escapes source directory: {path}")
            if not path.is_file():
                continue
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(source):
                raise ValueError(f"file escapes source directory: {path}")
            relative = path.relative_to(source).as_posix()
            key = _object_key(f"{normalized_prefix}{relative}")
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            existing = await self.head(key)
            if (
                existing is None
                or existing.sha256 != digest
                or existing.size != len(data)
            ):
                receipt = await self.put_bytes_if_version(
                    key,
                    data,
                    expected_etag=(
                        existing.etag if existing is not None else None
                    ),
                    content_type=_content_type(path),
                )
                transferred += len(data)
            else:
                receipt = existing
            manifest.append(
                {
                    "key": key,
                    "sha256": receipt.sha256,
                    "size": receipt.size,
                },
            )

        return MirrorReceipt(
            prefix=normalized_prefix,
            files=len(manifest),
            bytes_transferred=transferred,
            manifest_sha256=_manifest_digest(manifest),
        )


def _object_key(key: str) -> str:
    if not key or key.startswith("/") or key.endswith("/") or "\\" in key:
        raise ValueError(f"invalid object key: {key!r}")
    if any(ord(character) < 32 for character in key):
        raise ValueError("object key contains control characters")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"invalid object key: {key!r}")
    normalized = PurePosixPath(key).as_posix()
    if normalized != key:
        raise ValueError(f"object key is not canonical: {key!r}")
    return normalized


def _object_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    raw = prefix[:-1] if prefix.endswith("/") else prefix
    normalized = _object_key(raw)
    return f"{normalized}/"


def _receipt(key: str, response: Mapping[str, Any]) -> ObjectReceipt:
    metadata = {
        str(name).lower(): str(value)
        for name, value in dict(response.get("Metadata", {})).items()
    }
    digest = metadata.get("sha256", "")
    if len(digest) != 64:
        raise ObjectIntegrityError(
            f"object has no valid checksum metadata: {key}",
        )
    try:
        int(digest, 16)
    except ValueError as exc:
        raise ObjectIntegrityError(
            f"object has invalid checksum metadata: {key}",
        ) from exc
    return ObjectReceipt(
        key=key,
        etag=str(response.get("ETag", "")),
        sha256=digest,
        size=int(response.get("ContentLength", 0)),
        content_type=(
            str(response["ContentType"])
            if response.get("ContentType") is not None
            else None
        ),
        version_id=(
            str(response["VersionId"])
            if response.get("VersionId") is not None
            else None
        ),
    )


def _error_details(exc: Exception) -> tuple[str, int | None]:
    response = getattr(exc, "response", {})
    if not isinstance(response, Mapping):
        return "", None
    error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    code = str(error.get("Code", "")) if isinstance(error, Mapping) else ""
    status = (
        int(metadata["HTTPStatusCode"])
        if isinstance(metadata, Mapping)
        and metadata.get("HTTPStatusCode") is not None
        else None
    )
    return code, status


def _is_missing(exc: Exception) -> bool:
    code, status = _error_details(exc)
    return status == 404 or code in {"404", "NoSuchKey", "NotFound"}


def _is_precondition_failure(exc: Exception) -> bool:
    code, status = _error_details(exc)
    return status == 412 or code in {
        "412",
        "ConditionalRequestConflict",
        "PreconditionFailed",
    }


def _manifest_digest(entries: list[dict[str, Any]]) -> str:
    encoded = json.dumps(
        entries,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _content_type(path: Path) -> str:
    suffixes = {
        ".json": "application/json",
        ".md": "text/markdown",
        ".txt": "text/plain",
        ".yaml": "application/yaml",
        ".yml": "application/yaml",
    }
    return suffixes.get(path.suffix.lower(), "application/octet-stream")
