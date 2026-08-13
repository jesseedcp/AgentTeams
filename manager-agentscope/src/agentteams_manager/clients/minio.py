"""Verified, version-aware access to AgentTeams' S3-compatible storage.

以校验和与版本条件访问 AgentTeams 的 MinIO/S3 对象存储。

任务文件、远端 journal 和数据库快照需要跨 Pod 保存。本模块在上传和下载时验证大小、
SHA-256 与对象版本，并使用条件写入防止两个请求静默覆盖彼此。它返回稳定回执而不是
底层 SDK 对象；版本冲突和对象不存在会成为明确错误，供 workflow 决定恢复方式。
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
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
        # 逻辑说明：拒绝空 bucket，并记录 S3 client 及其可选上下文所有权，决定关闭责任。
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
        # 逻辑说明：运行期才导入 aioboto3、进入异步 client 上下文，并把上下文交给实例在 close 时退出。

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
        # 逻辑说明：若 client 由 connect 创建，只退出一次并先清空引用，使重复 close 仍安全。
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
        # 逻辑说明：异步上下文退出统一委托 close，无论调用体成功还是异常都释放自有 S3 client。
        await self.close()

    async def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> ObjectReceipt:
        """Upload bytes and verify the persisted object metadata."""
        # 逻辑说明：无条件写入口复用带摘要、HEAD 回读与完整性验证的内部实现。

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
        # 逻辑说明：把“预期不存在/预期版本”翻译成 S3 条件头，冲突由内部实现转换为领域异常。

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
        # 逻辑说明：先稳定序列化为规范 JSON，再按期望 ETag 条件写入，序列化失败不会触发远程副作用。
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
        # 逻辑说明：规范化 key、计算 SHA-256 并携元数据上传；条件冲突单独分类，随后 HEAD 回读校验。
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
        # 逻辑说明：读取元数据并优先使用记录的 SHA-256；旧对象无摘要时下载一次并以可验证 ETag 兜底。
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
        recorded_digest = _recorded_sha256(normalized, response)
        if recorded_digest is not None:
            return _receipt(
                normalized,
                response,
                sha256=recorded_digest,
            )

        # TeamHarness and other S3-compatible clients may upload objects
        # without AgentTeams' custom ``x-amz-meta-sha256`` field. Download
        # those legacy objects once so their standard single-part ETag can be
        # verified before exposing a SHA-256 receipt to the rest of Manager.
        data = await self.get_bytes(normalized)
        return _receipt(
            normalized,
            response,
            sha256=hashlib.sha256(data).hexdigest(),
        )

    async def get_bytes(self, key: str) -> bytes:
        # 逻辑说明：下载后始终关闭流，并校验长度及 SHA-256/单段 ETag，损坏内容绝不交给调用方。
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

        digest = hashlib.sha256(data).hexdigest()
        recorded_digest = _recorded_sha256(normalized, response)
        expected_size = int(response.get("ContentLength", 0))
        if expected_size != len(data):
            raise ObjectIntegrityError(
                f"object checksum or length mismatch: {normalized}",
            )
        if recorded_digest is not None:
            if recorded_digest != digest:
                raise ObjectIntegrityError(
                    f"object checksum or length mismatch: {normalized}",
                )
        else:
            _verify_single_part_etag(normalized, response, data)
        return data

    async def get_json(self, key: str) -> Any:
        # 逻辑说明：先走完整字节校验，再解析 JSON；编码或语法错误统一归类为对象完整性问题。
        data = await self.get_bytes(key)
        try:
            return json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObjectIntegrityError(f"object is not valid JSON: {key}") from exc

    async def list_prefix(self, prefix: str) -> tuple[ObjectReceipt, ...]:
        # 逻辑说明：规范化 prefix、遍历所有分页并逐对象 HEAD 验证；截断响应缺 token 时明确失败。
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
        # 逻辑说明：用 If-Match 只删除调用方观察过的版本，不存在或版本变化都报告冲突而非静默成功。
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
        # 逻辑说明：先在同级临时目录完整下载并建清单，再原子替换目标；任一步失败恢复备份并清理 stage。

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
                        "etag": receipt.etag,
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
        # 逻辑说明：限定文件仍在 source 内，按摘要跳过未变项；变更项以观测 ETag CAS 上传并汇总清单。

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
                    "etag": receipt.etag,
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
    # 逻辑说明：拒绝绝对路径、反斜杠、控制字符和点段，并要求 POSIX 规范化前后完全一致。
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
    # 逻辑说明：空前缀表示全 bucket；非空前缀先按对象 key 校验再统一补一个尾斜杠。
    if not prefix:
        return ""
    raw = prefix[:-1] if prefix.endswith("/") else prefix
    normalized = _object_key(raw)
    return f"{normalized}/"


def _recorded_sha256(
    key: str,
    response: Mapping[str, Any],
) -> str | None:
    # 逻辑说明：大小写无关读取 sha256 元数据，并验证它是严格 64 位十六进制；缺失返回 None 触发旧对象路径。
    metadata = {
        str(name).lower(): str(value)
        for name, value in dict(response.get("Metadata", {})).items()
    }
    if "sha256" not in metadata:
        return None
    digest = metadata["sha256"]
    if len(digest) != 64:
        raise ObjectIntegrityError(
            f"object has invalid checksum metadata: {key}",
        )
    try:
        int(digest, 16)
    except ValueError as exc:
        raise ObjectIntegrityError(
            f"object has invalid checksum metadata: {key}",
        ) from exc
    return digest.lower()


def _verify_single_part_etag(
    key: str,
    response: Mapping[str, Any],
    data: bytes,
) -> None:
    # 逻辑说明：仅接受可验证的单段 MD5 ETag，并与实际数据比较；多段/非标准 ETag 不被错误信任。
    etag = str(response.get("ETag", "")).strip().strip('"')
    if re.fullmatch(r"[0-9a-fA-F]{32}", etag) is None:
        raise ObjectIntegrityError(
            f"object has no verifiable checksum metadata or ETag: {key}",
        )
    if hashlib.md5(data).hexdigest() != etag.lower():
        raise ObjectIntegrityError(
            f"object ETag checksum mismatch: {key}",
        )


def _receipt(
    key: str,
    response: Mapping[str, Any],
    *,
    sha256: str,
) -> ObjectReceipt:
    # 逻辑说明：只从已验证响应提取稳定字段并规范化可选值，隔离底层 SDK 响应结构。
    return ObjectReceipt(
        key=key,
        etag=str(response.get("ETag", "")),
        sha256=sha256,
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
    # 逻辑说明：防御性解析 boto 风格异常中的 code/status，未知异常返回空值供上层走通用错误路径。
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
    # 逻辑说明：兼容 S3 SDK 的 HTTP 状态和错误码两种“对象不存在”表达，只将明确 404 映射为缺失，其他异常仍向上传播。
    code, status = _error_details(exc)
    return status == 404 or code in {"404", "NoSuchKey", "NotFound"}


def _is_precondition_failure(exc: Exception) -> bool:
    # 逻辑说明：识别条件写入的 412/冲突错误供 CAS 重试或版本冲突处理；不会把认证、网络等失败误标为并发冲突。
    code, status = _error_details(exc)
    return status == 412 or code in {
        "412",
        "ConditionalRequestConflict",
        "PreconditionFailed",
    }


def _manifest_digest(entries: list[dict[str, Any]]) -> str:
    # 逻辑说明：稳定序列化有序清单后计算摘要，使镜像结果可跨进程比较而不依赖字典输出顺序。
    encoded = json.dumps(
        entries,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _content_type(path: Path) -> str:
    # 逻辑说明：按有限扩展名映射常见文本格式，其余安全回退为二进制，避免猜测可执行内容类型。
    suffixes = {
        ".json": "application/json",
        ".md": "text/markdown",
        ".txt": "text/plain",
        ".yaml": "application/yaml",
        ".yml": "application/yaml",
    }
    return suffixes.get(path.suffix.lower(), "application/octet-stream")
