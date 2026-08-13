"""内存版 S3 测试替身，用于验证对象版本、条件写入和冲突恢复。

它模拟 ETag 与 ``If-None-Match`` 等并发语义，而不连接 MinIO；这使故障路径可重复，但不能覆盖真实网络、
权限和 bucket 配置。端到端测试仍需读取真实 MinIO 对象作为最终通过证据。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


class FakePreconditionFailed(RuntimeError):
    def __init__(self) -> None:
        super().__init__("precondition failed")
        self.response = {
            "Error": {"Code": "PreconditionFailed"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        }


class FakeBody:
    def __init__(self, value: bytes) -> None:
        self._value = value

    async def read(self) -> bytes:
        return self._value


@dataclass
class FakeObject:
    data: bytes
    content_type: str
    metadata: dict[str, str]
    etag: str
    version_id: str


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, FakeObject] = {}
        self.puts: list[str] = []
        self.deletes: list[str] = []
        self._version = 0

    def head(self, key: str) -> FakeObject:
        return self.objects[key]

    async def put_object(self, **kwargs: Any) -> dict[str, str]:
        key = str(kwargs["Key"])
        existing = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and existing is not None:
            raise FakePreconditionFailed()
        if "IfMatch" in kwargs and (
            existing is None or existing.etag != kwargs["IfMatch"]
        ):
            raise FakePreconditionFailed()
        data = bytes(kwargs["Body"])
        self._version += 1
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        version_id = str(self._version)
        self.objects[key] = FakeObject(
            data=data,
            content_type=str(
                kwargs.get("ContentType", "application/octet-stream"),
            ),
            metadata=dict(kwargs.get("Metadata", {})),
            etag=etag,
            version_id=version_id,
        )
        self.puts.append(key)
        return {"ETag": etag, "VersionId": version_id}

    async def head_object(self, **kwargs: Any) -> dict[str, Any]:
        key = str(kwargs["Key"])
        if key not in self.objects:
            error = KeyError(key)
            error.response = {
                "Error": {"Code": "NoSuchKey"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            }
            raise error
        item = self.objects[key]
        return {
            "ETag": item.etag,
            "VersionId": item.version_id,
            "ContentLength": len(item.data),
            "ContentType": item.content_type,
            "Metadata": dict(item.metadata),
        }

    async def get_object(self, **kwargs: Any) -> dict[str, Any]:
        head = await self.head_object(**kwargs)
        item = self.objects[str(kwargs["Key"])]
        return {**head, "Body": FakeBody(item.data)}

    async def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        prefix = str(kwargs.get("Prefix", ""))
        keys = sorted(
            key for key in self.objects if key.startswith(prefix)
        )
        return {
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": key,
                    "ETag": self.objects[key].etag,
                    "Size": len(self.objects[key].data),
                }
                for key in keys
            ],
        }

    async def delete_object(self, **kwargs: Any) -> dict[str, str]:
        key = str(kwargs["Key"])
        existing = self.objects.get(key)
        if "IfMatch" in kwargs and (
            existing is None or existing.etag != kwargs["IfMatch"]
        ):
            raise FakePreconditionFailed()
        if existing is None:
            raise FakePreconditionFailed()
        del self.objects[key]
        self.deletes.append(key)
        return {"VersionId": existing.version_id}

