"""Immutable MinIO/S3 recovery journal and verified snapshots.

在 MinIO/S3 中保存不可变操作 journal 和经过校验的 SQLite 快照。

本地 PVC 丢失时，Manager 先下载最新快照，再按 sequence 重放之后的事件。journal 对象
使用只增不改的 key 和条件写入，payload 在写入前已脱敏；快照附带大小与 SHA-256，
校验失败不会替换本地数据库。它是灾难恢复记录，不是聊天历史或 Secret 存储。
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.models import JournalEvent


class JournalObjectStore(Protocol):
    async def put(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        if_none_match: bool,
    ) -> str: ...

    async def get(self, key: str) -> bytes: ...

    async def list(self, prefix: str) -> tuple[str, ...]: ...


class SnapshotMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(ge=0)
    key: str
    sha256: str = Field(min_length=64, max_length=64)
    size: int = Field(ge=0)
    created_at: datetime


def event_key(prefix: str, event: JournalEvent) -> str:
    # 逻辑说明：把操作 ID 与全局递增 sequence 编进不可变对象键；固定宽度数字保证对象存储的字典序就是重放顺序。
    return _prefixed(
        prefix,
        (
            f"manager/journal/{event.operation_id}/"
            f"{event.sequence:020d}.json"
        ),
    )


class S3Journal:
    """Journal logic independent of the concrete aioboto3 adapter."""

    def __init__(self, store: JournalObjectStore, *, prefix: str) -> None:
        # 逻辑说明：保存对象存储适配器并规范化 key 前缀；构造阶段不访问远端，后续 journal/snapshot 操作共享同一命名空间。
        self._store = store
        self._prefix = prefix.strip("/")

    @property
    def journal_prefix(self) -> str:
        return _prefixed(self._prefix, "manager/journal/")

    @property
    def snapshot_prefix(self) -> str:
        return _prefixed(self._prefix, "manager/snapshots")

    async def append(self, event: JournalEvent) -> str:
        """Write one event exactly once."""
        # 逻辑说明：序列化一个已脱敏事件并用条件写入保存；相同 key 已存在时由对象存储报冲突，调用方可把它视为幂等重试而不会覆盖历史。
        return await self._store.put(
            event_key(self._prefix, event),
            event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            if_none_match=True,
        )

    async def list_after(self, sequence: int) -> tuple[JournalEvent, ...]:
        """Return globally sequenced journal events after a snapshot."""
        # 逻辑说明：列出 journal 对象，忽略非 JSON 项并反序列化比快照 sequence 更新的事件；最后稳定排序后返回，读取或校验失败直接中止恢复。
        keys = await self._store.list(self.journal_prefix)
        events: list[JournalEvent] = []
        for key in keys:
            if not key.endswith(".json"):
                continue
            event = JournalEvent.model_validate_json(
                await self._store.get(key),
            )
            if event.sequence > sequence:
                events.append(event)
        events.sort(key=lambda item: (item.sequence, item.operation_id))
        return tuple(events)

    async def upload_snapshot(
        self,
        path: Path,
        *,
        sequence: int,
    ) -> SnapshotMetadata:
        """Publish immutable bytes and metadata before the latest pointer."""
        # 逻辑说明：读取 SQLite 快照、计算大小与 SHA-256，先不可变写数据库和元数据，再更新 latest 指针；这样中途失败不会让恢复端看到未完整发布的快照。
        data = path.read_bytes()
        digest = sha256(data).hexdigest()
        stem = f"{self.snapshot_prefix}/{sequence:020d}"
        database_key = f"{stem}.db"
        metadata_key = f"{stem}.json"
        metadata = SnapshotMetadata(
            sequence=sequence,
            key=database_key,
            sha256=digest,
            size=len(data),
            created_at=datetime.now(UTC),
        )
        encoded = metadata.model_dump_json().encode("utf-8")

        await self._store.put(
            database_key,
            data,
            content_type="application/vnd.sqlite3",
            if_none_match=True,
        )
        await self._store.put(
            metadata_key,
            encoded,
            content_type="application/json",
            if_none_match=True,
        )
        await self._store.put(
            f"{self.snapshot_prefix}/latest.json",
            encoded,
            content_type="application/json",
            if_none_match=False,
        )
        return metadata
    async def download_latest_snapshot(
        self,
    ) -> tuple[SnapshotMetadata, bytes] | None:
        # 逻辑说明：读取 latest 指针及其数据库对象，缺少指针表示尚无快照并返回 None；恢复前同时校验大小和摘要，不一致时拒绝返回损坏数据。
        try:
            encoded = await self._store.get(
                f"{self.snapshot_prefix}/latest.json",
            )
        except KeyError:
            return None
        metadata = SnapshotMetadata.model_validate_json(encoded)
        data = await self._store.get(metadata.key)
        digest = sha256(data).hexdigest()
        if len(data) != metadata.size or digest != metadata.sha256:
            raise ValueError(
                "snapshot checksum/size mismatch; refusing recovery",
            )
        return metadata, data


def _prefixed(prefix: str, key: str) -> str:
    # 逻辑说明：规范化可选部署前缀并拼出对象键，空前缀时保持原 key，避免产生前导斜杠造成两个命名空间。
    normalized = prefix.strip("/")
    return f"{normalized}/{key}" if normalized else key
