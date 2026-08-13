"""Versioned task artifacts stored in the shared AgentTeams filesystem.

以版本化方式在共享 AgentTeams 存储中读写任务 artifact。

Task spec、result 和附件通过受限 prefix 映射到 MinIO，而不是接受任意对象 key。写入时
使用版本/校验和防止覆盖并发更新，下载后验证内容；Manager-owned 与 Worker-owned 路径
有不同边界。artifact 回执是任务 workflow 判断派发和验收是否完成的证据之一。
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.errors import NotFoundError, RecoveryError
from agentteams_manager.domain.ids import matrix_transaction_id
from agentteams_manager.domain.models import (
    ExternalEffect,
    MirrorReceipt,
    ObjectReceipt,
    OperationKind,
    OperationRecord,
    OperationStatus,
    TaskMetadata,
    TaskRecord,
)
from agentteams_manager.domain.ports import MatrixPort
from agentteams_manager.workflows.git_delegation import (
    ProcessingLease,
    ProcessingLeaseService,
)
from agentteams_manager.workflows.resources import MutationContext


class VersionedObjectStorage(Protocol):
    async def put_bytes_if_version(
        self,
        key: str,
        data: bytes,
        *,
        expected_etag: str | None,
        content_type: str = "application/octet-stream",
    ) -> ObjectReceipt: ...

    async def head(self, key: str) -> ObjectReceipt | None: ...

    async def get_bytes(self, key: str) -> bytes: ...

    async def mirror_down(
        self,
        prefix: str,
        destination: Path,
    ) -> MirrorReceipt: ...

    async def put_json_if_version(
        self,
        key: str,
        value: Any,
        *,
        expected_etag: str | None,
    ) -> ObjectReceipt: ...


class TaskArtifactSet:
    """Writes the immutable prepared form of a task in publish order."""

    def __init__(
        self,
        *,
        storage: VersionedObjectStorage,
        metadata: TaskMetadata,
        specification: str,
    ) -> None:
        # 逻辑说明：绑定对象存储及任务元数据/规格读写器；实际远端读取、写入和完整性校验由后续方法按任务执行。
        self._storage = storage
        self._metadata = metadata
        self._specification = specification

    async def write_prepared(self) -> tuple[ObjectReceipt, ObjectReceipt]:
        # 逻辑说明：先要求 metadata 处于 prepared 且 spec 非空，再按发布顺序条件写 meta.json 与 spec.md；对象已存在时版本冲突阻止覆盖不可变任务定义。
        if self._metadata.status != "prepared":
            raise ValueError("new task metadata must have prepared status")
        if not self._specification.strip():
            raise ValueError("task specification must not be empty")
        prefix = f"shared/tasks/{self._metadata.task_id}"
        metadata_receipt = await self._storage.put_json_if_version(
            f"{prefix}/meta.json",
            self._metadata.model_dump(mode="json"),
            expected_etag=None,
        )
        specification_receipt = await self._storage.put_bytes_if_version(
            f"{prefix}/spec.md",
            self._specification.encode("utf-8"),
            expected_etag=None,
            content_type="text/markdown",
        )
        return metadata_receipt, specification_receipt


class SyncTaskReader(Protocol):
    async def get(self, task_id: str) -> TaskRecord | None: ...


class FileSyncReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str
    prefix: str
    files: int = Field(ge=0)
    bytes_transferred: int = Field(ge=0)
    manifest_sha256: str
    instruction: str = "Run `agentteams-sync` before reading the files."


class FileRootReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    root: Literal["worker_workspace", "shared_knowledge"]
    target: str
    prefix: str
    files: int = Field(ge=0)
    bytes_transferred: int = Field(ge=0)
    manifest_sha256: str


class TaskFileReadReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str
    path: str
    content: str
    bytes_read: int = Field(ge=0)
    sha256: str


class FileSyncSupervisor(Protocol):
    async def begin(
        self,
        *,
        operation_id: str,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> OperationRecord: ...

    async def before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> object: ...

    async def effect_acknowledged(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...

    async def effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord: ...

    async def effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord: ...


class FileSyncService:
    """Make remote pull and protected Worker push explicit."""

    def __init__(
        self,
        *,
        storage: VersionedObjectStorage,
        leases: ProcessingLeaseService,
        tasks: SyncTaskReader,
        cache_root: Path,
        supervisor: FileSyncSupervisor | None = None,
        matrix: MatrixPort | None = None,
    ) -> None:
        # 逻辑说明：组合对象存储、lease、任务库、缓存根和通知依赖；仅规范化本地根路径，不在构造时同步任务文件。
        self._storage = storage
        self._leases = leases
        self._tasks = tasks
        self._cache_root = cache_root.resolve()
        self._supervisor = supervisor
        self._matrix = matrix

    def root_path(
        self,
        root: Literal[
            "worker_workspace",
            "shared_knowledge",
            "task_artifacts",
        ],
        *,
        worker_name: str | None = None,
        task_id: str | None = None,
    ) -> Path:
        # 逻辑说明：复用统一 root 解析器得到受限本地根路径并丢弃远端前缀；缺少对应 Worker/Task 身份时沿用校验错误，不猜测目录。
        local, _ = self._root_spec(
            root,
            worker_name=worker_name,
            task_id=task_id,
        )
        return local

    async def push_root(
        self,
        root: Literal["worker_workspace", "shared_knowledge"],
        *,
        processor: str,
        worker_name: str | None = None,
    ) -> FileRootReceipt:
        # 逻辑说明：解析允许的本地根与远端前缀，扫描并按内容差异条件上传整棵树，最后返回文件数、传输字节与稳定 manifest 摘要。
        del processor
        local, prefix = self._root_spec(
            root,
            worker_name=worker_name,
            task_id=None,
        )
        entries, transferred = await self._push_tree(
            local,
            prefix,
            protect_manager_files=False,
        )
        return FileRootReceipt(
            root=root,
            target=worker_name or "shared",
            prefix=prefix,
            files=len(entries),
            bytes_transferred=transferred,
            manifest_sha256=_manifest_sha256(entries),
        )

    async def pull_root(
        self,
        root: Literal["worker_workspace", "shared_knowledge"],
        *,
        worker_name: str | None = None,
    ) -> Path:
        # 逻辑说明：解析 root/target 到受控缓存目录和对象前缀，调用 storage mirror_down 完整同步后返回本地路径；失败不伪报可用缓存。
        local, prefix = self._root_spec(
            root,
            worker_name=worker_name,
            task_id=None,
        )
        await self._storage.mirror_down(prefix, local)
        return local

    async def sync_task(
        self,
        task_id: str,
        *,
        direction: Literal["pull", "push"],
        processor: str,
        context: MutationContext,
    ) -> FileSyncReceipt | Path:
        # 逻辑说明：pull 直接下载；push 无 supervisor 时走基础上传，有 supervisor 时创建可恢复 operation、记录 Storage 与 Matrix 效果及回执，网络异常标 ambiguous 后重抛。
        if direction == "pull":
            return await self.pull_task(task_id)
        if self._supervisor is None or self._matrix is None:
            return await self.push_task(
                task_id,
                processor=processor,
            )
        task = await self._require_task(task_id)
        prefix = _task_remote_prefix(task)
        request: dict[str, object] = {
            "action": "push_task",
            "task_id": task_id,
            "processor": processor,
            "source_room_id": context.room_id,
            "source_event_id": context.event_id,
            "source_tool_call_id": context.tool_call_id,
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.FILE_SYNC,
            target_key=f"task/{task_id}/files",
            request=request,
        )
        if operation.status is OperationStatus.SUCCEEDED:
            return _sync_receipt_from_operation(operation)
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {
                "operation": "upload_task_files",
                "task_id": task_id,
                "prefix": prefix,
            },
        )
        receipt = await self.push_task(
            task_id,
            processor=processor,
        )
        await self._supervisor.effect_acknowledged(
            operation.operation_id,
            ExternalEffect.STORAGE,
            {"sync": receipt.model_dump(mode="json")},
        )
        txn_id = matrix_transaction_id(operation.operation_id, 1)
        matrix_user_id = str(
            task.metadata.get("matrix_user_id")
            or f"@{task.assigned_to}:local"
        )
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "operation": "mention_worker_after_file_upload",
                "task_id": task_id,
                "room_id": task.room_id,
                "txn_id": txn_id,
            },
        )
        try:
            event_id = await self._matrix.send_text(
                task.room_id,
                f"{matrix_user_id} Files for task {task_id} are synchronized.",
                txn_id=txn_id,
                mentions=(matrix_user_id,),
            )
        except Exception as error:
            await self._supervisor.effect_ambiguous(
                operation.operation_id,
                ExternalEffect.MATRIX,
                type(error).__name__,
            )
            raise
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            ExternalEffect.MATRIX,
            {
                "sync": receipt.model_dump(mode="json"),
                "event_id": event_id,
                "txn_id": txn_id,
            },
        )
        return receipt

    async def resume_operation(
        self,
        operation: OperationRecord,
    ) -> FileSyncReceipt | Path:
        # 逻辑说明：验证恢复对象确为 FILE_SYNC/push_task，重建原 MutationContext 并核对 operation ID 未漂移，然后重入幂等 sync_task 对账后续效果。
        if operation.kind is not OperationKind.FILE_SYNC:
            raise RecoveryError("operation is not a file sync")
        request = operation.request
        if request.get("action") != "push_task":
            raise RecoveryError("unsupported file sync recovery action")
        context = MutationContext(
            room_id=str(request["source_room_id"]),
            event_id=str(request["source_event_id"]),
            tool_call_id=str(request["source_tool_call_id"]),
        )
        if context.operation_id != operation.operation_id:
            raise RecoveryError("file sync operation identity changed")
        return await self.sync_task(
            str(request["task_id"]),
            direction="push",
            processor=str(request["processor"]),
            context=context,
        )

    async def pull_task(self, task_id: str) -> Path:
        # 逻辑说明：确认任务存在，根据 Team 路由计算权威对象前缀，将其镜像到受控 task cache 并返回目录。
        task = await self._require_task(task_id)
        destination = self._task_root(task_id)
        await self._storage.mirror_down(
            _task_remote_prefix(task),
            destination,
        )
        return destination

    async def read_task_file(
        self,
        task_id: str,
        path: str,
        *,
        max_bytes: int = 256 * 1024,
    ) -> TaskFileReadReceipt:
        """Read one bounded UTF-8 file from the verified task cache."""
        # 逻辑说明：确认任务、大小范围和相对路径，逐级拒绝 symlink/越界/非文件，双重检查字节数并解码 UTF-8，最后返回正文与 SHA-256。

        await self._require_task(task_id)
        if not 1 <= max_bytes <= 1024 * 1024:
            raise ValueError("maximum read size must be 1 to 1048576 bytes")
        relative = Path(path)
        if (
            not path.strip()
            or relative.is_absolute()
            or ".." in relative.parts
        ):
            raise ValueError("task file path must be relative and contained")
        root = self._task_root(task_id).resolve()
        candidate = root.joinpath(relative)
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("task file path must not contain a symlink")
        absolute = candidate.resolve(strict=True)
        if not absolute.is_relative_to(root):
            raise ValueError("task file path escapes task root")
        if not absolute.is_file():
            raise ValueError("task file path is not a file")
        size = absolute.stat().st_size
        if size > max_bytes:
            raise ValueError(
                f"task file exceeds maximum read size of {max_bytes} bytes",
            )
        data = absolute.read_bytes()
        if len(data) > max_bytes:
            raise ValueError(
                f"task file exceeds maximum read size of {max_bytes} bytes",
            )
        try:
            content = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("task file is not valid UTF-8 text") from exc
        return TaskFileReadReceipt(
            task_id=task_id,
            path=relative.as_posix(),
            content=content,
            bytes_read=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )

    async def push_task(
        self,
        task_id: str,
        *,
        processor: str,
        lease: ProcessingLease | None = None,
    ) -> FileSyncReceipt:
        # 逻辑说明：验证 task cache 存在，递归枚举普通且非 Manager-owned 文件，再委托 push_files 获取 lease、做版本上传和生成 manifest。
        root = self._task_root(task_id)
        if not root.is_dir():
            raise FileNotFoundError(root)
        paths = tuple(
            path
            for path in sorted(root.rglob("*"))
            if path.is_file()
            and not path.is_symlink()
            and not _manager_owned(path.relative_to(root))
        )
        return await self.push_files(
            task_id,
            paths=paths,
            processor=processor,
            lease=lease,
        )

    async def push_files(
        self,
        task_id: str,
        *,
        paths: tuple[Path, ...],
        processor: str,
        lease: ProcessingLease | None = None,
    ) -> FileSyncReceipt:
        # 逻辑说明：确认任务和缓存根，获取或验证 processing lease；逐文件拒绝 symlink/越界/保护路径，按摘要+大小跳过未变对象并用 ETag 条件写，finally 释放自有 lease。
        task = await self._require_task(task_id)
        prefix = _task_remote_prefix(task)
        root = self._task_root(task_id)
        if not root.is_dir():
            raise FileNotFoundError(root)
        owned_lease = lease is None
        active_lease = lease or await self._leases.acquire(
            task_id,
            processor=processor,
            operation="file-sync",
        )
        if active_lease.task_id != task_id:
            raise ValueError("processing lease belongs to another task")

        entries: list[dict[str, object]] = []
        transferred = 0
        try:
            for path in sorted(set(paths)):
                if path.is_symlink():
                    raise ValueError(f"sync path must not be a symlink: {path}")
                absolute = (
                    path.resolve(strict=True)
                    if path.is_absolute()
                    else (root / path).resolve(strict=True)
                )
                if not absolute.is_relative_to(root):
                    raise ValueError(f"sync path escapes task root: {path}")
                relative = absolute.relative_to(root)
                if _manager_owned(relative):
                    raise PermissionError(
                        f"Worker sync cannot mutate {relative.as_posix()}",
                    )
                if not absolute.is_file():
                    raise ValueError(f"sync path is not a file: {path}")
                data = absolute.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                key = f"{prefix}{relative.as_posix()}"
                current = await self._storage.head(key)
                if (
                    current is None
                    or current.sha256 != digest
                    or current.size != len(data)
                ):
                    receipt = await self._storage.put_bytes_if_version(
                        key,
                        data,
                        expected_etag=(
                            current.etag if current is not None else None
                        ),
                        content_type=_content_type(absolute),
                    )
                    transferred += len(data)
                else:
                    receipt = current
                entries.append(
                    {
                        "etag": receipt.etag,
                        "key": key,
                        "sha256": receipt.sha256,
                        "size": receipt.size,
                    },
                )
        finally:
            if owned_lease:
                await self._leases.release(active_lease)
        encoded = json.dumps(
            entries,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return FileSyncReceipt(
            task_id=task_id,
            prefix=prefix,
            files=len(entries),
            bytes_transferred=transferred,
            manifest_sha256=hashlib.sha256(encoded).hexdigest(),
        )

    async def _push_tree(
        self,
        root: Path,
        prefix: str,
        *,
        protect_manager_files: bool,
    ) -> tuple[list[dict[str, object]], int]:
        # 逻辑说明：安全遍历受控 root，拒绝 symlink 与 resolve 越界，可选跳过 Manager 文件；只上传内容变化对象并返回完整 manifest entries 与实际传输字节。
        if not root.is_dir():
            raise FileNotFoundError(root)
        entries: list[dict[str, object]] = []
        transferred = 0
        for candidate in sorted(root.rglob("*")):
            if candidate.is_symlink():
                raise ValueError(
                    f"sync path must not be a symlink: {candidate}",
                )
            if not candidate.is_file():
                continue
            absolute = candidate.resolve(strict=True)
            if not absolute.is_relative_to(root):
                raise ValueError(f"sync path escapes root: {candidate}")
            relative = absolute.relative_to(root)
            if protect_manager_files and _manager_owned(relative):
                continue
            data = absolute.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            key = f"{prefix}{relative.as_posix()}"
            current = await self._storage.head(key)
            if (
                current is None
                or current.sha256 != digest
                or current.size != len(data)
            ):
                receipt = await self._storage.put_bytes_if_version(
                    key,
                    data,
                    expected_etag=current.etag if current else None,
                    content_type=_content_type(absolute),
                )
                transferred += len(data)
            else:
                receipt = current
            entries.append(
                {
                    "etag": receipt.etag,
                    "key": key,
                    "sha256": receipt.sha256,
                    "size": receipt.size,
                },
            )
        return entries, transferred

    async def _require_task(self, task_id: str) -> TaskRecord:
        # 逻辑说明：从任务端口读取持久记录，不存在立即抛出领域 NotFound；返回值作为后续 Team 前缀和权限判断依据。
        task = await self._tasks.get(task_id)
        if task is None:
            raise NotFoundError(f"task/{task_id} does not exist")
        return task

    def _task_root(self, task_id: str) -> Path:
        # 逻辑说明：严格限制 task ID 格式并拒绝路径分隔符/点目录，再拼入固定 cache 根，防止用户 ID 逃逸文件系统边界。
        if (
            not task_id.startswith("task-")
            or "/" in task_id
            or "\\" in task_id
            or task_id in {"task-.", "task-.."}
        ):
            raise ValueError(f"invalid task ID: {task_id!r}")
        return self._cache_root / "shared" / "tasks" / task_id

    def _root_spec(
        self,
        root: str,
        *,
        worker_name: str | None,
        task_id: str | None,
    ) -> tuple[Path, str]:
        # 逻辑说明：按 root 种类校验所需/禁止的 Worker 或 task 参数，并同时返回受控本地目录与规范远端前缀；未知组合立即拒绝。
        if root == "worker_workspace":
            if (
                worker_name is None
                or re.fullmatch(
                    r"[a-z0-9][a-z0-9-]*",
                    worker_name,
                )
                is None
            ):
                raise ValueError(f"invalid Worker name: {worker_name!r}")
            return (
                self._cache_root
                / "workers"
                / worker_name
                / "workspace",
                f"workers/{worker_name}/workspace/",
            )
        if root == "shared_knowledge":
            if worker_name is not None or task_id is not None:
                raise ValueError(
                    "shared knowledge root has no Worker or task target",
                )
            return (
                self._cache_root / "shared" / "knowledge",
                "shared/knowledge/",
            )
        if root == "task_artifacts":
            if task_id is None:
                raise ValueError("task artifact root requires task_id")
            return self._task_root(task_id), f"shared/tasks/{task_id}/"
        raise ValueError(f"unknown sync root: {root!r}")


def _manager_owned(relative: Path) -> bool:
    # 逻辑说明：仅把任务目录根及 Manager 的元数据/处理目录标记为 Manager 所有；Worker 产物因此不会被 Manager 写操作误覆盖。
    if not relative.parts:
        return True
    return relative.parts[0] in {
        ".processing",
        "base",
        "meta.json",
        "spec.md",
    }


def _task_remote_prefix(task: TaskRecord) -> str:
    # 逻辑说明：优先使用 delegated/storage Team 决定共享对象命名空间，无 Team 回退全局任务前缀；非法 Team 名触发恢复错误，防止写到意外 key。
    team_name = str(
        task.delegated_to_team
        or task.metadata.get("storage_team_name")
        or "",
    ).strip()
    if not team_name:
        return f"shared/tasks/{task.task_id}/"
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", team_name) is None:
        raise RecoveryError(
            f"task {task.task_id} has an invalid delegated Team name",
        )
    return f"teams/{team_name}/shared/tasks/{task.task_id}/"


def _content_type(path: Path) -> str:
    # 逻辑说明：按小写扩展名选择有限 MIME 映射，未知文件统一为二进制，避免从用户文件内容猜测类型。
    return {
        ".json": "application/json",
        ".md": "text/markdown",
        ".txt": "text/plain",
        ".yaml": "application/yaml",
        ".yml": "application/yaml",
    }.get(path.suffix.casefold(), "application/octet-stream")


def _manifest_sha256(entries: list[dict[str, object]]) -> str:
    # 逻辑说明：把 manifest 用稳定键序和紧凑 JSON 编码后计算 SHA-256，相同文件回执集合得到相同根摘要。
    encoded = json.dumps(
        entries,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _sync_receipt_from_operation(
    operation: OperationRecord,
) -> FileSyncReceipt:
    # 逻辑说明：从已成功 Operation 的持久 result 提取 sync 字典并重新验证 FileSyncReceipt；缺失或类型错误说明恢复状态不完整，拒绝伪造回执。
    raw = operation.result.get("sync")
    if not isinstance(raw, dict):
        raise RecoveryError("succeeded file sync has no durable receipt")
    return FileSyncReceipt.model_validate(raw)
