"""Explicitly allowlisted access to an optional host-share mount.

仅在显式 allowlist 的 host-share 根目录内读写文件。

路径会先 resolve 并确认仍位于配置根下，拒绝 ``..``、符号链接逃逸、超限文件和未允许
扩展名。这个工具不等于完整主机文件系统访问，且通常受高风险确认；不用 shell 执行
路径，避免文件名被解释成命令。
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.domain.models import RoomKind, RoomPolicy
from agentteams_manager.tools.base import ManagerTool

HOST_FILE_TOOL_NAMES = frozenset({"read_host_file", "write_host_file"})


class HostFileAccess:
    def __init__(
        self,
        *,
        root: Path | None,
        read_allowlist: tuple[str, ...],
        write_allowlist: tuple[str, ...],
        max_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        self._root = root.resolve() if root is not None else None
        self._read = tuple(read_allowlist)
        self._write = tuple(write_allowlist)
        self._max_bytes = max_bytes

    @property
    def enabled(self) -> bool:
        return self._root is not None

    def read_text(self, path: str) -> dict[str, object]:
        target = self._resolve(path, self._read)
        data = target.read_bytes()
        if len(data) > self._max_bytes:
            raise ValueError("host file exceeds size limit")
        return {
            "path": path,
            "content": data.decode("utf-8"),
            "bytes": len(data),
        }

    def write_text(self, path: str, content: str) -> dict[str, object]:
        target = self._resolve(path, self._write)
        data = content.encode("utf-8")
        if len(data) > self._max_bytes:
            raise ValueError("host file exceeds size limit")
        target.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(
            prefix=f".{target.name}.",
            dir=target.parent,
        )
        try:
            with os.fdopen(handle, "wb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return {"path": path, "bytes": len(data)}

    def _resolve(self, path: str, allowlist: tuple[str, ...]) -> Path:
        if self._root is None:
            raise PermissionError("host file access is disabled")
        relative = PurePosixPath(path.replace("\\", "/"))
        if relative.is_absolute() or ".." in relative.parts:
            raise PermissionError("host file path must be relative")
        normalized = relative.as_posix()
        if not normalized or not any(
            relative.match(pattern) for pattern in allowlist
        ):
            raise PermissionError("host file path is not allowlisted")
        target = (self._root / Path(*relative.parts)).resolve()
        if not target.is_relative_to(self._root):
            raise PermissionError("host file path escapes the mount")
        return target


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _Read(_Input):
    path: str = Field(min_length=1, max_length=1_024)


class _Write(_Read):
    content: str = Field(max_length=2 * 1024 * 1024)


class HostFileToolkitFactory:
    def __init__(self, *, access: HostFileAccess, yolo: bool) -> None:
        self._access = access
        self._yolo = yolo

    def tools_for_policy(
        self,
        policy: RoomPolicy,
    ) -> tuple[ManagerTool, ...]:
        if policy.kind is not RoomKind.ADMIN_DM or not self._access.enabled:
            return ()
        return (
            ManagerTool(
                name="read_host_file",
                description="Read one explicitly allowlisted host file.",
                input_schema=_Read.model_json_schema(),
                policy=policy,
                handler=self._access.read_text,
                is_read_only=True,
                yolo=self._yolo,
            ),
            ManagerTool(
                name="write_host_file",
                description="Atomically write one allowlisted host file.",
                input_schema=_Write.model_json_schema(),
                policy=policy,
                handler=self._access.write_text,
                yolo=self._yolo,
                confirmation_message=(
                    "Writing a host file requires administrator confirmation."
                ),
            ),
        )
