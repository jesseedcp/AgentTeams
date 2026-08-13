"""Allowlisted, shell-free subprocess execution.

提供只接受参数数组的 allowlist 子进程执行边界。

业务模块不能把模型文本拼成 shell 命令；它们必须选择允许的可执行文件并传入独立 argv。
本模块负责超时、退出码和有界输出，将操作系统错误转换成稳定异常。这样可以保留调用
CLI 的能力，同时避免引号转义、命令替换和意外凭据输出带来的风险。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


class ProcessError(RuntimeError):
    """Base process boundary error."""


class ProcessRejected(ProcessError):
    """The requested executable is outside the process allowlist."""


class ProcessTimeout(ProcessError):
    """A child process did not produce a result within its deadline."""


@dataclass(frozen=True, slots=True)
class ProcessResult:
    argv: tuple[str, ...] = field(repr=False)
    returncode: int
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)


class ProcessRunner:
    """Execute exact argv tuples without invoking a command shell."""

    def __init__(
        self,
        *,
        allowed_executables: Collection[str] = ("agt",),
        environment: Mapping[str, str] | None = None,
    ) -> None:
        # 逻辑说明：固化可执行文件白名单和可选最小环境，后续 argv 不能临时突破允许的程序集合。
        self._allowed = frozenset(allowed_executables)
        if not self._allowed:
            raise ValueError("process allowlist cannot be empty")
        self._environment = (
            dict(environment) if environment is not None else None
        )

    async def run(
        self,
        argv: tuple[str, ...],
        *,
        stdin: bytes | None = None,
        cwd: Path | None = None,
        timeout: float | None = 30,
    ) -> ProcessResult:
        # 逻辑说明：先验证 argv allowlist，再无 shell 启动；超时先 terminate、后 kill 并返回稳定异常。
        self._validate(argv)
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=(
                asyncio.subprocess.PIPE
                if stdin is not None
                else None
            ),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._environment,
            cwd=str(cwd) if cwd is not None else None,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin),
                timeout=timeout,
            )
        except TimeoutError as exc:
            logger.warning(
                "allowed process timed out",
                extra={
                    "executable": argv[0],
                    "argument_count": len(argv) - 1,
                    "timeout_seconds": timeout,
                },
            )
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except TimeoutError:
                process.kill()
                await process.wait()
            raise ProcessTimeout(
                f"{argv[0]} timed out after {timeout} seconds",
            ) from exc
        return ProcessResult(
            argv=argv,
            returncode=process.returncode or 0,
            stdout=stdout,
            stderr=stderr,
        )

    def _validate(self, argv: tuple[str, ...]) -> None:
        # 逻辑说明：只允许 allowlist 中的裸可执行名，阻止绝对路径或目录分隔符绕过配置。
        if not argv or not argv[0]:
            raise ProcessRejected("process argv cannot be empty")
        executable = argv[0]
        if "/" in executable or "\\" in executable:
            raise ProcessRejected(
                "path-qualified executables are not allowed",
            )
        if executable not in self._allowed:
            raise ProcessRejected(
                f"executable {executable!r} is not allowlisted",
            )
