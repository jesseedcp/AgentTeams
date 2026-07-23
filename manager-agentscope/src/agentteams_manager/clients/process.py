"""Allowlisted, shell-free subprocess execution."""

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
