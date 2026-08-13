"""Bounded, shell-free execution of explicitly configured coding CLIs.

在受限工作目录中执行显式允许的 Coding CLI。

Manager 可以把编码任务委托给已配置的 CLI，但不会把模型生成的整段文本交给 shell。
本模块使用固定可执行文件和参数列表、限制输出大小和运行时间，并在取消或超时时终止
整个子进程组。返回的输出还要经过安全裁剪，避免把本机环境和凭据原样带回聊天。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import signal
import subprocess
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

CodingCLIProvider = Literal["claude", "gemini", "qodercli"]

_PROVIDERS = frozenset({"claude", "gemini", "qodercli"})
_PROVIDER_ARGUMENTS: dict[CodingCLIProvider, tuple[str, ...]] = {
    "claude": (
        "--print",
        "--output-format",
        "text",
        "--permission-mode",
        "acceptEdits",
        "--disallowedTools",
        "Bash,WebFetch,WebSearch",
        "--max-turns",
        "20",
    ),
    "gemini": (
        "--approval-mode",
        "auto_edit",
        "--output-format",
        "text",
    ),
    "qodercli": (
        "--print",
        "--output-format",
        "text",
        "--permission-mode",
        "accept_edits",
        "--tools",
        "Read,Grep,Glob,Edit,Write",
        "--max-turns",
        "20",
    ),
}
_BASE_ENVIRONMENT = frozenset(
    {
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "NO_COLOR",
        "PATH",
        "PATHEXT",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USER",
        "USERPROFILE",
        "WINDIR",
    },
)
_PROVIDER_ENVIRONMENT: dict[CodingCLIProvider, frozenset[str]] = {
    "claude": frozenset(
        {
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
            "CLAUDE_CODE_OAUTH_TOKEN",
        },
    ),
    "gemini": frozenset(
        {
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_CLOUD_PROJECT",
        },
    ),
    "qodercli": frozenset({"QODER_PERSONAL_ACCESS_TOKEN"}),
}
_ANSI_ESCAPE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
_CREDENTIAL_TEXT = re.compile(
    r"(?i)(?P<prefix>\b(?:authorization|api[_-]?key|token|secret|password)"
    r"\s*[:=]\s*(?:bearer\s+)?)(?P<value>[^\s,;]+)",
)


class CodingCLIError(RuntimeError):
    """Base error for the coding CLI boundary."""


class CodingCLIProviderRejected(CodingCLIError):
    """The requested provider is not explicitly configured."""


class CodingCLIUnavailable(CodingCLIError):
    """The configured provider executable is not mounted."""


class CodingCLIWorkspaceEscape(CodingCLIError):
    """The requested working directory escapes the task cache."""


class CodingCLITimeout(CodingCLIError):
    """The child process exceeded its configured deadline."""


@dataclass(frozen=True, slots=True)
class RawCodingCLIResult:
    """Raw bounded process output before credential redaction."""

    returncode: int
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    duration_ms: int = 0


class CodingCLIReceipt(BaseModel):
    """Secret-free result safe for tools, SQLite, and Matrix."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: CodingCLIProvider
    success: bool
    returncode: int
    stdout: str = Field(max_length=100_000)
    stderr: str = Field(max_length=100_000)
    output_truncated: bool = False
    duration_ms: int = Field(default=0, ge=0)


class CodingCLIProcessRunner(Protocol):
    async def run(
        self,
        argv: tuple[str, ...],
        *,
        stdin: bytes,
        cwd: Path,
        environment: Mapping[str, str],
        timeout: float,
        max_output_bytes: int,
    ) -> RawCodingCLIResult: ...


class BoundedProcessRunner:
    """Drain bounded output and terminate a process group on stop/timeout."""

    async def run(
        self,
        argv: tuple[str, ...],
        *,
        stdin: bytes,
        cwd: Path,
        environment: Mapping[str, str],
        timeout: float,
        max_output_bytes: int,
    ) -> RawCodingCLIResult:
        # 逻辑说明：创建独立进程组并并行喂入/排空有界输出；超时、取消、异常都终止整组。
        if not argv or not Path(argv[0]).is_absolute():
            raise ValueError("coding CLI executable must be an absolute path")
        if timeout <= 0:
            raise ValueError("coding CLI timeout must be positive")
        if max_output_bytes < 1:
            raise ValueError("coding CLI output limit must be positive")

        started = asyncio.get_running_loop().time()
        if os.name == "nt":
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(cwd),
                env=dict(environment),
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(cwd),
                env=dict(environment),
                start_new_session=True,
            )
        if process.stdin is None or process.stdout is None or process.stderr is None:
            await _terminate_process_group(process)
            raise RuntimeError("coding CLI subprocess pipes are unavailable")

        feed_task = asyncio.create_task(_feed(process.stdin, stdin))
        stdout_task = asyncio.create_task(
            _read_bounded(process.stdout, max_output_bytes),
        )
        stderr_task = asyncio.create_task(
            _read_bounded(process.stderr, max_output_bytes),
        )
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
            await feed_task
            stdout_result, stderr_result = await asyncio.gather(
                stdout_task,
                stderr_task,
            )
        except TimeoutError as exc:
            await _terminate_process_group(process)
            await _settle(feed_task, stdout_task, stderr_task)
            raise CodingCLITimeout(
                f"coding CLI timed out after {timeout:g} seconds",
            ) from exc
        except asyncio.CancelledError:
            await _terminate_process_group(process)
            await _settle(feed_task, stdout_task, stderr_task)
            raise
        except BaseException:
            await _terminate_process_group(process)
            await _settle(feed_task, stdout_task, stderr_task)
            raise

        elapsed = asyncio.get_running_loop().time() - started
        return RawCodingCLIResult(
            returncode=process.returncode or 0,
            stdout=stdout_result[0],
            stderr=stderr_result[0],
            stdout_truncated=stdout_result[1],
            stderr_truncated=stderr_result[1],
            duration_ms=max(0, round(elapsed * 1000)),
        )


class CodingCLIClient:
    """Resolve only mounted providers and execute immutable argv templates."""

    def __init__(
        self,
        *,
        trusted_directory: Path,
        allowed_providers: Collection[str],
        workspace_root: Path,
        runner: CodingCLIProcessRunner | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = 600,
        max_output_bytes: int = 64 * 1024,
        max_prompt_bytes: int = 100_000,
    ) -> None:
        # 逻辑说明：校验 provider 和资源上限，再固化可信目录、工作区与环境，单次委托不能扩大宿主权限。
        invalid = sorted(set(allowed_providers) - _PROVIDERS)
        if invalid:
            raise ValueError(
                "unsupported coding CLI providers: " + ", ".join(invalid),
            )
        if timeout_seconds <= 0:
            raise ValueError("coding CLI timeout must be positive")
        if max_output_bytes < 1 or max_prompt_bytes < 1:
            raise ValueError("coding CLI byte limits must be positive")
        self._trusted_directory = trusted_directory.resolve()
        self._allowed = frozenset(allowed_providers)
        self._workspace_root = workspace_root.resolve()
        self._runner = runner or BoundedProcessRunner()
        self._environment = dict(
            os.environ if environment is None else environment,
        )
        self._timeout_seconds = timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._max_prompt_bytes = max_prompt_bytes

    def status(self) -> dict[str, dict[str, bool]]:
        """Report desired configuration separately from mount availability."""
        # 逻辑说明：逐个报告是否获准且可执行文件是否存在，不读取凭据，也不尝试真正启动第三方 CLI。
        return {
            provider: {
                "configured": provider in self._allowed,
                "available": (
                    provider in self._allowed
                    and self._find_executable(provider) is not None
                ),
            }
            for provider in sorted(_PROVIDERS)
        }

    async def run(
        self,
        provider: str,
        *,
        workspace: Path,
        prompt: str,
        timeout_seconds: float | None = None,
    ) -> CodingCLIReceipt:
        # 逻辑说明：验证 provider/workspace/prompt 后执行固定 argv，并在回执前脱敏环境 Secret。
        selected = self._provider(provider)
        executable = self._find_executable(selected)
        if executable is None:
            raise CodingCLIUnavailable(
                f"{selected} is configured but not available",
            )
        resolved_workspace = self.validate_workspace(workspace)
        prompt_bytes = prompt.encode("utf-8")
        if not prompt_bytes or len(prompt_bytes) > self._max_prompt_bytes:
            raise ValueError(
                "coding CLI prompt must be between 1 and "
                f"{self._max_prompt_bytes} UTF-8 bytes",
            )
        if "\x00" in prompt:
            raise ValueError("coding CLI prompt cannot contain NUL")
        deadline = self._timeout_seconds
        if timeout_seconds is not None:
            if timeout_seconds <= 0:
                raise ValueError("coding CLI timeout must be positive")
            deadline = min(timeout_seconds, self._timeout_seconds)
        environment = self._safe_environment(selected)
        raw = await self._runner.run(
            (str(executable), *_PROVIDER_ARGUMENTS[selected]),
            stdin=prompt_bytes,
            cwd=resolved_workspace,
            environment=environment,
            timeout=deadline,
            max_output_bytes=self._max_output_bytes,
        )
        secrets = tuple(
            environment[name]
            for name in _PROVIDER_ENVIRONMENT[selected]
            if environment.get(name)
        )
        return CodingCLIReceipt(
            provider=selected,
            success=raw.returncode == 0,
            returncode=raw.returncode,
            stdout=_safe_output(raw.stdout, secrets),
            stderr=_safe_output(raw.stderr, secrets),
            output_truncated=(
                raw.stdout_truncated or raw.stderr_truncated
            ),
            duration_ms=raw.duration_ms,
        )

    def validate_workspace(self, workspace: Path) -> Path:
        # 逻辑说明：resolve 后必须仍在任务 cache 根内且已存在，阻止符号链接或 .. 逃逸。
        resolved = workspace.resolve()
        if (
            resolved != self._workspace_root
            and not resolved.is_relative_to(self._workspace_root)
        ):
            raise CodingCLIWorkspaceEscape(
                "coding CLI workspace escapes the task cache",
            )
        if not resolved.is_dir():
            raise CodingCLIWorkspaceEscape(
                "coding CLI workspace is not an existing directory",
            )
        return resolved

    def _provider(self, provider: str) -> CodingCLIProvider:
        # 逻辑说明：provider 必须同时存在于内建表和部署 allowlist，避免任意可执行名注入。
        if provider not in self._allowed or provider not in _PROVIDERS:
            raise CodingCLIProviderRejected(
                f"coding CLI provider {provider!r} is not allowlisted",
            )
        return cast(CodingCLIProvider, provider)

    def _find_executable(
        self,
        provider: str,
    ) -> Path | None:
        # 逻辑说明：只在 trusted directory 查找常规文件，并在 Unix 上验证执行权限。
        names = (
            (provider, f"{provider}.exe")
            if os.name == "nt"
            else (provider,)
        )
        for name in names:
            candidate = self._trusted_directory / name
            if not candidate.is_file():
                continue
            resolved = candidate.resolve()
            if (
                resolved != self._trusted_directory
                and not resolved.is_relative_to(self._trusted_directory)
            ):
                continue
            if os.name != "nt" and not os.access(resolved, os.X_OK):
                continue
            return resolved
        return None

    def _safe_environment(
        self,
        provider: CodingCLIProvider,
    ) -> dict[str, str]:
        # 逻辑说明：子进程只继承基础变量和该 provider 所需凭据，隔离其余 Manager Secret。
        allowed = _BASE_ENVIRONMENT | _PROVIDER_ENVIRONMENT[provider]
        return {
            name: value
            for name, value in self._environment.items()
            if name.upper() in allowed and value
        }


async def _feed(
    writer: asyncio.StreamWriter,
    value: bytes,
) -> None:
    # 逻辑说明：写完 stdin 后无论断管与否都关闭 writer，避免子进程一直等待 EOF。
    try:
        writer.write(value)
        await writer.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        writer.close()
        with contextlib.suppress(
            BrokenPipeError,
            ConnectionResetError,
        ):
            await writer.wait_closed()


async def _read_bounded(
    reader: asyncio.StreamReader,
    limit: int,
) -> tuple[bytes, bool]:
    # 逻辑说明：持续排空 pipe 防止死锁，但内存仅保留 limit 字节并单独标记截断。
    captured = bytearray()
    observed = 0
    while True:
        chunk = await reader.read(16 * 1024)
        if not chunk:
            break
        observed += len(chunk)
        remaining = limit - len(captured)
        if remaining > 0:
            captured.extend(chunk[:remaining])
    return bytes(captured), observed > limit


async def _terminate_process_group(
    process: asyncio.subprocess.Process,
) -> None:
    # 逻辑说明：先温和终止并短等，仍未退出再强杀整个组，避免遗留 CLI 子孙进程。
    if process.returncode is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        with contextlib.suppress(ProcessLookupError):
            _kill_process_group(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=1)
        return
    except TimeoutError:
        pass
    if os.name == "nt":
        process.kill()
    else:
        with contextlib.suppress(ProcessLookupError):
            _kill_process_group(
                process.pid,
                cast(int, getattr(signal, "SIGKILL", 9)),
            )
    await process.wait()


def _kill_process_group(pid: int, sig: int) -> None:
    # 逻辑说明：通过动态取得的 POSIX killpg 向整个进程组发送信号，连同 CLI 派生子进程一起终止。
    killpg = cast(Callable[[int, int], None], vars(os)["killpg"])
    killpg(pid, sig)


async def _settle(*tasks: asyncio.Task[Any]) -> None:
    # 逻辑说明：取消未完成的 pipe task 并吞并其收尾异常，保留最初 timeout/cancel 原因。
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _safe_output(value: bytes, secrets: tuple[str, ...]) -> str:
    # 逻辑说明：容错解码、去 ANSI、替换已知 secret 和常见凭据模式后才交给 Agent。
    text = value.decode("utf-8", errors="replace")
    text = _ANSI_ESCAPE.sub("", text)
    for secret in secrets:
        if len(secret) >= 4:
            text = text.replace(secret, "[REDACTED]")
    return _CREDENTIAL_TEXT.sub(
        lambda match: match.group("prefix") + "[REDACTED]",
        text,
    ).strip()
