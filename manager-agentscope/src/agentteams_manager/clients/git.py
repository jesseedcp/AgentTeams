"""Shell-free parsing and execution of constrained Git delegations.

解析并执行受约束的 Git 委托，避免让 Agent 获得任意 shell。

请求先被拆成受支持的 Git 操作、仓库根和显式参数，再由固定 argv 执行。路径必须落在
允许的仓库内，高风险写操作还受上层确认和 processing lease 保护。本模块只报告经过
裁剪的 Git 回执，不把可能含凭据的远端 URL 或完整进程环境暴露给模型。
"""

from __future__ import annotations

import configparser
import re
import shlex
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from agentteams_manager.clients.process import ProcessResult


class GitError(RuntimeError):
    """Base error for Git delegation."""


class InvalidGitRequest(GitError):
    """A request is malformed or includes a denied Git capability."""


class WorkspaceEscape(GitError):
    """A workspace or path argument escapes the task workspace."""


class GitOperation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    argv: tuple[str, ...] = Field(min_length=2)
    risk: Literal["low", "medium", "high"]


class GitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(pattern=r"^task-[A-Za-z0-9-]+$")
    workspace: Path
    operations: tuple[GitOperation, ...] = Field(min_length=1)
    context: str | None = None

    @property
    def requires_confirmation(self) -> bool:
        return any(operation.risk == "high" for operation in self.operations)


class GitCommandReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


class GitReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    success: bool
    commands: tuple[GitCommandReceipt, ...]


class GitProcessPort(Protocol):
    async def run(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        timeout: float,
    ) -> ProcessResult: ...


class GitRequestParser:
    """Parse the exact Worker protocol without evaluating shell syntax."""

    _HEADER = re.compile(
        r"^(?:@\S+\s+)?(?P<task>task-[A-Za-z0-9-]+)"
        r"\s+git-request:\s*$",
    )
    _ALLOWED_COMMANDS = frozenset(
        {
            "add",
            "am",
            "apply",
            "archive",
            "bisect",
            "blame",
            "branch",
            "bundle",
            "cat-file",
            "checkout",
            "cherry-pick",
            "clean",
            "clone",
            "commit",
            "config",
            "describe",
            "diff",
            "fetch",
            "format-patch",
            "grep",
            "init",
            "log",
            "ls-files",
            "ls-tree",
            "merge",
            "mv",
            "notes",
            "pull",
            "push",
            "rebase",
            "reflog",
            "remote",
            "reset",
            "restore",
            "revert",
            "rev-parse",
            "rm",
            "shortlog",
            "show",
            "stash",
            "status",
            "submodule",
            "switch",
            "tag",
            "worktree",
        },
    )

    @classmethod
    def parse(cls, message: str) -> GitRequest:
        # 逻辑说明：按固定区段解析 task/workspace/operations/context，拒绝乱序或额外字段。
        lines = message.splitlines()
        if not lines:
            raise InvalidGitRequest("git-request block is empty")
        header = cls._HEADER.fullmatch(lines[0].strip())
        if header is None:
            raise InvalidGitRequest(
                "git-request must start with a task ID header",
            )
        workspace: str | None = None
        operation_lines: list[str] = []
        context_lines: list[str] = []
        section = "fields"
        saw_operations = False
        saw_context_end = False
        for raw_line in lines[1:]:
            stripped = raw_line.strip()
            if stripped == "---CONTEXT---":
                if section != "operations":
                    raise InvalidGitRequest("context marker is out of order")
                section = "context"
                continue
            if stripped == "---END---":
                if section != "context":
                    raise InvalidGitRequest("end marker is out of order")
                section = "ended"
                saw_context_end = True
                continue
            if section == "ended":
                if stripped:
                    raise InvalidGitRequest("content follows ---END---")
                continue
            if section == "context":
                context_lines.append(raw_line)
                continue
            if stripped.startswith("workspace:"):
                if section != "fields" or workspace is not None:
                    raise InvalidGitRequest("workspace must appear exactly once")
                workspace = stripped.removeprefix("workspace:").strip()
                if not workspace:
                    raise InvalidGitRequest("workspace must not be empty")
                continue
            if stripped == "operations:":
                if workspace is None or saw_operations:
                    raise InvalidGitRequest(
                        "operations must follow one workspace",
                    )
                saw_operations = True
                section = "operations"
                continue
            if section == "operations":
                if not raw_line.startswith("  - "):
                    if not stripped:
                        continue
                    raise InvalidGitRequest(
                        "each operation must use an indented '- ' entry",
                    )
                operation_lines.append(raw_line[4:].strip())
                continue
            if stripped:
                raise InvalidGitRequest(f"unknown git-request field: {stripped}")
        if section == "context" and not saw_context_end:
            raise InvalidGitRequest("context block is missing ---END---")
        if workspace is None or not saw_operations or not operation_lines:
            raise InvalidGitRequest(
                "git-request requires workspace and operations",
            )
        return GitRequest(
            task_id=header.group("task"),
            workspace=Path(workspace),
            operations=tuple(
                cls.parse_operation(line) for line in operation_lines
            ),
            context=(
                "\n".join(context_lines).strip()
                if context_lines
                else None
            ),
        )

    @classmethod
    def parse_operation(cls, command: str) -> GitOperation:
        # 逻辑说明：shlex 只负责 token 化而不执行；随后拒绝 shell operator、response file 和逃逸 flag。
        if not command.strip() or "\n" in command or "\r" in command:
            raise InvalidGitRequest("Git operation must be one line")
        if "`" in command or "$(" in command:
            raise InvalidGitRequest("shell substitution is not allowed")
        lexer = shlex.shlex(
            command,
            posix=True,
            punctuation_chars=True,
        )
        lexer.whitespace_split = True
        lexer.commenters = ""
        try:
            argv = tuple(lexer)
        except ValueError as exc:
            raise InvalidGitRequest("Git operation has invalid quoting") from exc
        if any(
            token and all(character in "();<>|&" for character in token)
            for token in argv
        ):
            raise InvalidGitRequest("shell operators are not allowed")
        if len(argv) < 2 or argv[0] != "git":
            raise InvalidGitRequest("operation executable must be git")
        if any(token.startswith("@") for token in argv[1:]):
            raise InvalidGitRequest("response-file arguments are not allowed")
        cls._reject_global_escape_flags(argv)
        subcommand = argv[1]
        if subcommand.startswith("-") or subcommand not in cls._ALLOWED_COMMANDS:
            raise InvalidGitRequest(
                f"Git subcommand {subcommand!r} is not allowed",
            )
        if any("ext::" in token.casefold() for token in argv):
            raise InvalidGitRequest("ext:: transports are never allowed")
        if subcommand == "push" and "--mirror" in argv:
            raise InvalidGitRequest(
                "overwriting an entire remote repository is denied",
            )
        cls._reject_execution_options(argv)
        cls._validate_config(argv)
        risk = cls._risk(argv)
        return GitOperation(argv=argv, risk=risk)

    @staticmethod
    def _reject_global_escape_flags(argv: tuple[str, ...]) -> None:
        # 逻辑说明：扫描全局参数并拒绝可改目录、配置或仓库边界的 escape hatch，防止离开受控 workspace。
        denied_exact = {
            "-C",
            "--git-dir",
            "--work-tree",
            "--namespace",
            "-c",
            "--config",
            "--config-env",
            "--exec-path",
            "--separate-git-dir",
            "--template",
        }
        denied_prefixes = (
            "--git-dir=",
            "--work-tree=",
            "--namespace=",
            "--config=",
            "--config-env=",
            "--exec-path=",
            "--separate-git-dir=",
            "--template=",
        )
        for token in argv[1:]:
            if token in denied_exact or token.startswith(denied_prefixes):
                raise InvalidGitRequest(
                    f"Git path/config escape flag is denied: {token}",
                )

    @staticmethod
    def _reject_execution_options(argv: tuple[str, ...]) -> None:
        # 逻辑说明：拒绝会启动外部程序、指定 helper 或注入命令的 Git 选项，封住隐式代码执行面。
        denied_exact = {
            "--exec",
            "--upload-pack",
            "--receive-pack",
            "--gpg-sign",
            "-S",
        }
        denied_prefixes = (
            "--exec=",
            "--upload-pack=",
            "--receive-pack=",
            "--gpg-sign=",
        )
        if any(
            token in denied_exact or token.startswith(denied_prefixes)
            for token in argv[2:]
        ):
            raise InvalidGitRequest(
                "Git options that invoke external programs are denied",
            )
        if argv[1] == "bisect" and len(argv) > 2 and argv[2] == "run":
            raise InvalidGitRequest("git bisect run is denied")
        if (
            argv[1] == "submodule"
            and len(argv) > 2
            and argv[2] == "foreach"
        ):
            raise InvalidGitRequest("git submodule foreach is denied")

    @staticmethod
    def _validate_config(argv: tuple[str, ...]) -> None:
        # 逻辑说明：config 仅允许本地作用域与安全键，禁止 system/global 以及可承载外部命令的配置。
        if argv[1] != "config":
            return
        if "--global" in argv or "--system" in argv:
            raise InvalidGitRequest("only local Git config is permitted")
        if "--file" in argv or any(
            token.startswith("--file=") for token in argv
        ):
            raise InvalidGitRequest("external Git config files are denied")
        if "--local" not in argv:
            raise InvalidGitRequest("Git config must explicitly use --local")
        lowered = " ".join(argv[2:]).casefold()
        denied = (
            "alias.",
            "credential.",
            "core.attributesfile",
            "core.hookspath",
            "core.sshcommand",
            "core.fsmonitor",
            "core.pager",
            "diff.external",
            "filter.",
            "include.",
            "protocol.",
            "uploadpack",
            "receivepack",
        )
        if any(value in lowered for value in denied):
            raise InvalidGitRequest("execution-bearing Git config is denied")

    @staticmethod
    def _risk(argv: tuple[str, ...]) -> Literal["low", "medium", "high"]:
        # 逻辑说明：结合子命令和危险参数计算风险级别，供调用侧选择是否要求管理员确认。
        command = argv[1]
        args = argv[2:]
        lowered = {argument.casefold() for argument in args}
        if command == "push" and (
            {"--force", "-f", "--force-with-lease", "--delete"} & lowered
            or any(argument.startswith(":") for argument in args)
            or any(argument.startswith("+") for argument in args)
            or any(
                argument.startswith("--force-with-lease=")
                for argument in args
            )
        ):
            return "high"
        if command == "reset" and "--hard" in lowered:
            return "high"
        if command == "clean":
            return "high"
        if command == "rebase":
            return "high"
        if command in {"branch", "tag"} and (
            {"-d", "-D", "-f", "--delete", "--force"} & set(args)
        ):
            return "high"
        if command in {"checkout", "switch"} and (
            {"-B", "-C"} & set(args)
        ):
            return "high"
        if command in {"checkout", "restore"} and (
            "--" in args
            or {"-f", "--force"} & lowered
            or command == "restore" and "--staged" not in lowered
        ):
            return "high"
        if command == "remote" and args and args[0] in {
            "remove",
            "rm",
            "set-url",
            "rename",
        }:
            return "high"
        if command == "submodule" and args and args[0] in {
            "deinit",
            "set-url",
        }:
            return "high"
        if command == "worktree" and args and args[0] in {
            "move",
            "prune",
            "remove",
        }:
            return "high"
        if command == "stash" and args and args[0] in {"clear", "drop"}:
            return "high"
        if command == "reflog" and args and args[0] in {"delete", "expire"}:
            return "high"
        if command == "notes" and args and args[0] in {"prune", "remove"}:
            return "high"
        if command == "config":
            joined = " ".join(args).casefold()
            if "remote." in joined and ".url" in joined:
                return "high"
            return "medium"
        if command in {
            "commit",
            "merge",
            "cherry-pick",
            "revert",
            "pull",
            "push",
            "rm",
            "mv",
            "stash",
            "switch",
        }:
            return "medium"
        return "low"


class GitClient:
    """Execute validated operations through an allowlisted process runner."""

    def __init__(self, process: GitProcessPort) -> None:
        # 逻辑说明：注入唯一受限进程边界，所有已解析 Git 操作最终都通过它执行而不经过 shell。
        self._process = process

    @staticmethod
    def validate_workspace(
        task_workspace_root: Path,
        requested: Path,
    ) -> Path:
        # 逻辑说明：resolve 后必须位于该 Task workspace 根内，阻止 .. 或符号链接逃逸。
        root = task_workspace_root.resolve()
        candidate = requested.resolve()
        if not candidate.is_relative_to(root):
            raise WorkspaceEscape(
                f"workspace escapes task root: {requested}",
            )
        return candidate

    async def run(
        self,
        workspace: Path,
        operations: tuple[GitOperation, ...],
    ) -> GitReceipt:
        # 逻辑说明：逐项验证并以禁用 hooks/ext protocol 的固定 argv 执行，首个失败即返回累计回执。
        workspace = workspace.resolve(strict=True)
        if not workspace.is_dir():
            raise InvalidGitRequest("Git workspace must be a directory")
        self._inspect_repository_config(workspace)
        receipts: list[GitCommandReceipt] = []
        for operation in operations:
            self._validate_operation_paths(workspace, operation)
            self._deny_existing_bare_init(workspace, operation)
            argv = (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "protocol.ext.allow=never",
                *operation.argv[1:],
            )
            result = await self._process.run(
                argv,
                cwd=workspace,
                timeout=self._timeout_for(operation),
            )
            receipt = GitCommandReceipt(
                argv=operation.argv,
                returncode=result.returncode,
                stdout=_decode_output(result.stdout),
                stderr=_decode_output(result.stderr),
            )
            receipts.append(receipt)
            if result.returncode != 0:
                return GitReceipt(
                    success=False,
                    commands=tuple(receipts),
                )
            self._inspect_repository_config(workspace)
        return GitReceipt(success=True, commands=tuple(receipts))

    @staticmethod
    def _timeout_for(operation: GitOperation) -> float:
        return (
            600
            if operation.argv[1] in {"clone", "fetch", "pull", "push"}
            else 120
        )

    @staticmethod
    def _validate_operation_paths(
        workspace: Path,
        operation: GitOperation,
    ) -> None:
        # 逻辑说明：收集所有可能表示本地路径的 option/参数，resolve 后拒绝越出 workspace。
        path_options = {
            "--directory",
            "--exclude-from",
            "--file",
            "--object-directory",
            "--output",
            "--pathspec-from-file",
            "--reference",
            "--reference-if-able",
            "-F",
            "-o",
        }
        arguments = operation.argv[2:]
        path_values: list[str] = []
        skip_next = False
        for index, token in enumerate(arguments):
            if skip_next:
                skip_next = False
                continue
            if token in path_options:
                if index + 1 >= len(arguments):
                    raise InvalidGitRequest(
                        f"Git path option has no value: {token}",
                    )
                path_values.append(arguments[index + 1])
                skip_next = True
                continue
            option, separator, value = token.partition("=")
            if separator and option in path_options:
                path_values.append(value)
                continue
            if (
                token.startswith(("/", "./", "../", "\\"))
                or re.match(r"^[A-Za-z]:[\\/]", token)
            ):
                path_values.append(token)
        for value in path_values:
            if "://" in value or re.match(r"^[^/@]+@[^:]+:", value):
                continue
            candidate = (
                Path(value)
                if Path(value).is_absolute()
                else workspace / value
            ).resolve()
            if not candidate.is_relative_to(workspace):
                raise WorkspaceEscape(
                    f"Git path escapes workspace: {value}",
                )

    @staticmethod
    def _deny_existing_bare_init(
        workspace: Path,
        operation: GitOperation,
    ) -> None:
        # 逻辑说明：拒绝在已存在目标上初始化 bare 仓库，避免重定义受控 workspace 边界。
        if operation.argv[1] != "init" or "--bare" not in operation.argv:
            return
        positional = [
            token
            for token in operation.argv[2:]
            if not token.startswith("-")
        ]
        target = (
            (workspace / positional[-1]).resolve()
            if positional
            else workspace
        )
        if target.exists():
            raise InvalidGitRequest(
                "git init --bare over an existing path is denied",
            )

    @staticmethod
    def _inspect_repository_config(workspace: Path) -> None:
        # 逻辑说明：解析仓库 config 并拒绝 alias/hooks/filter/credential 等可执行或敏感设置。
        candidates = [workspace / ".git" / "config", workspace / "config"]
        config_path = next(
            (path for path in candidates if path.is_file()),
            None,
        )
        if config_path is not None:
            parser = configparser.ConfigParser(
                interpolation=None,
                strict=False,
            )
            try:
                parser.read(config_path, encoding="utf-8")
            except (configparser.Error, UnicodeError) as exc:
                raise InvalidGitRequest("Git config is not safely parseable") from exc
            for section in parser.sections():
                lowered_section = section.casefold()
                if lowered_section == "alias" or lowered_section.startswith(
                    ("filter ", "diff ", "merge "),
                ):
                    raise InvalidGitRequest(
                        "execution-bearing repository config is denied",
                    )
                for key, value in parser.items(section):
                    combined = f"{lowered_section}.{key.casefold()}"
                    if any(
                        fragment in combined
                        for fragment in (
                            "hookspath",
                            "sshcommand",
                            "fsmonitor",
                            "credential",
                            "pager",
                            "uploadpack",
                            "receivepack",
                        )
                    ) or value.lstrip().startswith("!"):
                        raise InvalidGitRequest(
                            "execution-bearing repository config is denied",
                        )
        modules = workspace / ".gitmodules"
        if modules.is_file():
            text = modules.read_text(encoding="utf-8")
            if re.search(r"(?im)^\s*update\s*=\s*!", text):
                raise InvalidGitRequest(
                    "executable submodule update commands are denied",
                )


def _decode_output(value: bytes) -> str:
    # 逻辑说明：以替换非法字节的方式解码 Git 子进程输出，并只保留末尾 8 KiB，避免错误日志无限占用 Manager 上下文。
    decoded = value.decode("utf-8", errors="replace")
    return decoded[-8192:]
