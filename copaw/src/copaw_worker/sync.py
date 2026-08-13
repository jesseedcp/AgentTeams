"""MinIO file sync for copaw-worker.

All MinIO operations use the `mc` CLI (MinIO Client).

File Sync Design Principle:

  Remote -> Local (pull):
    - ``mirror_all`` restores the remote worker prefix to the local sync root on
      startup, excluding credentials.
    - ``sync_loop`` refreshes controller-managed worker files during runtime:
      ``openclaw.json``, ``config/mcporter.json``, and ``skills/``.
    - Shared-data pulls remain explicit (for example via ``filesync(action="pull")``).

  Local -> Remote (push):
    - ``push_loop`` periodically uploads eligible local changes back to the
      remote worker prefix so user/agent data is preserved as much as possible.
    - Explicit shared-data pushes use ``filesync(action="push")``.
    - Cache/temp files, local tool state, auto-mirrored shared content, and
      duplicate local projections are excluded from the background push.
"""

# 初学者导读：本模块明确区分三套东西：Controller 管理的标准配置、CoPaw
# runtime 的本地投影、以及多个成员可见的 shared 目录。拉取用于恢复/接收新配置，
# 推送用于保存 Worker 产生的记忆和产物。排除凭据、缓存与投影副本能防止泄密和
# 同一个文件在“拉取后又推回”之间无限循环。
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Protocol

from copaw_worker.bridge import bridge_runtime_to_standard

logger = logging.getLogger(__name__)

def _storage_alias() -> str:
    # 逻辑说明：`_storage_alias` 接收 当前对象/进程状态，执行 MinIO/OSS 与本地工作区同步 中的“storage alias”步骤，返回 str；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    explicit = os.environ.get("AGENTTEAMS_STORAGE_ALIAS")
    if explicit:
        return explicit
    prefix = os.environ.get("AGENTTEAMS_STORAGE_PREFIX") or ""
    if "/" in prefix:
        return prefix.split("/", 1)[0]
    return "agentteams"


# mc alias name used for this worker session
_MC_ALIAS = _storage_alias()


class HealthStateProtocol(Protocol):
    def update(
        self,
        component: str,
        healthiness: str,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> Any:
        ...


class BridgeRuntimeError(RuntimeError):
    """Runtime-to-standard bridge failed before storage persistence."""


@dataclass(frozen=True)
class SharedPath:
    """Resolved local and remote paths for a shared file operation."""

    kind: str
    subpath: str
    local: Path
    remote: str


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge override into base (override wins leaf conflicts)."""
    # 逻辑说明：`_deep_merge` 接收 base、override，执行 MinIO/OSS 与本地工作区同步 中的“deep merge”步骤，返回 dict[str, Any]；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _merge_openclaw_config(remote_text: str, local_text: str) -> str:
    """Merge remote and local openclaw.json (local-first, same as hermes_worker).

    Rules:
      - Base: local; tools, agents, mcp, and other keys not listed below stay local.
      - models, gateway: replaced from remote when present.
      - channels: deep merge with remote winning leaf conflicts; local-only keys kept.
      - channels.matrix.accessToken: local wins (Worker re-login after restart).
      - plugins.entries: deep merge with local winning on shared keys; load.paths union.
    """
    # 逻辑说明：`_merge_openclaw_config` 接收 remote_text、local_text，合并openclaw 配置，返回 str；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    remote = json.loads(remote_text)
    local = json.loads(local_text)
    merged: dict[str, Any] = dict(local)

    if remote.get("models") is not None:
        merged["models"] = remote["models"]
    if remote.get("gateway") is not None:
        merged["gateway"] = remote["gateway"]

    r_channels = remote.get("channels") or {}
    l_channels = local.get("channels") or {}
    if r_channels or l_channels:
        merged["channels"] = _deep_merge(dict(l_channels), dict(r_channels))
        l_token = local.get("channels", {}).get("matrix", {}).get("accessToken")
        if l_token:
            merged.setdefault("channels", {}).setdefault("matrix", {})[
                "accessToken"
            ] = l_token

    r_plugins = remote.get("plugins")
    l_plugins = local.get("plugins")
    if r_plugins or l_plugins:
        r_plugins = dict(r_plugins or {})
        l_plugins = dict(l_plugins or {})
        out_plugins: dict[str, Any] = dict(l_plugins)
        r_entries = r_plugins.get("entries") or {}
        l_entries = l_plugins.get("entries") or {}
        if r_entries or l_entries:
            out_plugins["entries"] = _deep_merge(dict(r_entries), dict(l_entries))
        r_paths = r_plugins.get("load", {}).get("paths")
        l_paths = l_plugins.get("load", {}).get("paths")
        if r_paths is not None or l_paths is not None:
            out_load = dict(l_plugins.get("load") or {})
            out_load["paths"] = sorted(set((r_paths or []) + (l_paths or [])))
            out_plugins["load"] = out_load
        merged["plugins"] = out_plugins

    return json.dumps(merged, indent=2, ensure_ascii=False)


def _preview_text(value: str | None, limit: int = 2000) -> str:
    # 逻辑说明：`_preview_text` 接收 value、limit，执行 MinIO/OSS 与本地工作区同步 中的“preview 文本”步骤，返回 str；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    if not value:
        return ""
    if len(value) <= limit:
        return value
    return value[:limit] + "...<truncated>"


def _redact_url_userinfo(value: str) -> str:
    # 逻辑说明：`_redact_url_userinfo` 接收 value，执行 MinIO/OSS 与本地工作区同步 中的“redact url userinfo”步骤，返回 str；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    if "://" not in value:
        return value
    scheme, rest = value.split("://", 1)
    if "@" not in rest:
        return value
    return f"{scheme}://<redacted>@{rest.split('@', 1)[1]}"


def _redacted_mc_command(cmd: list[str]) -> list[str]:
    # 逻辑说明：`_redacted_mc_command` 接收 cmd，执行 MinIO/OSS 与本地工作区同步 中的“redacted mc command”步骤，返回 list[str]；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    redacted = [_redact_url_userinfo(part) for part in cmd]
    args = redacted[1:]
    if len(args) >= 6 and args[0] == "alias" and args[1] == "set":
        redacted[5] = "<redacted-access-key>"
        redacted[6] = "<redacted-secret-key>"
    return redacted


def _looks_like_remote_directory_error(exc: subprocess.CalledProcessError) -> bool:
    """Return True when mc cp failed because the remote path is a prefix."""
    # 逻辑说明：`_looks_like_remote_directory_error` 接收 exc，执行 MinIO/OSS 与本地工作区同步 中的“looks like 远端 目录 error”步骤，返回 bool；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    stderr = str(exc.stderr or "")
    stdout = str(exc.stdout or "")
    text = f"{stderr}\n{stdout}"
    return "--recursive flag is required" in text


def _team_storage_name_from_worker_team(bucket: str, team_ref: str) -> str:
    """Derive the temporary storage team name from a WorkerResponse team ref."""
    # 逻辑说明：`_team_storage_name_from_worker_team` 接收 bucket、team_ref，
    # 执行 MinIO/OSS 与本地工作区同步 中的“团队 storage 名称 from Worker 团队”步骤，返回 str；
    #
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    team_name = team_ref.strip()
    bucket_name = (bucket or "").strip()
    prefixes = [bucket_name]
    if bucket_name.startswith("agentteams-"):
        prefixes.append(bucket_name.removeprefix("agentteams-"))

    for prefix in prefixes:
        if prefix and team_name.startswith(f"{prefix}-"):
            return team_name[len(prefix) + 1 :]
    return team_name


def _mc(
    *args: str,
    check: bool = True,
    warn_on_error: bool = True,
    log_output: bool = True,
) -> subprocess.CompletedProcess:
    """Run an mc command and return the result."""
    # 逻辑说明：`_mc` 接收 check、warn_on_error、log_output、*args，执行经过日志脱敏的 mc 子命令，并按 check 策略返回结果或抛错，
    # 返回 subprocess.CompletedProcess；
    #
    # 会访问网络服务、会执行外部命令。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    mc_bin = shutil.which("mc")
    if not mc_bin:
        raise RuntimeError("mc binary not found on PATH. Please install mc first.")
    cmd = [mc_bin, *args]
    redacted_cmd = _redacted_mc_command(cmd)
    logger.info("mc cmd: %s", " ".join(redacted_cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    except subprocess.CalledProcessError as exc:
        exc.cmd = redacted_cmd
        log = logger.warning if warn_on_error else logger.debug
        log(
            "mc command failed returncode=%s cmd=%s stdout=%r stderr=%r",
            exc.returncode,
            " ".join(redacted_cmd),
            _preview_text(exc.stdout),
            _preview_text(exc.stderr),
        )
        raise
    if log_output:
        logger.info("mc stdout (%d chars): %r", len(result.stdout), result.stdout[:200])
        if result.stderr:
            logger.info("mc stderr: %r", result.stderr[:200])
    return result


def _looks_like_missing_object_error(stderr: str | None) -> bool:
    # 逻辑说明：`_looks_like_missing_object_error` 接收 stderr，
    # 执行 MinIO/OSS 与本地工作区同步 中的“looks like missing object error”步骤，返回 bool；
    #
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    text = stderr or ""
    return "Object does not exist" in text or "The specified key does not exist" in text


_STARTUP_SYNC_FILES = (
    "openclaw.json",
    "AGENTS.md",
    "SOUL.md",
    "HEARTBEAT.md",
    "config/mcporter.json",
    "mcporter-servers.json",
)


class FileSync:
    """使用 MinIO Client 在一名 Worker 的本地目录与远端前缀间同步。

    每次同步都先确保本 Worker 的 alias 与 prefix 正确；worker_name 是私有状态的
    隔离边界，shared prefix 则是显式协作边界。两者写反会造成成员间会话串线。

    MinIO file sync using mc CLI.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        worker_name: str,
        worker_cr_name: Optional[str] = None,
        secure: bool = False,
        local_dir: Optional[Path] = None,
        shared_dir: Optional[Path] = None,
        global_shared_dir: Optional[Path] = None,
    ) -> None:
        # 逻辑说明：`__init__` 接收存储连接、Worker 身份及本地/共享目录，
        # 规范化各条远端前缀和本地同步根目录，返回 None；
        # 会读写本地文件、会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        self.endpoint = endpoint.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.worker_name = worker_name
        self.worker_cr_name = worker_cr_name or worker_name
        self._secure = secure
        configured_working_dir = os.environ.get("COPAW_WORKING_DIR")
        if local_dir is not None:
            self.local_dir = local_dir
        elif configured_working_dir:
            self.local_dir = Path(configured_working_dir).parent
        else:
            self.local_dir = Path.home() / ".copaw-worker" / worker_name
        self.local_dir.mkdir(parents=True, exist_ok=True)
        workspace_dir = self.local_dir / ".copaw" / "workspaces" / "default"
        self.shared_dir = shared_dir or workspace_dir / "shared"
        self.global_shared_dir = (
            global_shared_dir or workspace_dir / "global-shared"
        )
        self._prefix = f"agents/{worker_name}"
        self._alias_set = False
        runtime = os.environ.get("AGENTTEAMS_RUNTIME")
        self._cloud_mode = runtime == "aliyun"
        self._k8s_mode = runtime == "k8s"
        self._worker_info: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # mc alias management
    # ------------------------------------------------------------------

    def _refresh_cloud_credentials(self) -> None:
        """Refresh STS credentials by calling the shared shell function.

        The shell function is lazy: it checks /tmp/mc-oss-credentials.env
        and only hits the STS endpoint when the token is within 10 minutes
        of expiring.  Cheap no-op when credentials are still valid.
        """
        # 逻辑说明：`_refresh_cloud_credentials` 接收 当前对象/进程状态，调用凭据提供器刷新短期对象存储凭据与 endpoint，返回 None；
        # 会执行外部命令、会修改进程环境。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        result = subprocess.run(
            ["bash", "-c",
             "source /opt/agentteams/scripts/lib/agentteams-env.sh && "
             "ensure_mc_credentials && "
             f"_mc_host_var=MC_HOST_{_MC_ALIAS} && "
             "printf '%s' \"${!_mc_host_var}\""],
            capture_output=True, text=True, check=True,
        )
        mc_host = result.stdout.strip()
        if mc_host:
            os.environ[f"MC_HOST_{_MC_ALIAS}"] = mc_host
        else:
            logger.warning("ensure_mc_credentials returned empty MC_HOST_%s", _MC_ALIAS)

    def _ensure_alias(self) -> None:
        """Set up mc alias, refreshing STS credentials in cloud mode.

        Cloud mode (RRSA/STS): refresh credentials before every mc batch
        via the shared shell function (lazy, no-op when token is valid).
        Local mode: set mc alias once with static credentials.
        """
        # 逻辑说明：`_ensure_alias` 接收 当前对象/进程状态，刷新云凭据并配置当前 Worker 专用 mc alias，返回 None；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        runtime = os.environ.get("AGENTTEAMS_RUNTIME", "<unset>")
        mc_host_set = bool(os.environ.get(f"MC_HOST_{_MC_ALIAS}"))
        controller_url = os.environ.get("AGENTTEAMS_CONTROLLER_URL", "<unset>")
        logger.info(
            "_ensure_alias: runtime=%s cloud_mode=%s k8s_mode=%s endpoint=%s bucket=%s worker_name=%s access_key=%s alias_set=%s mc_host_set=%s controller_url=%s",
            runtime,
            self._cloud_mode,
            self._k8s_mode,
            _redact_url_userinfo(self.endpoint),
            self.bucket,
            self.worker_name,
            "<redacted>",
            self._alias_set,
            mc_host_set,
            controller_url,
        )
        if self._k8s_mode:
            logger.info("_ensure_alias: k8s mode, skipping mc alias set (mc-wrapper handles credentials)")
            self._alias_set = True
            return
        if self._cloud_mode:
            logger.info("_ensure_alias: credential path=sts, refreshing MC_HOST_%s", _MC_ALIAS)
            self._refresh_cloud_credentials()
            self._alias_set = True
            logger.info(
                "_ensure_alias: sts credentials ready alias_set=%s mc_host_set=%s",
                self._alias_set,
                bool(os.environ.get(f"MC_HOST_{_MC_ALIAS}")),
            )
            return
        if self._alias_set:
            logger.info("_ensure_alias: credential path=static, alias already set")
            return
        # Local mode: static credentials, set alias once
        if self.endpoint.startswith("http"):
            url = self.endpoint
        else:
            scheme = "https" if self._secure else "http"
            url = f"{scheme}://{self.endpoint}"
        logger.info(
            "_ensure_alias: credential path=static, setting alias url=%s access_key=%s",
            _redact_url_userinfo(url),
            "<redacted>",
        )
        _mc("alias", "set", _MC_ALIAS, url, self.access_key, self.secret_key)
        self._alias_set = True
        logger.info("_ensure_alias: static alias ready alias_set=%s", self._alias_set)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _object_path(self, key: str) -> str:
        """Return full mc path: alias/bucket/key"""
        return f"{_MC_ALIAS}/{self.bucket}/{key}"

    def _cat(self, key: str) -> Optional[str]:
        """Download object content as text using mc cat."""
        # 逻辑说明：`_cat` 接收 key，读取单个远端对象；不存在时返回 None，其他 mc 错误继续抛出，返回 Optional[str]；
        # 会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        self._ensure_alias()
        try:
            result = _mc(
                "cat",
                self._object_path(key),
                check=False,
                log_output=False,
            )
        except Exception as exc:
            logger.debug("mc cat error for %s: %s", key, exc)
            return None
        if result.returncode == 0:
            return result.stdout
        if _looks_like_missing_object_error(result.stderr):
            logger.info("mc cat missing object for %s: %s", key, _preview_text(result.stderr))
            return None
        logger.warning(
            "mc cat failed returncode=%s key=%s stderr=%r",
            result.returncode,
            key,
            _preview_text(result.stderr),
        )
        return None

    def _ls(self, prefix: str) -> list[str]:
        """List objects under prefix, return list of relative names."""
        # 逻辑说明：`_ls` 接收 prefix，列出远端前缀并把 mc 输出规范化为对象路径列表，返回 list[str]；
        # 会执行外部命令、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        self._ensure_alias()
        try:
            result = _mc("ls", "--recursive", self._object_path(prefix), check=True)
            names = []
            for line in result.stdout.splitlines():
                # mc ls output: "2024-01-01 00:00:00   1234 filename"
                parts = line.strip().split()
                if parts:
                    names.append(parts[-1])
            return names
        except subprocess.CalledProcessError as exc:
            logger.debug("mc ls failed for %s: %s", prefix, exc.stderr)
            return []
        except Exception as exc:
            logger.debug("mc ls error for %s: %s", prefix, exc)
            return []

    def _pull_startup_files(self) -> list[str]:
        """Pull known startup files when mc mirror cannot stat the prefix."""
        # 逻辑说明：`_pull_startup_files` 接收 当前对象/进程状态，从远端拉取startup files，返回 list[str]；
        # 会读写本地文件、会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        changed: list[str] = []
        for rel_path in _STARTUP_SYNC_FILES:
            content = self._cat(f"{self._prefix}/{rel_path}")
            if content is None:
                continue
            local_path = self.local_dir / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(content)
            changed.append(rel_path)
        return changed

    def mirror_all(self) -> None:
        """Full mirror of the worker's MinIO prefix to local_dir.

        Called once at startup to restore all state (config, sessions, sync
        token, etc.) — mirrors the OpenClaw worker's ``mc mirror`` approach.
        After this, runtime Remote -> Local pulls are explicit; background sync
        only pushes eligible local changes via ``push_local``.
        """
        # 逻辑说明：`mirror_all` 接收 当前对象/进程状态，从远端 Worker 前缀恢复完整本地状态，同时排除凭据和不应覆盖内容，返回 None；
        # 会读写本地文件、会访问网络服务、会执行外部命令、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        runtime = os.environ.get("AGENTTEAMS_RUNTIME", "<unset>")
        controller_url = os.environ.get("AGENTTEAMS_CONTROLLER_URL", "<unset>")
        logger.info(
            "mirror_all: preparing primary mirror runtime=%s cloud_mode=%s k8s_mode=%s endpoint=%s bucket=%s worker_name=%s access_key=%s alias_set=%s mc_host_set=%s controller_url=%s",
            runtime,
            self._cloud_mode,
            self._k8s_mode,
            _redact_url_userinfo(self.endpoint),
            self.bucket,
            self.worker_name,
            "<redacted>",
            self._alias_set,
            bool(os.environ.get(f"MC_HOST_{_MC_ALIAS}")),
            controller_url,
        )
        self._ensure_alias()
        remote = self._object_path(f"{self._prefix}/")
        local = str(self.local_dir) + "/"
        logger.info("mirror_all: primary mirror remote=%s local=%s", remote, local)
        primary_prefix_missing = False
        try:
            _mc("mirror", remote, local, "--overwrite",
                 "--exclude", "credentials/**", check=True)
            logger.info("mirror_all: full mirror completed from %s", remote)
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "mirror_all: primary mirror failed runtime=%s cloud_mode=%s k8s_mode=%s endpoint=%s bucket=%s worker_name=%s access_key=%s remote=%s local=%s mc_host_set=%s controller_url=%s stderr=%s",
                runtime,
                self._cloud_mode,
                self._k8s_mode,
                _redact_url_userinfo(self.endpoint),
                self.bucket,
                self.worker_name,
                "<redacted>",
                remote,
                local,
                bool(os.environ.get(f"MC_HOST_{_MC_ALIAS}")),
                controller_url,
                exc.stderr,
            )
            error_text = f"{exc.stderr or ''}\n{exc.stdout or ''}"
            if not _looks_like_missing_object_error(error_text):
                raise
            primary_prefix_missing = True
            logger.info(
                "mirror_all: primary mirror prefix missing; trying direct startup file pulls",
            )
            startup_changed = self._pull_startup_files()
            if startup_changed:
                logger.info(
                    "mirror_all: restored startup files after missing prefix: %s",
                    ", ".join(startup_changed),
                )

        if (
            primary_prefix_missing
            and not (self.local_dir / "openclaw.json").exists()
        ):
            raise RuntimeError(
                f"openclaw.json not found in MinIO for worker {self.worker_name}"
            )

        # Mirror shared/ — team members use teams/{team}/shared/, others use global shared/
        try:
            shared_remote = self._get_shared_remote()
        except RuntimeError:
            shared_remote = self._get_startup_shared_remote()
        shared_local = str(self.shared_dir) + "/"
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        logger.info("mirror_all: shared mirror remote=%s local=%s", shared_remote, shared_local)
        try:
            _mc("mirror", shared_remote, shared_local, "--overwrite", check=True)
            logger.info("mirror_all: shared/ mirror completed from %s", shared_remote)
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "mirror_all: shared/ mirror failed (non-fatal) remote=%s local=%s stderr=%s",
                shared_remote,
                shared_local,
                exc.stderr,
            )

        # Team Leader also gets global shared/ as global-shared/ (read-only, for Manager tasks)
        try:
            is_team_leader = self._is_team_leader()
        except RuntimeError:
            is_team_leader = False
        if is_team_leader:
            global_shared_remote = f"{_MC_ALIAS}/{self.bucket}/shared/"
            global_shared_local = str(self.global_shared_dir) + "/"
            self.global_shared_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "mirror_all: global-shared mirror remote=%s local=%s",
                global_shared_remote,
                global_shared_local,
            )
            try:
                _mc("mirror", global_shared_remote, global_shared_local, "--overwrite", check=True)
                logger.info("mirror_all: global-shared/ mirror completed")
            except subprocess.CalledProcessError as exc:
                logger.warning(
                    "mirror_all: global-shared/ mirror failed (non-fatal) remote=%s local=%s stderr=%s",
                    global_shared_remote,
                    global_shared_local,
                    exc.stderr,
                )


    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _get_worker_info(self) -> dict[str, Any]:
        """Return authoritative worker metadata from the AgentTeams controller."""
        # 逻辑说明：`_get_worker_info` 接收 当前对象/进程状态，读取Worker info，返回 dict[str, Any]；
        # 会执行外部命令、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        if self._worker_info is not None:
            return self._worker_info

        agentteams_bin = shutil.which("agt")
        if not agentteams_bin:
            raise RuntimeError("AgentTeams CLI not found; cannot resolve worker storage scope")

        try:
            result = subprocess.run(
                [agentteams_bin, "get", "workers", self.worker_cr_name, "-o", "json"],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            worker = json.loads(result.stdout)
        except Exception as exc:
            raise RuntimeError(
                f"failed to query worker metadata for {self.worker_cr_name}: {exc}",
            ) from exc

        if not isinstance(worker, dict):
            raise RuntimeError(f"invalid worker metadata for {self.worker_cr_name}")
        self._worker_info = worker
        return worker

    def _get_team_id(self) -> Optional[str]:
        """Resolve the temporary runtime/storage team name from worker metadata."""
        # 逻辑说明：`_get_team_id` 接收 当前对象/进程状态，读取团队 ID，返回 Optional[str]；会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；
        # 本函数不额外重试，避免掩盖持续故障。
        worker = self._get_worker_info()
        team_ref = worker.get("team")
        if not isinstance(team_ref, str) or not team_ref.strip():
            return None
        return _team_storage_name_from_worker_team(self.bucket, team_ref)

    def _is_team_leader(self) -> bool:
        """Check if this worker is a team leader according to the controller."""
        # 逻辑说明：`_is_team_leader` 接收 当前对象/进程状态，判断团队 Leader，返回 bool；会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        worker = self._get_worker_info()
        return worker.get("role") == "team_leader"

    def _get_shared_remote(self) -> str:
        """Return the MinIO remote path for shared/ directory.

        Team members sync from teams/{team}/shared/ instead of global shared/.
        Non-team workers sync from global shared/.
        """
        # 逻辑说明：`_get_shared_remote` 接收 当前对象/进程状态，读取共享路径 远端，返回 str；会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        team_id = self._get_team_id()
        if team_id:
            return f"{_MC_ALIAS}/{self.bucket}/teams/{team_id}/shared/"
        return f"{_MC_ALIAS}/{self.bucket}/shared/"

    def _get_startup_shared_remote(self) -> str:
        """Use the mirrored controller config only during bootstrap fallback."""
        # 逻辑说明：`_get_startup_shared_remote` 接收 当前对象/进程状态，读取startup 共享路径 远端，返回 str；
        # 会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        config_path = self.local_dir / "openclaw.json"
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return f"{_MC_ALIAS}/{self.bucket}/shared/"
        team_id = config.get("team_id") or config.get("teamId")
        if isinstance(team_id, str) and team_id.strip():
            team_name = _team_storage_name_from_worker_team(
                self.bucket,
                team_id,
            )
            return (
                f"{_MC_ALIAS}/{self.bucket}/teams/{team_name}/shared/"
            )
        return f"{_MC_ALIAS}/{self.bucket}/shared/"

    def _get_global_shared_remote(self) -> str:
        """Return the MinIO remote path for global-shared/ directory."""
        return f"{_MC_ALIAS}/{self.bucket}/shared/"

    def resolve_shared_path(self, path: str) -> SharedPath:
        """Resolve a user-facing shared path to local and remote paths."""
        # 逻辑说明：`resolve_shared_path` 接收 path，校验 shared 路径并解析其本地与远端位置，返回 SharedPath；
        # 会访问网络服务、会更新对象内存状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        raw = (path or "").strip()
        if not raw:
            raise ValueError("path is required")
        if raw.startswith("/") or "\\" in raw:
            raise ValueError("path must be a relative shared path")

        normalized = raw.strip("/")
        parts = Path(normalized).parts
        if not parts or any(part in ("", ".", "..") for part in parts):
            raise ValueError("path must not contain empty, '.', or '..' segments")

        if parts[0] == "shared":
            subpath = "/".join(parts[1:])
            local = self.shared_dir.joinpath(*parts[1:]) if len(parts) > 1 else self.shared_dir
            remote = self._get_shared_remote()
            if subpath:
                remote = f"{remote}{subpath}"
                if raw.endswith("/"):
                    remote += "/"
            return SharedPath("shared", subpath, local, remote)

        if parts[0] == "global-shared":
            subpath = "/".join(parts[1:])
            local = (
                self.global_shared_dir.joinpath(*parts[1:])
                if len(parts) > 1
                else self.global_shared_dir
            )
            remote = self._get_global_shared_remote()
            if subpath:
                remote = f"{remote}{subpath}"
                if raw.endswith("/"):
                    remote += "/"
            return SharedPath("global-shared", subpath, local, remote)

        raise ValueError("path must start with shared/ or global-shared/")

    def pull_shared_path(self, path: str) -> SharedPath:
        """Pull a shared path from MinIO into the local workspace."""
        # 逻辑说明：`pull_shared_path` 接收 path，解析 shared 路径后从对象存储拉取文件或目录，返回 SharedPath；
        # 会读写本地文件、会执行外部命令、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        resolved = self.resolve_shared_path(path)
        self._ensure_alias()
        if (path or "").strip().endswith("/"):
            resolved.local.mkdir(parents=True, exist_ok=True)
            _mc("mirror", resolved.remote, str(resolved.local) + "/", "--overwrite", check=True)
            return resolved

        resolved.local.parent.mkdir(parents=True, exist_ok=True)
        try:
            _mc("cp", resolved.remote, str(resolved.local), check=True)
        except subprocess.CalledProcessError as exc:
            if not _looks_like_remote_directory_error(exc):
                raise
            remote = resolved.remote if resolved.remote.endswith("/") else f"{resolved.remote}/"
            resolved.local.mkdir(parents=True, exist_ok=True)
            _mc("mirror", remote, str(resolved.local) + "/", "--overwrite", check=True)
        return resolved

    def push_shared_path(
        self,
        path: str,
        *,
        exclude: Optional[list[str]] = None,
    ) -> SharedPath:
        """Push a local shared path to MinIO."""
        # 逻辑说明：`push_shared_path` 接收 path、exclude，解析 shared 路径并把本地文件或目录上传到对象存储，返回 SharedPath；
        # 会更新对象内存状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        resolved = self.resolve_shared_path(path)
        if resolved.kind == "global-shared":
            raise ValueError("global-shared/ is read-only")
        if not resolved.local.exists():
            raise FileNotFoundError(f"local path does not exist: {resolved.local}")

        self._ensure_alias()
        if resolved.local.is_dir():
            remote = resolved.remote if resolved.remote.endswith("/") else f"{resolved.remote}/"
            args = ["mirror", str(resolved.local) + "/", remote, "--overwrite"]
            for item in exclude or []:
                args.extend(["--exclude", item])
            _mc(*args, check=True)
        else:
            _mc("cp", str(resolved.local), resolved.remote, check=True)
        return resolved

    def stat_shared_path(self, path: str) -> SharedPath:
        """Check that a shared path exists in MinIO."""
        # 逻辑说明：`stat_shared_path` 接收 path，查询 shared 路径是否存在及其解析结果，返回 SharedPath；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        resolved = self.resolve_shared_path(path)
        self._ensure_alias()
        _mc("stat", resolved.remote, check=True)
        return resolved

    def list_shared_path(self, path: str) -> tuple[SharedPath, list[str]]:
        """List a shared path in MinIO."""
        # 逻辑说明：`list_shared_path` 接收 path，列出 shared 远端前缀下的直接成员，返回 tuple[SharedPath, list[str]]；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        resolved = self.resolve_shared_path(path)
        self._ensure_alias()
        result = _mc("ls", "--recursive", resolved.remote, check=True)
        entries = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return resolved, entries

    def get_config(self) -> dict[str, Any]:
        """Pull openclaw.json and return parsed dict."""
        # 逻辑说明：`get_config` 接收 当前对象/进程状态，读取配置，返回 dict[str, Any]；会更新对象内存状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；
        # 本函数不额外重试，避免掩盖持续故障。
        text = self._cat(f"{self._prefix}/openclaw.json")
        if not text:
            raise RuntimeError(f"openclaw.json not found in MinIO for worker {self.worker_name}")
        logger.info("openclaw.json raw content (%d chars): %r", len(text), text[:500])
        return json.loads(text)

    def get_soul(self) -> str:
        """Return the canonical SOUL.md used as a missing-file fallback."""
        return self._cat(f"{self._prefix}/SOUL.md") or ""

    def get_agents_md(self) -> str:
        """Return the canonical AGENTS.md used as a missing-file fallback."""
        return self._cat(f"{self._prefix}/AGENTS.md") or ""

    def list_skills(self) -> list[str]:
        """Return list of skill names available in MinIO for this worker."""
        # 逻辑说明：`list_skills` 接收 当前对象/进程状态，列出Skill，返回 list[str]；会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        prefix = f"{self._prefix}/skills/"
        entries = self._ls(prefix)
        # entries look like "skill-name/SKILL.md"
        skill_names: list[str] = []
        seen: set[str] = set()
        for entry in entries:
            parts = entry.rstrip("/").split("/")
            if parts:
                name = parts[0]
                if name and name not in seen:
                    seen.add(name)
                    skill_names.append(name)
        return skill_names

    def pull_all(self) -> list[str]:
        """Pull controller-managed worker files, excluding shared data."""
        # 逻辑说明：`pull_all` 接收 当前对象/进程状态，拉取启动文件、配置和 Skill，并返回实际变化的相对路径，返回 list[str]；
        # 会读写本地文件、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        changed: list[str] = []
        files: dict[str, list[str]] = {
            "openclaw.json": [f"{self._prefix}/openclaw.json"],
            "SOUL.md": [f"{self._prefix}/SOUL.md"],
            "AGENTS.md": [f"{self._prefix}/AGENTS.md"],
            "PROFILE.md": [f"{self._prefix}/PROFILE.md"],
            "TOOLS.md": [f"{self._prefix}/TOOLS.md"],
            "HEARTBEAT.md": [f"{self._prefix}/HEARTBEAT.md"],
            "config/mcporter.json": [
                f"{self._prefix}/config/mcporter.json",
                f"{self._prefix}/mcporter-servers.json",
            ],
            "config/credagent.json": [f"{self._prefix}/config/credagent.json"],
        }
        for name, keys in files.items():
            content = None
            for key in keys:
                content = self._cat(key)
                if content is not None:
                    break
            if content is None:
                continue

            local = self.local_dir / name
            existing = local.read_text(encoding="utf-8") if local.exists() else None
            if name == "openclaw.json" and existing is not None:
                try:
                    content = _merge_openclaw_config(content, existing)
                except json.JSONDecodeError as exc:
                    logger.warning("openclaw.json merge failed, replacing from remote: %s", exc)

            if content != existing:
                local.parent.mkdir(parents=True, exist_ok=True)
                local.write_text(content, encoding="utf-8")
                changed.append(name)

        minio_skills = self.list_skills()
        for skill_name in minio_skills:
            remote_prefix = f"{self._prefix}/skills/{skill_name}/"
            local_skill_dir = self.local_dir / "skills" / skill_name
            local_skill_dir.mkdir(parents=True, exist_ok=True)
            try:
                result = _mc(
                    "mirror",
                    self._object_path(remote_prefix),
                    str(local_skill_dir) + "/",
                    "--overwrite",
                    check=False,
                )
                if result.returncode == 0:
                    for sh in local_skill_dir.rglob("*.sh"):
                        sh.chmod(sh.stat().st_mode | 0o111)
                    changed.append(f"skills/{skill_name}/")
                else:
                    logger.warning("mc mirror failed for skill %s: %s", skill_name, result.stderr)
            except Exception as exc:
                logger.warning("Failed to mirror skill %s: %s", skill_name, exc)

        local_skills_dir = self.local_dir / "skills"
        if local_skills_dir.is_dir():
            minio_skill_set = set(minio_skills)
            for child in list(local_skills_dir.iterdir()):
                if child.is_dir() and child.name not in minio_skill_set:
                    shutil.rmtree(child)
                    changed.append(f"skills/{child.name}/ (removed)")
                    logger.info("Removed local skill no longer in MinIO: %s", child.name)

        return changed

def push_local(sync: FileSync, since: float = 0) -> list[str]:
    """Push locally-changed files back to MinIO. Returns list of pushed keys.

    Mirrors the openclaw worker entrypoint behavior: only scans files whose
    mtime > `since` (epoch seconds), then content-compares before uploading.
    When since=0 (first run), scans all eligible files.

    Excludes Controller-managed files only. AGENTS.md, SOUL.md, .copaw/sessions/
    are Worker-managed and are pushed (including session backup).
    """
    # Controller-managed files that should never be pushed back
    # 逻辑说明：`push_local` 接收 sync、since，先把 runtime 状态桥接回标准目录，再筛选并上传本地变化，返回 list[str]；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    _EXCLUDE_FILES = {
        "openclaw.json",
        "mcporter-servers.json",
    }
    # Controller-managed files at specific relative paths (not just root)
    _EXCLUDE_PATHS = {
        "config/mcporter.json",
        ".copaw/workspaces/default/config/mcporter.json",
    }
    # Skip duplicate uploads through the runtime skills symlink; the canonical
    # standard-space skills/ directory is still pushed normally.
    # Auto-mirrored shared directories are handled by explicit filesync ops.
    _EXCLUDE_PATH_PREFIXES = (
        ".copaw/workspaces/default/skills",
        ".copaw/workspaces/default/shared",
        ".copaw/workspaces/default/global-shared",
        "shared",
        "global-shared",
    )
    # Directory name components to skip anywhere in the tree
    _EXCLUDE_DIRS = {
        ".agents",
        ".cache",
        ".npm",
        ".local",
        ".mc",
        # .copaw sub-dirs that are derived / installed at startup
        "custom_channels",
        "active_skills",
        "__pycache__",
    }
    # File extensions to skip (transient runtime files)
    _EXCLUDE_EXTENSIONS = {".lock"}
    pushed: list[str] = []
    failures: list[tuple[str, Exception]] = []
    local_dir = sync.local_dir
    if not local_dir.exists():
        return pushed

    # ── Inner → Outer sync ──────────────────────────────────────────────
    # CoPaw Agent reads/writes workspaces/default/AGENTS.md and SOUL.md at
    # runtime.  These are "inner" copies derived from the "outer" files at
    # the sync root.  If the Agent modifies them, propagate changes back to
    # the outer layer so the normal push cycle uploads them to MinIO.
    try:
        bridge_runtime_to_standard(local_dir)
    except Exception as exc:
        raise BridgeRuntimeError(str(exc)) from exc

    sync._ensure_alias()

    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue
        # Quick mtime check — skip files not modified since last push
        try:
            if path.stat().st_mtime <= since:
                continue
        except OSError:
            continue
        rel = path.relative_to(local_dir)
        # Skip Manager-owned config files at workspace root
        if len(rel.parts) == 1 and rel.name in _EXCLUDE_FILES:
            continue
        # Skip Manager-owned config files at specific paths
        if rel.as_posix() in _EXCLUDE_PATHS:
            continue
        # Skip runtime skill projection; standard-space skills/ is canonical.
        if any(
            rel.as_posix() == prefix or rel.as_posix().startswith(f"{prefix}/")
            for prefix in _EXCLUDE_PATH_PREFIXES
        ):
            continue
        # Skip excluded directory trees
        if any(p in _EXCLUDE_DIRS for p in rel.parts):
            continue
        # Skip transient runtime files by extension (e.g. .lock)
        if rel.suffix in _EXCLUDE_EXTENSIONS:
            continue

        key = f"{sync._prefix}/{rel.as_posix()}"
        try:
            remote = sync._cat(key)
            local_content = path.read_text(errors="replace")
            if remote == local_content:
                continue
            dest = sync._object_path(key)
            _mc("cp", str(path), dest, check=True)
            pushed.append(rel.as_posix())
            logger.debug("Pushed %s -> %s", rel, dest)
        except Exception as exc:
            logger.debug("push_local: failed for %s: %s", rel, exc)
            failures.append((rel.as_posix(), exc))

    if failures:
        failed_path, failure = failures[0]
        raise RuntimeError(
            f"failed to push {len(failures)} local file(s); "
            f"first={failed_path} ({type(failure).__name__})",
        ) from failure
    return pushed


async def push_loop(
    sync: FileSync,
    check_interval: int = 5,
    health: HealthStateProtocol | None = None,
) -> None:
    """Background task: push local changes to MinIO every `check_interval` seconds.

    Tracks last push timestamp and only triggers push_local when files with
    newer mtime are detected, similar to openclaw's find-newermt approach.

    The first iteration is a full scan (``since=0``) so that files written
    by bridge/bootstrap BEFORE push_loop was started (e.g. agent.json,
    AGENTS.md, SOUL.md) still get uploaded. Otherwise their mtime is always
    ≤ ``last_push_time`` and they'd never be pushed.
    """
    # 逻辑说明：`push_loop` 接收 sync、check_interval、health，按间隔持续执行本地持久化；失败更新 sync/bridge health 后继续下一轮，返回 None；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；循环/重试受现有次数、超时或间隔限制。
    last_push_time: float = 0.0

    while True:
        try:
            now = time.time()
            pushed = await asyncio.to_thread(
                push_local,
                sync,
                last_push_time,
            )
            last_push_time = now
            if pushed:
                logger.info("FileSync push: uploaded %s", pushed)
            if health is not None:
                health.update(
                    "bridge",
                    "healthy",
                    "runtime-to-standard bridge completed",
                    {"operation": "bridge_runtime_to_standard"},
                )
                health.update(
                    "sync",
                    "healthy",
                    "runtime file persistence completed",
                    {"operation": "push_loop"},
                )
        except asyncio.CancelledError:
            break
        except BridgeRuntimeError as exc:
            logger.warning("FileSync runtime bridge error: %s", exc)
            if health is not None:
                health.update(
                    "bridge",
                    "unhealthy",
                    f"runtime-to-standard bridge failed: {exc}",
                    {
                        "operation": "bridge_runtime_to_standard",
                        "error_type": type(exc).__name__,
                    },
                )
        except Exception as exc:
            logger.warning("FileSync push error: %s", exc)
            if health is not None:
                health.update(
                    "sync",
                    "unhealthy",
                    f"runtime file persistence failed: {exc}",
                    {
                        "operation": "push_loop",
                        "error_type": type(exc).__name__,
                    },
                )
        await asyncio.sleep(check_interval)


async def sync_loop(
    sync: FileSync,
    interval: int = 60,
    on_pull: Callable[[list[str]], Awaitable[None]] | None = None,
    health: HealthStateProtocol | None = None,
) -> None:
    """Background task: pull controller-managed worker files."""
    # 逻辑说明：`sync_loop` 接收 sync、interval、on_pull、health，按间隔拉取 Controller 管理文件，变化后调用刷新回调，返回 None；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；循环/重试受现有次数、超时或间隔限制。
    while True:
        try:
            changed = await asyncio.to_thread(sync.pull_all)
            if changed:
                logger.info("FileSync pull: files changed: %s", changed)
                if on_pull is not None:
                    await on_pull(changed)
            if health is not None:
                health.update(
                    "sync",
                    "healthy",
                    "runtime config pull completed",
                    {"operation": "sync_loop"},
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("FileSync pull error: %s", exc)
            if health is not None:
                health.update(
                    "sync",
                    "unhealthy",
                    f"runtime config pull failed: {exc}",
                    {
                        "operation": "sync_loop",
                        "error_type": type(exc).__name__,
                    },
                )
        await asyncio.sleep(interval)
