"""Object-storage restore and runtime-state persistence for qwenpaw-worker."""

# 初学者导读：Pod 的本地磁盘可能随重建而消失，所以 Worker 启动时先从 MinIO
# 恢复自己的工作区，运行中再把允许持久化的变化推回去。Controller 管理的配置
# 主要按“远端覆盖本地”流动；会话、记忆和产物则主要按“本地保存到远端”流动。
# 过滤规则非常重要：如果把凭据、缓存或刚拉下来的共享投影再次上传，不仅会泄密，
# 还可能形成反复覆盖的同步回路。

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

DEFAULT_MC_ALIAS = "agentteams"
COMPARE_CONTENT_MAX_BYTES = 20 * 1024 * 1024


def _to_text(value: object) -> str:
    # 逻辑说明：把 mc 的 bytes/空值统一为可记录文本；坏字节用替换字符保留诊断信息。
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def _looks_like_missing_object_error(stderr: Optional[str] | bytes) -> bool:
    # 逻辑说明：识别不同对象存储返回的“对象不存在”文案，使缺失文件可按正常初始状态处理。
    text = _to_text(stderr)
    return "Object does not exist" in text or "The specified key does not exist" in text


def _mc_error_message(exc: subprocess.CalledProcessError) -> str:
    # 逻辑说明：优先抽取 mc 标准错误/输出；若均为空则生成包含退出码的稳定错误消息。
    text = _to_text(exc.stderr or exc.stdout).strip()
    return text or f"mc exited with status {exc.returncode}"


def _redact_url_userinfo(value: str) -> str:
    # 逻辑说明：日志输出前遮蔽 URL 中的 userinfo；普通 URL 原样返回，不改变实际连接地址。
    if "://" not in value or "@" not in value:
        return value
    scheme, rest = value.split("://", 1)
    return f"{scheme}://<redacted>@{rest.split('@', 1)[1]}"


def _preview_list(values: List[str], limit: int = 20) -> List[str]:
    # 逻辑说明：限制日志列表长度并标出省略数量，避免一次同步产生过大的日志记录。
    if len(values) <= limit:
        return values
    return [*values[:limit], f"...({len(values) - limit} more)"]


def _storage_alias() -> str:
    # 逻辑说明：优先采用显式 mc alias，否则从完整存储前缀推断，最后使用兼容默认名。
    value = os.getenv("AGENTTEAMS_STORAGE_ALIAS", "").strip().strip("/")
    if value:
        return value
    prefix = os.getenv("AGENTTEAMS_STORAGE_PREFIX", "").strip().strip("/")
    if "/" in prefix:
        return prefix.split("/", 1)[0]
    return DEFAULT_MC_ALIAS


class FileSync:
    """封装一个 Worker 私有前缀和团队共享前缀的 MinIO 同步。

    ``mc`` 是 MinIO Client 命令行工具。该类始终以参数列表调用它，而不是拼接
    shell 字符串；这样带空格的路径不会被拆坏，也不会把对象名当作命令执行。
    ``ensure_alias`` 只在本进程第一次使用存储时配置连接，恢复后其余方法才能
    安全访问对象。
    """
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        worker_name: str,
        local_dir: Path,
        shared_dir: Path,
        remote_prefix: Optional[str] = None,
        shared_prefix: Optional[str] = None,
    ) -> None:
        # 逻辑说明：保存连接参数并计算 Worker/共享远端前缀；连接和目录写入延迟到实际同步时发生。
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.worker_name = worker_name
        self.local_dir = local_dir
        self.shared_dir = shared_dir
        self.remote_prefix = (remote_prefix or f"agents/{worker_name}").strip("/")
        self.shared_prefix = (shared_prefix or "shared").strip("/")
        self.mc_alias = _storage_alias()
        self._alias_set = False

    def _mc(self, *args: str, check: bool = True, text: bool = True) -> subprocess.CompletedProcess:
        # 逻辑说明：定位 mc 二进制并以捕获输出方式执行；缺失程序或按 check 判定失败时向上抛错。
        mc_bin = shutil.which("mc")
        if not mc_bin:
            raise RuntimeError("mc binary not found")
        return subprocess.run(
            [mc_bin, *args],
            check=check,
            capture_output=True,
            text=text,
        )

    def ensure_alias(self) -> None:
        # 逻辑说明：幂等建立 mc alias；优先复用环境注入凭据，K8s 缺少凭据时拒绝隐式明文配置。
        if self._alias_set:
            return
        if os.getenv(f"MC_HOST_{self.mc_alias}"):
            self._alias_set = True
            logger.info("storage alias ready component=sync worker=%s mode=env", self.worker_name)
            return
        if os.getenv("AGENTTEAMS_RUNTIME") == "k8s":
            self._alias_set = True
            logger.info("storage alias ready component=sync worker=%s mode=k8s-wrapper", self.worker_name)
            return
        missing = [
            name
            for name, value in (
                ("endpoint", self.endpoint),
                ("access_key", self.access_key),
                ("secret_key", self.secret_key),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(f"missing storage config: {', '.join(missing)}")

        endpoint = self.endpoint.rstrip("/")
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"
        logger.info(
            "configuring storage alias component=sync worker=%s endpoint=%s bucket=%s",
            self.worker_name,
            _redact_url_userinfo(endpoint),
            self.bucket,
        )
        try:
            self._mc("alias", "set", self.mc_alias, endpoint, self.access_key, self.secret_key)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"configure storage alias failed: {_mc_error_message(exc)}") from None
        self._alias_set = True
        logger.info("storage alias ready component=sync worker=%s mode=static", self.worker_name)

    def _object_path(self, key: str) -> str:
        return f"{self.mc_alias}/{self.bucket}/{key.strip('/')}"

    def _cat(self, key: str) -> Optional[str]:
        # 逻辑说明：从对象存储读取文本；对象缺失返回 None，其他错误记录后同样交由调用方决定是否重试。
        self.ensure_alias()
        result = self._mc("cat", self._object_path(key), check=False)
        if result.returncode == 0:
            return result.stdout
        if _looks_like_missing_object_error(result.stderr):
            return None
        logger.debug("mc cat failed component=sync key=%s returncode=%s", key, result.returncode)
        return None

    def _cat_bytes(self, key: str) -> Optional[bytes]:
        # 逻辑说明：以二进制模式读取对象，供不能 UTF-8 解码的文件恢复；失败或缺失返回 None。
        self.ensure_alias()
        try:
            result = self._mc("cat", self._object_path(key), check=False, text=False)
        except Exception as exc:
            logger.debug("mc cat failed component=sync key=%s error_type=%s", key, type(exc).__name__)
            return None
        if result.returncode == 0:
            stdout = result.stdout
            if isinstance(stdout, bytes):
                return stdout
            return _to_text(stdout).encode("utf-8")
        if _looks_like_missing_object_error(result.stderr):
            return None
        logger.debug("mc cat failed component=sync key=%s returncode=%s", key, result.returncode)
        return None

    def _mirror_prefix(self, remote_prefix: str, local_dir: Path) -> None:
        # 逻辑说明：确保本地目录存在并用 mc mirror 拉取一个远端前缀；凭据和远端错误会向上报告。
        local_dir.mkdir(parents=True, exist_ok=True)
        remote = f"{self.mc_alias}/{self.bucket}/{remote_prefix.strip('/')}/"
        logger.info(
            "mirroring storage prefix component=sync worker=%s remote=%s local=%s",
            self.worker_name,
            remote,
            local_dir,
        )
        try:
            self._mc(
                "mirror",
                remote,
                str(local_dir) + "/",
                "--overwrite",
                "--exclude",
                "credentials/**",
            )
        except subprocess.CalledProcessError as exc:
            if _looks_like_missing_object_error(exc.stderr):
                logger.info("storage prefix is empty component=sync remote=%s", remote)
                return
            raise RuntimeError(f"mirror storage failed: {_mc_error_message(exc)}") from None
        logger.info(
            "mirrored storage prefix component=sync worker=%s remote=%s local=%s",
            self.worker_name,
            remote,
            local_dir,
        )

    def mirror_all(self) -> None:
        # 逻辑说明：先保证 alias，然后依次恢复 Worker 私有目录和团队共享目录，供重启后继续工作。
        self.ensure_alias()
        self._mirror_prefix(self.remote_prefix, self.local_dir)
        self._mirror_prefix(self.shared_prefix, self.shared_dir)

    def mirror_prefix(self, remote_prefix: str, local_dir: Path) -> None:
        # 逻辑说明：公开单前缀恢复入口，复用相同 alias 初始化与镜像错误语义。
        self.ensure_alias()
        self._mirror_prefix(remote_prefix, local_dir)

    def pull_runtime_config(self, local_path: Path, remote_key: Optional[str] = None) -> bool:
        # 逻辑说明：将权威 runtime.yaml 下载到指定路径；对象尚未发布返回 False，其他错误抛出。
        self.ensure_alias()
        key = remote_key or f"{self.remote_prefix}/runtime/runtime.yaml"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        result = self._mc("cp", self._object_path(key), str(local_path), check=False)
        if result.returncode == 0:
            return True
        if _looks_like_missing_object_error(result.stderr):
            logger.info("runtime config not found in storage component=sync key=%s", key)
            return False
        raise RuntimeError(f"pull runtime config failed: {_mc_error_message(result)}")


def _skip_background_push(rel: Path) -> bool:
    # 逻辑说明：按保守规则排除凭据、运行时权威状态、缓存和临时文件，防止本地噪声覆盖远端。
    # 背景推送是自动发生的，因此必须采用保守边界。这里排除的文件即使对调试
    # 有用，也不应成为跨 Pod 的持久状态；否则重启会恢复陈旧锁、令牌或缓存。
    rel_path = rel.as_posix()
    excluded_prefixes = (
        "credentials",
        "runtime",
        "shared",
        "global-shared",
        ".qwenpaw/workspaces/default/shared",
        ".qwenpaw/workspaces/default/global-shared",
        ".qwenpaw/workspaces/default/tool_result",
        ".qwenpaw/workspaces/default/file_store",
        ".qwenpaw/workspaces/default/media",
        ".qwenpaw/workspaces/default/embedding_cache",
    )
    if any(rel_path == prefix or rel_path.startswith(f"{prefix}/") for prefix in excluded_prefixes):
        return True
    excluded_dirs = {".cache", ".local", ".mc", "__pycache__", "logs"}
    if any(part in excluded_dirs for part in rel.parts):
        return True
    excluded_files = {".DS_Store", "qwenpaw.log", "heartbeat.json", "token_usage.json"}
    if rel.name in excluded_files:
        return True
    return rel.suffix in {".lock", ".pyc"}


def push_local(sync: FileSync, since: float = 0) -> List[str]:
    """上传 ``since`` 之后变化且允许持久化的 Worker 文件。

    返回相对路径列表供日志和测试核对。函数按内容与修改时间筛选，而不是把整个
    目录无条件镜像到远端，因而不会删除 Controller 刚发布但本地尚未看到的文件。
    """
    # 逻辑说明：扫描 since 后变化且允许持久化的文件逐个上传；汇总成功键，任何失败最终统一抛出。
    pushed = []
    failures: List[tuple[str, Exception]] = []
    local_dir = sync.local_dir
    if not local_dir.exists():
        return pushed

    sync.ensure_alias()
    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
            if stat.st_mtime <= since:
                continue
        except OSError:
            continue

        rel = path.relative_to(local_dir)
        if _skip_background_push(rel):
            continue

        key = f"{sync.remote_prefix}/{rel.as_posix()}"
        try:
            if stat.st_size <= COMPARE_CONTENT_MAX_BYTES:
                remote = sync._cat_bytes(key)
                if remote == path.read_bytes():
                    continue
            sync._mc("cp", str(path), sync._object_path(key), check=True)
            pushed.append(rel.as_posix())
        except Exception as exc:
            logger.debug("push_local failed component=sync file=%s error_type=%s", rel, type(exc).__name__)
            failures.append((rel.as_posix(), exc))

    if failures:
        failed_path, failure = failures[0]
        raise RuntimeError(
            f"failed to push {len(failures)} local file(s); "
            f"first={failed_path} ({type(failure).__name__})"
        ) from failure
    return pushed


async def push_loop(sync: FileSync, check_interval: float = 5) -> None:
    # 逻辑说明：以循环启动时刻为基线周期推送新变化；同步异常只记录，取消信号则正常退出。
    # Startup state has just been mirrored from object storage. Only watch
    # changes made after the loop starts; a since=0 scan can spend minutes
    # comparing a QwenPaw 2 workdir and starve newly-created files.
    last_push_time = time.time()
    logger.info(
        "qwenpaw FileSync push loop started component=sync worker=%s interval_seconds=%s",
        sync.worker_name,
        check_interval,
    )
    while True:
        try:
            await asyncio.sleep(check_interval)
            now = time.time()
            pushed = await asyncio.to_thread(push_local, sync, last_push_time)
            last_push_time = now
            if pushed:
                logger.info(
                    "qwenpaw FileSync push uploaded component=sync worker=%s count=%d files=%s",
                    sync.worker_name,
                    len(pushed),
                    _preview_list(pushed),
                )
        except asyncio.CancelledError:
            logger.info("qwenpaw FileSync push loop stopped component=sync worker=%s", sync.worker_name)
            break
        except Exception as exc:
            logger.warning(
                "qwenpaw FileSync push failed component=sync worker=%s error_type=%s",
                sync.worker_name,
                type(exc).__name__,
            )
