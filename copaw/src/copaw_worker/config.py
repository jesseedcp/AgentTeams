"""WorkerConfig: parsed from CLI args / env vars."""

# 初学者导读：这里把 Kubernetes 环境变量收拢为一名 CoPaw Worker 的启动配置。
# Worker 私有目录按 worker_name 隔离，团队 shared 目录则供协作产物共享；运行中
# 变化的模型、身份与 Matrix 房间仍来自 Controller 发布的配置，而不是本类默认值。
from __future__ import annotations

import os
from pathlib import Path


class WorkerConfig:
    """CoPaw Worker 启动阶段所需的路径、存储连接和端口集合。"""
    def __init__(
        self,
        worker_name: str,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_bucket: str = "agentteams-storage",
        minio_secure: bool = False,
        sync_interval: int = 60,
        install_dir: Path | None = None,
        console_port: int | None = 8088,
        worker_port: int | None = None,
        worker_cr_name: str | None = None,
    ) -> None:
        self.worker_name = worker_name
        self.worker_cr_name = worker_cr_name or worker_name
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.minio_secure = minio_secure
        self.install_dir = install_dir or _default_install_dir()
        self.console_port = console_port
        self.worker_port = (
            worker_port
            if worker_port is not None
            else (console_port + 1 if console_port is not None else 8089)
        )
        self.sync_interval = sync_interval


def _default_install_dir() -> Path:
    if configured := os.environ.get("COPAW_INSTALL_DIR"):
        return Path(configured)
    if configured_home := os.environ.get("HOME"):
        return Path(configured_home) / ".agentteams-worker"

    return Path.home() / ".agentteams-worker"
