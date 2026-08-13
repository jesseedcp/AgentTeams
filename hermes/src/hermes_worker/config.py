"""WorkerConfig: parsed from CLI args / env vars."""

# 初学者导读：Kubernetes 把某个 Hermes Worker 的名字、MinIO 连接和同步周期注入
# 进程，这里统一计算该成员的私有工作区与 ``HERMES_HOME``。路径按 worker_name
# 隔离，避免不同成员恢复到同一个会话目录；模型和房间等可变状态不在这里决定。
from __future__ import annotations

from pathlib import Path


class WorkerConfig:
    """Hermes Worker 启动所需的本地路径和对象存储连接参数。"""
    def __init__(
        self,
        worker_name: str,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_bucket: str = "agentteams-storage",
        minio_secure: bool = False,
        sync_interval: int = 300,
        install_dir: Path | None = None,
    ) -> None:
        # 逻辑说明：保存 Worker 身份、存储连接和本地根目录；构造阶段不访问网络或创建文件。
        self.worker_name = worker_name
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.minio_secure = minio_secure
        self.sync_interval = sync_interval
        # Default to the openclaw-style layout: workspace == HOME (== MinIO
        # mirror root). The entrypoint passes --install-dir explicitly, so this
        # default only matters for direct `hermes-worker` invocations (CI / dev).
        self.install_dir = install_dir or Path("/root/agentteams-fs/agents")

    @property
    def workspace_dir(self) -> Path:
        """Per-worker workspace root (mirror of MinIO ``agents/<name>/``)."""
        return self.install_dir / self.worker_name

    @property
    def hermes_home(self) -> Path:
        """``HERMES_HOME`` for this worker (config.yaml, .env, skills/, sessions/)."""
        return self.workspace_dir / ".hermes"
