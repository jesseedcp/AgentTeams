"""CLI entry point: ``hermes-worker``."""

# 初学者导读：容器入口把参数交给此 CLI，CLI 组装 WorkerConfig 后进入 asyncio
# 生命周期。它只负责启动一名已有 Worker，不创建 Team、Project 或其他 Worker。
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Optional

import typer

from hermes_worker.config import WorkerConfig
from hermes_worker.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> None:
    """Entry point registered in pyproject.toml."""
    # 逻辑说明：声明 Typer 命令，把进程参数转换为 WorkerConfig 并进入异步生命周期。

    def _run(
        name: str = typer.Option(..., "--name", help="Worker name"),
        fs: str = typer.Option(..., "--fs", help="MinIO endpoint"),
        fs_key: str = typer.Option(..., "--fs-key", help="MinIO access key"),
        fs_secret: str = typer.Option(..., "--fs-secret", help="MinIO secret key"),
        fs_bucket: str = typer.Option(
            "agentteams-storage", "--fs-bucket", help="MinIO bucket"
        ),
        sync_interval: int = typer.Option(
            300, "--sync-interval", help="Sync interval (seconds)"
        ),
        install_dir: Optional[str] = typer.Option(
            None, "--install-dir", help="Base install dir"
        ),
    ) -> None:
        """Start the Hermes Worker and connect to Matrix via the Hermes gateway."""
        # 逻辑说明：组装配置和 Worker，再由 asyncio.run 启动；运行失败反映为非零退出。
        config = WorkerConfig(
            worker_name=name,
            minio_endpoint=fs,
            minio_access_key=fs_key,
            minio_secret_key=fs_secret,
            minio_bucket=fs_bucket,
            sync_interval=sync_interval,
            install_dir=Path(install_dir) if install_dir else None,
        )
        worker = Worker(config)

        async def _async_run() -> None:
            # 逻辑说明：注册优雅停止信号并等待 Worker 主循环；Windows 不支持信号 API 时继续。
            loop = asyncio.get_running_loop()

            def _shutdown() -> None:
                # 逻辑说明：收到终止信号时创建异步 stop 任务，使同步与 Hermes 子进程有机会清理。
                asyncio.create_task(worker.stop())

            try:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, _shutdown)
            except NotImplementedError:
                # Windows ProactorEventLoop — fall back to KeyboardInterrupt
                pass

            await worker.run()

        try:
            asyncio.run(_async_run())
        except KeyboardInterrupt:
            pass

    typer.run(_run)
