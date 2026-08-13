"""CLI entry point: qwenpaw-worker."""

# 初学者导读：Docker entrypoint 最终调用这里。CLI 只负责把命令行和环境变量
# 转成 WorkerConfig、配置日志并进入异步 Worker 生命周期；模型选择、Team 成员
# 和 Matrix 房间来自 Controller 发布的 runtime config，不由用户在此临时决定。

from __future__ import annotations

import asyncio
from pathlib import Path
import signal
from typing import Optional

import typer

from qwenpaw_worker.config import WorkerConfig
from qwenpaw_worker.log import configure_worker_logging
from qwenpaw_worker.worker import Worker


def main() -> None:
    """Entry point registered in pyproject.toml."""
    # 逻辑说明：声明 Typer 命令并把进程参数交给异步 Worker；参数或运行异常最终表现为 CLI 退出状态。

    def _run(
        name: str = typer.Option(..., "--name", help="Worker name"),
        cr_name: Optional[str] = typer.Option(None, "--cr-name", help="Worker CR name"),
        fs: str = typer.Option(..., "--fs", help="MinIO/OSS endpoint"),
        fs_key: str = typer.Option(..., "--fs-key", help="MinIO/OSS access key"),
        fs_secret: str = typer.Option(..., "--fs-secret", help="MinIO/OSS secret key"),
        fs_bucket: str = typer.Option("agentteams-storage", "--fs-bucket", help="Storage bucket"),
        install_dir: Optional[str] = typer.Option(None, "--install-dir", help="Base install dir"),
        storage_prefix: Optional[str] = typer.Option(None, "--storage-prefix", help="Storage prefix"),
        shared_prefix: Optional[str] = typer.Option(None, "--shared-prefix", help="Shared storage prefix"),
        runtime_config: Optional[str] = typer.Option(None, "--runtime-config", help="Local runtime.yaml path"),
        console_port: Optional[int] = typer.Option(
            None,
            "--console-port",
            help="Expose the QwenPaw web console on this port",
        ),
    ) -> None:
        """Start the QwenPaw Worker."""
        # 逻辑说明：将命令行值组装成 WorkerConfig、初始化日志和 Worker，再用 asyncio.run 托管生命周期。
        config = WorkerConfig(
            worker_name=name,
            worker_cr_name=cr_name,
            fs_endpoint=fs,
            fs_access_key=fs_key,
            fs_secret_key=fs_secret,
            fs_bucket=fs_bucket,
            install_dir=Path(install_dir) if install_dir else None,
            storage_prefix=storage_prefix,
            shared_prefix=shared_prefix,
            runtime_config_path=Path(runtime_config) if runtime_config else None,
            console_port=console_port or 8088,
            console_enabled=console_port is not None,
        )
        configure_worker_logging(config.qwenpaw_working_dir)
        worker = Worker(config)

        async def _async_run() -> None:
            # 逻辑说明：注册 SIGINT/SIGTERM 的优雅停止回调并等待 Worker；Windows 不支持信号 API 时继续运行。
            loop = asyncio.get_running_loop()

            def _shutdown() -> None:
                # 逻辑说明：收到终止信号时异步请求 Worker 停止，让后台任务和子进程有机会清理。
                asyncio.create_task(worker.stop())

            try:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, _shutdown)
            except NotImplementedError:
                pass

            await worker.run()

        try:
            asyncio.run(_async_run())
        except KeyboardInterrupt:
            pass

    typer.run(_run)
