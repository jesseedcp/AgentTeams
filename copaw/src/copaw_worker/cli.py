"""CLI entry point: copaw-worker"""

# 初学者导读：容器 entrypoint 最终调用这里。CLI 只完成参数解析、日志初始化和
# asyncio 生命周期入口；它不会承担 Manager 的规划职责，也不会自行发明 Team
# 或模型配置。
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Optional

import typer

from copaw_worker.config import WorkerConfig
from copaw_worker.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> None:
    """Entry point registered in pyproject.toml."""

    # 逻辑说明：声明并交给 Typer 运行 Worker 命令行入口，把用户传入的存储、安装目录、同步间隔和控制台选项组装为 WorkerConfig 后进入异步生命周期。
    # Ctrl+C 被视为正常退出；Worker 启动返回失败时以退出码 1 结束，参数解析错误由 Typer 负责报告。
    def _run(
        name: str = typer.Option(..., "--name", help="Worker name"),
        fs: str = typer.Option(..., "--fs", help="MinIO endpoint"),
        fs_key: str = typer.Option(..., "--fs-key", help="MinIO access key"),
        fs_secret: str = typer.Option(..., "--fs-secret", help="MinIO secret key"),
        fs_bucket: str = typer.Option("agentteams-storage", "--fs-bucket", help="MinIO bucket"),
        sync_interval: int = typer.Option(300, "--sync-interval", help="Sync interval (seconds)"),
        install_dir: Optional[str] = typer.Option(None, "--install-dir", help="Base install dir"),
        console_port: Optional[int] = typer.Option(None, "--console-port", help="Enable web console on this port (e.g. 8088, costs ~500MB extra RAM)"),
    ) -> None:
        """Start the CoPaw Worker and connect to Matrix."""
        # 逻辑说明：`_run` 接收 name、fs、fs_key、fs_secret、fs_bucket、sync_interval、install_dir、console_port，
        # 执行 CoPaw Worker 命令行启动 中的“run”步骤，返回 None；
        #
        # 会执行外部命令。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        config = WorkerConfig(
            worker_name=name,
            minio_endpoint=fs,
            minio_access_key=fs_key,
            minio_secret_key=fs_secret,
            minio_bucket=fs_bucket,
            sync_interval=sync_interval,
            install_dir=Path(install_dir) if install_dir else None,
            console_port=console_port,
        )
        worker = Worker(config)

        async def _async_run() -> None:
            # 逻辑说明：取得当前 asyncio loop，为 SIGINT/SIGTERM 注册 Worker 停机回调，然后等待完整 Worker 生命周期；Windows 不支持信号回调时退回外层 KeyboardInterrupt。
            # `worker.run()` 返回 False 表示启动门禁失败，此处转换为 Typer 非零退出码，避免容器把失败启动判为健康。
            loop = asyncio.get_running_loop()

            def _shutdown() -> None:
                # 逻辑说明：信号到达时在现有事件循环中调度 `worker.stop()`，避免在同步 signal handler 内阻塞等待异步资源关闭。
                asyncio.create_task(worker.stop())

            # Windows ProactorEventLoop does not support add_signal_handler;
            # fall back to KeyboardInterrupt handling below.
            try:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, _shutdown)
            except NotImplementedError:
                pass

            if not await worker.run():
                raise typer.Exit(1)

        try:
            asyncio.run(_async_run())
        except KeyboardInterrupt:
            pass

    typer.run(_run)
