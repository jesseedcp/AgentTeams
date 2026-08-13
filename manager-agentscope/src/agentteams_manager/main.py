"""Console entrypoint for the AgentScope Manager daemon.

AgentScope Manager 容器进程的命令行入口。

镜像启动时会进入这里：读取配置、调用 bootstrap 完成依赖装配、注册操作系统信号，
然后让 ``ManagerApplication`` 持续运行。收到 SIGTERM 等停止信号后不会直接退出，
而是请求 application 有序保存会话并关闭外部连接，因此 Kubernetes 滚动更新时
不会无故丢掉刚完成的 AgentScope turn。
"""

from __future__ import annotations

import asyncio
import signal
from typing import Any

from .config import ManagerConfig
from .observability.logging import configure_logging
from .observability.tracing import build_tracer_from_env


async def run_application(
    application: Any,
    *,
    install_signals: bool = True,
) -> None:
    if install_signals:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    signum,
                    application.request_stop,
                )
            except NotImplementedError:
                signal.signal(
                    signum,
                    lambda _signum, _frame: application.request_stop(),
                )
    await application.run()


def main() -> None:
    """Load deployment wiring and run until a termination signal."""
    from .bootstrap import create_application

    configure_logging()
    tracer = build_tracer_from_env()
    config = ManagerConfig.from_env()

    async def run() -> None:
        application = await create_application(config, tracer=tracer)
        await run_application(application)

    try:
        asyncio.run(run())
    finally:
        tracer.shutdown()


if __name__ == "__main__":
    main()
