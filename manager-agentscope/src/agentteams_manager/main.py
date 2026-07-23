"""Console entrypoint for the AgentScope Manager daemon."""

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
    application = create_application(config, tracer=tracer)
    try:
        asyncio.run(run_application(application))
    finally:
        tracer.shutdown()


if __name__ == "__main__":
    main()

