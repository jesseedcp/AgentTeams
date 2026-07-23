from __future__ import annotations

import logging

import pytest

from agentteams_manager.clients.process import (
    ProcessRejected,
    ProcessRunner,
    ProcessTimeout,
)


@pytest.mark.asyncio
async def test_process_runner_uses_argv_without_a_shell() -> None:
    runner = ProcessRunner(allowed_executables={"python"})

    result = await runner.run(
        (
            "python",
            "-c",
            "import sys; print(sys.argv[1])",
            "literal;not-a-command",
        ),
        timeout=10,
    )

    assert result.returncode == 0
    assert result.stdout.decode().strip() == "literal;not-a-command"


@pytest.mark.asyncio
async def test_process_timeout_does_not_log_stdin_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = ProcessRunner(allowed_executables={"python"})

    with caplog.at_level(logging.WARNING), pytest.raises(ProcessTimeout):
        await runner.run(
            (
                "python",
                "-c",
                "import time; time.sleep(10)",
            ),
            stdin=b'{"token":"never-log-this"}',
            timeout=0.01,
        )

    assert "never-log-this" not in caplog.text


@pytest.mark.asyncio
async def test_process_runner_rejects_path_qualified_executable() -> None:
    runner = ProcessRunner(allowed_executables={"agt"})

    with pytest.raises(ProcessRejected, match="path"):
        await runner.run((r"C:\tools\agt.exe", "get", "workers"))
