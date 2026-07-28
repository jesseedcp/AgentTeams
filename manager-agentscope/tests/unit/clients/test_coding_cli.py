from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest
from agentteams_manager.clients.coding_cli import (
    BoundedProcessRunner,
    CodingCLIClient,
    CodingCLIProviderRejected,
    CodingCLITimeout,
    CodingCLIUnavailable,
    CodingCLIWorkspaceEscape,
    RawCodingCLIResult,
)


class CaptureRunner:
    def __init__(
        self,
        result: RawCodingCLIResult | None = None,
    ) -> None:
        self.result = result or RawCodingCLIResult(
            returncode=0,
            stdout=b"completed",
            stderr=b"",
        )
        self.calls: list[dict[str, object]] = []

    async def run(self, argv: tuple[str, ...], **kwargs: object):
        self.calls.append({"argv": argv, **kwargs})
        return self.result


def _binary(directory: Path, provider: str) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    path = directory / f"{provider}{suffix}"
    path.write_bytes(b"fixture")
    path.chmod(0o755)
    return path


def _client(
    tmp_path: Path,
    *,
    runner: CaptureRunner | None = None,
    providers: tuple[str, ...] = ("claude", "gemini", "qodercli"),
    environment: dict[str, str] | None = None,
) -> tuple[CodingCLIClient, CaptureRunner, Path, Path]:
    trusted = tmp_path / "trusted"
    workspace_root = tmp_path / "cache" / "shared" / "tasks"
    trusted.mkdir()
    workspace_root.mkdir(parents=True)
    for provider in providers:
        _binary(trusted, provider)
    capture = runner or CaptureRunner()
    return (
        CodingCLIClient(
            trusted_directory=trusted,
            allowed_providers=providers,
            workspace_root=workspace_root,
            runner=capture,
            environment=environment or {},
        ),
        capture,
        trusted,
        workspace_root,
    )


@pytest.mark.asyncio
async def test_provider_templates_are_fixed_and_prompt_uses_stdin(
    tmp_path: Path,
) -> None:
    client, runner, _, workspace_root = _client(tmp_path)
    workspace = workspace_root / "task-1" / "workspace"
    workspace.mkdir(parents=True)
    prompt = "edit README; touch SHOULD_NOT_BE_INTERPRETED"

    for provider in ("claude", "gemini", "qodercli"):
        receipt = await client.run(
            provider,
            workspace=workspace,
            prompt=prompt,
        )
        assert receipt.success is True
        call = runner.calls[-1]
        argv = call["argv"]
        assert isinstance(argv, tuple)
        assert prompt not in argv
        assert call["stdin"] == prompt.encode()
        assert call["cwd"] == workspace.resolve()
        assert "--dangerously-skip-permissions" not in argv
        assert "--yolo" not in argv

    assert "--print" in runner.calls[0]["argv"]
    assert "acceptEdits" in runner.calls[0]["argv"]
    assert "auto_edit" in runner.calls[1]["argv"]
    assert "--print" in runner.calls[2]["argv"]
    assert "accept_edits" in runner.calls[2]["argv"]


@pytest.mark.asyncio
async def test_provider_and_workspace_boundaries_fail_closed(
    tmp_path: Path,
) -> None:
    client, _, _, workspace_root = _client(
        tmp_path,
        providers=("claude",),
    )
    workspace = workspace_root / "task-1" / "workspace"
    workspace.mkdir(parents=True)

    with pytest.raises(CodingCLIProviderRejected):
        await client.run(
            "gemini",
            workspace=workspace,
            prompt="hello",
        )
    with pytest.raises(CodingCLIWorkspaceEscape):
        await client.run(
            "claude",
            workspace=tmp_path,
            prompt="hello",
        )


@pytest.mark.asyncio
async def test_missing_cli_is_reported_as_configured_but_unavailable(
    tmp_path: Path,
) -> None:
    trusted = tmp_path / "trusted"
    root = tmp_path / "tasks"
    trusted.mkdir()
    root.mkdir()
    client = CodingCLIClient(
        trusted_directory=trusted,
        allowed_providers=("claude",),
        workspace_root=root,
        runner=CaptureRunner(),
        environment={},
    )

    assert client.status()["claude"]["configured"] is True
    assert client.status()["claude"]["available"] is False
    with pytest.raises(CodingCLIUnavailable):
        await client.run(
            "claude",
            workspace=root,
            prompt="hello",
        )


@pytest.mark.asyncio
async def test_output_is_bounded_and_known_credentials_are_redacted(
    tmp_path: Path,
) -> None:
    secret = "sk-test-secret-value"
    raw = RawCodingCLIResult(
        returncode=3,
        stdout=(f"Authorization: Bearer {secret}\n" + "x" * 500).encode(),
        stderr=f"ANTHROPIC_API_KEY={secret}".encode(),
        stdout_truncated=True,
    )
    client, _, _, workspace_root = _client(
        tmp_path,
        runner=CaptureRunner(raw),
        providers=("claude",),
        environment={
            "ANTHROPIC_API_KEY": secret,
            "AGENTTEAMS_MANAGER_MATRIX_TOKEN": "must-not-be-inherited",
        },
    )
    workspace = workspace_root / "task-1" / "workspace"
    workspace.mkdir(parents=True)

    receipt = await client.run(
        "claude",
        workspace=workspace,
        prompt="do work",
    )

    assert receipt.success is False
    assert receipt.returncode == 3
    assert secret not in receipt.stdout
    assert secret not in receipt.stderr
    assert "[REDACTED]" in receipt.stdout
    assert receipt.output_truncated is True


@pytest.mark.asyncio
async def test_bounded_runner_passes_stdin_without_a_shell(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "must-not-exist"
    prompt = f"hello; touch {marker}"
    result = await BoundedProcessRunner().run(
        (
            sys.executable,
            "-c",
            "import sys; print(sys.stdin.read())",
        ),
        stdin=prompt.encode(),
        cwd=tmp_path,
        environment=dict(os.environ),
        timeout=5,
        max_output_bytes=1024,
    )

    assert result.returncode == 0
    assert prompt.encode() in result.stdout
    assert not marker.exists()


@pytest.mark.asyncio
async def test_bounded_runner_truncates_and_terminates_on_timeout(
    tmp_path: Path,
) -> None:
    runner = BoundedProcessRunner()
    result = await runner.run(
        (
            sys.executable,
            "-c",
            "import sys; sys.stdout.write('x' * 10000)",
        ),
        stdin=b"",
        cwd=tmp_path,
        environment=dict(os.environ),
        timeout=5,
        max_output_bytes=128,
    )
    assert len(result.stdout) == 128
    assert result.stdout_truncated is True

    with pytest.raises(CodingCLITimeout):
        await runner.run(
            (
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            ),
            stdin=b"",
            cwd=tmp_path,
            environment=dict(os.environ),
            timeout=0.05,
            max_output_bytes=128,
        )


@pytest.mark.asyncio
async def test_bounded_runner_cleans_up_when_cancelled(
    tmp_path: Path,
) -> None:
    task = asyncio.create_task(
        BoundedProcessRunner().run(
            (
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            ),
            stdin=b"",
            cwd=tmp_path,
            environment=dict(os.environ),
            timeout=60,
            max_output_bytes=128,
        ),
    )
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
