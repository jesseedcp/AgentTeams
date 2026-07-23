"""Deterministic subprocess double for typed ``agt`` client tests."""

from __future__ import annotations

import json
from collections import deque


class FakeProcess:
    def __init__(self) -> None:
        self.results: deque[object] = deque()
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []

    @property
    def argv(self) -> tuple[str, ...]:
        return self.calls[-1][0]

    @property
    def stdin(self) -> bytes | None:
        return self.calls[-1][1]

    def queue_json(self, payload: object) -> None:
        from agentteams_manager.clients.process import ProcessResult

        self.results.append(
            ProcessResult(
                argv=("agt",),
                returncode=0,
                stdout=json.dumps(payload).encode(),
                stderr=b"",
            ),
        )

    def queue_error(self, stderr: str, *, returncode: int = 1) -> None:
        from agentteams_manager.clients.process import ProcessResult

        self.results.append(
            ProcessResult(
                argv=("agt",),
                returncode=returncode,
                stdout=b"",
                stderr=stderr.encode(),
            ),
        )

    async def run(
        self,
        argv: tuple[str, ...],
        *,
        stdin: bytes | None = None,
        cwd: object = None,
        timeout: float | None = None,
    ) -> object:
        del cwd, timeout
        self.calls.append((argv, stdin))
        if not self.results:
            raise AssertionError(f"no fake result queued for {argv}")
        result = self.results.popleft()
        if isinstance(result, BaseException):
            raise result
        return result
