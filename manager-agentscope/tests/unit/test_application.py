from dataclasses import dataclass

import pytest

from agentteams_manager.application import ManagerApplication


class FakeDatabase:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    async def open(self) -> None:
        self.log.append("database")

    async def close(self) -> None:
        self.log.append("stop:database")


class FakeRecovery:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    async def restore(self) -> None:
        self.log.append("recovery")


class FakeService:
    def __init__(
        self,
        name: str,
        log: list[str],
        *,
        ready: bool = True,
    ) -> None:
        self.name = name
        self.log = log
        self.ready = ready

    async def start(self) -> None:
        self.log.append(self.name)

    async def stop(self) -> None:
        self.log.append(f"stop:{self.name}")


class FakeSessions:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    async def save_all(self) -> None:
        self.log.append("save:sessions")


@pytest.mark.asyncio
async def test_application_starts_in_dependency_order() -> None:
    log: list[str] = []
    application = ManagerApplication(
        database=FakeDatabase(log),
        recovery=FakeRecovery(log),
        config_watcher=FakeService("config_watcher", log),
        matrix=FakeService("matrix", log),
        heartbeat=FakeService("heartbeat", log),
        health=FakeService("health", log),
        sessions=FakeSessions(log),
    )

    await application.start()

    assert application.start_log == [
        "database",
        "recovery",
        "config_watcher",
        "matrix",
        "heartbeat",
        "health",
    ]
    assert application.readiness.ready

    await application.stop()
    assert log[-6:] == [
        "stop:health",
        "stop:heartbeat",
        "stop:matrix",
        "stop:config_watcher",
        "save:sessions",
        "stop:database",
    ]


@pytest.mark.asyncio
async def test_application_stop_is_idempotent() -> None:
    log: list[str] = []
    application = ManagerApplication(
        database=FakeDatabase(log),
        recovery=FakeRecovery(log),
        config_watcher=FakeService("config_watcher", log),
        matrix=FakeService("matrix", log),
        heartbeat=FakeService("heartbeat", log),
        health=FakeService("health", log),
        sessions=FakeSessions(log),
    )
    await application.start()

    await application.stop()
    once = tuple(log)
    await application.stop()

    assert tuple(log) == once
