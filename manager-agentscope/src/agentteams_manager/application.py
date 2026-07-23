"""Lifecycle ownership for the single Manager daemon."""

from __future__ import annotations

import asyncio
from typing import Any

from .health import ReadinessState


class ManagerApplication:
    def __init__(
        self,
        *,
        database: Any,
        recovery: Any,
        config_watcher: Any,
        matrix: Any,
        heartbeat: Any,
        health: Any,
        sessions: Any,
        readiness: ReadinessState | None = None,
    ) -> None:
        self._database = database
        self._recovery = recovery
        self._config_watcher = config_watcher
        self._matrix = matrix
        self._heartbeat = heartbeat
        self._health = health
        self._sessions = sessions
        self.readiness = (
            readiness
            or getattr(health, "readiness", None)
            or ReadinessState()
        )
        self.start_log: list[str] = []
        self._started = False
        self._stopped = False
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._started:
            return
        await self._database.open()
        self.readiness.database_ready = True
        self.start_log.append("database")

        await self._recovery.restore()
        self.readiness.recovery_ready = True
        self.start_log.append("recovery")

        await self._config_watcher.start()
        self.readiness.config_ready = bool(
            getattr(self._config_watcher, "ready", True),
        )
        self.start_log.append("config_watcher")

        await self._matrix.start()
        self.readiness.matrix_ready = bool(
            getattr(self._matrix, "ready", True),
        )
        self.start_log.append("matrix")

        await self._heartbeat.start()
        self.readiness.heartbeat_ready = bool(
            getattr(self._heartbeat, "ready", True),
        )
        self.start_log.append("heartbeat")

        await self._health.start()
        self.start_log.append("health")
        self._started = True

    async def run(self) -> None:
        await self.start()
        await self._stop_event.wait()
        await self.stop()

    def request_stop(self) -> None:
        self._stop_event.set()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._started:
            await self._health.stop()
            await self._heartbeat.stop()
            await self._matrix.stop()
            await self._config_watcher.stop()
            await self._sessions.save_all()
            await self._database.close()
        self.readiness.database_ready = False
        self.readiness.recovery_ready = False
        self.readiness.config_ready = False
        self.readiness.matrix_ready = False
        self.readiness.heartbeat_ready = False
