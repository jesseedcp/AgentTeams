"""Lifecycle ownership for the single Manager daemon."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

from .health import ReadinessState

logger = logging.getLogger(__name__)


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
        startup_hooks: tuple[Any, ...] = (),
        closeables: tuple[Any, ...] = (),
    ) -> None:
        self._database = database
        self._recovery = recovery
        self._config_watcher = config_watcher
        self._matrix = matrix
        self._heartbeat = heartbeat
        self._health = health
        self._sessions = sessions
        self._startup_hooks = startup_hooks
        self._closeables = closeables
        self.readiness = (
            readiness
            or getattr(health, "readiness", None)
            or ReadinessState()
        )
        self.start_log: list[str] = []
        self._started = False
        self._stopped = False
        self._components_stopped = False
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._started:
            return
        try:
            await self._health.start()
            self.start_log.append("health")

            await self._database.open()
            self.readiness.database_ready = True
            self.start_log.append("database")

            await self._recovery.restore()
            self.readiness.recovery_ready = True
            self.start_log.append("recovery")

            # A restored snapshot can predate the running schema.
            await self._database.open()
            for hook in self._startup_hooks:
                result = hook()
                if inspect.isawaitable(result):
                    await result

            await self._config_watcher.start()
            self.readiness.config_ready = _service_ready(
                self._config_watcher,
            )
            if not self.readiness.config_ready:
                raise RuntimeError("runtime configuration is not ready")
            self.start_log.append("config_watcher")

            await self._matrix.start()
            self.readiness.matrix_ready = _service_ready(self._matrix)
            if not self.readiness.matrix_ready:
                raise RuntimeError("Matrix transport is not ready")
            self.start_log.append("matrix")

            await self._heartbeat.start()
            self.readiness.heartbeat_ready = _service_ready(
                self._heartbeat,
            )
            if not self.readiness.heartbeat_ready:
                raise RuntimeError("heartbeat is not ready")
            self.start_log.append("heartbeat")

            self._started = True
        except Exception:
            self._stopped = True
            self._clear_readiness()
            await self._stop_components()
            raise

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
        self._clear_readiness()
        await self._stop_components()

    def _clear_readiness(self) -> None:
        self.readiness.database_ready = False
        self.readiness.recovery_ready = False
        self.readiness.config_ready = False
        self.readiness.matrix_ready = False
        self.readiness.heartbeat_ready = False

    async def _stop_components(self) -> None:
        if self._components_stopped:
            return
        self._components_stopped = True
        started = set(self.start_log)
        for name, component in (
            ("heartbeat", self._heartbeat),
            ("matrix", self._matrix),
            ("config_watcher", self._config_watcher),
        ):
            if name not in started:
                continue
            try:
                await component.stop()
            except Exception:
                logger.exception(
                    "Manager component shutdown failed",
                    extra={"component": name},
                )
        if "database" in started:
            try:
                close_sessions = getattr(
                    self._sessions,
                    "close_all",
                    self._sessions.save_all,
                )
                await close_sessions()
            except Exception:
                logger.exception("Manager session shutdown failed")
            try:
                await self._database.close()
            except Exception:
                logger.exception("Manager database shutdown failed")
        for closeable in reversed(self._closeables):
            close = getattr(closeable, "close", None)
            if close is None:
                continue
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception(
                    "Manager external dependency shutdown failed",
                    extra={"dependency": type(closeable).__name__},
                )
        if "health" in started:
            try:
                await self._health.stop()
            except Exception:
                logger.exception("Manager health shutdown failed")


def _service_ready(service: Any) -> bool:
    value = getattr(service, "ready", True)
    is_set = getattr(value, "is_set", None)
    return bool(is_set()) if is_set is not None else bool(value)
