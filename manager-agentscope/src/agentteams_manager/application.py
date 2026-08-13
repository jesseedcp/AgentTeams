"""Lifecycle ownership for the single Manager daemon.

统一管理 AgentScope Manager 守护进程的启动和停止顺序。

以一次正常启动为例：本模块先开放健康检查端口，再打开 SQLite、恢复未完成操作、
加载 Controller 下发的运行配置，最后才连接 Matrix 并启动 heartbeat。这个顺序很
重要；如果恢复和配置尚未完成就接收管理员消息，新请求可能与旧操作重复执行。
停止时按大致相反的顺序收尾，并先撤销 readiness，避免流量进入正在关闭的进程。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

from .health import ReadinessState

logger = logging.getLogger(__name__)


class ManagerApplication:
    """拥有单个 Manager 进程全部长生命周期组件。

    ``start`` 只有在恢复、运行配置、Matrix 与 heartbeat 都准备好后才成功；任一步失败
    都会撤销 readiness 并关闭已启动组件。``stop`` 可重复调用，便于信号处理和异常路径
    同时请求停止而不会重复释放同一个连接。
    """
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
        # 逻辑说明：保存各生命周期组件和就绪状态依赖；构造阶段不启动网络服务或后台任务。
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
        """按依赖顺序启动组件，并只在安全服务时标记 ready。"""
        # 逻辑说明：按依赖顺序启动组件，全部通过就绪检查后开放流量；失败按反序回滚。
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
            self.readiness.bind_matrix_probe(
                lambda: _service_ready(self._matrix),
            )
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
        # 逻辑说明：先启动应用再等待停止事件，无论正常取消或异常都进入 stop 统一清理。
        await self.start()
        await self._stop_event.wait()
        await self.stop()

    def request_stop(self) -> None:
        # 逻辑说明：同步设置共享停止事件以唤醒 run_forever；Event.set 可重复调用，因此操作系统信号与管理路径同时请求停机也不会重复执行清理。
        self._stop_event.set()

    async def stop(self) -> None:
        """先拒绝新流量，再尽力保存会话并逆序关闭组件。"""
        # 逻辑说明：先清除 ready、保存会话，再逆序停止组件；重复调用不会重复清理。
        if self._stopped:
            return
        self._stopped = True
        self._clear_readiness()
        await self._stop_components()

    def _clear_readiness(self) -> None:
        # 逻辑说明：将健康状态恢复到未就绪，避免关闭期间负载均衡继续发送请求。
        self.readiness.bind_matrix_probe(None)
        self.readiness.database_ready = False
        self.readiness.recovery_ready = False
        self.readiness.config_ready = False
        self.readiness.matrix_ready = False
        self.readiness.heartbeat_ready = False

    async def _stop_components(self) -> None:
        # 逻辑说明：按启动相反顺序停止组件；单个失败仍继续清理其余组件并记录异常。
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
    # 逻辑说明：调用可选 ready 方法并规范为 bool；无该接口的已启动组件视为可用。
    value = getattr(service, "ready", True)
    is_set = getattr(value, "is_set", None)
    return bool(is_set()) if is_set is not None else bool(value)
