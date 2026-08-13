"""AgentTeams worker adapter API served beside the CoPaw app."""

# 初学者导读：这个轻量 HTTP 服务只暴露 liveness/readiness 等 Worker 适配状态，
# 让 Kubernetes 与 Controller 判断是否能派工。它不是 CoPaw 控制台、不是模型
# API，也不接受管理资源的写操作，因此不能替代 Manager 或 Controller API。

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


class WorkerAPIServer:
    """Small HTTP server for AgentTeams worker adapter endpoints.

    这里只服务 Worker 健康探针，是最小 asyncio HTTP server。
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        liveness_handler: Callable[[], Awaitable[dict[str, Any]]],
        readiness_handler: Callable[[], Awaitable[dict[str, Any]]],
    ) -> None:
        # 逻辑说明：`__init__` 接收 host、port、liveness_handler、readiness_handler，初始化健康 HTTP API 状态，返回 None；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        self.host = host
        self.port = port
        self._liveness_handler = liveness_handler
        self._readiness_handler = readiness_handler
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        # 逻辑说明：若健康 API 尚未启动，则在配置的 host/port 创建 asyncio TCP server 并保存实际 server；重复调用直接返回以避免二次绑定。
        # 端口占用或权限等监听失败由 asyncio 原样抛出，使 Worker 启动流程不能把未就绪的健康端点误报为成功。
        if self._server is not None:
            return
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        logger.info("worker API server listening host=%s port=%s", self.host, self.port)

    @property
    def bound_port(self) -> int:
        # 逻辑说明：`bound_port` 接收 当前对象/进程状态，返回服务器实际绑定端口；尚未启动时返回配置端口，返回 int；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
        # 本函数不额外重试，避免掩盖持续故障。
        if self._server is None or not self._server.sockets:
            return self.port
        return int(self._server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        # 逻辑说明：对已启动的健康 API 停止接收新连接、等待监听 socket 完全关闭，再清空 `_server` 使对象可重新启动；未启动时幂等返回。
        # wait_closed 失败会继续抛出，避免在操作系统仍持有监听资源时错误标记为已停止。
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        logger.info("worker API server stopped host=%s port=%s", self.host, self.port)

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # 逻辑说明：`_handle` 接收 reader、writer，读取一条 HTTP 请求，分派 livez/readyz handler 并始终关闭连接，返回 None；
        # 会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        try:
            request_line = await reader.readline()
            method, path = _parse_request_line(request_line)
            while True:
                line = await reader.readline()
                if not line or line in (b"\r\n", b"\n"):
                    break

            if method == "GET" and path == "/worker/livez":
                payload = await self._liveness_handler()
                _write_json(writer, 200, payload)
            elif method == "GET" and path == "/worker/readyz":
                payload = await self._readiness_handler()
                status = 200 if payload.get("readiness") == "ready" else 503
                _write_json(writer, status, payload)
            else:
                _write_json(writer, 404, {"message": "not found"})
            await writer.drain()
        except Exception:
            logger.exception("worker API request failed")
            with suppress(Exception):
                _write_json(writer, 500, {"message": "internal server error"})
                await writer.drain()
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()


def _parse_request_line(request_line: bytes) -> tuple[str, str]:
    # 逻辑说明：`_parse_request_line` 接收 request_line，从 HTTP 请求首行提取 method 与不含 query 的 path，返回 tuple[str, str]；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    parts = request_line.decode("ascii", errors="replace").strip().split()
    if len(parts) < 2:
        return "", ""
    return parts[0], parts[1].split("?", 1)[0]


def _write_json(
    writer: asyncio.StreamWriter,
    status: int,
    payload: dict[str, Any],
) -> None:
    # 逻辑说明：`_write_json` 接收 writer、status、payload，创建父目录并把字典格式化写入 JSON 文件，返回 None；
    # 会访问网络服务。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    reason = {
        200: "OK",
        404: "Not Found",
        500: "Error",
        503: "Service Unavailable",
    }.get(status, "Error")
    writer.write(
        b"".join(
            [
                f"HTTP/1.1 {status} {reason}\r\n".encode("ascii"),
                b"Content-Type: application/json\r\n",
                f"Content-Length: {len(body)}\r\n".encode("ascii"),
                b"Connection: close\r\n",
                b"\r\n",
                body,
            ]
        )
    )
