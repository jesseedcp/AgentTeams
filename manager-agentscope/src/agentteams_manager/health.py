"""Standard-library operational HTTP server."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass

from agentteams_manager.observability.metrics import MetricsRegistry


@dataclass(slots=True)
class ReadinessState:
    database_ready: bool = False
    recovery_ready: bool = False
    config_ready: bool = False
    matrix_ready: bool = False
    heartbeat_ready: bool = False

    @property
    def ready(self) -> bool:
        return all(asdict(self).values())

    def as_dict(self) -> dict[str, bool]:
        return {**asdict(self), "ready": self.ready}


class HealthServer:
    def __init__(
        self,
        *,
        readiness: ReadinessState,
        metrics: MetricsRegistry,
        host: str = "0.0.0.0",
        port: int = 18799,
    ) -> None:
        self.readiness = readiness
        self._metrics = metrics
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None

    @property
    def bound_port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("health server is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle,
            self._host,
            self._port,
            limit=16 * 1024,
        )

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=3,
            )
            first_line = request.split(b"\r\n", 1)[0].decode(
                "ascii",
                errors="replace",
            )
            parts = first_line.split()
            if len(parts) != 3:
                await self._respond(writer, 400, b"bad request\n")
                return
            method, raw_path, _ = parts
            path = raw_path.split("?", 1)[0]
            if method != "GET":
                await self._respond(writer, 405, b"method not allowed\n")
            elif path == "/healthz":
                await self._json(
                    writer,
                    200,
                    {"status": "ok"},
                )
            elif path == "/readyz":
                await self._json(
                    writer,
                    200 if self.readiness.ready else 503,
                    self.readiness.as_dict(),
                )
            elif path == "/metrics":
                await self._respond(
                    writer,
                    200,
                    self._metrics.render().encode("utf-8"),
                    content_type="text/plain; version=0.0.4",
                )
            else:
                await self._respond(writer, 404, b"not found\n")
        except (
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
            TimeoutError,
        ):
            await self._respond(writer, 400, b"bad request\n")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _json(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        payload: dict[str, object],
    ) -> None:
        await self._respond(
            writer,
            status,
            json.dumps(
                payload,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
            content_type="application/json",
        )

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
        *,
        content_type: str = "text/plain; charset=utf-8",
    ) -> None:
        reasons = {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
            503: "Service Unavailable",
        }
        header = (
            f"HTTP/1.1 {status} {reasons[status]}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        writer.write(header + body)
        await writer.drain()

