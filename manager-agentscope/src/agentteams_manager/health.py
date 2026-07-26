"""Standard-library operational HTTP server."""

from __future__ import annotations

import asyncio
import hmac
import json
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass

from pydantic import SecretStr

from agentteams_manager.admin.ui import ADMIN_HTML
from agentteams_manager.observability.metrics import MetricsRegistry

AdminSnapshot = Callable[[str], Awaitable[dict[str, object]]]
WebhookHandler = Callable[
    [str, dict[str, str], bytes],
    Awaitable[object],
]


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
        admin_token: SecretStr | None = None,
        admin_snapshot: AdminSnapshot | None = None,
        webhook_handler: WebhookHandler | None = None,
    ) -> None:
        self.readiness = readiness
        self._metrics = metrics
        self._host = host
        self._port = port
        self._admin_token = admin_token
        self._admin_snapshot = admin_snapshot
        self._webhook_handler = webhook_handler
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
            headers = _headers(request)
            if (
                method == "POST"
                and path.startswith("/manager-admin/hooks/")
            ):
                await self._channel_webhook(
                    reader,
                    writer,
                    path,
                    headers,
                )
            elif method != "GET":
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
            elif path in {"/manager-admin", "/manager-admin/"}:
                await self._respond(
                    writer,
                    200,
                    ADMIN_HTML.encode("utf-8"),
                    content_type="text/html; charset=utf-8",
                )
            elif path.startswith("/manager-admin/api/"):
                await self._admin_api(writer, path, headers)
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

    async def _channel_webhook(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        path: str,
        headers: dict[str, str],
    ) -> None:
        if self._webhook_handler is None:
            await self._respond(writer, 404, b"not found\n")
            return
        try:
            length = int(headers.get("content-length", "0"))
        except ValueError:
            await self._respond(writer, 400, b"bad content length\n")
            return
        if length < 1 or length > 1024 * 1024:
            await self._respond(writer, 413, b"payload too large\n")
            return
        provider = path.removeprefix("/manager-admin/hooks/")
        try:
            body = await asyncio.wait_for(
                reader.readexactly(length),
                timeout=3,
            )
            result = await self._webhook_handler(
                provider,
                headers,
                body,
            )
        except PermissionError:
            await self._respond(writer, 403, b"forbidden\n")
            return
        except (KeyError, ValueError):
            await self._respond(writer, 400, b"invalid webhook\n")
            return
        payload = (
            result.model_dump(mode="json")
            if hasattr(result, "model_dump")
            else result
        )
        await self._json(writer, 202, payload)

    async def _admin_api(
        self,
        writer: asyncio.StreamWriter,
        path: str,
        headers: dict[str, str],
    ) -> None:
        if self._admin_token is None or self._admin_snapshot is None:
            await self._respond(writer, 503, b"admin console disabled\n")
            return
        expected = (
            "Bearer " + self._admin_token.get_secret_value()
        ).encode()
        supplied = headers.get("authorization", "").encode()
        if not hmac.compare_digest(supplied, expected):
            await self._respond(writer, 401, b"unauthorized\n")
            return
        section = path.removeprefix("/manager-admin/api/")
        allowed = {
            "overview",
            "sessions",
            "confirmations",
            "projects",
            "workers",
            "teams",
            "heartbeat",
            "runtime",
        }
        if section not in allowed:
            await self._respond(writer, 404, b"not found\n")
            return
        await self._json(
            writer,
            200,
            await self._admin_snapshot(section),
        )

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
            202: "Accepted",
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            413: "Content Too Large",
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


def _headers(request: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in request.split(b"\r\n")[1:]:
        if not raw_line:
            break
        name, separator, value = raw_line.partition(b":")
        if not separator:
            continue
        result[name.decode("ascii", errors="ignore").lower()] = (
            value.decode("utf-8", errors="replace").strip()
        )
    return result
