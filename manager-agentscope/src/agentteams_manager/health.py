"""Standard-library operational HTTP server.

提供不依赖 Web 框架的健康检查、指标和只读运维 HTTP 接口。

Kubernetes 通过 liveness 判断进程是否活着，通过 readiness 判断数据库恢复、配置、
Matrix 和 heartbeat 是否都已就绪。readiness 不是“网页能打开”这么简单：只要关键
依赖尚未准备好，就应拒绝把 Manager 当成可服务实例。管理快照会主动过滤敏感信息，
避免排障页面变成凭据泄露入口。
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, unquote, urlsplit

from pydantic import SecretStr, ValidationError

from agentteams_manager.admin.commands import AdminAPIError, AdminCommand
from agentteams_manager.admin.ui import ADMIN_HTML
from agentteams_manager.channels.base import (
    ChannelWebhookRequest,
    ChannelWebhookResponse,
)
from agentteams_manager.observability.metrics import MetricsRegistry

logger = logging.getLogger(__name__)

AdminSnapshot = Callable[[str], Awaitable[dict[str, object]]]
AdminCommandHandler = Callable[
    [AdminCommand],
    Awaitable[dict[str, object]],
]
WebhookHandler = Callable[
    [str, ChannelWebhookRequest],
    Awaitable[ChannelWebhookResponse],
]
CapabilitySnapshot = Callable[[], Mapping[str, object]]
LivenessProbe = Callable[[], bool]


@dataclass(slots=True)
class ReadinessState:
    database_ready: bool = False
    recovery_ready: bool = False
    config_ready: bool = False
    matrix_ready: bool = False
    heartbeat_ready: bool = False
    _matrix_probe: Callable[[], bool] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def bind_matrix_probe(
        self,
        probe: Callable[[], bool] | None,
    ) -> None:
        # 逻辑说明：绑定 Matrix live/ready 探针供请求即时刷新；这里只保存回调不主动调用。
        self._matrix_probe = probe

    def _refresh_matrix(self) -> None:
        # 逻辑说明：调用可选探针更新 Matrix 状态；探针异常按未就绪处理而不使服务崩溃。
        if self._matrix_probe is None:
            return
        try:
            self.matrix_ready = bool(self._matrix_probe())
        except Exception:
            self.matrix_ready = False

    @property
    def ready(self) -> bool:
        # 逻辑说明：刷新 Matrix 后汇总所有必需组件，只有全部满足才对外报告 ready。
        self._refresh_matrix()
        return all(
            (
                self.database_ready,
                self.recovery_ready,
                self.config_ready,
                self.matrix_ready,
                self.heartbeat_ready,
            ),
        )

    def as_dict(self) -> dict[str, bool]:
        # 逻辑说明：生成健康快照，包含总状态与各组件布尔值，供页面和探针读取。
        ready = self.ready
        return {
            "database_ready": self.database_ready,
            "recovery_ready": self.recovery_ready,
            "config_ready": self.config_ready,
            "matrix_ready": self.matrix_ready,
            "heartbeat_ready": self.heartbeat_ready,
            "ready": ready,
        }


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
        admin_command: AdminCommandHandler | None = None,
        webhook_handler: WebhookHandler | None = None,
        capability_snapshot: CapabilitySnapshot | None = None,
        liveness_probe: LivenessProbe | None = None,
    ) -> None:
        # 逻辑说明：保存健康、指标、webhook 和管理 API 依赖；socket 延迟到 start 创建。
        self.readiness = readiness
        self._metrics = metrics
        self._host = host
        self._port = port
        self._admin_token = admin_token
        self._admin_snapshot = admin_snapshot
        self._admin_command = admin_command
        self._webhook_handler = webhook_handler
        self._capability_snapshot = capability_snapshot
        self._liveness_probe = liveness_probe
        self._server: asyncio.Server | None = None

    @property
    def bound_port(self) -> int:
        # 逻辑说明：从实际监听 socket 读取端口；尚未启动时返回零。
        if self._server is None or not self._server.sockets:
            raise RuntimeError("health server is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        # 逻辑说明：幂等启动 asyncio HTTP server 并保存句柄，端口为零时由 socket 分配。
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle,
            self._host,
            self._port,
            limit=16 * 1024,
        )

    async def stop(self) -> None:
        # 逻辑说明：取出 server、停止接受连接并等待关闭；未启动时为空操作。
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
        # 逻辑说明：解析请求并分派健康、指标、webhook 或管理 API；异常统一转 JSON 错误。
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
            parsed_path = urlsplit(raw_path)
            path = parsed_path.path
            query = dict(
                parse_qsl(
                    parsed_path.query,
                    keep_blank_values=True,
                ),
            )
            headers = _headers(request)
            if (
                method in {"GET", "POST"}
                and path.startswith("/manager-admin/hooks/")
            ):
                await self._channel_webhook(
                    reader,
                    writer,
                    method,
                    path,
                    headers,
                    query,
                )
            elif path.startswith("/manager-admin/api/v1/"):
                await self._admin_resource_api(
                    reader,
                    writer,
                    method,
                    path,
                    headers,
                )
            elif method != "GET":
                await self._respond(writer, 405, b"method not allowed\n")
            elif path == "/healthz":
                live = True
                if self._liveness_probe is not None:
                    try:
                        live = bool(self._liveness_probe())
                    except Exception:
                        live = False
                payload: dict[str, object] = {
                    "status": "ok" if live else "unhealthy",
                }
                if self._capability_snapshot is not None:
                    payload["capabilities"] = dict(
                        self._capability_snapshot(),
                    )
                await self._json(
                    writer,
                    200 if live else 503,
                    payload,
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
        method: str,
        path: str,
        headers: dict[str, str],
        query: dict[str, str],
    ) -> None:
        # 逻辑说明：校验 handler 和请求体后交给 channel 适配器，按结果返回标准 JSON。
        if self._webhook_handler is None:
            await self._respond(writer, 404, b"not found\n")
            return
        try:
            length = int(headers.get("content-length", "0"))
        except ValueError:
            await self._respond(writer, 400, b"bad content length\n")
            return
        if length < 0 or length > 1024 * 1024:
            await self._respond(writer, 413, b"payload too large\n")
            return
        content_type = headers.get("content-type", "")
        media_type = content_type.partition(";")[0].strip().casefold()
        if (
            method == "POST"
            and length
            and media_type
            and media_type != "application/json"
        ):
            await self._respond(
                writer,
                415,
                b"webhook body must be JSON\n",
            )
            return
        provider = path.removeprefix("/manager-admin/hooks/")
        try:
            body = (
                await asyncio.wait_for(
                    reader.readexactly(length),
                    timeout=3,
                )
                if length
                else b""
            )
            result = await self._webhook_handler(
                provider,
                ChannelWebhookRequest(
                    method=method,
                    headers=headers,
                    query=query,
                    body=body,
                ),
            )
        except PermissionError:
            await self._respond(writer, 401, b"unauthorized\n")
            return
        except (KeyError, TypeError, ValueError):
            await self._respond(writer, 400, b"invalid webhook\n")
            return
        await self._respond(
            writer,
            result.status_code,
            result.body,
            content_type=result.content_type,
            headers=result.response_headers,
        )

    async def _admin_api(
        self,
        writer: asyncio.StreamWriter,
        path: str,
        headers: dict[str, str],
    ) -> None:
        # 逻辑说明：验证管理员 token 并返回只读快照；未配置能力或未授权时拒绝。
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

    async def _admin_resource_api(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        method: str,
        path: str,
        headers: dict[str, str],
    ) -> None:
        # 逻辑说明：解析资源路径/方法、验证 token 与 JSON 后调用管理命令并映射错误。
        if self._admin_token is None or self._admin_command is None:
            await self._json_error(
                writer,
                503,
                "admin_console_disabled",
                "admin console is disabled",
            )
            return
        if not self._admin_authorized(headers):
            await self._json_error(
                writer,
                401,
                "unauthorized",
                "a valid admin bearer token is required",
            )
            return
        if method not in {"GET", "POST", "PATCH", "DELETE"}:
            await self._json_error(
                writer,
                405,
                "method_not_allowed",
                "method is not allowed for this endpoint",
            )
            return

        suffix = path.removeprefix("/manager-admin/api/v1/")
        segments = suffix.split("/")
        if (
            not segments
            or segments[0] not in {"workers", "teams", "projects"}
            or len(segments) > 2
            or any(not segment for segment in segments)
        ):
            await self._json_error(
                writer,
                404,
                "not_found",
                "admin resource endpoint does not exist",
            )
            return

        payload: dict[str, object] = {}
        idempotency_key = headers.get("idempotency-key")
        if method != "GET":
            if not idempotency_key:
                await self._json_error(
                    writer,
                    400,
                    "idempotency_key_required",
                    "Idempotency-Key is required for admin writes",
                )
                return
            body = await self._read_admin_json(reader, writer, headers)
            if body is None:
                return
            payload = body

        try:
            command = AdminCommand.model_validate(
                {
                    "method": method,
                    "resource": segments[0],
                    "name": (
                        unquote(segments[1])
                        if len(segments) == 2
                        else None
                    ),
                    "payload": payload,
                    "idempotency_key": idempotency_key,
                },
            )
            result = await self._admin_command(command)
        except ValidationError as exc:
            await self._json_error(
                writer,
                422,
                "validation_error",
                "request validation failed",
                details=json.loads(exc.json(include_url=False)),
            )
            return
        except AdminAPIError as exc:
            await self._json_error(
                writer,
                exc.status,
                exc.code,
                exc.message,
                details=exc.details,
            )
            return
        except Exception:
            logger.exception("unhandled Manager admin API failure")
            await self._json_error(
                writer,
                500,
                "internal_error",
                "the admin operation could not be completed",
            )
            return

        await self._json(
            writer,
            201 if method == "POST" else 200,
            result,
        )

    async def _read_admin_json(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
    ) -> dict[str, object] | None:
        # 逻辑说明：要求 JSON 类型、限制 body 大小并验证根对象；格式问题返回客户端错误。
        content_type = headers.get("content-type", "")
        media_type = content_type.partition(";")[0].strip().casefold()
        if media_type != "application/json":
            await self._json_error(
                writer,
                415,
                "unsupported_media_type",
                "admin write bodies must use application/json",
            )
            return None
        try:
            length = int(headers.get("content-length", "0"))
        except ValueError:
            await self._json_error(
                writer,
                400,
                "invalid_content_length",
                "Content-Length must be an integer",
            )
            return None
        if length < 0:
            await self._json_error(
                writer,
                400,
                "invalid_content_length",
                "Content-Length cannot be negative",
            )
            return None
        if length > 64 * 1024:
            await self._json_error(
                writer,
                413,
                "payload_too_large",
                "admin request bodies cannot exceed 64 KiB",
            )
            return None
        try:
            raw_body = await asyncio.wait_for(
                reader.readexactly(length),
                timeout=3,
            )
            parsed = json.loads(raw_body)
        except (asyncio.IncompleteReadError, TimeoutError):
            await self._json_error(
                writer,
                400,
                "incomplete_body",
                "request body was incomplete",
            )
            return None
        except (json.JSONDecodeError, UnicodeDecodeError):
            await self._json_error(
                writer,
                400,
                "invalid_json",
                "request body is not valid JSON",
            )
            return None
        if not isinstance(parsed, dict):
            await self._json_error(
                writer,
                400,
                "invalid_body",
                "request body must be a JSON object",
            )
            return None
        return parsed

    def _admin_authorized(self, headers: dict[str, str]) -> bool:
        # 逻辑说明：提取 Bearer token 并做常量时间比较，降低凭据比较侧信道。
        if self._admin_token is None:
            return False
        expected = (
            "Bearer " + self._admin_token.get_secret_value()
        ).encode()
        supplied = headers.get("authorization", "").encode()
        return hmac.compare_digest(supplied, expected)

    async def _json_error(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        code: str,
        message: str,
        *,
        details: object | None = None,
    ) -> None:
        # 逻辑说明：构造统一错误 envelope，可选附加详情，再通过 JSON responder 发送。
        error: dict[str, object] = {
            "code": code,
            "message": message,
        }
        if details is not None:
            error["details"] = details
        await self._json(writer, status, {"error": error})

    async def _json(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        payload: Mapping[str, object],
    ) -> None:
        # 逻辑说明：将对象 UTF-8 JSON 编码，补齐 content-type 后交给底层响应函数。
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
        headers: Mapping[str, str] | None = None,
    ) -> None:
        # 逻辑说明：构造 HTTP/1.1 状态、长度和关闭头，写入后可靠关闭 stream。
        reasons = {
            200: "OK",
            201: "Created",
            204: "No Content",
            202: "Accepted",
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            409: "Conflict",
            413: "Content Too Large",
            415: "Unsupported Media Type",
            422: "Unprocessable Content",
            500: "Internal Server Error",
            503: "Service Unavailable",
        }
        extra_headers = "".join(
            f"{name}: {value}\r\n"
            for name, value in (headers or {}).items()
        )
        header = (
            f"HTTP/1.1 {status} {reasons.get(status, 'Unknown')}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"{extra_headers}"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        writer.write(header + body)
        await writer.drain()


def _headers(request: bytes) -> dict[str, str]:
    # 逻辑说明：从原始请求解析小写 header 映射，跳过请求行和畸形行。
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
