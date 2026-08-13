"""Secret-safe structured JSON logging.

输出便于检索且默认脱敏的结构化 JSON 日志。

同一个 operation 会跨 Matrix、Controller 和 MinIO，结构化字段可用稳定 ID 串起整条
链路。但日志通常被长期保存，因此在序列化前按敏感字段名递归替换 token、password、
authorization 等内容；排障价值不能以泄露 Secret 为代价。
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

_SENSITIVE_PARTS = (
    "token",
    "secret",
    "password",
    "authorization",
    "api_key",
)


def redact_fields(value: Any, *, key: str = "") -> Any:
    # 逻辑说明：按字段名递归遍历容器并在序列化前替换敏感值，避免嵌套 token 被结构化日志泄露。
    if any(part in key.casefold() for part in _SENSITIVE_PARTS):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            child_key: redact_fields(child, key=str(child_key))
            for child_key, child in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_fields(child) for child in value)
    if isinstance(value, list):
        return [redact_fields(child) for child in value]
    return value


class JsonFormatter(logging.Formatter):
    """Render log records as one redacted JSON object per line."""

    _standard = frozenset(
        {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        },
    )

    def format(self, record: logging.LogRecord) -> str:
        # 逻辑说明：合并标准日志字段与业务上下文、格式化异常，然后统一脱敏并输出单行 JSON。
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        payload.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in self._standard and not key.startswith("_")
            },
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(
            redact_fields(payload),
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )


def configure_logging(level: str = "INFO") -> None:
    # 逻辑说明：原子式重设根 logger 的唯一 JSON handler，使依赖库日志也遵循同一脱敏格式和级别。
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
