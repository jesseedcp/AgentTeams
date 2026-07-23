"""Secret-safe structured JSON logging."""

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
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

