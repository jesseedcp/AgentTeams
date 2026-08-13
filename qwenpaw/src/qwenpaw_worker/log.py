"""Logging setup for qwenpaw-worker."""

# 初学者导读：统一日志配置让容器标准输出和持久化日志使用同一种格式，同时用
# rotating handler 限制文件大小。这里不能记录 access token、模型 key 或完整
# runtime config；日志可能被诊断工具导出，敏感值一旦写入就会跨越原本的权限边界。

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
from typing import Optional


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 20
MAX_LOG_MAX_BYTES = 20 * 1024 * 1024
MAX_LOG_BACKUP_COUNT = 50
DEFAULT_LOG_FILE_NAME = "qwenpaw-worker.log"

_CONSOLE_HANDLER_MARK = "_qwenpaw_worker_console_handler"
_FILE_HANDLER_MARK = "_qwenpaw_worker_file_handler"
_FALSE_VALUES = {"0", "false", "no", "off"}


def configure_worker_logging(working_dir: Optional[Path] = None) -> Optional[Path]:
    """Configure console and rotating file logging for qwenpaw-worker."""
    # 逻辑说明：幂等设置根日志级别和控制台 handler；若启用文件日志，再创建轮转文件并返回其路径。

    root = logging.getLogger()
    level = _log_level()
    formatter = logging.Formatter(LOG_FORMAT)
    root.setLevel(level)

    _ensure_console_handler(root, formatter, level)

    if not _file_logging_enabled():
        _remove_marked_handlers(root, _FILE_HANDLER_MARK)
        logging.getLogger(__name__).info(
            "worker logging configured component=worker stage=logging event=disabled file_enabled=False level=%s",
            logging.getLevelName(level),
        )
        return None

    log_file = _log_file_path(working_dir)
    max_bytes = _bounded_int(
        os.environ.get("QWENPAW_WORKER_LOG_MAX_BYTES"),
        DEFAULT_LOG_MAX_BYTES,
        minimum=1,
        maximum=MAX_LOG_MAX_BYTES,
    )
    backup_count = _bounded_int(
        os.environ.get("QWENPAW_WORKER_LOG_BACKUP_COUNT"),
        DEFAULT_LOG_BACKUP_COUNT,
        minimum=0,
        maximum=MAX_LOG_BACKUP_COUNT,
    )
    file_handler = _find_marked_handler(root, _FILE_HANDLER_MARK)
    if file_handler is not None:
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        if isinstance(file_handler, RotatingFileHandler):
            file_handler.maxBytes = max_bytes
            file_handler.backupCount = backup_count
        _log_configured(log_file, level, max_bytes, backup_count)
        return Path(getattr(file_handler, "baseFilename", log_file))

    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
    except OSError as exc:
        logging.getLogger(__name__).warning(
            "worker log file setup failed component=worker stage=logging event=failed path=%s error_type=%s",
            log_file,
            type(exc).__name__,
        )
        return None

    setattr(file_handler, _FILE_HANDLER_MARK, True)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    _log_configured(log_file, level, max_bytes, backup_count)
    return log_file


def _log_configured(log_file: Path, level: int, max_bytes: int, backup_count: int) -> None:
    # 逻辑说明：以结构化字段记录最终日志路径和轮转参数，便于部署排障核对实际配置。
    logging.getLogger(__name__).info(
        "worker logging configured component=worker stage=logging event=configured file_enabled=True "
        "path=%s max_bytes=%s backup_count=%s level=%s",
        log_file,
        max_bytes,
        backup_count,
        logging.getLevelName(level),
    )


def _ensure_console_handler(root: logging.Logger, formatter: logging.Formatter, level: int) -> None:
    # 逻辑说明：复用带标记的控制台 handler 或只创建一个，再同步级别与格式，避免重复输出。
    handler = _find_marked_handler(root, _CONSOLE_HANDLER_MARK)
    if handler is None:
        handler = logging.StreamHandler()
        setattr(handler, _CONSOLE_HANDLER_MARK, True)
        root.addHandler(handler)
    handler.setLevel(level)
    handler.setFormatter(formatter)


def _find_marked_handler(root: logging.Logger, mark: str) -> Optional[logging.Handler]:
    # 逻辑说明：在根 logger 中按私有标记查找本模块管理的 handler，不误改宿主程序的 handler。
    for handler in root.handlers:
        if getattr(handler, mark, False):
            return handler
    return None


def _remove_marked_handlers(root: logging.Logger, mark: str) -> None:
    # 逻辑说明：移除并关闭本模块创建的指定 handler，供关闭文件日志或重新配置时释放句柄。
    for handler in list(root.handlers):
        if getattr(handler, mark, False):
            root.removeHandler(handler)
            handler.close()


def _file_logging_enabled() -> bool:
    # 逻辑说明：解析文件日志开关；未配置默认开启，常见 false 文本会显式关闭。
    value = os.environ.get("QWENPAW_WORKER_LOG_FILE_ENABLED")
    if value is None:
        return True
    return value.strip().lower() not in _FALSE_VALUES


def _log_file_path(working_dir: Optional[Path]) -> Path:
    # 逻辑说明：依次按专用日志目录、环境工作目录、调用参数和当前目录选择日志文件位置。
    log_dir = os.environ.get("QWENPAW_WORKER_LOG_DIR")
    if log_dir:
        return Path(log_dir) / DEFAULT_LOG_FILE_NAME

    env_working_dir = os.environ.get("QWENPAW_WORKING_DIR")
    if env_working_dir:
        return Path(env_working_dir) / "logs" / DEFAULT_LOG_FILE_NAME

    if working_dir is not None:
        return Path(working_dir) / "logs" / DEFAULT_LOG_FILE_NAME

    return Path.cwd() / ".qwenpaw" / "logs" / DEFAULT_LOG_FILE_NAME


def _log_level() -> int:
    # 逻辑说明：接受数字或标准日志名称；无法识别时安全回落 INFO，避免配置拼写导致启动失败。
    value = os.environ.get("QWENPAW_LOG_LEVEL", "INFO").strip()
    if value.isdigit():
        return int(value)

    level = logging.getLevelName(value.upper())
    if isinstance(level, int):
        return level
    return logging.INFO


def _bounded_int(value: Optional[str], default: int, *, minimum: int, maximum: int) -> int:
    # 逻辑说明：把环境文本转成限定范围整数；格式错误或低于下限回落默认值，高于上限截断。
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    if parsed < minimum:
        return default
    return min(parsed, maximum)
