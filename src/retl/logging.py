"""Internal logging helpers for the RETL logger namespace."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping
from typing import IO, Literal

from retl.runtime.redaction import redact_text, redact_value

LOGGER_NAME = "retl"
LogFormat = Literal["text", "json"]

_DEFAULT_NULL_HANDLER = logging.NullHandler()
_DEFAULT_HANDLER_MARKER = "_retl_default_null_handler"
_CONFIGURED_HANDLER_MARKER = "_retl_configured_handler"
setattr(_DEFAULT_NULL_HANDLER, _DEFAULT_HANDLER_MARKER, True)

_TEXT_FORMAT = "%(levelname)s %(name)s %(message)s"
_RESERVED_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__) | {
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
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}
_STABLE_JSON_FIELDS = frozenset({"message", "level", "logger"})


def install_null_handler() -> None:
    """Attach RETL's default NullHandler to the package logger once."""
    logger = logging.getLogger(LOGGER_NAME)
    if any(getattr(handler, _DEFAULT_HANDLER_MARKER, False) for handler in logger.handlers):
        return
    logger.addHandler(_DEFAULT_NULL_HANDLER)


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger in RETL's package namespace."""
    if name is None or name == LOGGER_NAME:
        return logging.getLogger(LOGGER_NAME)
    if name.startswith(f"{LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def configure_logging(
    *,
    level: int | str = "INFO",
    format: LogFormat = "text",
    handler: logging.Handler | None = None,
    stream: IO[str] | None = None,
) -> logging.Logger:
    """Configure the RETL package logger without mutating root logging."""
    resolved_level = _resolve_level(level)
    formatter = _formatter_for(format)
    logger = logging.getLogger(LOGGER_NAME)

    logger.setLevel(resolved_level)
    logger.propagate = False

    if handler is None:
        configured_handler = _configured_handler(logger)
        if configured_handler is None:
            configured_handler = logging.StreamHandler(stream or sys.stderr)
            setattr(configured_handler, _CONFIGURED_HANDLER_MARKER, True)
            logger.addHandler(configured_handler)
        elif stream is not None and isinstance(configured_handler, logging.StreamHandler):
            configured_handler.setStream(stream)
        configured_handler.setLevel(resolved_level)
        configured_handler.setFormatter(formatter)
        return logger

    handler.setLevel(resolved_level)
    handler.setFormatter(formatter)
    if handler not in logger.handlers:
        logger.addHandler(handler)
    return logger


class _RedactedTextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__(_TEXT_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        copied = logging.makeLogRecord({**record.__dict__, "msg": redact_text(record.getMessage())})
        copied.args = ()
        return super().format(copied)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "message": redact_text(record.getMessage()),
            "level": record.levelname,
            "logger": record.name,
        }
        payload.update(_safe_extra_fields(record))
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _configured_handler(logger: logging.Logger) -> logging.Handler | None:
    for existing in logger.handlers:
        if getattr(existing, _CONFIGURED_HANDLER_MARKER, False):
            return existing
    return None


def _formatter_for(format: str) -> logging.Formatter:
    if format == "text":
        return _RedactedTextFormatter()
    if format == "json":
        return _JsonFormatter()
    raise ValueError("Logging format must be one of: 'text', 'json'.")


def _resolve_level(level: int | str) -> int:
    if isinstance(level, int):
        if level < 0:
            raise ValueError("Logging level must be a standard non-negative logging level.")
        return level
    if not isinstance(level, str):
        raise ValueError("Logging level must be an int or standard level name.")
    level_name = level.upper()
    resolved = logging.getLevelName(level_name)
    if isinstance(resolved, int):
        return resolved
    raise ValueError(
        "Logging level must be a standard level name such as DEBUG, INFO, WARNING, ERROR, "
        "or CRITICAL."
    )


def _safe_extra_fields(record: logging.LogRecord) -> Mapping[str, object]:
    fields: dict[str, object] = {}
    for key, value in record.__dict__.items():
        if key in _RESERVED_LOG_RECORD_FIELDS or key in _STABLE_JSON_FIELDS or key.startswith("_"):
            continue
        fields[key] = redact_value(key, value)
    return fields


install_null_handler()
