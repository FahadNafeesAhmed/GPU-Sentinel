"""Structured logging: one event per line, JSON or key=value, with bound context."""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

_STD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(record.created, 3),
            "iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        for key, val in record.__dict__.items():
            if key not in _STD_ATTRS and key != "fields" and not key.startswith("_"):
                payload[key] = val
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(record.created)) + f".{int(record.msecs):03d}"
        fields = getattr(record, "fields", None) or {}
        kv = " ".join(f"{k}={_short(v)}" for k, v in fields.items())
        line = f"{ts} {record.levelname:<5} {record.name}: {record.getMessage()}"
        if kv:
            line += "  " + kv
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def _short(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.3f}"
    s = str(v)
    return s if len(s) <= 80 else s[:77] + "..."


class StructLogger:
    """Thin wrapper so call sites can write ``log.info("probe_failed", worker="w1")``."""

    def __init__(self, logger: logging.Logger, **bound: Any) -> None:
        self._logger = logger
        self._bound = bound

    def bind(self, **more: Any) -> StructLogger:
        return StructLogger(self._logger, **{**self._bound, **more})

    def _log(self, level: int, event: str, exc_info: Any = None, **fields: Any) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(
                level, event, extra={"fields": {**self._bound, **fields}}, exc_info=exc_info
            )

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, exc_info: Any = None, **fields: Any) -> None:
        self._log(logging.ERROR, event, exc_info=exc_info, **fields)


def get_logger(name: str, **bound: Any) -> StructLogger:
    return StructLogger(logging.getLogger(name), **bound)


def setup_logging(level: str = "INFO", fmt: str = "json", stream: Any = None) -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
