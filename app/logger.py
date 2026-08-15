"""Logging centralizado: formato JSON + request_id via contextvars (H-043).

`get_logger(name)` conserva la firma pública; cada log incluye `request_id`
si el middleware de correlation ID lo seteó (app.main).
"""
from __future__ import annotations

import contextvars
import json
import logging
import time

import app.settings as settings  # H-045: LOG_LEVEL viene de settings

# ContextVar que el middleware de main setea por request.
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def set_request_id(value: str) -> None:
    """Setea el request_id del contexto actual (llamado por el middleware)."""
    request_id_var.set(value or "-")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "request_id": request_id_var.get(),
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


_LEVEL_MAP = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR}

_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())

root = logging.getLogger()
root.handlers = [_handler]
root.setLevel(_LEVEL_MAP.get(getattr(settings, "LOG_LEVEL", "INFO"), logging.INFO))


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
