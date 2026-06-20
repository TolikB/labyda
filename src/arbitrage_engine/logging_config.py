from __future__ import annotations

import atexit
import copy
import json
import logging
import logging.handlers
import os
import queue
import re
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _redact_text(record.getMessage()),
        }
        if record.exc_info:
            payload["exc_info"] = _redact_text(self.formatException(record.exc_info))
        for key, value in record.__dict__.items():
            if key.startswith("_") and key not in payload:
                payload[key[1:]] = _redact_value(key[1:], value)
        return json.dumps(payload, ensure_ascii=False, default=str)


_LISTENER: logging.handlers.QueueListener | None = None
_ATEXIT_REGISTERED = False
_REDACTION_SECRETS: tuple[str, ...] = ()


class RawQueueHandler(logging.handlers.QueueHandler):
    dropped_records = 0

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return copy.copy(record)

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            type(self).dropped_records += 1


def configure_logging(level: int = logging.INFO) -> None:
    global _LISTENER, _ATEXIT_REGISTERED, _REDACTION_SECRETS
    _REDACTION_SECRETS = _secret_values()
    if _LISTENER is not None:
        _LISTENER.stop()
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10_000)
    output = logging.StreamHandler()
    output.setFormatter(JsonFormatter())
    _LISTENER = logging.handlers.QueueListener(log_queue, output, respect_handler_level=True)
    _LISTENER.start()
    logging.basicConfig(level=level, handlers=[RawQueueHandler(log_queue)], force=True)
    if not _ATEXIT_REGISTERED:
        atexit.register(shutdown_logging)
        _ATEXIT_REGISTERED = True


def shutdown_logging() -> None:
    global _LISTENER
    if _LISTENER is not None:
        _LISTENER.stop()
        _LISTENER = None


_SENSITIVE_KEY = re.compile(r"(?i)(api[_-]?key|private[_-]?key|token|secret|authorization|signature)")
_PRIVATE_KEY = re.compile(r"(?i)0x[a-f0-9]{64}")
_TELEGRAM_TOKEN_PATH = re.compile(r"(?i)(api\.telegram\.org/bot)[^/\s]+")
_URI_PASSWORD = re.compile(r"(?i)([a-z][a-z0-9+.-]*://[^:/@\s]+:)[^@\s]+(@)")
_BEARER_TOKEN = re.compile(r"(?i)(authorization[\"'=:\s]+bearer\s+)[^\s,}\"]+")


def _secret_values() -> tuple[str, ...]:
    return tuple(value for key, value in os.environ.items() if value and len(value) >= 8 and _SENSITIVE_KEY.search(key))


def _redact_text(value: str) -> str:
    redacted = _PRIVATE_KEY.sub("<redacted-private-key>", value)
    redacted = _TELEGRAM_TOKEN_PATH.sub(r"\1<redacted-token>", redacted)
    redacted = _URI_PASSWORD.sub(r"\1<redacted>\2", redacted)
    redacted = _BEARER_TOKEN.sub(r"\1<redacted>", redacted)
    for secret in _REDACTION_SECRETS:
        redacted = redacted.replace(secret, "<redacted>")
    return redacted


def _redact_value(key: str, value: Any) -> Any:
    if _SENSITIVE_KEY.search(key):
        return "<redacted>"
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, dict):
        return {str(nested_key): _redact_value(str(nested_key), nested) for nested_key, nested in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact_value(key, nested) for nested in value]
    return value
