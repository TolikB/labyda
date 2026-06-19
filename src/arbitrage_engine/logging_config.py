from __future__ import annotations

import json
import logging
import logging.handlers
import atexit
import copy
import queue
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key.startswith("_") and key not in payload:
                payload[key[1:]] = value
        return json.dumps(payload, ensure_ascii=False, default=str)


_LISTENER: logging.handlers.QueueListener | None = None
_ATEXIT_REGISTERED = False


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
    global _LISTENER, _ATEXIT_REGISTERED
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
