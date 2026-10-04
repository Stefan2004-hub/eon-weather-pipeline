"""Contextual logging helpers for command-line pipeline runs."""

from __future__ import annotations

import logging
from typing import Any

_STANDARD_RECORD_ATTRIBUTES = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)
_STANDARD_RECORD_ATTRIBUTES.update({"asctime", "message"})


class ContextAdapter(logging.LoggerAdapter):
    """Merge persistent run context with context supplied by an individual event."""

    def process(self, message: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        event_context = kwargs.pop("extra", {})
        kwargs["extra"] = {**self.extra, **event_context}
        return message, kwargs


class KeyValueFormatter(logging.Formatter):
    """Render standard log records with stable key-value context."""

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_ATTRIBUTES and not key.startswith("_")
        }
        if not context:
            return rendered
        fields = " ".join(f"{key}={value!s}" for key, value in sorted(context.items()))
        return f"{rendered} {fields}"


def configure_logging(level: str) -> None:
    """Configure the process logger once for human-readable contextual output."""

    handler = logging.StreamHandler()
    handler.setFormatter(KeyValueFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)


def get_logger(name: str, execution_id: str) -> ContextAdapter:
    """Return a logger which includes the pipeline execution identifier."""

    return ContextAdapter(logging.getLogger(name), {"execution_id": execution_id})


def with_context(logger: ContextAdapter, **context: Any) -> ContextAdapter:
    """Return a child adapter with additional immutable contextual fields."""

    return ContextAdapter(logger.logger, {**logger.extra, **context})
