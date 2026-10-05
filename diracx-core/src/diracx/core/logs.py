"""Configuration of the logs written by the DiracX processes.

All the processes (API servers, scheduler, workers, command line) call
:func:`configure_logging`, so that they write their logs in the same way,
driven by :class:`diracx.core.settings.LoggingSettings`.
"""

from __future__ import annotations

__all__ = [
    "AccessLogFilter",
    "JSONFormatter",
    "LogContextFilter",
    "TextFormatter",
    "configure_logging",
    "diracx_logger_names",
    "log_context",
    "set_log_context",
    "set_trace_context_getter",
]

import json
import logging
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from .extensions import extensions_by_priority
from .settings import LoggingSettings

# uvicorn configures its own loggers, which do not propagate to the root logger
UVICORN_LOGGERS = ("uvicorn", "uvicorn.access")

# The handler installed by configure_logging, to replace it if called again
_handler: logging.Handler | None = None

# Returns the (trace ID, span ID) of the current span, if any.
# Set by diracx.core.otel when OpenTelemetry is enabled: the OpenTelemetry
# SDK is an optional dependency of diracx-core.
_trace_context_getter: Callable[[], tuple[str, str] | None] | None = None

# Attributes of all the LogRecord: the others were given with ``extra=``
_STANDARD_RECORD_ATTRIBUTES = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", None, None))
) | {"message", "asctime", "taskName"}


# Attributes added to all the log records emitted in the current context
# (e.g. the task being executed, or the user of the request)
_log_context: ContextVar[Mapping[str, Any]] = ContextVar(
    "diracx_log_context", default=MappingProxyType({})
)


@contextmanager
def log_context(**attributes: Any) -> Iterator[None]:
    """Add attributes to the log records emitted within the block.

    The names contain dots, so they are given as ``**{"task.name": ...}``.
    """
    token = _log_context.set({**_log_context.get(), **attributes})
    try:
        yield
    finally:
        _log_context.reset(token)


def set_log_context(**attributes: Any) -> None:
    """Add attributes to the log records emitted in the rest of the current context.

    For code which cannot wrap what follows in :func:`log_context`, e.g. a
    FastAPI dependency: each request runs in its own context.
    """
    _log_context.set({**_log_context.get(), **attributes})


class LogContextFilter(logging.Filter):
    """Copy the attributes of the log context to the records.

    Attached to the handlers rather than done in a record factory, so that
    the attributes given explicitly with ``extra=`` take precedence (a record
    factory runs before ``extra`` is applied, which then raises on conflicts).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Copy the attributes of the current log context to the record."""
        for key, value in _log_context.get().items():
            record.__dict__.setdefault(key, value)
        return True


class AccessLogFilter(logging.Filter):
    """Structure the access logs of uvicorn.

    They are logged as ``'%s - "%s %s HTTP/%s" %d'`` with (client address,
    method, path, HTTP version, status code): these are added to the record
    as attributes, so that they are in the JSON logs and in the records
    exported with OpenTelemetry alike.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Structure the uvicorn access records, when they have the expected format."""
        if (
            record.name == "uvicorn.access"
            and isinstance(record.args, tuple)
            and len(record.args) == 5
        ):
            client, method, path, http_version, status = record.args
            for key, value in {
                "client.address": client,
                "http.request.method": method,
                "url.path": path,
                "network.protocol.version": http_version,
                "http.response.status_code": status,
            }.items():
                record.__dict__.setdefault(key, value)
        return True


def set_trace_context_getter(
    getter: Callable[[], tuple[str, str] | None] | None,
) -> None:
    """Set the function giving the trace and span IDs added to the JSON logs."""
    global _trace_context_getter
    _trace_context_getter = getter


def _utc_timestamp(record: logging.LogRecord) -> str:
    """The time of the record, in UTC, in ISO 8601 with milliseconds."""
    return (
        datetime.fromtimestamp(record.created, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class TextFormatter(logging.Formatter):
    """Human readable format: ``<date> <level> <logger>: <message> [<context>]``.

    The date is in UTC, as in the JSON logs: ``2026-09-30T21:30:35.173Z``.
    """

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    def formatTime(  # noqa: N802
        self, record: logging.LogRecord, datefmt: str | None = None
    ) -> str:
        """The time of the record, in UTC, in ISO 8601 with milliseconds."""
        return _utc_timestamp(record)

    def formatMessage(self, record: logging.LogRecord) -> str:  # noqa: N802
        """The message, followed by the attributes of the current log context."""
        message = super().formatMessage(record)
        # Before the traceback, which is added after the message
        if context := _log_context.get():
            # The values of the record: those given with extra= take precedence
            values = (f"{k}={record.__dict__.get(k, v)}" for k, v in context.items())
            message += " [" + " ".join(values) + "]"
        return message


class JSONFormatter(logging.Formatter):
    """One JSON object per line, with the names of the OpenTelemetry log data model.

    ``timestamp`` (UTC), ``severity_text``, ``logger``, ``body`` (the message),
    ``trace_id`` and ``span_id`` (if there is a current span and OpenTelemetry
    is enabled), ``exception.type``, ``exception.message`` and
    ``exception.stacktrace`` (if the record has an exception), and the
    other attributes of the record: those given with ``extra=``, and those
    added by :class:`LogContextFilter` and :class:`AccessLogFilter`.
    """

    def format(self, record: logging.LogRecord) -> str:
        """The record as a JSON object on one line."""
        entry: dict[str, Any] = {
            "timestamp": _utc_timestamp(record),
            "severity_text": record.levelname,
            "logger": record.name,
            "body": record.getMessage(),
        }
        if _trace_context_getter is not None and (ids := _trace_context_getter()):
            entry["trace_id"], entry["span_id"] = ids
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception.type"] = record.exc_info[0].__name__
            entry["exception.message"] = str(record.exc_info[1])
            entry["exception.stacktrace"] = self.formatException(record.exc_info)
        if record.stack_info:
            entry["code.stacktrace"] = record.stack_info
        for key, value in vars(record).items():
            if key not in _STANDARD_RECORD_ATTRIBUTES and key not in entry:
                entry[key] = value
        # Values which are not JSON serialisable are written as strings
        return json.dumps(entry, default=str, ensure_ascii=False)


def diracx_logger_names() -> list[str]:
    """Names of the top level loggers of DiracX and of its extension."""
    try:
        return extensions_by_priority()
    except NotImplementedError:
        # DiracX is not installed (e.g. only diracx-core, in some tests)
        return ["diracx"]


def configure_logging(settings: LoggingSettings | None = None) -> logging.Handler:
    """Write the logs of the process to stderr.

    A single handler is attached to the root logger, and replaces the ones
    of uvicorn (whose loggers do not propagate). The DiracX loggers are set
    to ``settings.level``, the other ones to ``settings.libraries_level``.

    It can be called several times (e.g. when creating several applications
    in the tests): the handler installed by the previous call is replaced.
    """
    global _handler
    if settings is None:
        settings = LoggingSettings()

    handler = logging.StreamHandler()
    handler.setFormatter(
        JSONFormatter() if settings.format == "json" else TextFormatter()
    )
    handler.addFilter(LogContextFilter())
    handler.addFilter(AccessLogFilter())

    root = logging.getLogger()
    root.setLevel(settings.libraries_level)
    for name in diracx_logger_names():
        logging.getLogger(name).setLevel(settings.level)
    if _handler is not None:
        root.removeHandler(_handler)
    root.addHandler(handler)

    for name in UVICORN_LOGGERS:
        uvicorn_logger = logging.getLogger(name)
        if uvicorn_logger.propagate:
            # Not configured by uvicorn (e.g. not running in uvicorn):
            # the records reach the root handler
            continue
        # Replace the console handlers set by uvicorn (and ours, if called
        # again), but keep the others (e.g. the OpenTelemetry one, or a
        # FileHandler, which is a subclass of StreamHandler)
        uvicorn_logger.handlers = [
            h for h in uvicorn_logger.handlers if type(h) is not logging.StreamHandler
        ] + [handler]

    _handler = handler
    return handler
