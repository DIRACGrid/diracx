"""OpenTelemetry initialization shared by all DiracX processes.

Both the routers (``diracx.routers.otel``) and the task system
(``diracx-tasks worker``, ``diracx-tasks scheduler``, ...) call
:func:`configure_otel` so that every process exports its traces,
metrics and logs in exactly the same way, driven by
:class:`diracx.core.settings.OTELSettings`.

On top of the exporters, the SQL queries of all the SQLAlchemy engines are
traced and measured (see :func:`_instrument_sqlalchemy`).

Note: this is highly experimental, and OpenTelemetry is a quickly moving target
"""

from __future__ import annotations

__all__ = ["OTELProviders", "configure_otel"]

import functools
import logging
import os
import time
import weakref
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

# https://opentelemetry.io/blog/2023/logs-collection/
# https://github.com/mhausenblas/ref.otel.help/blob/main/how-to/logs-collection/yoda/main.py
from opentelemetry import _logs, metrics, trace
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode

from diracx.core.settings import OTELSettings

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine, ExceptionContext
    from sqlalchemy.engine.interfaces import ExecutionContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OTELProviders:
    """The global OpenTelemetry providers installed by :func:`configure_otel`."""

    tracer_provider: TracerProvider
    meter_provider: MeterProvider
    logger_provider: LoggerProvider


# The OpenTelemetry global providers can only be set once per process,
# so we remember what we installed and hand it back on subsequent calls.
_providers: OTELProviders | None = None


def configure_otel(
    component: str,
    *,
    extra_logger_names: Iterable[str] = (),
    settings: OTELSettings | None = None,
) -> OTELProviders | None:
    """Configure the process to send OpenTelemetry data.

    Metrics, Traces and Logs are sent to an OTEL collector.
    The Collector can then redirect it to whatever is configured.
    Typically: Jaeger for traces, Prometheus for metrics, ElasticSearch for logs.

    The providers are registered globally, so code instrumented with the
    ``opentelemetry-api`` (e.g. ``trace.get_tracer(__name__)``) automatically
    exports through them, even if the tracer/meter was created at import time.

    Args:
        component: Name of the DiracX component (e.g. ``routers``, ``tasks-worker``),
            exported as the ``diracx.component`` resource attribute.
        extra_logger_names: Loggers, in addition to ``diracx``, to which the OTEL
            log handler is attached. Needed for loggers which do not propagate
            (e.g. uvicorn's).
        settings: The settings to use. Read from the environment if not given.

    Returns:
        The installed providers, or ``None`` if OpenTelemetry is disabled.

    """
    global _providers

    if settings is None:
        settings = OTELSettings()
    if not settings.enabled:
        return None

    if _providers is not None:
        logger.debug("OpenTelemetry already configured, not reconfiguring")
        return _providers

    try:
        service_version = version("diracx-core")
    except PackageNotFoundError:
        service_version = "unknown"

    # set the service name to show in traces
    # Additional attributes can be given with OTEL_RESOURCE_ATTRIBUTES
    hostname = os.uname().nodename
    resource = Resource.create(
        attributes={
            "service.name": settings.application_name,
            "service.version": service_version,
            # Must be unique per process: several processes can run on the
            # same host (workers, CLI, uvicorn --workers...), and their
            # metrics would otherwise overwrite each other
            "service.instance.id": f"{hostname}-{os.getpid()}",
            "host.name": hostname,
            "process.pid": os.getpid(),
            "diracx.component": component,
        }
    )

    span_exporter, metric_exporter, log_exporter = _create_exporters(settings)

    # Traces
    # The sampling can be configured with OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG
    # (default: parentbased_always_on)
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(tracer_provider)

    # Metrics
    metric_reader = PeriodicExportingMetricReader(
        metric_exporter, export_interval_millis=3000
    )
    meter_provider = MeterProvider(metric_readers=[metric_reader], resource=resource)
    metrics.set_meter_provider(meter_provider)

    # Logs
    logger_provider = LoggerProvider(resource=resource)
    _logs.set_logger_provider(logger_provider)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    _setup_log_handler(logger_provider, {"diracx", *extra_logger_names})

    _instrument_sqlalchemy(tracer_provider, meter_provider)

    _providers = OTELProviders(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
    )
    return _providers


def _setup_log_handler(
    logger_provider: LoggerProvider, logger_names: Iterable[str]
) -> LoggingHandler:
    """Export the log records of the given loggers.

    All the diracx loggers propagate to the ``diracx`` one. Loggers which do
    not propagate (like uvicorn's) have to be given explicitly.

    The records are exported as they are: the body is the message, and the
    trace context, the logger, the code location and the exception are
    attributes of the record. No formatter is set, as it would end up in the
    body. The ``opentelemetry-instrumentation-logging`` package is not used
    either: it attaches its own handler to the root logger, which exports
    every record a second time (and those of all the other libraries).
    """
    handler = LoggingHandler(level=logging.DEBUG, logger_provider=logger_provider)
    for logger_name in logger_names:
        logging.getLogger(logger_name).addHandler(handler)
    return handler


def _create_exporters(
    settings: OTELSettings,
) -> tuple[SpanExporter, MetricExporter, LogRecordExporter]:
    """Create the OTLP exporters for the protocol of the settings.

    An empty endpoint lets the exporters use the standard
    ``OTEL_EXPORTER_OTLP_*`` environment variables, or their defaults.
    """
    if settings.protocol == "http":
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter as HTTPLogExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter as HTTPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HTTPSpanExporter,
        )

        base_url = settings.http_endpoint.rstrip("/")

        def url(signal: str) -> str | None:
            return f"{base_url}/v1/{signal}" if base_url else None

        return (
            HTTPSpanExporter(endpoint=url("traces"), headers=settings.headers),
            HTTPMetricExporter(endpoint=url("metrics"), headers=settings.headers),
            HTTPLogExporter(endpoint=url("logs"), headers=settings.headers),
        )

    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    endpoint = settings.grpc_endpoint or None
    return (
        OTLPSpanExporter(
            endpoint=endpoint, insecure=settings.grpc_insecure, headers=settings.headers
        ),
        OTLPMetricExporter(
            endpoint=endpoint, insecure=settings.grpc_insecure, headers=settings.headers
        ),
        OTLPLogExporter(
            endpoint=endpoint, insecure=settings.grpc_insecure, headers=settings.headers
        ),
    )


_DB_DURATION_BUCKETS_SECONDS = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
]  # fmt: skip
_DB_CONNECTION_WAIT_BUCKETS_SECONDS = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30,
]  # fmt: skip
_MAX_QUERY_TEXT_LENGTH = 2000


def _instrument_sqlalchemy(
    tracer_provider: TracerProvider, meter_provider: MeterProvider
) -> Callable[[], None]:
    """Trace and measure the SQL queries of all the SQLAlchemy engines.

    The listeners are registered on the ``Engine`` class rather than on
    specific engines, so they apply to all the engines, including the ones
    created by ``diracx-db`` (which does not depend on OpenTelemetry) before
    this function is called.

    This follows the OpenTelemetry semantic conventions for database clients:
    https://opentelemetry.io/docs/specs/semconv/database/

    Spans are only created when there is already an active span (an HTTP
    request or a task), so that queries done outside of any context
    (e.g. health checks of the connection pool) do not create orphan traces.
    Metrics are always recorded.

    The time spent waiting for a connection of the pool is measured by
    wrapping ``Engine.raw_connection``, through which SQLAlchemy obtains
    all its connections: there is no event before a connection is taken.

    Returns a function removing the instrumentation (only meant for the tests).
    """
    from sqlalchemy import event
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import TimeoutError as PoolTimeoutError

    tracer = tracer_provider.get_tracer("diracx.db.sql")
    meter = meter_provider.get_meter("diracx.db.sql")
    operation_duration = meter.create_histogram(
        "db.client.operation.duration",
        unit="s",
        description="Duration of the SQL queries",
        explicit_bucket_boundaries_advisory=_DB_DURATION_BUCKETS_SECONDS,
    )
    # Engines seen so far, used to report the state of their connection pools
    engines: weakref.WeakSet[Engine] = weakref.WeakSet()

    def _metric_attributes(conn: Connection, statement: str) -> dict[str, str]:
        words = statement.split(maxsplit=1)
        return {
            "db.system.name": conn.dialect.name,
            "db.namespace": conn.engine.url.database or "",
            "db.operation.name": words[0].upper() if words else "",
        }

    def _before_cursor_execute(
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: ExecutionContext | None,
        executemany: bool,
    ) -> None:
        if context is None:
            return
        engines.add(conn.engine)
        attributes = _metric_attributes(conn, statement)
        span = None
        if trace.get_current_span().get_span_context().is_valid:
            url = conn.engine.url
            span_attributes: dict[str, str | int] = {
                **attributes,
                # Statements are parametrized, so they do not contain the values
                "db.query.text": statement[:_MAX_QUERY_TEXT_LENGTH],
            }
            if url.host:
                span_attributes["server.address"] = url.host
            if url.port:
                span_attributes["server.port"] = url.port
            span = tracer.start_span(
                f"{attributes['db.operation.name']} {attributes['db.namespace']}".strip(),
                kind=SpanKind.CLIENT,
                attributes=span_attributes,
            )
        context._diracx_otel = (time.perf_counter(), attributes, span)  # type: ignore[attr-defined]

    def _finish(
        context: ExecutionContext | None, exception: BaseException | None
    ) -> None:
        state = getattr(context, "_diracx_otel", None)
        if state is None:
            return
        context._diracx_otel = None  # type: ignore[union-attr]
        start, attributes, span = state
        if exception is not None:
            attributes = {**attributes, "error.type": type(exception).__qualname__}
        operation_duration.record(time.perf_counter() - start, attributes=attributes)
        if span is not None:
            if exception is not None:
                span.set_attribute("error.type", attributes["error.type"])
                span.record_exception(exception)
                span.set_status(Status(StatusCode.ERROR, str(exception)))
            span.end()

    def _after_cursor_execute(
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: ExecutionContext | None,
        executemany: bool,
    ) -> None:
        _finish(context, None)

    def _handle_error(exception_context: ExceptionContext) -> None:
        _finish(
            exception_context.execution_context,
            exception_context.original_exception,
        )

    def _pools() -> Iterable[tuple[str, Any]]:
        for engine in list(engines):
            pool = engine.pool
            # Only QueuePool (the default) keeps track of its connections
            if hasattr(pool, "checkedout"):
                yield engine.url.database or "", pool

    def _observe_connection_count(
        options: metrics.CallbackOptions,
    ) -> Iterable[metrics.Observation]:
        for name, pool in _pools():
            for state, count in (
                ("used", pool.checkedout()),
                ("idle", pool.checkedin()),
            ):
                yield metrics.Observation(
                    count,
                    {
                        "db.client.connection.pool.name": name,
                        "db.client.connection.state": state,
                    },
                )

    def _observe_connection_max(
        options: metrics.CallbackOptions,
    ) -> Iterable[metrics.Observation]:
        for name, pool in _pools():
            max_overflow = getattr(pool, "_max_overflow", 0)
            # A negative overflow means that the pool is unbounded
            if max_overflow >= 0:
                yield metrics.Observation(
                    pool.size() + max_overflow,
                    {"db.client.connection.pool.name": name},
                )

    meter.create_observable_up_down_counter(
        "db.client.connection.count",
        callbacks=[_observe_connection_count],
        description="Number of connections in the SQLAlchemy pool, by state",
    )
    meter.create_observable_up_down_counter(
        "db.client.connection.max",
        callbacks=[_observe_connection_max],
        description="Maximum number of connections allowed by the SQLAlchemy pool",
    )

    connection_wait_time = meter.create_histogram(
        "db.client.connection.wait_time",
        unit="s",
        description="Time spent obtaining a connection from the SQLAlchemy pool",
        explicit_bucket_boundaries_advisory=_DB_CONNECTION_WAIT_BUCKETS_SECONDS,
    )
    connection_timeouts = meter.create_counter(
        "db.client.connection.timeouts",
        description=(
            "Connections which could not be obtained from the SQLAlchemy pool "
            "before its timeout (pool_timeout)"
        ),
    )
    original_raw_connection = Engine.raw_connection

    @functools.wraps(original_raw_connection)
    def _raw_connection(self: Engine, *args: Any, **kwargs: Any) -> Any:
        engines.add(self)
        attributes = {"db.client.connection.pool.name": self.url.database or ""}
        start = time.perf_counter()
        try:
            return original_raw_connection(self, *args, **kwargs)
        except PoolTimeoutError:
            connection_timeouts.add(1, attributes=attributes)
            raise
        finally:
            connection_wait_time.record(
                time.perf_counter() - start, attributes=attributes
            )

    Engine.raw_connection = _raw_connection  # type: ignore[method-assign]

    listeners: list[tuple[str, Callable[..., None]]] = [
        ("before_cursor_execute", _before_cursor_execute),
        ("after_cursor_execute", _after_cursor_execute),
        ("handle_error", _handle_error),
    ]
    for identifier, listener in listeners:
        event.listen(Engine, identifier, listener)

    def _uninstrument() -> None:
        Engine.raw_connection = original_raw_connection  # type: ignore[method-assign]
        for identifier, listener in listeners:
            event.remove(Engine, identifier, listener)
        engines.clear()

    return _uninstrument
