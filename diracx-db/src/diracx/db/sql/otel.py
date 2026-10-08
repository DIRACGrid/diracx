"""OpenTelemetry instrumentation of the SQL queries.

Called by the processes which enable OpenTelemetry (see
:func:`diracx.core.otel.configure_otel`), with the providers it installed.
Only the ``opentelemetry-api`` is needed here.
"""

from __future__ import annotations

import functools
import time
import weakref
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine, ExceptionContext
    from sqlalchemy.engine.interfaces import ExecutionContext


_DB_DURATION_BUCKETS_SECONDS = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
]  # fmt: skip
_DB_CONNECTION_WAIT_BUCKETS_SECONDS = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30,
]  # fmt: skip
_MAX_QUERY_TEXT_LENGTH = 2000


# Removes the instrumentation, set while it is installed
_uninstrument: Callable[[], None] | None = None


def instrument_sqlalchemy(
    tracer_provider: trace.TracerProvider, meter_provider: metrics.MeterProvider
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

    The time spent obtaining a connection is measured by wrapping
    ``Engine.raw_connection``, through which SQLAlchemy obtains all its
    connections: there is no event before a connection is taken. This
    includes waiting for a connection of the pool to be released, but also
    establishing a new connection when the pool is not full (DNS, TLS,
    authentication).

    ``opentelemetry-instrumentation-sqlalchemy`` is not used: it wraps
    ``create_engine``, so it misses the engines created before it is enabled
    unless they are passed explicitly, and it does not measure the time spent
    obtaining a connection.

    It can be called several times (e.g. when creating several applications
    in the tests): the instrumentation is only installed once.

    Args:
        tracer_provider: Provider of the tracer creating the query spans.
        meter_provider: Provider of the meter recording the query and pool metrics.

    Returns:
        A function removing the instrumentation (only meant for the tests).

    """
    global _uninstrument
    if _uninstrument is not None:
        return _uninstrument

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
        description=(
            "Time spent obtaining a connection from the SQLAlchemy pool, "
            "including establishing new connections"
        ),
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

    def uninstrument() -> None:
        global _uninstrument
        Engine.raw_connection = original_raw_connection  # type: ignore[method-assign]
        for identifier, listener in listeners:
            event.remove(Engine, identifier, listener)
        engines.clear()
        _uninstrument = None

    _uninstrument = uninstrument
    return uninstrument
