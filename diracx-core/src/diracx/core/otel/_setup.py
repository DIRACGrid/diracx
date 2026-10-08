"""Installation of the OpenTelemetry providers and exporters.

Only imported by :func:`diracx.core.otel.configure_otel` when OpenTelemetry
is enabled: it requires the ``otel`` extra of ``diracx-core``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version

# https://opentelemetry.io/blog/2023/logs-collection/
# https://github.com/mhausenblas/ref.otel.help/blob/main/how-to/logs-collection/yoda/main.py
from opentelemetry import _logs, metrics, trace
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

from diracx.core.logs import (
    AccessLogFilter,
    LogContextFilter,
    diracx_logger_names,
    set_trace_context_getter,
)
from diracx.core.settings import OTELSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OTELProviders:
    """The global OpenTelemetry providers installed by :func:`configure_providers`."""

    tracer_provider: TracerProvider
    meter_provider: MeterProvider
    logger_provider: LoggerProvider


# The OpenTelemetry global providers can only be set once per process,
# so we remember what we installed and hand it back on subsequent calls.
_providers: OTELProviders | None = None


def configure_providers(
    component: str,
    *,
    extra_logger_names: Iterable[str],
    settings: OTELSettings,
) -> OTELProviders:
    """Install the global providers, see :func:`diracx.core.otel.configure_otel`."""
    global _providers

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
            # same host (workers, uvicorn --workers...), and their
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
    # The export interval can be configured with OTEL_METRIC_EXPORT_INTERVAL
    # (default: 60s)
    metric_reader = PeriodicExportingMetricReader(metric_exporter)
    meter_provider = MeterProvider(metric_readers=[metric_reader], resource=resource)
    metrics.set_meter_provider(meter_provider)

    # Logs
    logger_provider = LoggerProvider(resource=resource)
    _logs.set_logger_provider(logger_provider)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    _setup_log_handler(logger_provider, {*diracx_logger_names(), *extra_logger_names})
    # Add the trace context to the JSON logs written to stderr
    set_trace_context_getter(_current_trace_context)

    _providers = OTELProviders(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
    )
    return _providers


def _current_trace_context() -> tuple[str, str] | None:
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return None
    return format(span_context.trace_id, "032x"), format(span_context.span_id, "016x")


def _setup_log_handler(
    logger_provider: LoggerProvider, logger_names: Iterable[str]
) -> LoggingHandler:
    """Export the log records of the given loggers.

    All the diracx loggers propagate to the ``diracx`` one. Loggers which do
    not propagate (like uvicorn's) have to be given explicitly.

    The records are exported as they are: the body is the message, and the
    trace context, the logger, the code location and the exception are
    attributes of the record. No formatter is set, as it would end up in the
    body. The handler comes from ``opentelemetry-instrumentation-logging``
    (the one of ``opentelemetry-sdk`` is deprecated), but its
    ``LoggingInstrumentor`` is not used: it attaches the handler to the root
    logger, which exports the records of all the other libraries, and every
    record a second time.
    """
    handler = LoggingHandler(
        level=logging.DEBUG,
        logger_provider=logger_provider,
        log_code_attributes=True,
    )
    # e.g. the task being executed, or the user of the request
    handler.addFilter(LogContextFilter())
    # The fields of the uvicorn access logs, as in the JSON logs
    handler.addFilter(AccessLogFilter())
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
