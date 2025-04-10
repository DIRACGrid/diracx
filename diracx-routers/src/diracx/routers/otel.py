from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import FastAPI

# Required by FastAPIInstrumentor
# to follow semantic conventions for HTTP metrics
# https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http"

# https://opentelemetry.io/blog/2023/logs-collection/
# https://github.com/mhausenblas/ref.otel.help/blob/main/how-to/logs-collection/yoda/main.py
from opentelemetry import _logs, metrics, trace
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.logging.constants import DEFAULT_LOGGING_FORMAT
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic_settings import SettingsConfigDict

from diracx.core.settings import ServiceSettingsBase


class OTELSettings(ServiceSettingsBase):
    """Settings for the Open Telemetry Configuration."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_OTEL_")

    enabled: bool = False
    application_name: str = "diracx"
    grpc_endpoint: str = ""
    grpc_insecure: bool = True
    # headers to pass to the OTEL Collector
    # e.g. {"tenant_id": "lhcbdiracx-cert"}
    headers: Optional[dict[str, str]] = None


def instrument_otel(app: FastAPI) -> None:
    """Instrument the application to send OpenTelemetryData.
    Metrics, Traces and Logs are sent to an OTEL collector.
    The Collector can then redirect it to whatever is configured.
    Typically: Jaeger for traces, Prometheus for metrics, ElasticSearch for logs.

    Note: this is highly experimental, and OpenTelemetry is a quickly moving target

    """
    otel_settings = OTELSettings()
    if not otel_settings.enabled:
        return

    # set the service name to show in traces
    resource = Resource.create(
        attributes={
            "service.name": otel_settings.application_name,
            "service.instance.id": os.uname().nodename,
        }
    )

    # set the tracer provider
    tracer_provider = TracerProvider(resource=resource)

    # elif MODE == "otel-collector-http":
    #     tracer.add_span_processor(
    #         BatchSpanProcessor(OTLPSpanExporterHTTP(endpoint=OTEL_HTTP_ENDPOINT))
    #     )
    # else:
    # default otel-collector-grpc
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=otel_settings.grpc_endpoint,
                insecure=otel_settings.grpc_insecure,
                headers=otel_settings.headers,
            )
        )
    )
    trace.set_tracer_provider(tracer_provider)
    # http_exporter = httpOTPLMetricExporter()
    # metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter(),export_interval_millis=1000)
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(
            endpoint=otel_settings.grpc_endpoint,
            insecure=otel_settings.grpc_insecure,
            headers=otel_settings.headers,
        ),
        export_interval_millis=3000,
    )
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    ###################################

    # # override logger format which with trace id and span id
    # https://github.com/mhausenblas/ref.otel.help/blob/main/how-to/logs-collection/yoda/main.py

    LoggingInstrumentor().instrument(set_logging_format=False)

    logger_provider = LoggerProvider(resource=resource)
    _logs.set_logger_provider(logger_provider)

    otlp_exporter = OTLPLogExporter(
        endpoint=otel_settings.grpc_endpoint,
        insecure=otel_settings.grpc_insecure,
        headers=otel_settings.headers,
    )
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))
    handler = LoggingHandler(level=logging.DEBUG, logger_provider=logger_provider)
    handler.setFormatter(logging.Formatter(DEFAULT_LOGGING_FORMAT))
    # Add the handler to diracx and all uvicorn logger
    # Note adding it to just 'uvicorn' or the root logger
    # is not enough because uvicorn sets propagate=False
    for logger_name in logging.root.manager.loggerDict:
        if "diracx" == logger_name or "uvicorn" in logger_name:
            logging.getLogger(logger_name).addHandler(handler)

    ####################

    FastAPIInstrumentor.instrument_app(
        app, tracer_provider=tracer_provider, meter_provider=meter_provider
    )
