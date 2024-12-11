import logging
import os

from fastapi import FastAPI

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

from functools import wraps
import inspect

from collections import UserDict
from timeit import default_timer as timer


def async_tracer(name=None):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Obtain the module that contains the decorated function
            package_name = get_module_name_from_func(func)
            
            if not name:
                tace_name = func.__name__
            
            tracer = trace.get_tracer_provider().get_tracer(package_name)

            # Create a span with name: diracx.diracx_xxx.(...).package.function
            with tracer.start_as_current_span(f"{package_name}.{tace_name}"):
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator

def sync_tracer(name=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Obtain the module that contains the decorated function
            module_name = get_module_name_from_func(func)

            if not name:
                tace_name = func.__name__
            
            tracer = trace.get_tracer_provider().get_tracer(module_name)

            # Create a span with name: diracx.diracx_xxx.(...).package.function
            with tracer.start_as_current_span(f"{module_name}.{tace_name}"):
                return func(*args, **kwargs)

        return wrapper
    return decorator

def set_trace_attribute(key, value, stringify=False):
    span = trace.get_current_span()
    if stringify:
        span.set_attribute(f"diracx.{key}", str(value))
    else:
        _recursive_set_trace_attribute(span, f"diracx.{key}", value)


def _recursive_set_trace_attribute(span, key, value):
    if isinstance(value, list):
        zeros = len(str(len(value)))
        for idx, item in enumerate(value):
            _recursive_set_trace_attribute(span, f"{key}[{str(idx).zfill(zeros)}]", item)
    
    elif isinstance(value, set) or isinstance(value, tuple):
        zeros = len(str(len(value)))
        for idx, item in enumerate(value):
            _recursive_set_trace_attribute(span, f"{key}.item_{str(idx).zfill(zeros)}", item)

    elif isinstance(value, dict) or isinstance(value, UserDict):
        for k, v in value.items():
            _recursive_set_trace_attribute(span, f"{key}.{k}", v)

    else:
        span.set_attribute(key, value)
        

def increase_counter(meter_name, counter_name, amount=1, is_updown=False):
    meter = metrics.get_meter_provider().get_meter(meter_name)
    
    if is_updown:
        metric = meter.create_up_down_counter(counter_name)
    else:
        metric = meter.create_counter(counter_name)

    metric.add(amount)  

def get_module_name_from_func(func):
    from_module = inspect.getmodule(func)
    
    if not from_module:
        return "diracx"

    module_name = from_module.__name__
    return module_name

class OTELSettings(ServiceSettingsBase):
    """Settings for the Open Telemetry Configuration."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_OTEL_")

    enabled: bool = False
    application_name: str = "diracx"
    grpc_endpoint: str = ""
    grpc_insecure: bool = True


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
            )
        )
    )
    trace.set_tracer_provider(tracer_provider)

    # metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter(),export_interval_millis=1000)
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(
            endpoint=otel_settings.grpc_endpoint,
            insecure=otel_settings.grpc_insecure,
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
