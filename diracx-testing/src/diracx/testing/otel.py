"""In-memory OpenTelemetry providers for the tests.

The global tracer and meter providers can only be set once per process, and
the tests of several packages run in the same process (e.g. the integration
tests): they all have to share the same providers, exporter and reader.
"""

from __future__ import annotations

from functools import cache

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

__all__ = ["SPAN_EXPORTER", "install_otel_providers", "metric_value"]

SPAN_EXPORTER = InMemorySpanExporter()
_METRIC_READER = InMemoryMetricReader()


@cache
def install_otel_providers() -> tuple[TracerProvider, MeterProvider]:
    """Set the global providers, exporting to SPAN_EXPORTER and the metric reader."""
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(SPAN_EXPORTER))
    meter_provider = MeterProvider(metric_readers=[_METRIC_READER])
    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(meter_provider)
    return tracer_provider, meter_provider


def metric_value(name: str, **attributes) -> float:
    """Sum of the data points of a metric matching the given attributes.

    The reader is shared by all the tests: compare with the value before the
    action rather than with an absolute value.
    """
    data = _METRIC_READER.get_metrics_data()
    total = 0.0
    if data is None:
        return total
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != name:
                    continue
                for point in metric.data.data_points:
                    if all(point.attributes.get(k) == v for k, v in attributes.items()):
                        total += getattr(point, "value", None) or getattr(
                            point, "count", 0
                        )
    return total
