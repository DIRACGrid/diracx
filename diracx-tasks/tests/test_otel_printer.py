"""Tests for the minimal OpenTelemetry collector used by ``run_local.sh --otel``."""

from __future__ import annotations

from opentelemetry import propagate
from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import Link, SpanKind, Status, StatusCode

from diracx.testing.otel_printer import MetricPrinter, TracePrinter


def _provider(component: str) -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource.create({"diracx.component": component}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_trace_printer_joins_the_spans_of_several_processes():
    routers, routers_spans = _provider("routers")
    worker, worker_spans = _provider("tasks-worker")

    carrier: dict[str, str] = {}
    tracer = routers.get_tracer("test")
    with tracer.start_as_current_span(
        "POST /api/jobs/jdl",
        kind=SpanKind.SERVER,
        attributes={"http.response.status_code": 201, "task.retry_count": 0},
    ):
        with tracer.start_as_current_span("task.submit jobs:X"):
            propagate.inject(carrier)
    with worker.get_tracer("test").start_as_current_span(
        "task.execute jobs:X", context=propagate.extract(carrier)
    ) as span:
        span.set_status(Status(StatusCode.ERROR, "ValueError: boom"))

    printer = TracePrinter(idle_seconds=3600)
    # The processes export independently, in any order
    printer.add(encode_spans(worker_spans.get_finished_spans()))
    assert printer.flush() == []  # the trace is not idle yet
    printer.add(encode_spans(routers_spans.get_finished_spans()))

    [rendered] = printer.flush(force=True)
    lines = rendered.splitlines()
    assert "3 spans: routers → tasks-worker" in lines[0]
    assert lines[1].startswith("POST /api/jobs/jdl  [routers, server]")
    assert "status=201" in lines[1]
    # Attributes without information are hidden
    assert "retry" not in lines[1]
    assert lines[2].startswith("└─ task.submit jobs:X  [routers, internal]")
    assert lines[3].startswith("   └─ task.execute jobs:X  [tasks-worker, internal]")
    assert "ERROR ValueError: boom" in lines[3]

    # Nothing new: not printed again
    assert printer.flush(force=True) == []


def test_trace_printer_shows_the_links():
    provider, spans = _provider("tasks-worker")
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("task.submit jobs:X") as submit:
        pass
    with tracer.start_as_current_span(
        "task.process jobs:X", links=[Link(submit.get_span_context())]
    ):
        pass

    printer = TracePrinter(idle_seconds=3600)
    printer.add(encode_spans(spans.get_finished_spans()))
    submit_trace_id = f"{submit.get_span_context().trace_id:032x}"
    rendered = printer.flush(force=True)
    [process] = [r for r in rendered if "task.process" in r]
    assert f"link={submit_trace_id}" in process


def test_metric_printer_only_prints_changed_values():
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"diracx.component": "tasks-worker"}),
    )
    counter = provider.get_meter("test").create_counter("tasks_completed_total")
    histogram = provider.get_meter("test").create_histogram("task_duration_seconds")
    printer = MetricPrinter()

    counter.add(2, {"task_name": "jobs:X"})
    histogram.record(1.0, {"task_name": "jobs:X"})
    histogram.record(3.0, {"task_name": "jobs:X"})
    printer.add(encode_metrics(reader.get_metrics_data()))
    [rendered] = printer.flush()
    assert (
        "tasks_completed_total [tasks-worker] {task_name=jobs:X} 2"
        in rendered.splitlines()
    )
    assert (
        "task_duration_seconds [tasks-worker] {task_name=jobs:X} count=2 mean=2"
        in rendered.splitlines()
    )

    # Same values again: nothing to print
    printer.add(encode_metrics(reader.get_metrics_data()))
    assert printer.flush() == []

    counter.add(1, {"task_name": "jobs:X"})
    printer.add(encode_metrics(reader.get_metrics_data()))
    [rendered] = printer.flush()
    assert "(1 changed)" in rendered
    assert rendered.splitlines()[1].endswith(" 3")


def test_http_receiver():
    import threading

    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.trace.export import SpanExportResult

    from diracx.testing.otel_printer import _OTLPHTTPServer

    printer = TracePrinter(idle_seconds=3600)
    server = _OTLPHTTPServer(("localhost", 0), printer, MetricPrinter(), False)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        provider, spans = _provider("routers")
        with provider.get_tracer("test").start_as_current_span("GET /api/over-http"):
            pass
        exporter = OTLPSpanExporter(
            endpoint=f"http://localhost:{server.server_port}/v1/traces"
        )
        assert exporter.export(spans.get_finished_spans()) == SpanExportResult.SUCCESS
    finally:
        server.shutdown()

    [rendered] = printer.flush(force=True)
    assert "GET /api/over-http  [routers, internal]" in rendered
