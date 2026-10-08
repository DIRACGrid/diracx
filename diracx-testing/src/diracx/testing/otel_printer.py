"""Minimal OpenTelemetry collector printing what it receives, for local development.

It listens for OTLP over gRPC and over HTTP (like a real collector) and prints:

* traces, as a tree once no new span arrived for a few seconds, gathering the
  spans of all the processes (routers, workers, scheduler, CLI);
* metrics, as a table of the values which changed, periodically;
* optionally, logs.

Usage::

    python -m diracx.testing.otel_printer [--port 4317] [--http-port 4318] [--metrics-interval 30] [--logs]

and point DiracX to it with ``DIRACX_OTEL_ENABLED=true`` and
``DIRACX_OTEL_GRPC_ENDPOINT=localhost:4317``
(or ``DIRACX_OTEL_PROTOCOL=http`` and ``DIRACX_OTEL_HTTP_ENDPOINT=http://localhost:4318``).
``pixi run local-start --otel`` does all of that.

This is not meant to be used in production: use a real OpenTelemetry collector.
"""

from __future__ import annotations

__all__ = ["main"]

import argparse
import gzip
import signal
import sys
import threading
import time
from collections.abc import Iterable
from concurrent import futures
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
from opentelemetry.proto.collector.logs.v1 import (
    logs_service_pb2,
    logs_service_pb2_grpc,
)
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2,
    metrics_service_pb2_grpc,
)
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2,
    trace_service_pb2_grpc,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span, Status

# Span attributes worth showing next to the span name
_SHOWN_ATTRIBUTES = {
    "http.response.status_code": "status",
    "enduser.id": "user",
    "task.status": "task",
    "task.retry_count": "retry",
    "task.queue_wait_s": "queue_wait",
    "error.type": "error",
}


def _value(value: AnyValue) -> object:
    kind = value.WhichOneof("value")
    if kind is None:
        return None
    if kind == "array_value":
        return [_value(v) for v in value.array_value.values]
    if kind == "kvlist_value":
        return _attributes(value.kvlist_value.values)
    return getattr(value, kind)


def _attributes(attributes: Iterable[KeyValue]) -> dict[str, object]:
    return {kv.key: _value(kv.value) for kv in attributes}


def _component(resource_attributes: dict[str, object]) -> str:
    return str(
        resource_attributes.get("diracx.component")
        or resource_attributes.get("service.name")
        or "?"
    )


def _utc_time(unix_nano: int) -> datetime:
    """Convert an OTLP timestamp to a UTC datetime, for display."""
    return datetime.fromtimestamp(unix_nano / 1e9, tz=UTC)


def _format_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.3g}"
    return str(value)


@dataclass
class _ReceivedSpan:
    span: Span
    component: str


@dataclass
class _Trace:
    spans: dict[bytes, _ReceivedSpan] = field(default_factory=dict)
    last_received: float = 0.0
    printed_spans: int = 0


class TracePrinter:
    """Accumulate the spans per trace and print each trace as a tree.

    A trace is printed once no new span arrived for ``idle_seconds``: the
    processes export their spans in batches, so the spans of a trace
    arrive from several processes at different times.
    If more spans arrive afterwards, the whole trace is printed again.
    """

    def __init__(self, idle_seconds: float, retention_seconds: float = 600) -> None:
        self.idle_seconds = idle_seconds
        self.retention_seconds = retention_seconds
        self._traces: dict[bytes, _Trace] = {}
        self._lock = threading.Lock()

    def add(self, request: trace_service_pb2.ExportTraceServiceRequest) -> None:
        now = time.monotonic()
        with self._lock:
            for resource_spans in request.resource_spans:
                component = _component(_attributes(resource_spans.resource.attributes))
                for scope_spans in resource_spans.scope_spans:
                    for span in scope_spans.spans:
                        trace = self._traces.setdefault(span.trace_id, _Trace())
                        trace.spans[span.span_id] = _ReceivedSpan(span, component)
                        trace.last_received = now

    def flush(self, force: bool = False) -> list[str]:
        """Return the rendering of the traces which are complete."""
        now = time.monotonic()
        output = []
        with self._lock:
            for trace_id, trace in list(self._traces.items()):
                idle = now - trace.last_received
                if (force or idle >= self.idle_seconds) and trace.printed_spans < len(
                    trace.spans
                ):
                    output.append(self.render(trace_id, trace))
                    trace.printed_spans = len(trace.spans)
                if idle >= self.retention_seconds:
                    del self._traces[trace_id]
        return output

    @staticmethod
    def render(trace_id: bytes, trace: _Trace) -> str:
        spans = sorted(trace.spans.values(), key=lambda s: s.span.start_time_unix_nano)
        children: dict[bytes, list[_ReceivedSpan]] = {}
        roots = []
        for received in spans:
            parent = received.span.parent_span_id
            if parent and parent in trace.spans:
                children.setdefault(parent, []).append(received)
            else:
                roots.append(received)

        components = []
        for received in spans:
            if received.component not in components:
                components.append(received.component)
        start = _utc_time(spans[0].span.start_time_unix_nano)
        header = (
            f"━━ trace {trace_id.hex()} {start:%H:%M:%SZ} "
            f"({len(spans)} span{'s' if len(spans) > 1 else ''}: "
            f"{' → '.join(components)})"
        )
        if trace.printed_spans:
            header += " [updated]"
        lines = [header]

        def walk(received: _ReceivedSpan, prefix: str, is_last: bool, depth: int):
            connector = "" if depth == 0 else ("└─ " if is_last else "├─ ")
            lines.append(prefix + connector + _render_span(received))
            kids = children.get(received.span.span_id, [])
            child_prefix = prefix + (
                "" if depth == 0 else ("   " if is_last else "│  ")
            )
            for i, kid in enumerate(kids):
                walk(kid, child_prefix, i == len(kids) - 1, depth + 1)

        for root in roots:
            walk(root, "", True, 0)
        return "\n".join(lines)


def _render_span(received: _ReceivedSpan) -> str:
    span = received.span
    duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) / 1e6
    attributes = _attributes(span.attributes)
    details = [
        f"{label}={_format_value(attributes[key])}"
        for key, label in _SHOWN_ATTRIBUTES.items()
        # Hide the attributes with no information (e.g. retry=0)
        if attributes.get(key) not in (None, 0, "")
    ]
    if span.status.code == Status.STATUS_CODE_ERROR:
        details.insert(0, f"ERROR {span.status.message}".strip())
    for event in span.events:
        if event.name != "exception":
            details.append(f"event={event.name}")
    # e.g. the task.submit span of the task being processed, in another trace
    for link in span.links:
        details.append(f"link={link.trace_id.hex()}")
    kind = Span.SpanKind.Name(span.kind).removeprefix("SPAN_KIND_").lower()
    return f"{span.name}  [{received.component}, {kind}] {duration_ms:.1f}ms" + (
        f"  {' '.join(details)}" if details else ""
    )


class MetricPrinter:
    """Keep the last value of each time series, and print the ones which changed."""

    def __init__(self) -> None:
        self._values: dict[tuple, str] = {}
        self._printed: dict[tuple, str] = {}
        self._lock = threading.Lock()

    def add(self, request: metrics_service_pb2.ExportMetricsServiceRequest) -> None:
        with self._lock:
            for resource_metrics in request.resource_metrics:
                resource = _attributes(resource_metrics.resource.attributes)
                # Several processes of the same component report the same
                # time series (e.g. the workers): keep them apart
                component = _component(resource)
                if "process.pid" in resource:
                    component += f" pid={resource['process.pid']}"
                for scope_metrics in resource_metrics.scope_metrics:
                    for metric in scope_metrics.metrics:
                        for attributes, value in _data_points(metric):
                            key = (
                                metric.name,
                                component,
                                tuple(sorted(attributes.items())),
                            )
                            self._values[key] = value

    def flush(self) -> list[str]:
        with self._lock:
            changed = {
                key: value
                for key, value in self._values.items()
                if self._printed.get(key) != value
            }
            self._printed.update(changed)
        if not changed:
            return []
        lines = [
            f"━━ metrics {datetime.now(tz=UTC):%H:%M:%SZ} ({len(changed)} changed)"
        ]
        for (name, component, attributes), value in sorted(changed.items()):
            rendered = ", ".join(f"{k}={_format_value(v)}" for k, v in attributes)
            lines.append(f"{name} [{component}] {{{rendered}}} {value}")
        return ["\n".join(lines)]


def _data_points(metric) -> Iterable[tuple[dict[str, object], str]]:
    kind = metric.WhichOneof("data")
    if kind in ("sum", "gauge"):
        for point in getattr(metric, kind).data_points:
            number = point.as_double if point.HasField("as_double") else point.as_int
            yield _attributes(point.attributes), _format_value(number)
    elif kind in ("histogram", "exponential_histogram"):
        for point in getattr(metric, kind).data_points:
            mean = point.sum / point.count if point.count else 0
            yield (
                _attributes(point.attributes),
                f"count={point.count} mean={_format_value(mean)}",
            )


def _render_logs(request: logs_service_pb2.ExportLogsServiceRequest) -> list[str]:
    lines = []
    for resource_logs in request.resource_logs:
        component = _component(_attributes(resource_logs.resource.attributes))
        for scope_logs in resource_logs.scope_logs:
            for record in scope_logs.log_records:
                when = _utc_time(
                    record.time_unix_nano or record.observed_time_unix_nano
                )
                trace = f" trace={record.trace_id.hex()}" if record.trace_id else ""
                lines.append(
                    f"log {when:%H:%M:%SZ} [{component}] {record.severity_text} "
                    f"{scope_logs.scope.name}: {_value(record.body)}{trace}"
                )
    return lines


def _emit(blocks: Iterable[str]) -> None:
    for block in blocks:
        print(block, flush=True)


class _TraceService(trace_service_pb2_grpc.TraceServiceServicer):
    def __init__(self, printer: TracePrinter) -> None:
        self.printer = printer

    def Export(self, request, context):  # noqa: N802
        self.printer.add(request)
        return trace_service_pb2.ExportTraceServiceResponse()


class _MetricsService(metrics_service_pb2_grpc.MetricsServiceServicer):
    def __init__(self, printer: MetricPrinter) -> None:
        self.printer = printer

    def Export(self, request, context):  # noqa: N802
        self.printer.add(request)
        return metrics_service_pb2.ExportMetricsServiceResponse()


class _LogsService(logs_service_pb2_grpc.LogsServiceServicer):
    def __init__(self, show: bool) -> None:
        self.show = show

    def Export(self, request, context):  # noqa: N802
        if self.show:
            _emit(_render_logs(request))
        return logs_service_pb2.ExportLogsServiceResponse()


class _OTLPHTTPServer(ThreadingHTTPServer):
    """Receive OTLP over HTTP (protobuf encoded), as sent by the HTTP exporters."""

    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        traces: TracePrinter,
        metrics: MetricPrinter,
        show_logs: bool,
    ) -> None:
        super().__init__(address, _OTLPHTTPHandler)
        self.traces = traces
        self.metrics = metrics
        self.show_logs = show_logs


class _OTLPHTTPHandler(BaseHTTPRequestHandler):
    server: _OTLPHTTPServer

    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        response: object
        if self.path == "/v1/traces":
            traces = trace_service_pb2.ExportTraceServiceRequest.FromString(body)
            self.server.traces.add(traces)
            response = trace_service_pb2.ExportTraceServiceResponse()
        elif self.path == "/v1/metrics":
            metrics = metrics_service_pb2.ExportMetricsServiceRequest.FromString(body)
            self.server.metrics.add(metrics)
            response = metrics_service_pb2.ExportMetricsServiceResponse()
        elif self.path == "/v1/logs":
            logs = logs_service_pb2.ExportLogsServiceRequest.FromString(body)
            if self.server.show_logs:
                _emit(_render_logs(logs))
            response = logs_service_pb2.ExportLogsServiceResponse()
        else:
            self.send_error(404)
            return
        payload = response.SerializeToString()  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):  # noqa: A002
        # Do not print a line per request
        pass


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=4317, help="OTLP/gRPC port")
    parser.add_argument(
        "--http-port", type=int, default=4318, help="OTLP/HTTP port (0: disabled)"
    )
    parser.add_argument(
        "--trace-idle",
        type=float,
        default=10,
        help="Print a trace once no span arrived for it for that many seconds",
    )
    parser.add_argument(
        "--metrics-interval",
        type=float,
        default=30,
        help="Print the metrics which changed every that many seconds (0: never)",
    )
    parser.add_argument("--logs", action="store_true", help="Also print the logs")
    args = parser.parse_args(argv)

    traces = TracePrinter(idle_seconds=args.trace_idle)
    metrics = MetricPrinter()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    trace_service_pb2_grpc.add_TraceServiceServicer_to_server(
        _TraceService(traces), server
    )
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(
        _MetricsService(metrics), server
    )
    logs_service_pb2_grpc.add_LogsServiceServicer_to_server(
        _LogsService(args.logs), server
    )
    address = f"{args.host}:{args.port}"
    server.add_insecure_port(address)
    server.start()
    print(f"Listening for OTLP/gRPC on {address}", flush=True)

    http_server = None
    if args.http_port:
        http_server = _OTLPHTTPServer(
            (args.host, args.http_port), traces, metrics, args.logs
        )
        threading.Thread(target=http_server.serve_forever, daemon=True).start()
        print(
            f"Listening for OTLP/HTTP on http://{args.host}:{args.http_port}",
            flush=True,
        )

    # run_local.sh stops its services with SIGTERM: print the pending traces
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    last_metrics = time.monotonic()
    try:
        while True:
            time.sleep(1)
            _emit(traces.flush())
            if args.metrics_interval and (
                time.monotonic() - last_metrics >= args.metrics_interval
            ):
                _emit(metrics.flush())
                last_metrics = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        _emit(traces.flush(force=True))
        server.stop(grace=1)
        if http_server is not None:
            http_server.shutdown()


if __name__ == "__main__":
    main()
