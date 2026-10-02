# Add telemetry to your code

DiracX uses [OpenTelemetry](../../admin/explanations/opentelemetry.md) for its traces and metrics.
This guide explains how to add spans, attributes and metrics to a router, a task or an extension, and how to check the result.

## Know what you get for free

Before adding anything, remember that the following is already instrumented:

- every HTTP request is a span, with the route, the status code and the user;
- every SQL query is a span, with the database and the query;
- every task submission and execution is a span, connected to whatever submitted it;
- HTTP, SQL and task metrics (see the [reference](../../admin/reference/opentelemetry.md)).

So there is no need, for example, to create a span around a database call or a whole route.
Instrument what these do not show: a significant step of an algorithm, a call to an external service, a business event.

## Decide where the instrumentation goes

Only use the OpenTelemetry **API** (`opentelemetry-api`), never the SDK: the SDK is configured once per process by `diracx.tasks.otel.configure_otel`.
Without it (e.g. in the tests, or when `DIRACX_OTEL_ENABLED` is false), the API does nothing and costs almost nothing.

`diracx-routers` and `diracx-tasks` (and their extensions) depend on `opentelemetry-api`.
`diracx-core`, `diracx-db` and `diracx-logic` deliberately do not: if you need telemetry about something happening in the logic, add it in the router or the task calling it, or add attributes to the current span (see below) from the router or task.
An extension can of course add `opentelemetry-api` to the dependencies of any of its packages.

## Add information to the current span

The cheapest and often most useful instrumentation is to add attributes to the span which is already there (the request or the task):

```python
from opentelemetry import trace


async def execute(self, job_db: JobDB, **kwargs):
    job_ids = await job_db.get_jobs_to_kill()
    trace.get_current_span().set_attribute("diracx.jobs.count", len(job_ids))
```

Use the [semantic conventions](https://opentelemetry.io/docs/specs/semconv/) names when one exists, and prefix the others with `diracx.`.

## Create a span

For a significant operation, create a span with a tracer created once per module:

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


async def refresh_proxies(vo: str):
    with tracer.start_as_current_span(
        "refresh_proxies", attributes={"diracx.vo": vo}
    ) as span:
        ...
        span.set_attribute("diracx.proxies.refreshed", count)
```

The span automatically becomes a child of the current span, and the spans created inside it (SQL queries, submitted tasks...) become its children.

If an exception escapes the `with` block, the span records it and gets the `ERROR` status.
To mark a handled error:

```python
from opentelemetry.trace import Status, StatusCode

try:
    ...
except ExternalServiceError as exc:
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
```

Name spans after the operation, not after its parameters: `refresh_proxies`, not `refresh_proxies lhcb`. Parameters go into attributes.

## Add a metric

Create the instruments once, at the module level:

```python
from opentelemetry import metrics

meter = metrics.get_meter(__name__)
proxies_refreshed = meter.create_counter(
    "proxies_refreshed_total",
    description="Proxies refreshed, per VO",
)
refresh_duration = meter.create_histogram(
    "proxy_refresh_duration_seconds",
    description="Time to refresh a proxy",
    unit="s",
    # The default buckets are meant for milliseconds
    explicit_bucket_boundaries_advisory=[0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30],
)


async def refresh(vo: str):
    ...
    proxies_refreshed.add(1, attributes={"vo": vo})
    refresh_duration.record(elapsed, attributes={"vo": vo})
```

Choose the instrument by what the value means:

| Instrument       | Use it for                                            | Example                                             |
| ---------------- | ----------------------------------------------------- | --------------------------------------------------- |
| Counter          | Something which happens                               | `tasks_completed_total`                             |
| Histogram        | A duration or a size, to get percentiles              | `task_duration_seconds`                             |
| UpDownCounter    | Something which goes up and down, and that you change | `tasks_in_progress`                                 |
| Observable gauge | A state which is read periodically                    | `task_stream_lag`, read from Redis by the scheduler |

Rules:

- **Bounded attributes only.** Each combination of attribute values is a time series stored by Prometheus. A VO, a task name, a status are fine; a job ID, a user, a URL or an error message are not: they belong to spans.
- **Report a global state from one process only.** If every worker reported the length of a queue, it would be counted as many times as there are workers. The scheduler, which is a singleton, reports such values.
- **Give a unit** (`s`, `By`) for durations and sizes: the Prometheus name gets the corresponding suffix.
- **Document the metric** in the [OpenTelemetry reference](../../admin/reference/opentelemetry.md#metrics), and consider adding it to the dashboards of `diracx-charts`.

## Add information to the logs

Use the standard `logging` module, with a logger per module (`logger = logging.getLogger(__name__)`).
The logs are [configured](../../admin/explanations/logs.md) by `diracx.core.logs.configure_logging`: do not add handlers.

Give the values which could be searched for as attributes, rather than only in the message:

```python
logger.info("Killed %d jobs", len(job_ids), extra={"diracx.jobs.count": len(job_ids)})
```

They become fields of the JSON logs and attributes of the OpenTelemetry log records.

The task being executed and the user of the request are added automatically.
To add attributes to all the records emitted in a block (e.g. while processing a pilot), use `log_context`:

```python
from diracx.core.logs import log_context

with log_context(**{"diracx.pilot.stamp": pilot_stamp}):
    await process_pilot(...)  # all its logs carry diracx.pilot.stamp
```

`set_log_context` does the same for the rest of the current context, for code which cannot wrap what follows (e.g. a FastAPI dependency: each request has its own context).

## Test it

The providers are global and can only be set once per process: set them once for the test session, with in memory exporters.
`diracx-tasks/tests/test_otel.py` has ready-made fixtures:

```python
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_SPAN_EXPORTER = InMemorySpanExporter()
_METRIC_READER = InMemoryMetricReader()


@pytest.fixture(scope="session")
def otel_providers():
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(MeterProvider(metric_readers=[_METRIC_READER]))


async def test_refresh_is_traced(otel_providers):
    _SPAN_EXPORTER.clear()
    await refresh_proxies("lhcb")
    [span] = [
        s for s in _SPAN_EXPORTER.get_finished_spans() if s.name == "refresh_proxies"
    ]
    assert span.attributes["diracx.proxies.refreshed"] == 3
```

Instruments created at import time (before the providers are set) work: the API forwards them to the providers once they are set.

## Look at it

Run the full stack with a collector printing what it receives:

```bash
pixi run local-start --otel
```

Your spans appear in the trace trees printed with the `[otel]` prefix, and your metrics in the tables printed every 30 seconds.
See [Running locally](../tutorials/advanced-tutorial/running-locally.md#observing-the-telemetry).
