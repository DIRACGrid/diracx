# Logs reference

This page describes the logs written by the DiracX processes.
See [Collect and read the logs](../how-to/monitoring/collect-and-read-logs.md) to use them.

## Settings

| Environment variable         | Default   | Description                                                        |
| ---------------------------- | --------- | ------------------------------------------------------------------ |
| `DIRACX_LOG_FORMAT`          | `text`    | `text` (human readable) or `json` (one JSON object per line)       |
| `DIRACX_LOG_LEVEL`           | `INFO`    | Level of the DiracX loggers, and of those of the extension         |
| `DIRACX_LOG_LIBRARIES_LEVEL` | `WARNING` | Level of the loggers of the other libraries (SQLAlchemy, httpx...) |

They are read by all the processes: API servers, scheduler, workers and the `diracx-tasks` command line.
The level names are case insensitive (`debug` or `DEBUG`); an unknown level is rejected at startup with the list of valid ones.
The logs are written to stderr. The access logs of uvicorn use the same format.

## Text format

```text
<date (UTC)> <level> <logger>: <message> [<context>]
```

For example:

```text
2026-09-30T21:30:35.173Z INFO     diracx.tasks.plumbing.worker.worker: Executing task lollygag:SyncOwnersTask (ID: 6bd6549ba1e5) [task.name=lollygag:SyncOwnersTask task.id=6bd6549ba1e5]
```

The traceback, if any, follows on the next lines.

## JSON format

One JSON object per line. The field names follow the [OpenTelemetry log data model](https://opentelemetry.io/docs/specs/otel/logs/data-model/) and [semantic conventions](https://opentelemetry.io/docs/specs/semconv/).

| Field                                                         | Present                                    | Description                                                                                                                         |
| ------------------------------------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `timestamp`                                                   | always                                     | ISO 8601, in UTC, with milliseconds: `2026-09-30T21:30:35.173Z`                                                                     |
| `severity_text`                                               | always                                     | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`                                                                                     |
| `logger`                                                      | always                                     | Name of the logger, e.g. `diracx.tasks.plumbing.worker.worker`                                                                      |
| `body`                                                        | always                                     | The message                                                                                                                         |
| `trace_id`, `span_id`                                         | within a span, if OpenTelemetry is enabled | The trace and span IDs, as in the traces (32 and 16 hexadecimal characters)                                                         |
| `exception.type`, `exception.message`, `exception.stacktrace` | if the record has an exception             | The exception and its traceback                                                                                                     |
| `code.stacktrace`                                             | with `stack_info=True`                     | The stack                                                                                                                           |
| context attributes                                            | see below                                  |                                                                                                                                     |
| attributes given with `extra=`                                |                                            | e.g. `logger.info("...", extra={"job_id": 42})` gives `"job_id": 42`. Values which cannot be written in JSON are written as strings |

The access logs of uvicorn (`"logger": "uvicorn.access"`) also have `http.request.method`, `url.path`, `http.response.status_code`, `client.address` and `network.protocol.version`, as do the corresponding [OpenTelemetry log records](opentelemetry.md#logs).

For example:

```json
{"timestamp": "2026-09-30T21:30:35.173Z", "severity_text": "ERROR", "logger": "diracx.tasks.plumbing.worker.worker", "body": "Exception in task lollygag:SyncOwnersTask", "exception.type": "TypeError", "exception.message": "SyncOwnersTask.__init__() missing 1 required positional argument: 'owner_name'", "exception.stacktrace": "Traceback (most recent call last):\n ...", "task.name": "lollygag:SyncOwnersTask", "task.id": "6bd6549ba1e5"}
```

## Context attributes

These attributes are added to all the records emitted in a given context, in both formats and in the [OpenTelemetry log records](opentelemetry.md#logs).
They have the same names as the span attributes.

| Attribute      | Added to the records emitted...                      | Description                                         |
| -------------- | ---------------------------------------------------- | --------------------------------------------------- |
| `task.name`    | while a worker processes a task                      | Name of the task, e.g. `jobs:CleanSandboxStoreTask` |
| `task.id`      | while a worker processes a task                      | ID of the task                                      |
| `enduser.id`   | during a request, once the access token is validated | `sub` of the token                                  |
| `diracx.vo`    | during a request, once the access token is validated | VO of the token                                     |
| `diracx.group` | during a request, once the access token is validated | DIRAC group of the token                            |

An attribute given explicitly with `extra=` takes precedence over the context.

The access logs of uvicorn (`uvicorn.access`) do not carry the user attributes: they are emitted by uvicorn outside of the context in which the token is validated. The span of the request carries them.

## JSON logs and OpenTelemetry log records

When [OpenTelemetry is enabled](../how-to/monitoring/enable-opentelemetry.md), the same records are also exported over OTLP.
Their attributes (context, uvicorn access fields, `extra=`) are the same, but some fields are carried differently:

| JSON logs                                              | OpenTelemetry log record                                                         |
| ------------------------------------------------------ | -------------------------------------------------------------------------------- |
| `timestamp`                                            | `Timestamp` (nanoseconds)                                                        |
| `severity_text`                                        | `SeverityText`, and `SeverityNumber`                                             |
| `body`                                                 | `Body`                                                                           |
| `logger`                                               | name of the instrumentation scope (`Scope.name`)                                 |
| `trace_id`, `span_id`                                  | `TraceId`, `SpanId`, `TraceFlags`                                                |
| `exception.*`                                          | `exception.*` attributes                                                         |
| (none)                                                 | `code.file.path`, `code.function.name`, `code.line.number` attributes            |
| (none: added by the collector from the pod, if at all) | resource attributes: `service.*`, `host.name`, `process.pid`, `diracx.component` |
