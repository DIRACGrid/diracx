# OpenTelemetry reference

This page lists the telemetry produced by DiracX: configuration, resource attributes, spans and metrics.
See the [OpenTelemetry explanation](../explanations/opentelemetry.md) for the reasoning behind it, and the [monitoring how-to guides](../how-to/monitoring/index.md) to use it.

## Configuration

### DiracX settings

| Environment variable           | Default  | Description                                                                                                                                                                                             |
| ------------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DIRACX_OTEL_ENABLED`          | `false`  | Send telemetry to an OpenTelemetry collector                                                                                                                                                            |
| `DIRACX_OTEL_APPLICATION_NAME` | `diracx` | Value of the `service.name` resource attribute                                                                                                                                                          |
| `DIRACX_OTEL_PROTOCOL`         | `grpc`   | Protocol used to send the data to the collector: `grpc`, or `http` (protobuf encoded)                                                                                                                   |
| `DIRACX_OTEL_GRPC_ENDPOINT`    |          | With `grpc`: address of the collector OTLP/gRPC receiver, e.g. `otel-collector:4317`                                                                                                                    |
| `DIRACX_OTEL_GRPC_INSECURE`    | `true`   | With `grpc`: do not use TLS to talk to the collector                                                                                                                                                    |
| `DIRACX_OTEL_HTTP_ENDPOINT`    |          | With `http`: base URL of the collector OTLP/HTTP receiver, e.g. `http://otel-collector:4318`. `/v1/traces`, `/v1/metrics` and `/v1/logs` are appended to it, and the scheme decides whether TLS is used |
| `DIRACX_OTEL_HEADERS`          |          | JSON dictionary of headers sent to the collector, e.g. `{"tenant_id": "lhcbdiracx-cert"}`                                                                                                               |

These settings are read by all the processes: API servers, scheduler, workers and the `diracx-tasks` command line.
If the endpoint of the chosen protocol is empty, the standard `OTEL_EXPORTER_OTLP_ENDPOINT` variables (or their defaults, `localhost:4317` and `http://localhost:4318`) are used.
They are also listed with the other [environment variables](env-variables.md#otelsettings).

### Standard OpenTelemetry variables

The standard [OpenTelemetry SDK variables](https://opentelemetry.io/docs/languages/sdk-configuration/general/) are honoured, in particular:

| Environment variable                | Default                 | Description                                                                                              |
| ----------------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------- |
| `OTEL_RESOURCE_ATTRIBUTES`          |                         | Additional resource attributes, e.g. `deployment.environment.name=production`                            |
| `OTEL_TRACES_SAMPLER`               | `parentbased_always_on` | Sampler, e.g. `parentbased_traceidratio`                                                                 |
| `OTEL_TRACES_SAMPLER_ARG`           |                         | Argument of the sampler, e.g. `0.1` to keep 10% of the traces                                            |
| `OTEL_PYTHON_FASTAPI_EXCLUDED_URLS` | `api/health/`           | Comma separated regular expressions of the URLs which are not instrumented (neither traced nor measured) |

### Local development

| Environment variable          | Default | Description                                                                                                                  |
| ----------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `DIRACX_LOCAL_OTEL_PROTOCOL`  | `grpc`  | Protocol used by DiracX to send the data to the minimal collector started by `pixi run local-start --otel`: `grpc` or `http` |
| `DIRACX_LOCAL_OTEL_PORT`      | `4317`  | OTLP/gRPC port of the minimal collector                                                                                      |
| `DIRACX_LOCAL_OTEL_HTTP_PORT` | `4318`  | OTLP/HTTP port of the minimal collector                                                                                      |

## Resource attributes

Attached to all the spans, metrics and logs of a process.

| Attribute             | Example                          | Description                                                                |
| --------------------- | -------------------------------- | -------------------------------------------------------------------------- |
| `service.name`        | `diracx`                         | `DIRACX_OTEL_APPLICATION_NAME`                                             |
| `service.version`     | `0.5.0`                          | Version of `diracx-core`                                                   |
| `service.instance.id` | `diracx-demo-7c9d7d8b4f-xk2lp-7` | Host name (the pod name in kubernetes) and process ID: unique per process  |
| `host.name`           | `diracx-demo-7c9d7d8b4f-xk2lp`   | Host name (the pod name in kubernetes)                                     |
| `process.pid`         | `7`                              | Process ID                                                                 |
| `diracx.component`    | `routers`                        | `routers`, `tasks-worker`, `tasks-scheduler`, `tasks-submit`, `tasks-call` |

## Spans

### HTTP requests

Created by the [OpenTelemetry FastAPI instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html), following the [HTTP semantic conventions](https://opentelemetry.io/docs/specs/semconv/http/http-spans/).

| Span                                              | Kind   | Attributes                                                                                          |
| ------------------------------------------------- | ------ | --------------------------------------------------------------------------------------------------- |
| `<method> <route>`, e.g. `GET /api/jobs/{job_id}` | SERVER | `http.request.method`, `http.route`, `http.response.status_code`, `url.path`, `client.address`, ... |

DiracX adds `diracx.client.version`, the version of the DiracX client (`DiracX-Client-Version` header, `none` if absent).
Once the access token is validated, it also adds:

| Attribute      | Description              |
| -------------- | ------------------------ |
| `enduser.id`   | `sub` of the token       |
| `diracx.vo`    | VO of the token          |
| `diracx.group` | DIRAC group of the token |

### SQL queries

Following the [database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/).
Only created within another span (a request or a task).

| Span                                          | Kind   | Attributes                                                                                                                                                                                                          |
| --------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<operation> <database>`, e.g. `SELECT JobDB` | CLIENT | `db.system.name` (`mysql`, `sqlite`), `db.namespace` (database), `db.operation.name` (`SELECT`, `INSERT`, ...), `db.query.text` (parametrised query, truncated to 2000 characters), `server.address`, `server.port` |

A failed query has the `ERROR` status, the `error.type` attribute (the exception class), and an `exception` event with the traceback.

### Tasks

| Span                  | Kind     | Emitted by      | Description                                                                                                            |
| --------------------- | -------- | --------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `task.submit <task>`  | PRODUCER | whoever submits | Submission of a task to the broker. Its context is stored in the message.                                              |
| `task.process <task>` | CONSUMER | worker          | Whole handling of a message: execution, retry or dead letter queue, callbacks, result storage. Child of `task.submit`. |
| `task.execute <task>` | INTERNAL | worker          | Execution of the task code. Child of `task.process`.                                                                   |

`<task>` is the name of the task in the registry, e.g. `jobs:CleanSandboxStoreTask`.

**`task.submit`** attributes:

| Attribute       | Description                                |
| --------------- | ------------------------------------------ |
| `task.name`     | Name of the task                           |
| `task.id`       | ID of the task                             |
| `task.priority` | `realtime`, `normal` or `background`       |
| `task.size`     | `small`, `medium` or `large`               |
| `task.delayed`  | `true` if the task is scheduled for later  |
| `task.run_at`   | When the task is scheduled for, if delayed |

**`task.process`** attributes:

| Attribute                                            | Description                                                                             |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `task.name`, `task.id`, `task.priority`, `task.size` | As above                                                                                |
| `task.retry_count`                                   | Number of previous attempts                                                             |
| `task.queue_wait_s`                                  | Seconds spent in the stream before being picked up (not set for reclaimed messages)     |
| `task.reclaimed`                                     | `true` if the message was taken over from a worker which did not acknowledge it in time |
| `messaging.destination.name`                         | Stream, e.g. `diracx:tasks:normal:small`                                                |
| `messaging.message.id`                               | ID of the message in the stream                                                         |

and events:

| Event                  | Attributes                                                                                 | Description                         |
| ---------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------- |
| `task.retry_scheduled` | `task.retry.reason` (`error`), `task.retry.attempt`, `task.retry.at`, `task.retry.task_id` | The task failed and will be retried |
| `task.given_up`        | `task.action` (`dlq`, `discarded`, `dlq_failed`)                                           | The task exhausted its retries      |

**`task.execute`** attributes:

| Attribute                                                                | Description                                                                    |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `task.name`, `task.id`, `task.priority`, `task.size`, `task.retry_count` | As above                                                                       |
| `task.status`                                                            | `ok`, `error`, or `lock_contention` if a lock or limiter could not be acquired |
| `task.duration_ms`                                                       | Execution time                                                                 |
| `error.type`                                                             | Exception class, if the task failed                                            |

A failed task has the `ERROR` status and an `exception` event with the traceback.
On lock contention, a `task.retry_scheduled` event (`task.retry.reason=lock_contention`) is added.

### Logs

The log records of the DiracX and extension loggers (and of the `uvicorn` ones in the API servers) are exported once each, with the [context attributes](logs.md#context-attributes) (task, user...) as attributes:

| Field                                                         | Content                                                            |
| ------------------------------------------------------------- | ------------------------------------------------------------------ |
| Body                                                          | The message                                                        |
| Severity                                                      | The level of the record                                            |
| Trace and span IDs                                            | Those of the active span, if any                                   |
| Scope name                                                    | The name of the logger, e.g. `diracx.tasks.plumbing.worker.worker` |
| `code.file.path`, `code.function.name`, `code.line.number`    | Where the record was emitted                                       |
| `exception.type`, `exception.message`, `exception.stacktrace` | If the record has an exception (`logger.exception(...)`)           |

The logs of the other libraries (SQLAlchemy, httpx...) are not exported.

## Metrics

The names are given as exported by DiracX (OpenTelemetry) and as they appear in Prometheus with the default translation of the collector.
Unless stated otherwise, the resource attributes are available as labels (`service_name`, `service_instance_id`, `diracx_component`...) only if the collector converts them (`resource_to_telemetry_conversion`), as done by the DiracX chart.

### HTTP (API servers)

From the FastAPI instrumentation, following the [HTTP metrics semantic conventions](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/).
The `api/health/` routes are not measured.

| Prometheus name                        | Type      | Labels                                                                                                     | Description                 |
| -------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------- | --------------------------- |
| `http_server_request_duration_seconds` | histogram | `http_request_method`, `http_route`, `http_response_status_code`, `url_scheme`, `network_protocol_version` | Duration of the requests    |
| `http_server_active_requests`          | gauge     | `http_request_method`, `url_scheme`                                                                        | Requests being served       |
| `http_server_request_body_size_bytes`  | histogram | as for the duration                                                                                        | Size of the request bodies  |
| `http_server_response_body_size_bytes` | histogram | as for the duration                                                                                        | Size of the response bodies |

Requests per client version, from DiracX:

| Prometheus name         | Type    | Labels                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------------------- | ------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `client_requests_total` | counter | `client_version`, `outcome` | Requests per version of the DiracX client (`DiracX-Client-Version` header). `client_version` is the normalised version, `none` without header (web interface, direct HTTP requests), `invalid` if it cannot be parsed, and `other` once 50 distinct versions have been seen by the process (the header is set by the client, so the number of versions is bounded). `outcome`: `accepted`, or `rejected` if the client is older than the minimum supported version or sent an invalid version |

### Tasks

`task_name`, `priority` and `size` are the name of the task (e.g. `jobs:CleanSandboxStoreTask`), its priority and its size.

| Prometheus name                        | Type      | Labels                                     | Emitted by      | Description                                                                                                                                                                                  |
| -------------------------------------- | --------- | ------------------------------------------ | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tasks_submitted_total`                | counter   | `task_name`, `priority`, `size`, `delayed` | whoever submits | Tasks submitted                                                                                                                                                                              |
| `tasks_completed_total`                | counter   | `task_name`, `priority`, `size`            | worker          | Tasks completed successfully                                                                                                                                                                 |
| `tasks_failed_total`                   | counter   | `task_name`, `priority`, `size`            | worker          | Executions which raised an exception                                                                                                                                                         |
| `task_duration_seconds`                | histogram | `task_name`, `priority`, `size`            | worker          | Execution time (not recorded on lock contention). Buckets from 10 ms to 1 h                                                                                                                  |
| `task_queue_wait_seconds`              | histogram | `task_name`, `priority`, `size`            | worker          | Time spent in the stream before being picked up. Buckets from 10 ms to 1 h                                                                                                                   |
| `tasks_in_progress`                    | gauge     | `task_name`, `priority`, `size`            | worker          | Tasks being processed                                                                                                                                                                        |
| `worker_max_concurrent_tasks`          | gauge     | `worker_size`                              | worker          | Capacity of the worker (`--max-concurrent-tasks`)                                                                                                                                            |
| `tasks_retried_total`                  | counter   | `task_name`, `priority`, `size`, `reason`  | worker          | Tasks rescheduled. `reason`: `error`, `lock_contention`                                                                                                                                      |
| `tasks_given_up_total`                 | counter   | `task_name`, `priority`, `size`, `action`  | worker          | Tasks which exhausted their retries. `action`: `dlq` (persisted in the dead letter queue), `discarded` (not dlq-eligible), `dlq_failed` (could not be persisted: lost)                       |
| `task_messages_rejected_total`         | counter   | `reason`, `task_name`                      | worker          | Messages dropped. `reason`: `unparsable`, `unknown_task` (the task is not installed on the worker)                                                                                           |
| `task_messages_reclaimed_total`        | counter   | `stream`                                   | worker          | Messages taken over from a worker which did not acknowledge them in time, typically because it died                                                                                          |
| `task_stream_length`                   | gauge     | `stream`, `priority`, `size`               | scheduler       | Entries in the stream                                                                                                                                                                        |
| `task_stream_lag`                      | gauge     | `stream`, `priority`, `size`               | scheduler       | Messages not delivered to any worker yet (the backlog). Requires Redis 7                                                                                                                     |
| `task_stream_pending`                  | gauge     | `stream`, `priority`, `size`               | scheduler       | Messages delivered to a worker but not acknowledged yet                                                                                                                                      |
| `delayed_tasks_pending`                | gauge     |                                            | scheduler       | Tasks in the delayed ZSET (scheduled tasks and retries)                                                                                                                                      |
| `delayed_tasks_promoted_total`         | counter   |                                            | scheduler       | Delayed tasks moved to their stream                                                                                                                                                          |
| `scheduler_periodic_submissions_total` | counter   | `task_name`, `outcome`                     | scheduler       | Periodic tasks submitted. `outcome`: `success`, `failure`                                                                                                                                    |
| `scheduler_leader`                     | gauge     |                                            | scheduler       | `1` if the instance holds the scheduler lock, `0` otherwise                                                                                                                                  |
| `dead_letter_queue_tasks`              | gauge     | `task_name`, `status`                      | scheduler       | Tasks in the dead letter queue. `status`: `PENDING` (waiting to be resubmitted), `DISPATCHED` (resubmitted), `FAILED`. Requires the `TaskDB` (`DIRACX_DB_URL_TASKDB`), reported every minute |

The stream, delayed and dead letter queue gauges are only reported by the scheduler holding the lock, every 10 seconds.

### SQL databases (all processes)

Following the [database metrics semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-metrics/).

| Prometheus name                          | Type      | Labels                                                                                    | Description                                                                                                                        |
| ---------------------------------------- | --------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `db_client_operation_duration_seconds`   | histogram | `db_system_name`, `db_namespace`, `db_operation_name`, `error_type` (if the query failed) | Duration of the SQL queries. Buckets from 0.5 ms to 10 s                                                                           |
| `db_client_connection_count`             | gauge     | `db_client_connection_pool_name`, `db_client_connection_state` (`used`, `idle`)           | Connections in the SQLAlchemy pool                                                                                                 |
| `db_client_connection_max`               | gauge     | `db_client_connection_pool_name`                                                          | Capacity of the pool (`pool_size + max_overflow`)                                                                                  |
| `db_client_connection_wait_time_seconds` | histogram | `db_client_connection_pool_name`                                                          | Time spent obtaining a connection from the pool, including the creation of a new connection if needed. Buckets from 0.1 ms to 30 s |
| `db_client_connection_timeouts_total`    | counter   | `db_client_connection_pool_name`                                                          | Connections which could not be obtained before the pool timeout (`pool_timeout`, 30 s by default): the request or task failed      |

The pool name is the name of the database.

## Dashboards

The DiracX chart ships Grafana dashboards in [`diracx/dashboards`](https://github.com/DIRACGrid/diracx-charts/tree/master/diracx/dashboards):

| File                  | Title          | Content                                                                                                                                                |
| --------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `diracx-routers.json` | DiracX Routers | Traffic, errors and latency per route and per instance, SQL databases of the API servers                                                               |
| `diracx-tasks.json`   | DiracX Tasks   | Throughput, execution time and queue wait, streams backlog, worker utilisation, retries and dead letter queue, scheduler, SQL databases of the workers |
| `metrics.json`        | Metrics        | HTTP requests rate, sizes and duration per route                                                                                                       |

They use a Prometheus data source, and the `service_name`, `service_instance_id` and `diracx_component` labels.
