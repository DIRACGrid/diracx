# Part 8: Running locally

## Starting the full stack

Start the complete DiracX stack with:

```bash
pixi run local-start
```

Once everything is ready you'll see:

```
✅ DiracX is running on http://localhost:8000
📋 To interact with DiracX you can:
  1️⃣  Open a configured shell:  pixi run local-shell
  2️⃣  Submit a task:  pixi run local-tasks submit <entry_point> [--args JSON]
  3️⃣  Swagger UI: http://localhost:8000/api/docs

📊 Services: ✅ seaweedfs ✅ redis ✅ uvicorn ✅ scheduler ✅ worker-sm ✅ worker-md ✅ worker-lg
```

This launches:

| Component           | Description                                         |
| ------------------- | --------------------------------------------------- |
| **seaweedfs**       | S3-compatible object storage (sandboxes)            |
| **Redis**           | Message broker for the task system                  |
| **uvicorn**         | DiracX API server                                   |
| **scheduler**       | Periodic task scheduling and delayed-task promotion |
| **worker-sm/md/lg** | One worker per size (small, medium, large)          |

Press ++ctrl+c++ to stop all services.

## Interacting with the running instance

Open a shell that is pre-configured to talk to the local instance:

```bash
pixi run local-shell
```

From this shell you can use the `dirac` CLI as normal, for example:

```bash
dirac jobs submit ...
```

## Submitting tasks

With the full stack running, tasks can be submitted via the helper command:

```bash
pixi run local-tasks submit <entry_point> [--args JSON]
```

The scheduler handles periodic task scheduling, and workers pick up
tasks from the Redis streams based on their size and priority.

## Interactive task execution

You can also run a task directly, bypassing the broker — useful for
development and debugging:

```bash
pixi run local-tasks call <entry_point> [args...]
```

## Observing the telemetry

To see the traces and metrics produced by your code, start the stack with:

```bash
pixi run local-start --otel
```

This enables OpenTelemetry in all the services, and starts a minimal collector (`python -m diracx.testing.otel_printer`) which prints what it receives with the `[otel]` prefix:

- each trace as a tree, gathering the spans of all the processes: a request, the tasks it submitted, and their SQL queries;
- every 30 seconds, the metrics whose value changed.

```
[otel      ] ━━ trace 894855bd1c5b7b3d5be5ce663ffe5edd 20:12:51Z (22 spans: tasks-scheduler → tasks-worker)
[otel      ] task.submit jobs:DummyJobExecutorMonitorTask  [tasks-scheduler, producer] 0.4ms
[otel      ] └─ task.process jobs:DummyJobExecutorMonitorTask  [tasks-worker, consumer] 49.2ms  queue_wait=0.00155
[otel      ]    └─ task.execute jobs:DummyJobExecutorMonitorTask  [tasks-worker, internal] 48.7ms  task=ok
[otel      ]       ├─ SELECT /tmp/tmp.NLsk31WKwQ/jobdb.db  [tasks-worker, client] 19.4ms
[otel      ]       └─ task.submit jobs:DummyJobExecutorTask  [tasks-worker, producer] 0.9ms
[otel      ]          └─ task.process jobs:DummyJobExecutorTask  [tasks-worker, consumer] 61.1ms
```

The collector listens for OTLP over gRPC on port 4317 and over HTTP on port 4318 (`DIRACX_LOCAL_OTEL_PORT` and `DIRACX_LOCAL_OTEL_HTTP_PORT` change them).
DiracX sends its data over gRPC; set `DIRACX_LOCAL_OTEL_PROTOCOL=http` to use HTTP instead.
See [Add telemetry to your code](../../how-to/add-telemetry.md) to instrument your own code.

## What's next

- Read the [Tasks explanation](../../explanations/tasks/index.md) for
    deeper understanding of the broker lifecycle
- Check the [admin tasks guide](../../../admin/how-to/tasks/index.md)
    for operational guidance
