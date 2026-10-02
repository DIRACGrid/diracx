# Monitor the task system

In this tutorial, you will run a complete DiracX instance on your machine, and follow tasks through the telemetry DiracX produces and the state of Redis:

- see periodic tasks being scheduled and executed;
- submit a task and follow it from its submission to its SQL queries;
- make a task fail, and watch it being retried and sent to the dead letter queue;
- look at the same information in Redis and in the database.

No monitoring infrastructure is needed: DiracX sends its telemetry to a minimal collector which prints it.
It takes about 15 minutes.

## Prerequisites

- A clone of the [diracx repository](https://github.com/DIRACGrid/diracx) and [pixi](https://pixi.sh/) installed (see the [developer getting started](../../dev/tutorials/getting-started.md)).
- The ports 4317 and 4318 (collector), 6379 (Redis), 8000 (DiracX) and 8333 (S3) free.

The local instance includes `gubbins`, the example extension of DiracX, whose `lollygag` tasks are used below.

## 1. Start DiracX with OpenTelemetry

In the `diracx` directory, run:

```bash
pixi run local-start --otel
```

After a minute, you should see:

```
✅ DiracX is running on http://localhost:8000
📋 To interact with DiracX you can:
  1️⃣  Open a configured shell:  pixi run local-shell
  2️⃣  Submit a task:  pixi run local-tasks submit <entry_point> [--args JSON]
  3️⃣  Swagger UI: http://localhost:8000/api/docs
  📡 OpenTelemetry: traces and metrics are printed with the [otel] prefix

📊 Services: ✅ seaweedfs ✅ redis ✅ uvicorn ✅ scheduler ✅ worker-sm ✅ worker-md ✅ worker-lg ✅ otel
```

Besides DiracX itself, this started:

- Redis, which holds the task queues (streams);
- a scheduler, which submits the periodic tasks and the delayed ones;
- three workers, one for each task size (`small`, `medium`, `large`);
- `otel`, a minimal OpenTelemetry collector: all the other services send it their traces and metrics, and it prints them.

Keep this terminal open: the logs of all the services appear there, and you will read the `[otel]` lines.

## 2. Watch a periodic task

The local instance runs `jobs:DummyJobExecutorMonitorTask` every 10 seconds.
After a few seconds, a trace like this one is printed:

```
[otel      ] ━━ trace dd8b0f331030ee3fd543cf9d076051cd 20:11:21Z (5 spans: tasks-scheduler → tasks-worker)
[otel      ] task.submit jobs:DummyJobExecutorMonitorTask  [tasks-scheduler, producer] 2.4ms
[otel      ] └─ task.process jobs:DummyJobExecutorMonitorTask  [tasks-worker, consumer] 110.8ms  queue_wait=0.00375
[otel      ]    └─ task.execute jobs:DummyJobExecutorMonitorTask  [tasks-worker, internal] 109.1ms  task=ok
[otel      ]       ├─ SELECT /tmp/tmp.NLsk31WKwQ/jobdb.db  [tasks-worker, client] 0.8ms
[otel      ]       └─ SELECT /tmp/tmp.NLsk31WKwQ/jobdb.db  [tasks-worker, client] 0.2ms
```

Read it from the top:

1. The **scheduler** submitted the task (`task.submit`, a *producer* span).
2. A **worker** picked it up 3.75 ms later (`queue_wait`), and processed the message (`task.process`, a *consumer* span).
3. The task itself ran for 109 ms (`task.execute`) and succeeded (`task=ok`).
4. While running, it made two SQL queries to the job database.

Two different processes appear in the same trace: the scheduler stored its trace context in the task message, and the worker continued it.
This is how DiracX can follow a piece of work across processes.

## 3. Submit a task yourself

In a second terminal, submit a task which inserts an owner called `alice` in the gubbins `LollygagDB`:

```bash
pixi run local-tasks submit lollygag:SyncOwnersTask --args '["alice"]'
```

The trace starts from the command line this time (`tasks-submit`):

```
[otel      ] ━━ trace 2f9c836fdab3031253a7989e509cea35 20:19:46Z (4 spans: tasks-submit → tasks-worker)
[otel      ] task.submit lollygag:SyncOwnersTask  [tasks-submit, producer] 0.4ms
[otel      ] └─ task.process lollygag:SyncOwnersTask  [tasks-worker, consumer] 6.5ms  queue_wait=0.00167
[otel      ]    └─ task.execute lollygag:SyncOwnersTask  [tasks-worker, internal] 5.7ms  task=ok
[otel      ]       └─ INSERT /tmp/tmp.lNn1symIA9/lollygagdb.db  [tasks-worker, client] 0.5ms
```

!!! tip "Traces are printed after a pause"

    Each process sends its spans every few seconds, so the collector waits for 10 seconds without new spans before printing a trace.
    If more spans arrive afterwards (for example a retry), the whole trace is printed again, marked `[updated]`.

## 4. Make a task fail

`SyncOwnersTask` needs the name of the owner. Submit it without:

```bash
pixi run local-tasks submit lollygag:SyncOwnersTask
```

This task is configured to be retried up to 3 times with an exponential backoff (10 then 20 seconds), and to go to the dead letter queue if it keeps failing.
Within a minute, the trace looks like this:

```
[otel      ] ━━ trace 8eaf0def8b20e50cd4fcafc6d8399d9c 20:19:48Z (8 spans: tasks-submit → tasks-worker) [updated]
[otel      ] task.submit lollygag:SyncOwnersTask  [tasks-submit, producer] 0.3ms
[otel      ] ├─ task.process lollygag:SyncOwnersTask  [tasks-worker, consumer] 4.0ms  queue_wait=0.00138 event=task.retry_scheduled
[otel      ] │  └─ task.execute lollygag:SyncOwnersTask  [tasks-worker, internal] 2.5ms  ERROR TypeError: SyncOwnersTask.__init__() missing 1 required positional argument: 'owner_name' task=error error=TypeError
[otel      ] ├─ task.process lollygag:SyncOwnersTask  [tasks-worker, consumer] 3.5ms  retry=1 queue_wait=0.0011 event=task.retry_scheduled
[otel      ] │  └─ task.execute lollygag:SyncOwnersTask  [tasks-worker, internal] 2.2ms  ERROR TypeError: ... task=error retry=1 error=TypeError
[otel      ] └─ task.process lollygag:SyncOwnersTask  [tasks-worker, consumer] 8.4ms  retry=2 queue_wait=0.0014 event=task.given_up
[otel      ]    ├─ task.execute lollygag:SyncOwnersTask  [tasks-worker, internal] 2.6ms  ERROR TypeError: ... task=error retry=2 error=TypeError
[otel      ]    └─ INSERT /tmp/tmp.lNn1symIA9/taskdb.db  [tasks-worker, client] 0.5ms
```

All the attempts are in the trace of the submission:

- each `task.execute` is marked `ERROR`, with the exception (in a real tracing backend, the full traceback is attached to the span);
- the first two attempts end with `task.retry_scheduled`: the task was put back in the delayed queue;
- the third one ends with `task.given_up`, and the task is inserted in the dead letter queue (`INSERT ... taskdb.db`).

## 5. Read the metrics

Every 30 seconds, the collector prints the metrics which changed. Look for the ones of `lollygag:SyncOwnersTask`:

```
[otel      ] tasks_submitted_total [tasks-submit pid=…] {delayed=False, priority=normal, size=small, task_name=lollygag:SyncOwnersTask} 1
[otel      ] tasks_completed_total [tasks-worker pid=…] {priority=normal, size=small, task_name=lollygag:SyncOwnersTask} 1
[otel      ] tasks_failed_total [tasks-worker pid=…] {priority=normal, size=small, task_name=lollygag:SyncOwnersTask} 3
[otel      ] tasks_retried_total [tasks-worker pid=…] {priority=normal, reason=error, size=small, task_name=lollygag:SyncOwnersTask} 2
[otel      ] tasks_given_up_total [tasks-worker pid=…] {action=dlq, priority=normal, size=small, task_name=lollygag:SyncOwnersTask} 1
[otel      ] task_duration_seconds [tasks-worker pid=…] {priority=normal, size=small, task_name=lollygag:SyncOwnersTask} count=4 mean=0.00242
```

They tell the same story, counted: 1 success and 3 failed executions, 2 retries, 1 task in the dead letter queue.
Each command line invocation is a separate process, hence one `tasks_submitted_total` line per `pid`.

Where the traces describe *one* piece of work, the metrics describe the whole system: this is what the [Grafana dashboards](../how-to/monitoring/use-the-dashboards.md) are built from.
The metrics of the scheduler describe the queues:

```
[otel      ] scheduler_leader [tasks-scheduler pid=…] {} 1
[otel      ] delayed_tasks_pending [tasks-scheduler pid=…] {} 0
[otel      ] task_stream_lag [tasks-scheduler pid=…] {priority=normal, size=small, stream=diracx:tasks:normal:small} 0
```

- `scheduler_leader` is 1: this scheduler holds the lock, and does the scheduling;
- `delayed_tasks_pending` counts the tasks waiting for their time (such as the retries above);
- `task_stream_lag` is the backlog of each stream: tasks waiting for a worker. It stays at 0 as long as the workers keep up.

## 6. Look at the same state in Redis

The metrics are computed from the state of Redis, which you can inspect directly.
The Redis CLI is available in the pixi environment:

```bash
pixi run -e default-gubbins redis-cli XINFO GROUPS diracx:tasks:normal:small
```

```
name
diracx:tasks:workers
consumers
1
pending
0
last-delivered-id
1790799598607-0
entries-read
3
lag
0
```

`pending` (tasks being processed) and `lag` (tasks not delivered yet) are the values reported as `task_stream_pending` and `task_stream_lag`.

Submit the failing task again, and look at the delayed tasks while it waits for its retry:

```bash
pixi run local-tasks submit lollygag:SyncOwnersTask
pixi run -e default-gubbins redis-cli ZRANGE diracx:tasks:delayed 0 -1 WITHSCORES
```

The score is the Unix time at which the scheduler will put the task back in its stream.

## 7. Find the task in the dead letter queue

The tasks given up are stored in the `dlq_tasks` table of the `TaskDB`.
Locally, it is a SQLite file, whose path is shown in the `INSERT ... taskdb.db` span above:

```bash
pixi run -e default-gubbins python -c "
import sqlite3
db = sqlite3.connect('/tmp/tmp.lNn1symIA9/taskdb.db')  # use your path
for row in db.execute('SELECT id, task_class, status, submitted_at, last_error FROM dlq_tasks'):
    print(row)
"
```

```
(1, 'lollygag:SyncOwnersTask', 'PENDING', '2026-09-30 20:20:18', 'Traceback (most recent call last):\n ... TypeError: SyncOwnersTask.__init__() missing 1 required positional argument: \'owner_name\'\n')
```

`last_error` holds the traceback of the last attempt.
The scheduler also counts the tasks of the dead letter queue every minute; among the metrics:

```
[otel      ] dead_letter_queue_tasks [tasks-scheduler pid=…] {status=PENDING, task_name=lollygag:SyncOwnersTask} 1
```

## 8. Stop

Press ++ctrl+c++ in the first terminal to stop all the services.

## What you learned

- DiracX follows a task from its submission to its execution in one **trace**, even across processes, including its retries.
- The **metrics** count what happens (tasks completed, failed, retried, given up) and describe the queues.
- The same state can be inspected in **Redis** and in the **dead letter queue**.

## Next steps

- [Enable OpenTelemetry](../how-to/monitoring/enable-opentelemetry.md) on a real installation, and [use the Grafana dashboards](../how-to/monitoring/use-the-dashboards.md).
- [Troubleshoot with telemetry](../how-to/monitoring/troubleshoot-with-telemetry.md) when something goes wrong.
- [Operate the task system](../how-to/tasks/operate.md): locks, dead letter queue, scheduled tasks.
- Look up every span and metric in the [OpenTelemetry reference](../reference/opentelemetry.md).
