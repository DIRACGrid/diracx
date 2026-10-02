# Operate the task system

This page covers day-to-day operational tasks for the DiracX task system.
When [OpenTelemetry is enabled](../monitoring/enable-opentelemetry.md), most of the state described here is also available as metrics, and shown by the *DiracX Tasks* [dashboard](../monitoring/use-the-dashboards.md).

## Monitor streams

The broker uses nine Redis Streams (one per priority/size combination) named `diracx:tasks:{priority}:{size}`.

With OpenTelemetry, the scheduler reports for each stream its length (`task_stream_length`), the messages not delivered to any worker yet (`task_stream_lag`) and the messages being processed (`task_stream_pending`).
See the *Queues* row of the *DiracX Tasks* dashboard.

The same information is available directly from Redis:

```bash
redis-cli XINFO STREAM diracx:tasks:normal:medium
# "pending" and "lag" of the diracx:tasks:workers consumer group
redis-cli XINFO GROUPS diracx:tasks:normal:medium
redis-cli XLEN diracx:tasks:normal:medium
```

## Check scheduled tasks

The scheduler maintains a delayed task sorted set at `diracx:tasks:delayed`, where each member's score is the Unix timestamp when it should be promoted to a stream.

The number of delayed tasks is reported as `delayed_tasks_pending`.
The scheduler also logs the next run of the periodic tasks every 10 minutes (`Next schedule snapshot`).

```bash
# View the next 10 delayed tasks
redis-cli ZRANGEBYSCORE diracx:tasks:delayed -inf +inf WITHSCORES LIMIT 0 10
```

## Check locks

Locks are stored as Redis keys with prefixes `lock:mutex:`, `lock:rw:`, `limiter:rate:`, and `limiter:conc:`.

TODO: Document how to inspect lock state, identify stuck locks, and manually release locks if needed.

```bash
# List all active mutex locks
redis-cli KEYS "lock:mutex:*"

# Check a specific lock's TTL
redis-cli PTTL "lock:mutex:task:SyncOwnersTask:alice"
```

## Handle the dead-letter queue

Tasks marked with `dlq_eligible = True` that exhaust their retries are persisted to the `TaskDB` SQL database in the `dlq_tasks` table. Dead letter queue tasks have a status of `PENDING`, `DISPATCHED`, or `FAILED`.
The `last_error` column holds the traceback of the last failed attempt.

With OpenTelemetry, the scheduler reports the number of tasks in the dead letter queue per task and status (`dead_letter_queue_tasks`), shown by the *Dead letter queue* panel of the *DiracX Tasks* dashboard.

TODO: Document how to query dead letter queue tasks, resubmit them, and remove successfully processed entries. This will be part of the monitoring dashboard effort.
