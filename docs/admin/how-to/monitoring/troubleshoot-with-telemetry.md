# Troubleshoot with telemetry

This guide lists common problems, how they show in the [dashboards](use-the-dashboards.md) and the traces, and where to look next.
The metric names are those of the [OpenTelemetry reference](../../reference/opentelemetry.md#metrics).

## A route is slow

1. In *DiracX Routers*, the *Slowest routes (p95)* panel and the table at the bottom tell which route is slow, and since when.
2. Search the traces for that span name (e.g. `POST /api/jobs/search`) with a minimum duration.
3. In a slow trace, look at the child spans:
    - long SQL spans: the query (`db.query.text`) is slow. Check the *Query latency p95 by database* panel to see if the whole database is slow, or only this query;
    - a large gap with no child span: time spent in Python (serialisation of a large response, CPU bound code), or waiting for a connection of the pool (see [below](#the-database-connection-pool-is-exhausted));
    - many short SQL spans: the route does many queries (N+1 pattern).
4. If only some users are affected, filter the traces on `enduser.id` or `diracx.vo`.

## Requests fail

- **5xx**: *Server errors (5xx) by route* tells which route. The trace of a failed request has the `ERROR` status, and the logs of the request carry its trace ID.
- **401/403 bursts** in *Client errors (4xx) by route* usually mean a token or policy problem: an expired signing key, a misconfigured identity provider, or a client using an outdated token.
- **503** means a database is unavailable, or the configuration is not loaded yet.
- **400 for some users only**: their client may be too old. *Rejected clients* in the *Clients* row shows the refused versions; tell these users to upgrade.

## Raise the minimum client version

Before raising the minimum version of the client supported by the server, check who would be refused:

1. In *DiracX Routers*, select a long time range (e.g. the last 30 days).
2. In the *Client versions over the selected time range* table, look at the requests made with the versions below the new minimum.
3. To find who uses these versions, filter the traces on `diracx.client.version`: the spans also carry the `enduser.id` and `diracx.vo`.

## Tasks pile up

Symptom: the *Backlog* stat and the *Backlog by stream* panel grow, and *Submitted vs processed* shows more submissions than executions.

1. Check *Worker utilisation*:
    - **close to 100%** for the size of the stream: the workers are saturated. Either add workers of that size, increase their `--max-concurrent-tasks`, or find why the tasks became slower (*Execution time p95 by task*);
    - **low**: the workers are not the bottleneck. Check that workers of that size are running (a stream of size `large` is only consumed by `large` workers), and look for lock contention below.
2. Check *Queue wait p95 by priority*: `realtime` tasks are always picked first, so if `realtime` waits, the workers are really overloaded; if only `background` waits, this may be acceptable.

## Tasks keep retrying

*Retries* shows the reason:

- **`error`**: the task raises an exception. The `task.execute` spans of the task have the `ERROR` status and the traceback; the retries of a task are in the same trace, so one trace shows all the attempts.
- **`lock_contention`**: the task could not acquire a lock or a limiter, and is rescheduled 5 seconds later. A few are normal; many mean that too many instances of the task run concurrently (e.g. a periodic task slower than its period), or that a lock is stuck. See [Operate the task system](../tasks/operate.md#check-locks) to inspect the locks.

## Tasks are lost or end in the dead letter queue

*Given up* shows the tasks which exhausted their retries:

- `dlq`: persisted in the dead letter queue. The *Dead letter queue* panel shows how many tasks it contains, per task and status; the traceback of the last attempt is in the `last_error` column of the `dlq_tasks` table. See [Operate the task system](../tasks/operate.md#handle-the-dead-letter-queue);
- `discarded`: the task is not dlq-eligible, it is dropped by design;
- `dlq_failed`: the task should have been persisted but could not be (no `TaskDB` configured, or database error). **It is lost**: check the worker logs.

*Rejected messages* shows messages the workers could not process at all:

- `unknown_task`: a task was submitted which is not installed on the workers. Typically, an extension is not installed on all the worker images, or a task was removed while some were still queued;
- `unparsable`: a message is corrupted, or produced by an incompatible version.

## Workers crash

*Reclaimed messages* counts the messages taken over from a worker which did not acknowledge them in time (10 minutes by default), typically because it was killed (OOM, node failure) while running the task.
The reclaimed executions have `task.reclaimed=true` on their `task.process` span.
Check the restarts and the memory of the worker pods of the corresponding size.

## Periodic tasks do not run

1. The *Scheduler* stat must be `OK` (exactly one instance holds the scheduler lock). `No leader` means that no scheduler is running, or that it cannot reach Redis.
2. *Periodic submissions* shows, per task, whether the submissions succeed.
3. If the submissions succeed but nothing is executed, the tasks are queued: see [Tasks pile up](#tasks-pile-up).

## The database connection pool is exhausted

Symptom: *Connection pool usage* is at 100% for a database, *Connection wait p95 by database* grows, and requests or tasks become slow without their SQL queries being slow.
In the worst case, *Connection timeouts* shows requests or tasks failing because no connection became available in time.

Each process has a pool per database, of `pool_size + max_overflow` connections (15 by default).
When they are all in use, the next request waits for a connection to be released.
Either some queries or transactions are too long (look at *Query latency p95 by database*, and for long transactions in the traces), or the number of concurrent requests/tasks per process is too high for the pool.

## Find everything about a user's request

1. Filter the traces on `enduser.id` (the `sub` of the user's token), `diracx.vo` or `diracx.group`, and the time of the problem.
2. The trace contains the request, its SQL queries, and the tasks it submitted, including their retries.
3. The logs with the same trace ID are those of the request and of the tasks.
