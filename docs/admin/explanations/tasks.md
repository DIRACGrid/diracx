# How the task system works

This page explains the DiracX task system from an operational perspective. For the design rationale and architecture, see the [developer explanation](../../dev/explanations/tasks/index.md) and the [ADR](../../adr/DX-ADR-001_tasks.md).

## Task lifecycle

1. A task is **submitted** — either by application code, the scheduler (for periodic tasks), or the CLI (`diracx-task-run call`)
2. The broker places it on one of **nine Redis Streams**, selected by the task's priority (realtime, normal, background) and size (small, medium, large)
3. A **worker** picks up the message from the stream, consuming in strict priority order (realtime first)
4. The worker **acquires locks** required by the task (mutex, RW, limiters). If a lock cannot be acquired, the task is rescheduled with a short delay
5. The worker **resolves dependencies** (database connections, settings) via the dependency injection system
6. The task's `execute()` method **runs**. A watchdog thread periodically extends lock TTLs for long-running tasks
7. On success, the message is **acknowledged** and removed from the stream. If the task belongs to a callback group, the worker checks whether all siblings have completed
8. On failure, the **retry policy** is consulted. If retries remain, the task is placed in the delayed sorted set for later promotion. If retries are exhausted and the task is dead-letter-queue-eligible, it is persisted to SQL

## Streams

The broker uses nine Redis Streams named `diracx:tasks:{priority}:{size}`:

|                | Small                           | Medium                           | Large                           |
| -------------- | ------------------------------- | -------------------------------- | ------------------------------- |
| **Realtime**   | `diracx:tasks:realtime:small`   | `diracx:tasks:realtime:medium`   | `diracx:tasks:realtime:large`   |
| **Normal**     | `diracx:tasks:normal:small`     | `diracx:tasks:normal:medium`     | `diracx:tasks:normal:large`     |
| **Background** | `diracx:tasks:background:small` | `diracx:tasks:background:medium` | `diracx:tasks:background:large` |

Workers are configured for a specific size class and consume from the three priority streams for that size. This allows different worker pools to be scaled independently based on resource requirements.

## Locking

Two categories of lock protect tasks:

- **Structural locks** prevent data corruption. `MutexLock` provides exclusive access; `ExclusiveRWLock`/`SharedRWLock` allow concurrent readers or a single writer. These are always enforced, including in interactive CLI mode.
- **Limiters** control throughput. `RateLimiter` caps operations per time window; `ConcurrencyLimiter` caps simultaneous executions. These are skipped in interactive mode.

All locks have TTLs. A **watchdog** thread extends lock TTLs during execution so that long-running tasks don't lose their locks. If a worker crashes, locks auto-expire and are released.

## Scheduler

The scheduler is a **singleton** process (enforced by a Redis mutex with a 30-second TTL). It runs four concurrent loops:

1. **Periodic loop** — checks if any periodic task is due (based on `IntervalSeconds`, `CronSchedule`, or `RRuleSchedule`) and submits it to the broker
2. **Delayed poll loop** — promotes tasks from the delayed sorted set (`diracx:tasks:delayed`) to their target stream when their scheduled time arrives. Uses an atomic Lua script to prevent race conditions
3. **Lock extend loop** — periodically refreshes the scheduler's own Redis mutex
4. **Config watch loop** — detects changes to the VO list and adds/removes periodic task schedules for new/removed VOs

## Dead-letter queue

Tasks marked with `dlq_eligible = True` that exhaust their retries are persisted to the `dlq_tasks` table in the SQL database. This provides a durable safety net — critical tasks are never silently dropped, even if Redis is restarted. Dead letter queue tasks can be inspected and resubmitted.

## Ephemeral broker state

The broker's Redis state is designed to be **ephemeral**. On restart:

- The scheduler repopulates periodic task schedules from entry points and configuration
- Consumer groups are recreated for all nine streams
- Dead-letter-queue-eligible tasks that were persisted in SQL can be resubmitted
- Delayed one-off tasks that were in flight are lost (periodic tasks are automatically rescheduled)

See the [broker lifecycle explanation](../../dev/explanations/tasks/broker-lifecycle.md) for details on how this affects application design.
