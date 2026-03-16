# Broker lifecycle

The task system's broker state is **ephemeral** — Redis can be restarted or flushed and the system will recover automatically. This is a deliberate design choice that simplifies operations and avoids the need for careful Redis persistence configuration.

## What happens on broker restart

When the broker (Redis) starts fresh, all in-flight state is lost:

- The nine task streams (3 priorities x 3 sizes) are empty
- The delayed task ZSET is empty
- All locks are released
- All callback group state is gone

The system recovers through the following mechanisms:

### Scheduler repopulates periodic tasks

The scheduler process loads all `PeriodicBaseTask` and `PeriodicVoAwareBaseTask` subclasses from the entry point registry and computes their initial schedules. For VO-aware tasks, it reads the VO list from the DiracX configuration and creates one schedule entry per VO. This happens automatically in the scheduler's periodic loop — no manual intervention is needed.

### Dead-letter-queue-eligible tasks are persisted in SQL

Tasks marked with `dlq_eligible = True` that exhaust their retries are persisted to the `TaskDB` (a SQL database), not Redis. On startup, pending dead letter queue tasks can be re-submitted to the broker for another attempt. This ensures that critical tasks are never lost even if Redis is completely wiped.

### Consumer groups are recreated

The broker's `startup()` method calls `xgroup_create` for all nine streams, creating consumer groups if they don't already exist. This is idempotent — it's safe to call on every startup.

### Delayed tasks in flight are lost

Tasks that were in the delayed ZSET (scheduled for future execution via `task.schedule(at_time=...)`) are lost on Redis restart. For periodic tasks this is harmless — the scheduler will recompute and resubmit them. For one-off delayed tasks, the caller is responsible for re-submitting if the task is critical (or marking it `dlq_eligible`).

## Design implications

This ephemeral broker model imposes a constraint on how DiracX objects interact with the task system: **state machines must be designed to handle broker restarts gracefully**.

For example, if a job enters a `PENDING` state while waiting for an in-flight task, and the broker restarts, that task is lost. The job's state machine must account for this by either:

- Resetting jobs in `PENDING` back to `RECEIVED` on startup, so the task is re-triggered
- Using a periodic task to sweep for jobs stuck in `PENDING` beyond a timeout

This pattern is described in the [ADR](../../../adr/DX-ADR-001_tasks.md#broker) and applies to any DiracX object that depends on task completion to transition state.
