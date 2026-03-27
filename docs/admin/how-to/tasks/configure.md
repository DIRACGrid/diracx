# Configure the task system

## Redis connection

The broker, workers, and scheduler all connect to Redis using the `DIRACX_TASKS_REDIS_URL` environment variable:

```bash
export DIRACX_TASKS_REDIS_URL="redis://redis-host:6379"
```

If unset, defaults to `redis://localhost`.

## Starting workers

Workers consume tasks from the broker and execute them:

```bash
diracx-task-run worker --redis-url redis://redis-host:6379 --max-concurrent-tasks 10
```

- **`--max-concurrent-tasks`**: maximum number of tasks a worker runs concurrently (default: 10). Tune based on whether tasks are I/O-bound (higher) or CPU-bound (lower).
- **`--redis-url`**: overrides `DIRACX_TASKS_REDIS_URL`.

Workers consume tasks from the three priority streams for their configured size in strict priority order (realtime first, then normal, then background).

## Starting the scheduler

The scheduler is a singleton process responsible for submitting periodic tasks and promoting delayed tasks:

```bash
diracx-task-run scheduler --redis-url redis://redis-host:6379
```

Only one scheduler instance should run at a time. This is enforced by a Redis mutex — if a second scheduler starts, it will wait for the first to release the lock before taking over.

## Helm chart configuration

TODO: Document the specific Helm chart values for configuring the number of worker replicas, scheduler deployment, and Redis connection.

## Overriding task defaults

Task behaviour (rate limits, concurrency limits, periodic schedules) can be overridden via the DiracX configuration system without code changes. See the [tasks configuration reference](../../reference/tasks-configuration.md) for the YAML structure.
