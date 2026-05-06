# Tasks reference

## Lock types

All locks are Redis-backed with ownership tracking. Each lock instance generates a random owner ID so that release operations verify ownership, preventing one worker from accidentally releasing another's lock.

Locks are split into two categories:

- **Structural locks** (`MutexLock`, `ExclusiveRWLock`, `SharedRWLock`) — always acquired, including in interactive mode (`diracx-task-run call`).
- **Limiters** (`RateLimiter`, `ConcurrencyLimiter`) — skipped in interactive mode. These are subclasses of `BaseLimiter`.

### MutexLock

Mutual-exclusion lock. At most one owner can hold the lock at a time.

```python
from diracx.tasks.plumbing.locks import MutexLock

MutexLock(obj, key, *extra_keys, ttl_ms=30000)
```

- **`obj`**: `LockedObjectType` — the type of object being locked
- **`key`**: `str | int` — identifier for the locked object
- **`*extra_keys`**: additional key segments (e.g. VO name)
- **`ttl_ms`**: lock auto-expires after this many milliseconds (safety net for crashes)
- **Redis key**: `lock:mutex:{obj}:{key}:{extra_keys...}`
- Supports `extend()` for the watchdog pattern

### ExclusiveRWLock

Writer side of a readers-writer lock. Acquire succeeds only when there are zero readers and no existing writer.

```python
from diracx.tasks.plumbing.locks import ExclusiveRWLock

ExclusiveRWLock(obj, key, *extra_keys, ttl_ms=30000)
```

- **Redis key**: `lock:rw:{obj}:{key}:{extra_keys...}`
- Pair with `SharedRWLock` on the same `(obj, key)` to allow concurrent readers or a single exclusive writer
- Supports `extend()`

### SharedRWLock

Reader side of a readers-writer lock. Multiple readers can hold the lock concurrently. Acquire succeeds as long as no writer holds the exclusive side.

```python
from diracx.tasks.plumbing.locks import SharedRWLock

SharedRWLock(obj, key, *extra_keys)
```

- **Redis key**: `lock:rw:{obj}:{key}:{extra_keys...}` (same hash as `ExclusiveRWLock`)
- No TTL — readers are tracked via an atomic counter, not a per-owner key
- Does not support `extend()`

### RateLimiter

Sliding-window rate limiter. Limits the number of operations within a fixed time window.

```python
from diracx.tasks.plumbing.locks import RateLimiter

RateLimiter(obj, key, *extra_keys, n_items=1)
```

- **`n_items`**: number of quota units consumed per acquire (default: 1)
- **`limit`**: class variable — maximum operations per window (`None` = disabled)
- **`window_seconds`**: class variable — window duration (`None` = disabled)
- **Redis key**: `limiter:rate:{obj}:{key}:{extra_keys...}`
- `release()` is a no-op — consumed quota is not returned
- Skipped in interactive mode

### ConcurrencyLimiter

Semaphore-style concurrency cap backed by a Redis sorted set.

```python
from diracx.tasks.plumbing.locks import ConcurrencyLimiter

ConcurrencyLimiter(obj, key, *extra_keys, ttl_ms=30000)
```

- **`limit`**: class variable — maximum concurrent holders (`None` = disabled)
- **`ttl_ms`**: per-holder expiry — crashed workers' slots are automatically reclaimed
- **Redis key**: `limiter:conc:{obj}:{key}:{extra_keys...}`
- Supports `extend()` to push back expiry for long-running tasks
- Skipped in interactive mode

### Default locks

`BaseTask` returns a disabled `RateLimiter` and `ConcurrencyLimiter` by default (both with `limit=None`). This means limits can be enabled via configuration without code changes.

`PeriodicBaseTask` overrides this with a `MutexLock` keyed by the task class name, preventing concurrent execution. `PeriodicVoAwareBaseTask` adds the VO name to the lock key, so each VO gets its own mutex.

### Lock watchdog vs message heartbeat

Two independent watchdog-style mechanisms exist while a worker executes tasks:

- **Lock watchdog** (`_lock_watchdog` in `factory.py`) periodically calls `extend()` on acquired locks, so lock TTLs do not expire mid-execution.
- **Message heartbeat** (`_message_heartbeat` in `worker.py`) periodically renews stream-message ownership while a task is running.

The message heartbeat uses Redis `XCLAIM` with `min_idle_time=0` to reset the pending-entry idle timer for the in-flight message. This prevents `XAUTOCLAIM` from reclaiming a long-running task simply because it exceeded the idle timeout.

These mechanisms protect different things:

- lock watchdog protects lock ownership
- message heartbeat protects consumer-group message ownership

______________________________________________________________________

## Schedules

Schedules determine when periodic tasks are submitted by the scheduler. All schedules implement `next_occurrence() -> datetime`.

### IntervalSeconds

Fixed-interval schedule.

```python
from diracx.tasks.plumbing.schedules import IntervalSeconds

IntervalSeconds(seconds=3600)  # every hour
```

### CronSchedule

Cron-expression schedule using [croniter](https://pypi.org/project/croniter/).

```python
from diracx.tasks.plumbing.schedules import CronSchedule

CronSchedule("0 6 * * *")  # daily at 06:00
CronSchedule("*/15 * * * *")  # every 15 minutes
CronSchedule("0 0 * * 0")  # weekly on Sunday at midnight
```

### RRuleSchedule

RFC 2445 recurrence rule using [dateutil](https://pypi.org/project/python-dateutil/).

```python
from diracx.tasks.plumbing.schedules import RRuleSchedule

RRuleSchedule("FREQ=WEEKLY;BYDAY=FR")  # every Friday
RRuleSchedule("FREQ=HOURLY;INTERVAL=2")  # every 2 hours
RRuleSchedule("FREQ=MONTHLY;BYDAY=-1FR")  # last Friday of each month
```

Raises `ValueError` if the rule has no future occurrences.

______________________________________________________________________

## Retry policies

Retry policies determine whether and when a failed task is retried. They are set as class variables on task classes.

### NoRetry

Never retries. This is the default.

```python
from diracx.tasks.plumbing.retry_policies import NoRetry


class MyTask(BaseTask):
    retry_policy = NoRetry()
```

### ExponentialBackoff

Retries with exponentially increasing delays: `delay = base_delay_seconds * 2^attempt`.

```python
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff


class MyTask(BaseTask):
    retry_policy = ExponentialBackoff(base_delay_seconds=10, max_retries=5)
```

- **`base_delay_seconds`**: initial delay (default: 10)
- **`max_retries`**: maximum number of retry attempts (default: 5)
- After `max_retries` is reached, the task is either dropped or sent to the dead-letter queue (if `dlq_eligible = True`)
