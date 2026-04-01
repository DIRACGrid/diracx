## Add a task

Tasks are async Python classes that define a unit of work executed by the DiracX task system. This guide walks through defining tasks, registering them, and testing them interactively. All examples reference the `gubbins-tasks` extension package.

### Define a one-shot task

Subclass `BaseTask`, add arguments as dataclass fields, and implement `execute()`. Dependencies are injected as keyword arguments to `execute()`.

```python
import dataclasses
from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff

from gubbins.db.sql import LollygagDB


@dataclasses.dataclass
class SyncOwnersTask(BaseTask):
    owner_name: str

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = ExponentialBackoff(base_delay_seconds=5, max_retries=3)
    dlq_eligible = True

    async def execute(self, lollygag_db: LollygagDB, **kwargs):
        owner_id = await lollygag_db.insert_owner(self.owner_name)
        return owner_id
```

The class-level attributes control how the task is queued and retried:

- `priority` — `REALTIME`, `NORMAL`, or `BACKGROUND`
- `size` — `SMALL`, `MEDIUM`, or `LARGE`
- `retry_policy` — `NoRetry()` (default) or `ExponentialBackoff()`
- `dlq_eligible` — if `True`, the task is persisted to a dead-letter queue after exhausting retries

### Register via entry point

Add the task to a `diracx.tasks.<category>` entry point group in your package's `pyproject.toml`:

```toml
[project.entry-points."diracx.tasks.lollygag"]
SyncOwnersTask = "gubbins.tasks.lollygag:SyncOwnersTask"
```

### Custom locks

Override `execution_locks` to control concurrency. The example below uses a per-owner mutex so two syncs of the same owner are mutually exclusive:

```python
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.lock_registry import LockedObjectType
from .lock_types import LOLLYGAG


@dataclasses.dataclass
class SyncOwnersTask(BaseTask):
    owner_name: str

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(LockedObjectType(LOLLYGAG), self.owner_name)]
```

To use a custom `LockedObjectType`, register it in a module and expose it via the `diracx.lock_object_types` entry point:

```python
# gubbins/tasks/lock_types.py
from diracx.tasks.plumbing.lock_registry import register_locked_object_type

LOLLYGAG = register_locked_object_type("lollygag")
```

```toml
[project.entry-points."diracx.lock_object_types"]
lollygag = "gubbins.tasks.lock_types"
```

### Periodic tasks

For tasks that run on a schedule, subclass `PeriodicBaseTask` and set `default_schedule`:

```python
from diracx.tasks.plumbing.base_task import PeriodicBaseTask
from diracx.tasks.plumbing.schedules import IntervalSeconds


class OwnerCleanupTask(PeriodicBaseTask):
    default_schedule = IntervalSeconds(3600)  # every hour

    async def execute(self, lollygag_db: LollygagDB, **kwargs):
        owners = await lollygag_db.get_owner()
        return owners
```

Periodic tasks automatically get a `MutexLock` to prevent concurrent execution.

For tasks that should run once per VO, subclass `PeriodicVoAwareBaseTask`. The scheduler creates one instance per VO, each with its own VO-scoped mutex:

```python
from diracx.tasks.plumbing.base_task import PeriodicVoAwareBaseTask
from diracx.tasks.plumbing.schedules import CronSchedule


@dataclasses.dataclass
class OwnerReportTask(PeriodicVoAwareBaseTask):
    vo: str

    default_schedule = CronSchedule("0 6 * * *")  # daily at 06:00

    async def execute(self, lollygag_db: LollygagDB, **kwargs):
        owners = await lollygag_db.get_owner()
        return owners
```

Available schedules: `IntervalSeconds`, `CronSchedule`, `RRuleSchedule`.

### Retries and error handling

The task system has two independent retry mechanisms:

**Failure retries** apply when `execute()` raises an exception. The worker consults the task's `retry_policy` to decide whether and when to retry:

- `NoRetry()` (default) — the task is not retried.
- `ExponentialBackoff(base_delay_seconds=10, max_retries=5)` — retries with increasing delays (`base * 2^attempt`).

Both policies implement `schedule_retry(attempt, exception)`, which returns a `datetime` for the next attempt or `None` to stop retrying. You can subclass `RetryPolicyBase` to write a custom policy.

**Lock contention retries** happen automatically when a task cannot acquire its execution locks (e.g. another instance holds the mutex). These bypass the retry policy entirely — the worker reschedules the task after a fixed delay and does not decrement the retry budget.

#### Choosing `dlq_eligible`

When a task exhausts its retries, the `dlq_eligible` flag controls what happens next:

- `dlq_eligible = True` — the task is persisted to a dead-letter queue (DLQ) for later inspection or manual replay. Use this for tasks where losing the work is unacceptable, such as syncing external state or processing user requests.
- `dlq_eligible = False` (default) — the task is discarded with a warning log. Use this for periodic or self-healing tasks where the next scheduled run will cover the missed work.

#### Explicitly requesting a retry

Raise `TaskRetryRequestedError` inside `execute()` to signal that the task should be retried regardless of the exception type. The retry policy still controls the timing and maximum attempts:

```python
from diracx.tasks.plumbing.exceptions import TaskRetryRequestedError


async def execute(self, lollygag_db: LollygagDB, **kwargs):
    result = await lollygag_db.try_sync(self.owner_name)
    if result.needs_retry:
        raise TaskRetryRequestedError("upstream not ready")
    return result.value
```

### Test interactively

Use `diracx-task-run call` to execute a task directly:

```bash
diracx-task-run call lollygag:SyncOwnersTask --args '["alice"]'
```

The `call` subcommand resolves dependencies and acquires structural locks, but skips limiters (`RateLimiter`, `ConcurrencyLimiter`), making it suitable for debugging and manual recovery.
