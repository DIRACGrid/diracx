## Spawn a task from a task

Tasks can schedule other tasks during their execution using the `BaseTask.schedule()` method. The broker connection is automatically available inside `execute()` — no manual wiring is needed.

### Basic spawning

Call `schedule()` on a task instance to submit it to the broker immediately:

```python
from gubbins.tasks.lollygag import SyncOwnersTask


class MyParentTask(BaseTask):
    async def execute(self, **kwargs):
        # Spawn a child task for each owner
        for name in ["alice", "bob"]:
            await SyncOwnersTask(owner_name=name).schedule()
```

The child task is enqueued on the stream matching its `priority` and `size` class variables and will be picked up by the next available worker.

### Delayed scheduling

To schedule a task for future execution, pass `at_time`:

```python
from datetime import UTC, datetime, timedelta


class RetryLaterTask(BaseTask):
    async def execute(self, **kwargs):
        # Schedule a follow-up task in 5 minutes
        run_at = datetime.now(tz=UTC) + timedelta(minutes=5)
        await SyncOwnersTask(owner_name="alice").schedule(at_time=run_at)
```

Delayed tasks are placed in a Redis sorted set (ZSET) and promoted to the appropriate stream by the scheduler when their time arrives.

### How it works

The broker is bound to task classes via a `ContextVar` when the worker starts up (`BaseTask.bind_broker()`). When `schedule()` is called, it:

1. Serializes the task's constructor arguments via `serialize()`
2. Looks up the broker's decorated task for this class
3. Submits the message to the broker (or the delayed ZSET if `at_time` is set)

This means `schedule()` only works inside a worker process (or any context where the broker has been bound). It will raise `RuntimeError` if called outside of a broker context.

### Labels

Extra metadata can be attached to spawned tasks via the `labels` parameter:

```python
await SyncOwnersTask(owner_name="alice").schedule(
    labels={"source": "parent_task", "batch_id": "abc123"}
)
```

Labels are passed through to the broker message and can be used for tracking and debugging. The callback system uses labels internally to track group membership (see [Use task callbacks](use-task-callbacks.md)).
