## Use task callbacks

The callback system provides a fan-out/fan-in pattern: spawn multiple child tasks and fire a callback task when all children complete.

### Basic usage

Use `CallbackSpawner` to schedule children and a callback together:

```python
from diracx.tasks.depends import CallbackSpawner


class OrchestrationTask(BaseTask):
    async def execute(self, spawn_with_callback: CallbackSpawner, **kwargs):
        children = [
            SyncOwnersTask(owner_name="alice"),
            SyncOwnersTask(owner_name="bob"),
            SyncOwnersTask(owner_name="charlie"),
        ]
        callback = OwnerCleanupTask()

        group_id = await spawn_with_callback(children, callback)
```

The children are scheduled immediately. When the last child completes, the worker automatically submits the callback task to the broker.

### How it works

1. `spawn_with_callback` generates a unique `group_id` and stores:
    - The serialized callback task in Redis
    - An atomic counter set to the number of children
2. Each child is scheduled with a `group_id` label
3. When a worker completes a child task that has a `group_id` label, it calls `on_child_complete` which:
    - Stores the child's result
    - Atomically decrements the remaining counter
4. When the counter reaches zero, the worker fires the callback

### Using `CallbackSpawner` in `execute()`

`CallbackSpawner` is a dependency injection type resolved by the worker. Declare it as a typed parameter in `execute()`:

```python
from diracx.tasks.depends import CallbackSpawner


class MyTask(BaseTask):
    async def execute(self, spawn_with_callback: CallbackSpawner, **kwargs):
        group_id = await spawn_with_callback(children, callback)
        ...
```

### Cleanup

All Redis keys created by the callback system are set with a TTL (default 24 hours). This means callback state is automatically cleaned up even if something goes wrong and the callback never fires.

The TTL can be configured via the `ttl_seconds` parameter:

```python
await spawn_with_callback(children, callback, ttl_seconds=3600)  # 1 hour
```
