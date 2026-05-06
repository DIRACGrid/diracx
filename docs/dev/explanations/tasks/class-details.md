# Class details

This page describes the key classes and type definitions that make up the task system's plumbing layer in `diracx.tasks.plumbing`. Read the [architecture overview](index.md) first for the high-level design.

## Redis type aliases

The module `plumbing/_redis_types.py` defines four `TypeAlias` values. All resolve to `redis.asyncio.Redis` at runtime — they exist purely for readability and grep-ability, so that each function signature documents *why* it needs a Redis connection.

| Alias              | Semantics                                      |
| ------------------ | ---------------------------------------------- |
| `LockCoordinator`  | Acquiring, releasing, and extending locks      |
| `MessageTransport` | Enqueuing, reading, or promoting task messages |
| `ResultCache`      | Storing and retrieving task results            |
| `CallbackRegistry` | Tracking callback groups and firing callbacks  |

## Lock subsystem

All lock primitives live in `plumbing/locks.py`. Every lock is scoped to a `(LockedObjectType, key, *extra_keys)` tuple which is joined into a Redis key. A random owner ID is generated per instance so that release operations can verify ownership.

```mermaid
classDiagram
    class BaseLock {
        <<abstract>>
        +acquire(redis) bool
        +release(redis)
        +extend(redis) bool
        +redis_key str
    }

    class MutexLock {
        +ttl_ms: int
        SET NX PX with Lua release
    }

    class ExclusiveRWLock {
        +ttl_ms: int
        Hash-based writer side
    }

    class SharedRWLock {
        Hash-based reader side
    }

    class BaseLimiter {
        <<abstract>>
        Skipped in interactive mode
    }

    class RateLimiter {
        +limit: int | None
        +window_seconds: int | None
        Sliding-window counter
    }

    class ConcurrencyLimiter {
        +limit: int | None
        +ttl_ms: int
        ZSET-based semaphore
    }

    BaseLock <|-- MutexLock
    BaseLock <|-- ExclusiveRWLock
    BaseLock <|-- SharedRWLock
    BaseLock <|-- BaseLimiter
    BaseLimiter <|-- RateLimiter
    BaseLimiter <|-- ConcurrencyLimiter
```

**Reader-writer locks** (`ExclusiveRWLock` and `SharedRWLock`) share the same Redis hash key (`lock:rw:{obj}:{key}`) — multiple readers can hold the lock concurrently, but a writer requires exclusive access.

**Limiters** inherit from `BaseLock` but are skipped when a task is executed interactively via `diracx-task-run call`. Their `limit` defaults to `None` (disabled); configuration can enable them without code changes.

## Task subsystem

Task base classes live in `plumbing/base_task.py`, with supporting types in `plumbing/retry_policies.py` and `plumbing/schedules.py`.

```mermaid
classDiagram
    class BaseTask {
        <<abstract>>
        +priority: Priority
        +size: Size
        +retry_policy: RetryPolicyBase
        +dlq_eligible: bool
        +execute(**kwargs)
        +schedule(at_time, labels)
        +execution_locks list~BaseLock~
    }

    class PeriodicBaseTask {
        +default_schedule: TaskScheduleBase
        +_enabled: bool
    }

    class PeriodicVoAwareBaseTask {
        +vo: str
    }

    class RetryPolicyBase {
        <<abstract>>
        +schedule_retry(attempt, exception) datetime?
    }
    class NoRetry
    class ExponentialBackoff {
        +base_delay_seconds: int
        +max_retries: int
    }

    class TaskScheduleBase {
        <<abstract>>
        +next_occurrence() datetime
    }
    class IntervalSeconds {
        +seconds: int
    }
    class CronSchedule {
        +expression: str
    }
    class RRuleSchedule {
        +rule: str
    }

    BaseTask <|-- PeriodicBaseTask
    PeriodicBaseTask <|-- PeriodicVoAwareBaseTask
    RetryPolicyBase <|-- NoRetry
    RetryPolicyBase <|-- ExponentialBackoff
    TaskScheduleBase <|-- IntervalSeconds
    TaskScheduleBase <|-- CronSchedule
    TaskScheduleBase <|-- RRuleSchedule
```

Each task tier uses different locks:

| Class                     | Default `execution_locks`                            |
| ------------------------- | ---------------------------------------------------- |
| `BaseTask`                | `RateLimiter` + `ConcurrencyLimiter` (both disabled) |
| `PeriodicBaseTask`        | `MutexLock(TASK, class_name)`                        |
| `PeriodicVoAwareBaseTask` | `MutexLock(TASK, class_name, vo)`                    |

`PeriodicBaseTask` intentionally does **not** call `super().execution_locks` — it replaces the limiter pair with a single mutex. `PeriodicVoAwareBaseTask` adds the VO name to the mutex key so each VO gets its own lock.

## Broker and result backend

The broker models live in `plumbing/broker/models.py`, the stream broker in `plumbing/broker/redis_streams.py`, and the result backend in `plumbing/broker/result_backend.py`.

```mermaid
classDiagram
    class RedisStreamBroker {
        9 streams: 3 priorities × 3 sizes
        +enqueue(TaskMessage)
        +listen() AsyncGenerator~ReceivedMessage~
        +startup()
    }

    class TaskMessage {
        +task_id: str
        +task_name: str
        +labels: dict
        +task_args: list
        +task_kwargs: dict
        +dumpb() bytes
        +loadb(bytes) TaskMessage
    }

    class TaskResult~T~ {
        +is_err: bool
        +return_value: T
        +execution_time: float
        +error: dict | None
        +labels: dict
        +from_exception()
        +from_value()
    }

    class ReceivedMessage {
        +data: bytes
        +ack() awaitable
        +renew() awaitable
    }

    class TaskBinding {
        +broker: RedisStreamBroker
        +task_name: str
        +labels: dict
        +submit(*args, **kwargs)
    }

    class RedisResultBackend {
        +set_result(task_id, TaskResult)
        +get_result(task_id) TaskResult
    }

    RedisStreamBroker ..> TaskMessage : enqueues
    RedisStreamBroker ..> ReceivedMessage : yields
    RedisResultBackend ..> TaskResult : stores
    TaskBinding --> RedisStreamBroker : references
```

`TaskMessage` is the wire-protocol message serialized to msgpack. `ReceivedMessage` wraps the raw bytes with `ack()` and `renew()` callbacks: `ack()` acknowledges completion, while `renew()` refreshes ownership of in-flight messages during long executions. `TaskBinding` maps a task class to its broker, providing the `submit()` method used by `BaseTask.schedule()`.

## Callback subsystem

The callback module (`plumbing/callbacks.py`) implements fan-out/fan-in: a parent spawns N child tasks and a callback fires automatically when all children complete.

```mermaid
flowchart LR
    A["spawn_with_callback()"] -->|schedules N children| B["child tasks"]
    B -->|worker calls| C["on_child_complete()"]
    C -->|remaining == 0| D["fire_callback()"]
    D -->|submits callback task| E["broker"]
```

`spawn_with_callback` stores the callback and an atomic counter in Redis, then schedules each child with a `group_id` label. After each child completes, the worker calls `on_child_complete` which decrements the counter. When the counter reaches zero, `fire_callback` deserializes and submits the callback task to the broker.

Redis keys used per callback group:

| Key pattern                                        | Type   | Contents                         |
| -------------------------------------------------- | ------ | -------------------------------- |
| `diracx:groups:{group_id}:callback`                | string | msgpack-serialized callback task |
| `diracx:groups:{group_id}:remaining`               | string | atomic counter (int)             |
| `diracx:groups:{group_id}:results:{child_task_id}` | string | msgpack-serialized child result  |

All keys are created with a TTL (default 86400s) for automatic cleanup.
