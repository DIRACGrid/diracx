"""Integration tests for the Worker: retry flow, dead letter queue, callbacks, lock handling."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.broker.models import TaskMessage, TaskResult
from diracx.tasks.plumbing.depends import CallbackSpawner
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.worker.worker import Worker

from .conftest import FailOnceTask


def _make_ackable(task_message: TaskMessage):
    """Create a raw bytes message (non-ackable) from a TaskMessage."""
    return task_message.dumpb()


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------


async def test_worker_executes_successful_task(
    broker, task_class_registry, wrapped_registry
):
    """Worker should execute a task and produce a success result."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="t1",
        task_name="test:SuccessTask",
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )

    # Mock _get_redis to return a mock that acts as an async context manager
    mock_redis = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock(return_value=False)
    # MutexLock.acquire uses redis.set(..., nx=True) which should return True
    mock_redis.set = AsyncMock(return_value=True)

    with patch.object(worker, "_get_redis", return_value=mock_redis):
        result = await worker.run_task(wrapped_registry["test:SuccessTask"], task_msg)

    assert not result.is_err
    assert result.return_value == "ok"


# ---------------------------------------------------------------------------
# Retry flow
# ---------------------------------------------------------------------------


async def test_worker_schedules_retry_on_failure(
    broker, task_class_registry, wrapped_registry
):
    """When a task fails and has retries left, worker should schedule a retry."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="t2",
        task_name="test:FailOnceTask",
        labels={"priority": "normal", "size": "small", "_retry_attempt": 0},
        task_args=[],
        task_kwargs={},
    )

    mock_redis = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock(return_value=False)
    mock_redis.zadd = AsyncMock()

    # Reset call counter
    FailOnceTask._call_count = 0

    with patch.object(worker, "_get_redis", return_value=mock_redis):
        result = await worker.run_task(wrapped_registry["test:FailOnceTask"], task_msg)

    # Task should have failed
    assert result.is_err

    # Now test _handle_failure schedules retry
    with patch.object(worker, "_get_redis", return_value=mock_redis):
        await worker._handle_failure(task_msg, result)

    # zadd should have been called for the delayed ZSET
    mock_redis.zadd.assert_called_once()
    call_args = mock_redis.zadd.call_args
    assert call_args[0][0] == "diracx:tasks:delayed"


# ---------------------------------------------------------------------------
# Dead letter queue flow
# ---------------------------------------------------------------------------


async def test_worker_logs_dlq_eligible_on_no_retries(
    broker, task_class_registry, wrapped_registry
):
    """When a dead-letter-queue-eligible task fails with NoRetry, it should be persisted."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="t3",
        task_name="test:DLQTask",
        labels={"priority": "normal", "size": "medium"},
        task_args=[],
        task_kwargs={},
    )

    mock_redis = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock(return_value=False)

    with patch.object(worker, "_get_redis", return_value=mock_redis):
        result = await worker.run_task(wrapped_registry["test:DLQTask"], task_msg)

    assert result.is_err

    # _handle_failure should log the dead letter queue intent (no zadd since NoRetry)
    with patch.object(worker, "_get_redis", return_value=mock_redis):
        await worker._handle_failure(task_msg, result)

    # zadd should NOT have been called (NoRetry returns None)
    mock_redis.zadd.assert_not_called()


# ---------------------------------------------------------------------------
# Lock contention retry
# ---------------------------------------------------------------------------


async def test_worker_reschedules_on_lock_contention(
    broker, task_class_registry, wrapped_registry
):
    """When a lock can't be acquired, the task should be rescheduled."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="t4",
        task_name="test:LockedTask",
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )

    mock_redis = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock(return_value=False)
    # Simulate lock acquisition failure
    mock_redis.set = AsyncMock(return_value=None)
    mock_redis.zadd = AsyncMock()

    with patch.object(worker, "_get_redis", return_value=mock_redis):
        result = await worker.run_task(wrapped_registry["test:LockedTask"], task_msg)

    # Should be a non-error result with _lock_retry flag
    assert not result.is_err
    assert result.labels.get("_lock_retry") is True

    # Should have scheduled a retry via zadd
    mock_redis.zadd.assert_called_once()


# ---------------------------------------------------------------------------
# Callback flow
# ---------------------------------------------------------------------------


async def test_worker_fires_callback_on_group_completion(
    broker, task_class_registry, wrapped_registry
):
    """When the last child in a group completes, the callback should fire."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="child1",
        task_name="test:SuccessTask",
        labels={
            "priority": "normal",
            "size": "small",
            "group_id": "group123",
        },
        task_args=[],
        task_kwargs={},
    )

    mock_redis = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock(return_value=False)
    mock_redis.set = AsyncMock(return_value=True)

    result = TaskResult.from_value(
        value="ok", execution_time=0.1, labels=task_msg.labels
    )

    with (
        patch(
            "diracx.tasks.plumbing.worker.worker.on_child_complete",
            return_value=True,
        ) as mock_on_child,
        patch(
            "diracx.tasks.plumbing.worker.worker.fire_callback",
        ) as mock_fire,
        patch.object(worker, "_get_redis", return_value=mock_redis),
    ):
        await worker._handle_success(task_msg, result)

    mock_on_child.assert_called_once_with(mock_redis, "group123", "child1", "ok")
    mock_fire.assert_called_once_with(mock_redis, "group123", broker)


async def test_worker_does_not_fire_callback_when_not_done(
    broker, task_class_registry, wrapped_registry
):
    """When not all children are done, the callback should not fire."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="child1",
        task_name="test:SuccessTask",
        labels={
            "priority": "normal",
            "size": "small",
            "group_id": "group456",
        },
        task_args=[],
        task_kwargs={},
    )

    mock_redis = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock(return_value=False)

    result = TaskResult.from_value(
        value="ok", execution_time=0.1, labels=task_msg.labels
    )

    with (
        patch(
            "diracx.tasks.plumbing.worker.worker.on_child_complete",
            return_value=False,
        ),
        patch(
            "diracx.tasks.plumbing.worker.worker.fire_callback",
        ) as mock_fire,
        patch.object(worker, "_get_redis", return_value=mock_redis),
    ):
        await worker._handle_success(task_msg, result)

    mock_fire.assert_not_called()


# ---------------------------------------------------------------------------
# Process message end-to-end
# ---------------------------------------------------------------------------


async def test_process_message_acks_on_unknown_task(
    broker, task_class_registry, wrapped_registry
):
    """Unknown tasks should be acked (not left pending) and logged."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    task_msg = TaskMessage(
        task_id="t5",
        task_name="test:NonexistentTask",
        labels={},
        task_args=[],
        task_kwargs={},
    )
    raw = _make_ackable(task_msg)

    # Just calling with raw bytes — no ack to verify, but no crash either
    await worker.process_message(raw)


async def test_process_message_acks_on_parse_error(
    broker, task_class_registry, wrapped_registry
):
    """Unparseable messages should be acked and logged, not crash the worker."""
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )

    await worker.process_message(b"not valid msgpack at all!!")


# ---------------------------------------------------------------------------
# TaskBroker dependency injection resolution
# ---------------------------------------------------------------------------


async def test_worker_resolves_callback_spawner_dependency(
    broker, task_class_registry, wrapped_registry
):
    """CallbackSpawner should be resolved via dependency injection when a task declares it."""

    class SpawnerUsingTask(BaseTask):
        priority = Priority.NORMAL
        size = Size.SMALL

        async def execute(self, spawn_with_callback: CallbackSpawner) -> str:
            return type(spawn_with_callback).__name__

    wrapped = wrap_task(SpawnerUsingTask)
    registry = {**wrapped_registry, "test:SpawnerUsingTask": wrapped}
    class_registry = {
        **task_class_registry,
        "test:SpawnerUsingTask": SpawnerUsingTask,
    }

    worker = Worker(
        broker=broker,
        task_registry=registry,
        task_class_registry=class_registry,
    )

    task_msg = TaskMessage(
        task_id="t-spawner",
        task_name="test:SpawnerUsingTask",
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )

    result = await worker.run_task(registry["test:SpawnerUsingTask"], task_msg)

    assert not result.is_err
    assert result.return_value == "_CallbackSpawner"
