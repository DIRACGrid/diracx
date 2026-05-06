from __future__ import annotations

__all__ = ["Worker"]

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from time import time
from typing import Any, Awaitable, Callable

import msgpack
from opentelemetry import metrics, trace
from redis.asyncio import Redis

from .._redis_types import CallbackRegistry, LockCoordinator
from ..base_task import BaseTask
from ..broker.models import ReceivedMessage, TaskMessage, TaskResult
from ..broker.redis_streams import RedisStreamBroker
from ..callbacks import fire_callback, on_child_complete
from ..exceptions import UnableToAcquireLockError
from ..persistence.dlq import TaskDB
from ..scheduler.scheduler import schedule_delayed
from .di_resolver import solve_task_dependencies

logger = logging.getLogger(__name__)

_tracer = trace.get_tracer(__name__)
_meter = metrics.get_meter(__name__)
_tasks_completed = _meter.create_counter(
    "tasks_completed_total",
    description="Total number of tasks completed successfully",
)
_tasks_failed = _meter.create_counter(
    "tasks_failed_total",
    description="Total number of tasks that failed",
)
_task_duration = _meter.create_histogram(
    "task_duration_seconds",
    description="Duration of task execution in seconds",
    unit="s",
)

# Sentinel value to signal queue completion
QUEUE_DONE = b"-1"

# Default backoff for lock contention retries
_LOCK_RETRY_DELAY_SECONDS = 5


async def _message_heartbeat(
    renew: Callable[[], Awaitable[None]],
    stop_event: asyncio.Event,
    interval: float,
) -> None:
    """Periodically reset the PEL idle timer while a message is being processed.

    Calls ``renew()`` at ``interval`` seconds so the autoclaim loop never
    considers the message stale as long as execution is in progress.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            pass
        try:
            await renew()
        except Exception:
            logger.warning("Failed to renew message ownership", exc_info=True)


class Worker:
    """Execute tasks consumed from a broker.

    The worker uses a two-loop architecture:

    **prefetcher** — reads messages from the broker (Redis streams) and
    places them into an internal ``asyncio.Queue``.  When ``max_prefetch``
    is set, a semaphore limits how far ahead the prefetcher can read,
    providing backpressure so we don't buffer unbounded messages in memory.

    **runner** — pulls messages from the queue, resolves FastAPI-style
    dependencies via ``solve_task_dependencies``, executes the task
    function, persists the result to the result backend, and finally
    acknowledges the message.  ``max_concurrent_tasks`` controls how
    many tasks execute in parallel (also semaphore-gated).

    On failure, the worker consults the task's ``retry_policy`` to decide
    whether to reschedule via the delayed ZSET or persist to the dead letter queue.

    Flow::

        Broker  ──▸  prefetcher  ──▸  queue  ──▸  runner  ──▸  task_func
                     (sem_prefetch)               (sem)

    Shutdown is cooperative: setting ``finish_event`` causes the
    prefetcher to drain, send ``QUEUE_DONE`` to the runner, and the
    runner waits for in-flight tasks before exiting.
    """

    def __init__(
        self,
        broker: RedisStreamBroker,
        task_registry: dict[str, Callable[..., Any]],
        task_class_registry: dict[str, type[BaseTask]],
        max_concurrent_tasks: int = 10,
        max_prefetch: int = 0,
        task_db: TaskDB | None = None,
    ) -> None:
        self.broker = broker
        self.task_registry = task_registry
        self.task_class_registry = task_class_registry
        self.task_db = task_db

        # Register CallbackSpawner dependency injection override
        from ..depends import _callback_spawner_placeholder, _CallbackSpawner

        pool = broker.connection_pool

        async def _create_callback_spawner() -> _CallbackSpawner:
            redis: CallbackRegistry = Redis(connection_pool=pool)
            return _CallbackSpawner(redis)

        broker.dependency_overrides[_callback_spawner_placeholder] = (
            _create_callback_spawner
        )

        self.sem: asyncio.Semaphore | None = None
        if max_concurrent_tasks > 0:
            self.sem = asyncio.Semaphore(max_concurrent_tasks)

        self.sem_prefetch: asyncio.Semaphore | None = None
        if max_prefetch > 0:
            self.sem_prefetch = asyncio.Semaphore(max_prefetch)

    async def listen(self, finish_event: asyncio.Event) -> None:
        """Start the prefetcher and runner tasks."""
        await self.broker.startup()

        logger.info("Worker started listening for tasks")

        queue: asyncio.Queue[bytes | ReceivedMessage] = asyncio.Queue()

        prefetcher_task = asyncio.create_task(self.prefetcher(queue, finish_event))
        runner_task = asyncio.create_task(self.runner(queue))

        await asyncio.gather(prefetcher_task, runner_task)

        logger.info("Worker shutting down")

    async def prefetcher(
        self,
        queue: asyncio.Queue[bytes | ReceivedMessage],
        finish_event: asyncio.Event,
    ) -> None:
        """Prefetch messages from broker into the internal queue.

        This is a backpressure-controlled pump between the broker
        (Redis streams) and the runner.  We wrap broker.__anext__()
        in a task so we can poll it with a timeout — otherwise we'd
        block forever and never notice finish_event.
        """
        iterator = self.broker.listen()
        # Kick off the first read from the broker as a background task
        current_message_task = asyncio.create_task(iterator.__anext__())  # type: ignore[arg-type]

        while True:
            if finish_event.is_set():
                break

            try:
                # Block until the runner has capacity for another message.
                # The runner releases this semaphore when it picks a message
                # off the queue, so this is how we apply backpressure — if
                # the runner is saturated we stop pulling from Redis.
                if self.sem_prefetch is not None:
                    await self.sem_prefetch.acquire()

                # Poll the in-flight Redis read with a short timeout so we
                # can loop back and re-check finish_event if nothing arrived.
                done, _ = await asyncio.wait({current_message_task}, timeout=0.3)

                if not done:
                    # No message yet — give back the slot and try again
                    if self.sem_prefetch is not None:
                        self.sem_prefetch.release()
                    continue

                # A message arrived — grab it and immediately start the
                # next read so Redis I/O overlaps with queue insertion.
                message = current_message_task.result()
                current_message_task = asyncio.create_task(iterator.__anext__())  # type: ignore[arg-type]

                await queue.put(message)

            except (asyncio.CancelledError, StopAsyncIteration):
                break

        # Shutting down: cancel outstanding read, tell the runner we're
        # done, and release the semaphore so the runner isn't stuck.
        logger.info("Prefetcher stopping")
        current_message_task.cancel()
        await queue.put(QUEUE_DONE)  # type: ignore[arg-type]
        if self.sem_prefetch is not None:
            self.sem_prefetch.release()

    async def runner(
        self,
        queue: asyncio.Queue[bytes | ReceivedMessage],
    ) -> None:
        """Pull messages from the queue and execute them concurrently.

        Each message is dispatched to ``process_message`` in its own
        ``asyncio.Task``.  The concurrency semaphore (``self.sem``)
        limits how many tasks run at once; the prefetch semaphore
        (``self.sem_prefetch``) is released here to signal the
        prefetcher that another slot is available.
        """
        tasks: set[asyncio.Task[Any]] = set()

        def task_done_callback(task: asyncio.Task[Any]) -> None:
            tasks.discard(task)
            if self.sem is not None:
                self.sem.release()

        while True:
            try:
                if self.sem is not None:
                    await self.sem.acquire()

                if self.sem_prefetch is not None:
                    self.sem_prefetch.release()

                message = await queue.get()

                if message is QUEUE_DONE:
                    if tasks:
                        logger.info("Waiting for %d running tasks...", len(tasks))
                        await asyncio.wait(tasks)
                    break

                task = asyncio.create_task(self.process_message(message))
                tasks.add(task)
                task.add_done_callback(task_done_callback)

            except asyncio.CancelledError:
                break

        logger.info("Runner stopped")

    async def _get_redis(self) -> LockCoordinator:
        """Get a Redis connection from the broker's connection pool."""
        return Redis(connection_pool=self.broker.connection_pool)

    async def process_message(self, message: bytes | ReceivedMessage) -> None:
        """Deserialize, look up, execute, and ack a single broker message.

        After execution, handles retry scheduling or dead letter queue persistence for
        failed tasks, and fires callbacks for group-member tasks.
        """
        message_data = message.data if isinstance(message, ReceivedMessage) else message

        try:
            task_message = TaskMessage.loadb(message_data)
        except Exception:
            logger.warning(
                "Cannot parse message (first 200 bytes: %s), skipping",
                message_data[:200].hex(),
                exc_info=True,
            )
            if isinstance(message, ReceivedMessage):
                await message.ack()
            return

        task_func = self.task_registry.get(task_message.task_name)
        if task_func is None:
            logger.warning("Task %r not found in registry", task_message.task_name)
            if isinstance(message, ReceivedMessage):
                await message.ack()
            return

        logger.info(
            "Executing task %s (ID: %s)", task_message.task_name, task_message.task_id
        )

        # Start heartbeat to keep PEL ownership while the task runs.
        # Renew at half the idle_timeout so there's always a safety margin.
        heartbeat_stop = asyncio.Event()
        heartbeat_interval = self.broker.idle_timeout / 1000 / 2
        heartbeat_task: asyncio.Task[None] | None = None
        if isinstance(message, ReceivedMessage):
            heartbeat_task = asyncio.create_task(
                _message_heartbeat(message.renew, heartbeat_stop, heartbeat_interval)
            )

        try:
            result = await self.run_task(task_func, task_message)

            # Handle failure: retry or dead letter queue
            if result.is_err:
                await self._handle_failure(task_message, result)
            else:
                await self._handle_success(task_message, result)

            # Always persist the result to the backend
            try:
                if self.broker.result_backend:
                    await self.broker.result_backend.set_result(
                        task_message.task_id, result
                    )
            except Exception:
                logger.exception("Failed to save result")

        finally:
            if heartbeat_task is not None:
                heartbeat_stop.set()
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

        if isinstance(message, ReceivedMessage):
            await message.ack()

    async def _handle_failure(
        self,
        task_message: TaskMessage,
        result: TaskResult[Any],
    ) -> None:
        """Consult retry policy and either reschedule or send to dead letter queue."""
        task_cls = self.task_class_registry.get(task_message.task_name)
        if task_cls is None:
            logger.warning(
                "Task class %r not in class registry, cannot retry",
                task_message.task_name,
            )
            return

        attempt = task_message.labels.get("_retry_attempt", 0)
        error_info = result.error or {}
        error_type = error_info.get("type", "Exception")
        error_msg = error_info.get("message", "")

        # Reconstruct exception for the retry policy
        exc = Exception(f"[{error_type}] {error_msg}")

        retry_at = task_cls.retry_policy.schedule_retry(attempt + 1, exc)

        if retry_at is not None:
            await self._schedule_retry(task_message, retry_at, attempt + 1)
        elif task_cls.dlq_eligible:
            await self._send_to_dlq(task_message, task_cls, error_msg)
        else:
            logger.warning(
                "Task %s (ID: %s) failed after %d attempts, discarding",
                task_message.task_name,
                task_message.task_id,
                attempt + 1,
            )

    async def _schedule_retry(
        self,
        task_message: TaskMessage,
        retry_at: datetime,
        attempt: int,
    ) -> None:
        """Reschedule a failed task via the delayed ZSET."""
        # Build a new TaskMessage with incremented retry attempt
        retry_labels = {**task_message.labels, "_retry_attempt": attempt}
        retry_task_message = TaskMessage(
            task_id=self.broker.id_generator(),
            task_name=task_message.task_name,
            labels=retry_labels,
            task_args=task_message.task_args,
            task_kwargs=task_message.task_kwargs,
        )

        try:
            redis = await self._get_redis()
            async with redis:
                await schedule_delayed(redis, retry_task_message, retry_at)
            logger.info(
                "Scheduled retry %d for task %s at %s",
                attempt,
                task_message.task_name,
                retry_at.isoformat(),
            )
        except Exception:
            logger.exception(
                "Failed to schedule retry for task %s", task_message.task_name
            )

    async def _send_to_dlq(
        self,
        task_message: TaskMessage,
        task_cls: type[BaseTask],
        error_msg: str,
    ) -> None:
        """Persist a permanently failed task to the Dead Letter Queue."""
        if self.task_db is None:
            logger.warning(
                "Task %s (ID: %s) exhausted retries, dead-letter-queue-eligible but no TaskDB "
                "configured. Error: %s",
                task_message.task_name,
                task_message.task_id,
                error_msg,
            )
            return

        task_args = msgpack.packb(
            {
                "task_args": task_message.task_args,
                "task_kwargs": task_message.task_kwargs,
                "labels": task_message.labels,
            },
            datetime=True,
        )
        max_retries = getattr(task_cls.retry_policy, "max_retries", 0)

        try:
            async with self.task_db:
                dlq_id = await self.task_db.insert_dlq_task(
                    task_class=task_message.task_name,
                    task_args=task_args,
                    max_retries=max_retries,
                )
            logger.info(
                "Task %s (ID: %s) persisted to dead letter queue (dlq_id=%d). Error: %s",
                task_message.task_name,
                task_message.task_id,
                dlq_id,
                error_msg,
            )
        except Exception:
            logger.exception(
                "Failed to persist task %s to dead letter queue", task_message.task_name
            )

    async def _handle_success(
        self,
        task_message: TaskMessage,
        result: TaskResult[Any],
    ) -> None:
        """Fire callbacks for group-member tasks on success."""
        group_id = task_message.labels.get("group_id")
        if not group_id:
            return

        try:
            redis = await self._get_redis()
            async with redis:
                all_done = await on_child_complete(
                    redis,
                    group_id,
                    task_message.task_id,
                    result.return_value,
                )
                if all_done:
                    await fire_callback(redis, group_id, self.broker)
        except Exception:
            logger.exception("Failed to process callback for group %s", group_id)

    async def _handle_lock_retry(
        self,
        task_message: TaskMessage,
    ) -> None:
        """Reschedule a task that couldn't acquire a lock."""
        retry_at = datetime.now(tz=UTC) + timedelta(seconds=_LOCK_RETRY_DELAY_SECONDS)
        attempt = task_message.labels.get("_retry_attempt", 0)
        await self._schedule_retry(task_message, retry_at, attempt)

    async def run_task(
        self,
        task_func: Callable[..., Any],
        task_message: TaskMessage,
    ) -> TaskResult[Any]:
        """Execute a task function with dependency resolution.

        Resolves FastAPI-style dependencies (declared via ``Depends``)
        through ``solve_task_dependencies``, merges them with the
        kwargs from the message, and calls the task.  Returns a
        ``TaskResult`` wrapping either the return value or the
        exception.

        Passes a Redis connection as ``_redis`` to ``task_wrapper`` for
        lock acquisition.  Task-level callback spawning is available via
        the ``CallbackSpawner`` dependency type.
        """
        retry_count = task_message.labels.get("_retry_attempt", 0)
        span_attrs = {
            "task.name": task_message.task_name,
            "task.id": task_message.task_id,
            "task.retry_count": retry_count,
        }
        # Add priority/size if present in labels
        if "priority" in task_message.labels:
            span_attrs["task.priority"] = str(task_message.labels["priority"])
        if "size" in task_message.labels:
            span_attrs["task.size"] = str(task_message.labels["size"])

        with _tracer.start_as_current_span(
            f"task.execute {task_message.task_name}",
            attributes=span_attrs,
        ) as span:
            result = await self._execute_task(task_func, task_message)

            # Record metrics
            attrs = {"task_name": task_message.task_name}
            _task_duration.record(result.execution_time, attributes=attrs)
            if result.is_err:
                _tasks_failed.add(1, attributes=attrs)
                span.set_attribute("task.status", "error")
                if result.error:
                    span.set_attribute("task.error", result.error.get("message", ""))
            else:
                _tasks_completed.add(1, attributes=attrs)
                span.set_attribute("task.status", "ok")
            span.set_attribute("task.duration_ms", result.execution_time * 1000)

        return result

    async def _execute_task(
        self,
        task_func: Callable[..., Any],
        task_message: TaskMessage,
    ) -> TaskResult[Any]:
        """Inner task execution logic."""
        start_time = time()
        returned = None
        found_exception: BaseException | None = None

        dep_kwargs: dict[str, Any] = {}
        async_exit_stack = None

        try:
            if hasattr(task_func, "_dependant"):
                dep_kwargs, async_exit_stack = await solve_task_dependencies(
                    call=task_func,
                    dependency_overrides=self.broker.dependency_overrides,
                )

            # Obtain a Redis connection for lock acquisition
            redis = await self._get_redis()
            async with redis:
                all_kwargs = {
                    **dep_kwargs,
                    **task_message.task_kwargs,
                }
                returned = await task_func(
                    *task_message.task_args, _redis=redis, **all_kwargs
                )

        except UnableToAcquireLockError:
            logger.info(
                "Lock contention for task %s, rescheduling",
                task_message.task_name,
            )
            await self._handle_lock_retry(task_message)
            # Return a non-error result so process_message doesn't
            # double-handle this as a failure
            return TaskResult.from_value(
                value=None,
                execution_time=time() - start_time,
                labels={**task_message.labels, "_lock_retry": True},
            )

        except BaseException as exc:
            found_exception = exc
            logger.error(
                "Exception in task %s: %s",
                task_message.task_name,
                exc,
                exc_info=True,
            )

        finally:
            if async_exit_stack:
                try:
                    await async_exit_stack.aclose()
                except Exception:
                    logger.debug("Error closing exit stack", exc_info=True)

        execution_time = time() - start_time

        if found_exception is not None:
            return TaskResult.from_exception(
                exc=found_exception,
                execution_time=execution_time,
                labels=task_message.labels,
            )
        return TaskResult.from_value(
            value=returned,
            execution_time=execution_time,
            labels=task_message.labels,
        )
