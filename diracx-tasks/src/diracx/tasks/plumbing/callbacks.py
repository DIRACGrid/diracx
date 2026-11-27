"""Callback groups: run a task once a set of child tasks has completed."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any

import msgpack

from ._redis_types import CallbackRegistry
from .base_task import BaseTask

if TYPE_CHECKING:
    from .broker.redis_streams import RedisStreamBroker

logger = logging.getLogger(__name__)


def _group_key(group_id: str) -> str:
    return f"diracx:groups:{group_id}"


async def spawn_with_callback(
    children: list[BaseTask],
    callback: BaseTask,
    *,
    redis: CallbackRegistry,
    ttl_seconds: int = 86400,
) -> str:
    """Schedule child tasks and a callback that fires when all children complete.

    Redis data structures:
      - ``diracx:groups:{group_id}:callback`` — serialized callback task
      - ``diracx:groups:{group_id}:remaining`` — atomic counter
      - ``diracx:groups:{group_id}:results:{child_task_id}`` — per-child results

    All keys get a TTL for automatic cleanup.

    Args:
        children: Tasks to schedule.
        callback: Task scheduled once all children have completed.
        redis: Redis client used to store the group state.
        ttl_seconds: Time to live of the group keys, in seconds.

    Returns:
        The group_id for this callback group.

    Raises:
        RuntimeError: If tasks are not bound to a broker.
        ValueError: If the callback task is not in the broker registry.
    """
    group_id = uuid.uuid4().hex

    # Look up the callback's registered task name from the broker registry
    registry = BaseTask._broker_registry.get()
    if registry is None:
        raise RuntimeError("Tasks must be bound to a broker before spawning callbacks")
    decorated = registry.get(type(callback))
    if decorated is None:
        raise ValueError(
            f"Callback task {type(callback).__name__} not found in broker registry"
        )
    task_name = decorated.task_name

    # Store callback task
    callback_data = msgpack.packb(
        {
            "task_class": task_name,
            "args": list(callback.serialize()),
        },
        datetime=True,
    )
    pipe = redis.pipeline()
    pipe.set(f"{_group_key(group_id)}:callback", callback_data, ex=ttl_seconds)
    pipe.set(f"{_group_key(group_id)}:remaining", len(children), ex=ttl_seconds)
    await pipe.execute()

    # Schedule each child with the group_id label so the worker can
    # detect them as belonging to this callback group on completion.
    await asyncio.gather(
        *(child.schedule(labels={"group_id": group_id}) for child in children)
    )

    logger.info(
        "Spawned %d children with callback (group_id=%s)", len(children), group_id
    )
    return group_id


async def on_child_complete(
    redis: CallbackRegistry,
    group_id: str,
    child_task_id: str,
    result: Any,
    *,
    ttl_seconds: int = 86400,
) -> bool:
    """Record a child's completion.

    Called by the worker after a child task (one with ``group_id`` in labels)
    completes.

    Args:
        redis: Redis client holding the group state.
        group_id: ID of the callback group.
        child_task_id: ID of the completed child task.
        result: Return value of the child task.
        ttl_seconds: Time to live of the stored result, in seconds.

    Returns:
        True if all children have completed and the callback should fire.
    """
    # Store child result
    result_data = msgpack.packb(result, datetime=True)
    await redis.set(
        f"{_group_key(group_id)}:results:{child_task_id}",
        result_data,
        ex=ttl_seconds,
    )

    # Atomically decrement remaining counter
    remaining = await redis.decr(f"{_group_key(group_id)}:remaining")
    return remaining <= 0


async def fire_callback(
    redis: CallbackRegistry,
    group_id: str,
    broker: RedisStreamBroker,
) -> None:
    """Deserialize and schedule the callback task for a completed group.

    Called by the worker when ``on_child_complete`` returns True.
    Reads the callback data from Redis and submits the task to the
    broker for execution.

    Args:
        redis: Redis client holding the group state.
        group_id: ID of the callback group.
        broker: Broker the callback task is submitted to.
    """
    from .broker.models import submit_task

    callback_data = await redis.get(f"{_group_key(group_id)}:callback")
    if callback_data is None:
        logger.warning("No callback data found for group %s", group_id)
        return

    payload = msgpack.unpackb(callback_data, timestamp=3)
    task_class_path: str = payload["task_class"]
    task_args: list[Any] = payload["args"]

    await submit_task(
        broker=broker,
        task_name=task_class_path,
        task_args=task_args,
        labels={"callback_group_id": group_id},
    )

    logger.info("Fired callback for group %s (task: %s)", group_id, task_class_path)
