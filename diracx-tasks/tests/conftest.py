"""Shared fixtures for diracx-tasks integration tests."""

from __future__ import annotations

import dataclasses
from typing import Any

import fakeredis
import fakeredis.aioredis
import pytest
from redis.asyncio import Redis

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.broker.models import TaskMessage
from diracx.tasks.plumbing.broker.redis_streams import (
    ALL_STREAM_NAMES,
    RedisStreamBroker,
)
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.lock_registry import TASK
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff, NoRetry

# ---------------------------------------------------------------------------
# Test task definitions
# ---------------------------------------------------------------------------


class SuccessTask(BaseTask):
    """A task that always succeeds."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


@dataclasses.dataclass
class FailOnceTask(BaseTask):
    """A task that fails on the first attempt, succeeds on retries."""

    call_count: int = 0
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = ExponentialBackoff(base_delay_seconds=1, max_retries=3)

    async def execute(self, **kwargs: Any) -> str:
        # Use a class-level counter to track calls across instances
        FailOnceTask._call_count = getattr(FailOnceTask, "_call_count", 0) + 1
        if FailOnceTask._call_count <= 1:
            raise RuntimeError("Simulated failure")
        return "recovered"


class DLQTask(BaseTask):
    """A task that always fails and is dead-letter-queue-eligible."""

    priority = Priority.NORMAL
    size = Size.MEDIUM
    retry_policy = NoRetry()
    dlq_eligible = True

    async def execute(self, **kwargs: Any) -> str:
        raise RuntimeError("Always fails")


class LockedTask(BaseTask):
    """A task with a mutex lock."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(TASK, "LockedTask")]

    async def execute(self, **kwargs: Any) -> str:
        return "locked_ok"


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


async def get_enqueued_messages(broker: RedisStreamBroker) -> list[TaskMessage]:
    """Read all messages from all broker streams."""
    messages = []
    async with Redis(connection_pool=broker.connection_pool) as redis:
        for stream in ALL_STREAM_NAMES:
            entries = await redis.xrange(stream)
            for _, fields in entries:
                messages.append(TaskMessage.loadb(fields[b"data"]))
    return messages


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def broker():
    server = fakeredis.FakeServer()
    b = RedisStreamBroker(
        url="redis://fake",
        connection_class=fakeredis.aioredis.FakeConnection,
        server=server,
    )
    await b.startup()
    yield b
    await b.shutdown()


@pytest.fixture
def task_class_registry():
    return {
        "test:SuccessTask": SuccessTask,
        "test:FailOnceTask": FailOnceTask,
        "test:DLQTask": DLQTask,
        "test:LockedTask": LockedTask,
    }


@pytest.fixture
def wrapped_registry(task_class_registry):
    return {name: wrap_task(cls) for name, cls in task_class_registry.items()}
