"""Tests for running tasks from the command line."""

from __future__ import annotations

from typing import Any

import fakeredis
import fakeredis.aioredis

from diracx.tasks.plumbing import broker as broker_module
from diracx.tasks.plumbing import factory
from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.broker.redis_streams import RedisStreamBroker
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.retry_policies import NoRetry
from diracx.tasks.task_run import DebugOptions, call_task

from .conftest import SuccessTask, get_enqueued_messages


class SchedulingTask(BaseTask):
    """A task that schedules another one, as a monitor task does."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, **kwargs: Any) -> str:
        await SuccessTask().schedule()
        return "scheduled"


async def test_call_task_binds_the_broker(monkeypatch, capsys):
    """A task run with ``diracx-tasks call`` can schedule other tasks."""
    server = fakeredis.FakeServer()

    def make_broker(url: str) -> RedisStreamBroker:
        return RedisStreamBroker(
            url="redis://fake",
            connection_class=fakeredis.aioredis.FakeAsyncRedisConnection,
            server=server,
        )

    registry = {"test:SchedulingTask": SchedulingTask, "test:SuccessTask": SuccessTask}
    monkeypatch.setattr(factory, "load_task_registry", lambda: registry)
    monkeypatch.setattr(broker_module, "RedisStreamBroker", make_broker)

    await call_task("test:SchedulingTask", [], {}, DebugOptions.NONE)

    assert "Result: scheduled" in capsys.readouterr().out
    # The scheduled task was submitted for the workers
    reader = make_broker("redis://fake")
    await reader.startup()
    try:
        [message] = await get_enqueued_messages(reader)
    finally:
        await reader.shutdown()
    assert message.task_name == "test:SuccessTask"
