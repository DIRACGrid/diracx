"""Tests for the result backend and per-task result TTLs."""

from __future__ import annotations

from typing import Any

import fakeredis
import fakeredis.aioredis
import pytest
from redis.asyncio import Redis

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.broker import RedisResultBackend, RedisStreamBroker
from diracx.tasks.plumbing.broker.models import TaskMessage, TaskResult
from diracx.tasks.plumbing.broker.result_backend import DEFAULT_RESULT_TTL
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.retry_policies import NoRetry
from diracx.tasks.plumbing.worker.worker import Worker


class DefaultTTLTask(BaseTask):
    """A task that keeps the result backend's default TTL."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, **kwargs: Any) -> str:
        return "default"


class ShortTTLTask(BaseTask):
    """A task whose result only needs to live for a minute."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()
    result_ttl_seconds = 60

    async def execute(self, **kwargs: Any) -> str:
        return "short"


class LongTTLFailingTask(BaseTask):
    """A failing task whose (error) result should be kept for a few days."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()
    result_ttl_seconds = 3 * 86400

    async def execute(self, **kwargs: Any) -> str:
        raise RuntimeError("Always fails")


TASKS: dict[str, type[BaseTask]] = {
    "test:DefaultTTLTask": DefaultTTLTask,
    "test:ShortTTLTask": ShortTTLTask,
    "test:LongTTLFailingTask": LongTTLFailingTask,
}


@pytest.fixture
async def result_backend():
    server = fakeredis.FakeServer()
    backend = RedisResultBackend(
        "redis://fake",
        connection_class=fakeredis.aioredis.FakeConnection,
        server=server,
    )
    broker = RedisStreamBroker(
        url="redis://fake",
        result_backend=backend,
        connection_class=fakeredis.aioredis.FakeConnection,
        server=server,
    )
    await broker.startup()
    yield broker, backend
    await broker.shutdown()


async def _stored_ttl(backend: RedisResultBackend, task_id: str) -> int:
    async with Redis(connection_pool=backend.redis_pool) as redis:
        return await redis.ttl(backend._task_key(task_id))


def _ok_result() -> TaskResult:
    return TaskResult(is_err=False, return_value="ok", execution_time=0.1)


# ---------------------------------------------------------------------------
# RedisResultBackend.set_result
# ---------------------------------------------------------------------------


async def test_set_result_uses_backend_default_ttl(result_backend):
    _, backend = result_backend
    await backend.set_result("r1", _ok_result())
    ttl = await _stored_ttl(backend, "r1")
    assert DEFAULT_RESULT_TTL - 5 <= ttl <= DEFAULT_RESULT_TTL
    assert (await backend.get_result("r1")).return_value == "ok"


async def test_set_result_uses_configured_backend_ttl():
    backend = RedisResultBackend(
        "redis://fake",
        result_ttl_seconds=600,
        connection_class=fakeredis.aioredis.FakeConnection,
        server=fakeredis.FakeServer(),
    )
    await backend.set_result("r1", _ok_result())
    assert 595 <= await _stored_ttl(backend, "r1") <= 600
    await backend.shutdown()


async def test_set_result_ttl_override(result_backend):
    _, backend = result_backend
    await backend.set_result("r1", _ok_result(), ttl_seconds=30)
    assert 25 <= await _stored_ttl(backend, "r1") <= 30
    assert (await backend.get_result("r1")).return_value == "ok"


@pytest.mark.parametrize("ttl", [0, -1])
async def test_set_result_rejects_non_positive_ttl(result_backend, ttl):
    _, backend = result_backend
    with pytest.raises(ValueError, match="must be positive"):
        await backend.set_result("r1", _ok_result(), ttl_seconds=ttl)
    assert not await backend.is_result_ready("r1")


# ---------------------------------------------------------------------------
# Worker honours BaseTask.result_ttl_seconds
# ---------------------------------------------------------------------------


async def _run(broker: RedisStreamBroker, task_name: str, task_id: str) -> None:
    worker = Worker(
        broker=broker,
        task_registry={name: wrap_task(cls) for name, cls in TASKS.items()},
        task_class_registry=TASKS,
    )
    msg = TaskMessage(
        task_id=task_id,
        task_name=task_name,
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )
    await worker.process_message(msg.dumpb())


def test_base_task_has_no_result_ttl_by_default():
    assert BaseTask.result_ttl_seconds is None
    assert DefaultTTLTask.result_ttl_seconds is None


async def test_worker_uses_default_ttl_when_task_does_not_set_one(result_backend):
    broker, backend = result_backend
    await _run(broker, "test:DefaultTTLTask", "t-default")
    result = await backend.get_result("t-default")
    assert result.return_value == "default"
    ttl = await _stored_ttl(backend, "t-default")
    assert DEFAULT_RESULT_TTL - 5 <= ttl <= DEFAULT_RESULT_TTL


async def test_worker_uses_task_result_ttl(result_backend):
    broker, backend = result_backend
    await _run(broker, "test:ShortTTLTask", "t-short")
    result = await backend.get_result("t-short")
    assert result.return_value == "short"
    assert 55 <= await _stored_ttl(backend, "t-short") <= 60


async def test_worker_uses_task_result_ttl_for_failed_tasks(result_backend):
    broker, backend = result_backend
    await _run(broker, "test:LongTTLFailingTask", "t-fail")
    result = await backend.get_result("t-fail")
    assert result.is_err
    assert 3 * 86400 - 5 <= await _stored_ttl(backend, "t-fail") <= 3 * 86400
