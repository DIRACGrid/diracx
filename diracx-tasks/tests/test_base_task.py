"""Tests for BaseTask, PeriodicBaseTask, and serialization."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from diracx.tasks.plumbing.base_task import BaseTask, PeriodicBaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import TASK
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.schedules import IntervalSeconds


class SimpleTask(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(TASK, "SimpleTask")]

    async def execute(self, **kwargs: Any) -> str:
        return "done"

    def serialize(self) -> tuple[Any, ...]:
        return ()


@dataclasses.dataclass
class DataclassTask(BaseTask):
    transformation_id: int
    priority = Priority.NORMAL
    size = Size.MEDIUM

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(TASK, "DataclassTask", self.transformation_id)]

    async def execute(self, **kwargs: Any) -> int:
        return self.transformation_id


class MyPeriodicTask(PeriodicBaseTask):
    default_schedule = IntervalSeconds(300)

    async def execute(self, **kwargs: Any) -> str:
        return "periodic"


def test_simple_task_serialize():
    task = SimpleTask()
    assert task.serialize() == ()


def test_dataclass_task_serialize():
    task = DataclassTask(transformation_id=42)
    assert task.serialize() == (42,)


def test_dataclass_task_locks():
    task = DataclassTask(transformation_id=99)
    locks = task.execution_locks
    assert len(locks) == 1
    assert locks[0].redis_key == "lock:mutex:task:DataclassTask:99"


def test_periodic_task_default_locks():
    task = MyPeriodicTask()
    locks = task.execution_locks
    assert len(locks) == 1
    assert "lock:mutex:task:MyPeriodicTask" in locks[0].redis_key


async def test_simple_task_execute():
    task = SimpleTask()
    result = await task.execute()
    assert result == "done"


async def test_dataclass_task_execute():
    task = DataclassTask(transformation_id=7)
    result = await task.execute()
    assert result == 7


def test_schedule_without_broker():
    task = SimpleTask()
    with pytest.raises(RuntimeError, match="not bound to a broker"):
        import asyncio

        asyncio.run(task.schedule())
