"""Tests for the task factory (wrapping, registry)."""

from __future__ import annotations

import dataclasses
from typing import Any

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.lock_registry import TASK
from diracx.tasks.plumbing.locks import BaseLock, MutexLock


@dataclasses.dataclass
class SampleTask(BaseTask):
    value: int
    priority = Priority.NORMAL
    size = Size.SMALL

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(TASK, "SampleTask")]

    async def execute(self, **kwargs: Any) -> int:
        return self.value * 2


def test_wrap_task_creates_callable():
    wrapped = wrap_task(SampleTask)
    assert callable(wrapped)
    assert hasattr(wrapped, "_dependant")
    assert wrapped.__name__ == "SampleTask"


def test_wrap_task_signature():
    wrapped = wrap_task(SampleTask)
    import inspect

    sig = inspect.signature(wrapped)
    param_names = list(sig.parameters.keys())
    # Should have positional args and kwargs, but no 'self'
    assert "self" not in param_names
